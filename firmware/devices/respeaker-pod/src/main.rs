//! respeaker-pod firmware entry point.
//!
//! Implements a framed request/response protocol over the USB-serial-JTAG
//! console port. A custom `log::Log` backend emits every `log::*` record as a
//! `DeviceFrame::Log` frame over the same port, interleaved with responses.
//!
//! # USB-serial-JTAG RX
//!
//! Under the default (nonblocking) VFS mode with
//! `CONFIG_ESP_CONSOLE_USB_SERIAL_JTAG=y`, `std::io::stdin()` does not deliver
//! host→device bytes — reads return immediately with 0 bytes. `main` installs
//! the `usb_serial_jtag` driver and calls
//! `esp_vfs_usb_serial_jtag_use_driver()` to put the console VFS in driver
//! mode, after which `stdin().read()` blocks on the RX ring buffer correctly.
//!

// ── Module declarations ─────────────────────────────────────────────────────────
//
// FFI-heavy, testless modules build only for the device target and are gated
// `#[cfg(target_os = "espidf")]`. The remaining modules carry the host-logic unit tests
// (or are imported by modules that do) and stay host-visible; their device-only items are
// gated at item granularity within each module.

#[cfg(target_os = "espidf")]
mod alloc_probe;
#[cfg(target_os = "espidf")]
mod console;
#[cfg(target_os = "espidf")]
mod gpio;
#[cfg(target_os = "espidf")]
mod health;
#[cfg(target_os = "espidf")]
mod i2c;
#[cfg(target_os = "espidf")]
mod psram;
#[cfg(target_os = "espidf")]
mod telemetry;
#[cfg(target_os = "espidf")]
mod wifi;

mod aic3104;
mod capture;
mod hil;
mod inbound;
mod net_tests;
mod netpoll;
mod nvs;
mod speaker;
mod streamer;
mod xvf3800;

#[cfg(target_os = "espidf")]
use audio_pipeline::playback::{InboundPcmRing, INBOUND_PCM_RING_BYTES};
#[cfg(target_os = "espidf")]
use audio_pipeline::ring::RING_CAPACITY_SAMPLES;
#[cfg(target_os = "espidf")]
use device_protocol::{log_tokens, Command, DeviceFrame, Payload, Request, Response, Status};
#[cfg(target_os = "espidf")]
use esp_idf_svc::eventloop::EspSystemEventLoop;
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::{
    delay::FreeRtos,
    gpio::{PinDriver, Pull},
    peripherals::Peripherals,
};
#[cfg(target_os = "espidf")]
use esp_idf_svc::ipv4::DHCPClientSettings;
#[cfg(target_os = "espidf")]
use esp_idf_svc::netif::{EspNetif, IpEvent, NetifConfiguration, NetifStack};
#[cfg(target_os = "espidf")]
use esp_idf_svc::nvs::EspDefaultNvsPartition;
#[cfg(target_os = "espidf")]
use esp_idf_svc::wifi::{BlockingWifi, EspWifi, WifiDeviceId};
#[cfg(target_os = "espidf")]
use esp_idf_svc::wifi::{ClientConfiguration, Configuration, WifiEvent};
#[cfg(target_os = "espidf")]
use postcard::accumulator::{CobsAccumulator, FeedResult};
#[cfg(target_os = "espidf")]
use std::io::Read;
#[cfg(target_os = "espidf")]
use wifi_diag::fmt_ipv4;

#[cfg(target_os = "espidf")]
use aic3104::AIC3104_ADDR;
use capture::DEVICE_PLAYBACK_FORMAT;
#[cfg(target_os = "espidf")]
use capture::{run_i2s_waveform_sanity, spawn_capture_thread, CaptureRing, CAPTURE_RING};
#[cfg(target_os = "espidf")]
use console::{write_frame, UsbSerialTxSink, LOGGER, WRITER};
#[cfg(target_os = "espidf")]
use gpio::{run_gpio_self_test, LED};
#[cfg(target_os = "espidf")]
use health::{run_device_health_check, run_psram_identity};
#[cfg(target_os = "espidf")]
use hil::run_handler;
#[cfg(target_os = "espidf")]
use i2c::{make_i2c_driver, run_i2c_bus_scan, I2C_BUS};
#[cfg(target_os = "espidf")]
use net_tests::{
    run_poll_readiness_bidir, run_stream_realtime_duplex, run_tcp_inbound_backpressure,
    run_tcp_inbound_frames, run_tcp_roundtrip, run_tcp_send_backpressure, run_tls_reachability,
    run_udp_roundtrip,
};
#[cfg(target_os = "espidf")]
use nvs::{
    handle_clear_wifi_credentials, handle_provision_audio, handle_provision_peer,
    handle_provision_wifi, handle_set_vad_hangover, handle_set_vad_threshold, nvs_get_str,
    open_wifi_nvs,
};
#[cfg(target_os = "espidf")]
use speaker::{
    build_inbound_stream_sink, is_tx_wedged, run_capture_periodic_line,
    run_full_duplex_rx_integrity, run_playback_drain_rate, run_playback_sequence,
    run_speaker_output, rx_deficit_frames, should_rearm_preroll, speaker_stream_init,
    write_silence_frames, PlaybackPhase, PlaybackRequest, CAPTURE_I2S_BUF_BYTES, I2S_DMA_DESC_NUM,
    I2S_DMA_FRAME_NUM, INBOUND_PCM_CONSUMER, INBOUND_PCM_PRODUCER, PLAYBACK_CHAN_CAPACITY,
    PLAYBACK_DAC_UNMUTE_SETTLE_FRAMES, PLAYBACK_PREROLL_MAX_WAIT_MS, PLAYBACK_REQUEST_TX,
    STREAM_EOA_MUTE_DELAY_MS, TX_WEDGE_WARN_US,
};
#[cfg(target_os = "espidf")]
use streamer::{
    send_frame_bp, send_frame_bp_counted, spawn_streamer_thread, StreamerMsg, POD_ID,
    STREAMER_CHAN_CAPACITY, STREAMER_RX, VAD_CLOSED_FLAG,
};
#[cfg(target_os = "espidf")]
use telemetry::{spawn_telemetry_vad_thread, DOA_POLL_HZ, VAD_POLL_HZ};
#[cfg(target_os = "espidf")]
use wifi::{
    ring_wifi_wake, ring_wifi_wake_on_disconnect, run_gateway_probe_gate, run_wifi_associate,
    run_wifi_power_save_check, run_wifi_reassociation, run_wifi_scan, spawn_wifi_supervisor_thread,
    WifiEventSubs, WifiStack, WIFI_EVENT_SUBS, WIFI_STACK,
};
#[cfg(target_os = "espidf")]
use xvf3800::{
    run_amp_always_on_gpo_inert, run_xvf3800_doa_plausibility, run_xvf3800_reg_read,
    run_xvf3800_sp_energy, XVF3800_ADDR,
};

// The host build exists only to run `cargo test`; the firmware binary is never meant to run
// on the host. All device runtime — the esp-idf imports above, the boot/init helpers, and
// `fn main` — is gated `#[cfg(target_os = "espidf")]`; the host build gets a stub `main`.
#[cfg(not(target_os = "espidf"))]
fn main() {}

// ── Main ──────────────────────────────────────────────────────────────────────

/// Map an ESP reset reason to a human-readable label for the boot log line.
///
/// Unrecognized codes fall through to `"unknown"`.
#[cfg(target_os = "espidf")]
fn decode_reset_reason(reason: esp_idf_svc::sys::esp_reset_reason_t) -> &'static str {
    use esp_idf_svc::sys;
    match reason {
        sys::esp_reset_reason_t_ESP_RST_POWERON => "POWERON",
        sys::esp_reset_reason_t_ESP_RST_SW => "SW",
        sys::esp_reset_reason_t_ESP_RST_DEEPSLEEP => "DEEPSLEEP",
        sys::esp_reset_reason_t_ESP_RST_SDIO => "SDIO",
        sys::esp_reset_reason_t_ESP_RST_PANIC => "PANIC",
        sys::esp_reset_reason_t_ESP_RST_INT_WDT => "INT_WDT",
        sys::esp_reset_reason_t_ESP_RST_TASK_WDT => "TASK_WDT",
        sys::esp_reset_reason_t_ESP_RST_WDT => "WDT",
        sys::esp_reset_reason_t_ESP_RST_BROWNOUT => "BROWNOUT",
        sys::esp_reset_reason_t_ESP_RST_EXT => "EXT",
        _ => "unknown",
    }
}

/// Argument + result shuttle for the cross-core watchpoint-arm IPC callback.
#[cfg(target_os = "espidf")]
#[repr(C)]
struct WatchpointArm {
    /// Hardware watchpoint slot.
    slot: core::ffi::c_int,
    /// Aligned word address to watch.
    word: *const core::ffi::c_void,
    /// Region size in bytes (4 or 8).
    size: usize,
    /// `esp_cpu_set_watchpoint` return value, written back by the callback.
    result: esp_idf_svc::sys::esp_err_t,
}

/// IPC callback: arm the write watchpoint on the core this runs on.
///
/// SAFETY: `arg` points to a live `WatchpointArm` owned by the caller for the blocking
/// duration of the IPC call. `esp_cpu_set_watchpoint` arms a hardware watchpoint on the current
/// core only, which is why this is dispatched per core.
#[cfg(target_os = "espidf")]
unsafe extern "C" fn arm_watchpoint_cb(arg: *mut core::ffi::c_void) {
    let arm = &mut *(arg as *mut WatchpointArm);
    arm.result = esp_idf_svc::sys::esp_cpu_set_watchpoint(
        arm.slot,
        arm.word,
        arm.size,
        esp_idf_svc::sys::esp_cpu_watchpoint_trigger_t_ESP_CPU_WATCHPOINT_STORE,
    );
}

/// Arm a `size`-byte write watchpoint on `addr` in `slot`, on every core (watchpoints are
/// per-core hardware). `label` tags the log lines. Returns `true` iff every core armed
/// successfully; a failed IPC dispatch or watchpoint set degrades to reduced coverage with a
/// logged warning and a `false` return rather than aborting.
#[cfg(target_os = "espidf")]
fn arm_watchpoint(slot: core::ffi::c_int, addr: usize, size: usize, label: &str) -> bool {
    let mut arm = WatchpointArm {
        slot,
        word: addr as *const core::ffi::c_void,
        size,
        result: esp_idf_svc::sys::ESP_OK,
    };
    let mut all_ok = true;
    for core in 0..esp_idf_svc::sys::CONFIG_FREERTOS_NUMBER_OF_CORES {
        arm.result = esp_idf_svc::sys::ESP_OK;
        // SAFETY: blocking IPC; `arm` outlives the call. The callback only arms a per-core
        // hardware watchpoint and writes its result back into `arm`.
        let ipc_err = unsafe {
            esp_idf_svc::sys::esp_ipc_call_blocking(
                core,
                Some(arm_watchpoint_cb),
                &mut arm as *mut WatchpointArm as *mut core::ffi::c_void,
            )
        };
        if ipc_err != esp_idf_svc::sys::ESP_OK {
            all_ok = false;
            log::warn!(
                "{label}: IPC arm on core {core} failed (err {ipc_err}); coverage reduced on that core"
            );
        } else if arm.result != esp_idf_svc::sys::ESP_OK {
            all_ok = false;
            log::warn!(
                "{label}: esp_cpu_set_watchpoint on core {core} returned {}; coverage reduced on that core",
                arm.result
            );
        } else {
            log::info!("{label}: armed slot {slot} @{addr:08x} size {size} on core {core}");
        }
    }
    all_ok
}

/// Arm the WRITER-corruption tripwire: a 4-byte STORE watchpoint in slot 0 on the WRITER state
/// word.
#[cfg(target_os = "espidf")]
fn arm_writer_watchpoint(word: usize) {
    arm_watchpoint(0, word, 4, "WRITER watchpoint");
}

/// No-op sink for C-level ESP-IDF log output. Installed via `esp_log_set_vprintf`
/// so that raw ASCII C logs can never reach the USB-serial-JTAG stream and corrupt
/// COBS framing, regardless of any later `esp_log_level_set` call. Never reads its
/// `va_list`, so no vararg/ABI handling is required. Trivially re-entrant (ESP-IDF
/// may invoke the sink from multiple tasks in parallel).
#[cfg(target_os = "espidf")]
unsafe extern "C" fn noop_vprintf(
    _fmt: *const core::ffi::c_char,
    _args: esp_idf_svc::sys::va_list,
) -> core::ffi::c_int {
    0
}

#[cfg(target_os = "espidf")]
fn main() {
    esp_idf_svc::sys::link_patches();

    // Register the failed-allocation hook first thing so it covers every later
    // allocation. It writes the ROM UART directly (alloc-free), independent of the
    // serial driver and COBS logger set up below.
    alloc_probe::register();

    // Install the USB-serial-JTAG driver so stdin reads block instead of returning
    // 0 immediately (the default VFS console mode is write-only/nonblocking).
    //
    // SAFETY: called once from main before any thread uses stdin/stdout.
    unsafe {
        let mut cfg = esp_idf_svc::sys::usb_serial_jtag_driver_config_t {
            // TX ring must hold a full COBS frame (~275 bytes); 256 is too small.
            tx_buffer_size: 2048,
            rx_buffer_size: 256,
        };
        let err = esp_idf_svc::sys::usb_serial_jtag_driver_install(&mut cfg);
        assert_eq!(
            err,
            esp_idf_svc::sys::ESP_OK,
            "usb_serial_jtag_driver_install failed"
        );
        esp_idf_svc::sys::esp_vfs_usb_serial_jtag_use_driver();
        // Disable CRLF translation — the VFS default corrupts COBS binary frames
        // by expanding 0x0A on TX and rewriting 0x0D on RX.
        esp_idf_svc::sys::esp_vfs_dev_usb_serial_jtag_set_tx_line_endings(
            esp_idf_svc::sys::esp_line_endings_t_ESP_LINE_ENDINGS_LF,
        );
        esp_idf_svc::sys::esp_vfs_dev_usb_serial_jtag_set_rx_line_endings(
            esp_idf_svc::sys::esp_line_endings_t_ESP_LINE_ENDINGS_LF,
        );

        // Suppress C-level ESP log output so raw ASCII doesn't corrupt COBS
        // frames. Our FramedLogger routes through COBS and is unaffected.
        // Layered: the level gate below is defense in depth, but the structural
        // guarantee is the vprintf sink redirect — it sits downstream of any level
        // check, so it stays effective even if something later calls
        // esp_log_level_set to re-enable a level. Only another
        // esp_log_set_vprintf call can undo it.
        esp_idf_svc::sys::esp_log_level_set(
            c"*".as_ptr(),
            esp_idf_svc::sys::esp_log_level_t_ESP_LOG_NONE,
        );
        let _prev = esp_idf_svc::sys::esp_log_set_vprintf(Some(noop_vprintf));
    }

    log::set_logger(&LOGGER).expect("log::set_logger failed — called more than once");
    log::set_max_level(log::LevelFilter::Info);

    let writer_data_addr: usize = {
        let mut guard = WRITER.lock().expect("WRITER mutex poisoned");
        *guard = Some(UsbSerialTxSink);
        // Runtime address of the Option<UsbSerialTxSink> payload inside the mutex, for the
        // corruption watchpoint armed below.
        &mut *guard as *mut Option<UsbSerialTxSink> as usize
    };

    // Hardware watchpoint tripwire on the WRITER state byte. After this store no legitimate
    // instruction ever writes WRITER's bytes again (set-once gate; lazy lock pointer installed
    // at first lock(); poison written only during unwind, unreachable under panic=abort), so
    // any subsequent write is corruption — the watchpoint raises a debug exception at the
    // culprit instruction, whose PC survives in the raw panic dump. Watchpoints are per-core
    // hardware; arm slot 0 on every core via IPC. The FreeRTOS end-of-stack watchpoint
    // (CONFIG_FREERTOS_WATCHPOINT_END_OF_STACK) occupies slot 1, re-armed per task at each
    // context switch, so the two do not collide. The word is 4-aligned to satisfy the
    // region-alignment requirement; the only writable byte that can cohabit it is the poison
    // flag (unwind-only under panic=abort). Occupies debug watchpoint slot 0; with slot 1 taken
    // by the end-of-stack watchpoint, on-chip JTAG has no hardware watchpoint free.
    arm_writer_watchpoint(writer_data_addr & !0b11);

    log::info!("respeaker-pod firmware starting");

    // Log the previous reset reason so crash causes are visible on the next boot.
    let reset_reason = unsafe { esp_idf_svc::sys::esp_reset_reason() };
    log::info!(
        "respeaker_pod: boot: reset reason = {}",
        decode_reset_reason(reset_reason)
    );

    let peripherals = Peripherals::take()
        .expect("Peripherals::take() returned None — already taken; only one caller allowed");

    // GPIO21 = user LED (active-LOW). InputOutput mode lets self-tests read back
    // the actual pad level to catch stuck-pad faults.
    {
        let led = PinDriver::input_output(peripherals.pins.gpio21, Pull::Down)
            .expect("failed to configure GPIO21 as input_output");
        let mut guard = LED.lock().expect("LED mutex poisoned during init");
        *guard = Some(led);
    }

    // ── I2C bus ───────────────────────────────────────────────────────────────
    {
        let i2c_driver = make_i2c_driver(
            peripherals.i2c0,
            peripherals.pins.gpio5,
            peripherals.pins.gpio6,
        )
        .expect("I2C0 init failed (SDA=GPIO5, SCL=GPIO6, 100 kHz) — hardware fault");
        let mut guard = I2C_BUS.lock().expect("I2C_BUS mutex poisoned during init");
        *guard = Some(i2c_driver);
        log::info!("I2C bus initialized (SDA=GPIO5, SCL=GPIO6, 100 kHz)");
    }

    // ── WiFi stack ─────────────────────────────────────────────────────────────
    //
    // Initializes EspWifi + BlockingWifi but does NOT connect — the WiFi supervisor
    // thread handles association. DHCP hostname is `pod-<last3-mac-hex>`.
    {
        let sysloop = EspSystemEventLoop::take()
            .expect("EspSystemEventLoop::take() failed — already taken or unsupported");
        let nvs = EspDefaultNvsPartition::take()
            .expect("EspDefaultNvsPartition::take() failed — already taken or unsupported");

        let driver = esp_idf_svc::wifi::WifiDriver::new(
            peripherals.modem,
            sysloop.clone(),
            Some(nvs.clone()),
        )
        .expect("WifiDriver::new failed");

        let mac = driver
            .get_mac(WifiDeviceId::Sta)
            .expect("WifiDriver::get_mac(Sta) failed");

        let mut hostname = heapless::String::<16>::new();
        core::fmt::write(
            &mut hostname,
            format_args!("pod-{:02x}{:02x}{:02x}", mac[3], mac[4], mac[5]),
        )
        .expect("hostname format failed — pod-xxyyzz must fit in String::<16>");
        log::info!("WiFi hostname: {}", hostname.as_str());

        {
            let mut guard = POD_ID.lock().expect("POD_ID mutex poisoned during init");
            let _ = guard.push_str(hostname.as_str());
        }

        let mut sta_netif_conf = NetifConfiguration::wifi_default_client();
        sta_netif_conf.ip_configuration = Some(esp_idf_svc::ipv4::Configuration::Client(
            esp_idf_svc::ipv4::ClientConfiguration::DHCP(DHCPClientSettings {
                hostname: Some(
                    hostname
                        .as_str()
                        .try_into()
                        .expect("hostname too long for heapless::String<30>"),
                ),
            }),
        ));
        let sta_netif = EspNetif::new_with_conf(&sta_netif_conf)
            .expect("EspNetif::new_with_conf for STA failed");

        // STA-only; the AP netif is required by the type signature when softap
        // support is compiled in, but we don't use it.
        #[allow(unexpected_cfgs)]
        let esp_wifi = EspWifi::wrap_all(
            driver,
            sta_netif,
            #[cfg(esp_idf_esp_wifi_softap_support)]
            EspNetif::new(NetifStack::Ap).expect("EspNetif::new for AP netif failed"),
        )
        .expect("EspWifi::wrap_all failed");

        let sysloop_for_subs = sysloop.clone();

        let mut blocking_wifi =
            BlockingWifi::wrap(esp_wifi, sysloop).expect("BlockingWifi::wrap failed");

        // Destroy any credential copy an earlier firmware left in the driver's own
        // `nvs.net80211` namespace. esp_wifi_restore() erases those stored parameters;
        // it must run while the driver is still in flash-storage mode (the ESP-IDF
        // default), i.e. before the WIFI_STORAGE_RAM switch below. Idempotent and cheap
        // on a device that has no such copy.
        let restore_rc = unsafe { esp_idf_svc::sys::esp_wifi_restore() };
        assert_eq!(
            restore_rc,
            esp_idf_svc::sys::ESP_OK,
            "esp_wifi_restore() failed (rc={restore_rc:#x}) — a stale driver-NVS WiFi \
             credential copy may survive ClearWifiCredentials"
        );

        // Keep the driver's credential store in RAM only. The ESP-IDF default
        // (WIFI_STORAGE_FLASH) persists every set_configuration SSID/passphrase into the
        // driver's own `nvs.net80211` namespace, which the app's "wifi" namespace clear
        // does not reach — a cleared device would still hold a readable passphrase copy
        // in flash. The app NVS namespace is the single source of truth.
        let storage_rc = unsafe {
            esp_idf_svc::sys::esp_wifi_set_storage(
                esp_idf_svc::sys::wifi_storage_t_WIFI_STORAGE_RAM,
            )
        };
        assert_eq!(
            storage_rc,
            esp_idf_svc::sys::ESP_OK,
            "esp_wifi_set_storage(WIFI_STORAGE_RAM) failed (rc={storage_rc:#x}) — driver would \
             persist WiFi credentials to flash beyond the reach of ClearWifiCredentials"
        );

        // Set STA mode with empty credentials; the supervisor fills in real
        // credentials at association time. Does not start the radio.
        blocking_wifi
            .set_configuration(&Configuration::Client(ClientConfiguration::default()))
            .expect("wifi set_configuration (STA mode) failed at boot");

        let mut guard = WIFI_STACK
            .lock()
            .expect("WIFI_STACK mutex poisoned during init");
        *guard = Some(WifiStack {
            wifi: blocking_wifi,
            nvs,
        });
        log::info!("WiFi stack initialized (not yet connected)");

        // ── WiFi/IP event subscriptions ───────────────────────────────────────
        //
        // Log-only callbacks for WiFi/DHCP state transitions. These must not
        // acquire WIFI_STACK (lock ordering: callbacks run on the event-loop task).
        let wifi_sub = sysloop_for_subs
            .subscribe::<WifiEvent, _>(|event| match event {
                WifiEvent::StaDisconnected(info) => {
                    log::warn!("{}{}", log_tokens::WIFI_DISCONNECTED, info.reason());
                    ring_wifi_wake_on_disconnect();
                }
                WifiEvent::StaConnected(_) => {
                    log::info!("{}", log_tokens::WIFI_CONNECTED);
                }
                _ => {}
            })
            .expect("failed to subscribe to WifiEvent — system event loop unavailable");

        let ip_sub = sysloop_for_subs
            .subscribe::<IpEvent, _>(|event| match event {
                IpEvent::DhcpIpAssigned(assignment) => {
                    log::info!(
                        "{} ip={} gw={}",
                        log_tokens::WIFI_DHCP_LEASE,
                        fmt_ipv4(assignment.ip().octets()),
                        fmt_ipv4(assignment.gateway().octets()),
                    );
                }
                IpEvent::DhcpIpDeassigned(_) => {
                    log::warn!("{} lost", log_tokens::WIFI_DHCP_LEASE);
                }
                _ => {}
            })
            .expect("failed to subscribe to IpEvent — system event loop unavailable");

        {
            let mut guard = WIFI_EVENT_SUBS
                .lock()
                .expect("WIFI_EVENT_SUBS mutex poisoned during init");
            *guard = Some(WifiEventSubs {
                _wifi_sub: wifi_sub,
                _ip_sub: ip_sub,
                _sysloop: sysloop_for_subs,
            });
        }
    }

    // ── WiFi supervisor thread ────────────────────────────────────────────────
    //
    // Owns "keep WiFi associated" for the process lifetime. First association is
    // async (concurrent with streamer startup); early-boot audio may be dropped
    // if WiFi is not yet up.
    match open_wifi_nvs(false) {
        Ok(nvs) => {
            let mut ssid_buf = [0u8; 33];
            let has_creds =
                matches!(nvs_get_str(&nvs, "ssid", &mut ssid_buf), Ok(Some(s)) if !s.is_empty());
            drop(nvs);
            if has_creds {
                log::info!("boot: NVS credentials found — WiFi supervisor will associate");
            } else {
                // The park behavior this announces is exercised by the hil-host
                // `NoCredentialsPark` step through the shared supervisor park path
                // (wifi.rs); boot rings the same doorbell the step's clear does.
                log::info!("boot: no NVS credentials — WiFi supervisor parked until provisioned");
            }
        }
        Err(msg) => {
            log::warn!(
                "boot: cannot open NVS for WiFi credential check: {}",
                msg.as_str()
            );
        }
    }
    spawn_wifi_supervisor_thread();

    // ── Audio capture ring + capture thread ───────────────────────────────────
    //
    // Sample storage lives in PSRAM (CAPS-only pool): CPU-access-only, never a DMA
    // target, so it frees an equal amount of the starved internal heap for Wi-Fi/lwIP.
    {
        let ring = CaptureRing {
            samples: psram::PsramBuf::<i16>::new_zeroed(RING_CAPACITY_SAMPLES),
            write_head: 0,
            anchor_sample: 0,
            anchor_ts_us: 0,
        };
        let mut guard = CAPTURE_RING
            .lock()
            .expect("CAPTURE_RING mutex poisoned during init");
        *guard = Some(ring);
        let spiram_free = psram::spiram_free_bytes();
        log::info!(
            "capture ring initialized in PSRAM ({} mono samples, {} KB; SPIRAM free {} B)",
            RING_CAPACITY_SAMPLES,
            (RING_CAPACITY_SAMPLES * core::mem::size_of::<i16>()) / 1024,
            spiram_free,
        );
    }

    // ── Playback request channel ────────────────────────────────────────────
    //
    // Capacity-1 channel for HIL speaker-output requests → capture thread.
    // Wired before spawn so the sender is never uninitialized.
    let (playback_tx, playback_rx) =
        std::sync::mpsc::sync_channel::<PlaybackRequest>(PLAYBACK_CHAN_CAPACITY);
    {
        let mut guard = PLAYBACK_REQUEST_TX
            .lock()
            .expect("PLAYBACK_REQUEST_TX mutex poisoned during init");
        *guard = Some(playback_tx);
    }

    // ── Inbound-PCM streaming ring ──────────────────────────────────────────
    //
    // Single-producer byte ring for inbound PCM from the network → capture thread.
    // Sample storage lives in PSRAM (CAPS-only pool): CPU-access-only, never a DMA
    // target, so it frees an equal amount of the starved internal heap. Both halves
    // wired before spawn so neither end finds an uninitialized ring.
    let (inbound_pcm_producer, inbound_pcm_consumer) = InboundPcmRing::with_storage(Box::new(
        psram::PsramBuf::<u8>::new_zeroed(INBOUND_PCM_RING_BYTES),
    ))
    .split();
    {
        let spiram_free = psram::spiram_free_bytes();
        log::info!(
            "inbound PCM ring initialized in PSRAM ({} B, {} KB; SPIRAM free {} B)",
            INBOUND_PCM_RING_BYTES,
            INBOUND_PCM_RING_BYTES / 1024,
            spiram_free,
        );
    }
    {
        let mut guard = INBOUND_PCM_PRODUCER
            .lock()
            .expect("INBOUND_PCM_PRODUCER mutex poisoned during init");
        *guard = Some(inbound_pcm_producer);
    }
    {
        let mut guard = INBOUND_PCM_CONSUMER
            .lock()
            .expect("INBOUND_PCM_CONSUMER mutex poisoned during init");
        *guard = Some(inbound_pcm_consumer);
    }

    spawn_capture_thread(
        peripherals.i2s0,
        peripherals.pins.gpio8,
        peripherals.pins.gpio43,
        peripherals.pins.gpio44,
        peripherals.pins.gpio7,
        playback_rx,
    );

    // ── Telemetry/VAD thread + streamer channel ───────────────────────────────
    {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(STREAMER_CHAN_CAPACITY);
        {
            let mut guard = STREAMER_RX
                .lock()
                .expect("STREAMER_RX mutex poisoned during init");
            *guard = Some(rx);
        }
        spawn_telemetry_vad_thread(tx);
        log::info!(
            "telemetry/VAD thread spawned (VAD_POLL_HZ={}, DOA_POLL_HZ={})",
            VAD_POLL_HZ,
            DOA_POLL_HZ
        );
    }

    // ── Streamer thread ───────────────────────────────────────────────────────
    spawn_streamer_thread();
    log::info!("streamer thread spawned");

    // ── Protocol service loop ─────────────────────────────────────────────────
    //
    // Read COBS-framed host→device requests from stdin, dispatch through the
    // handler registry. LED blink runs on a background thread.

    std::thread::spawn(|| {
        let mut led_on = false;
        loop {
            {
                let mut guard = LED
                    .lock()
                    .unwrap_or_else(|_| panic!("LED mutex poisoned in blink task"));
                let led = guard.as_mut().unwrap_or_else(|| {
                    panic!("LED is None in blink task — initialized before thread spawn")
                });
                if led_on {
                    led.set_low()
                        .unwrap_or_else(|e| panic!("GPIO21 set_low failed: {:?}", e));
                } else {
                    led.set_high()
                        .unwrap_or_else(|e| panic!("GPIO21 set_high failed: {:?}", e));
                }
            }
            led_on = !led_on;
            FreeRtos::delay_ms(500);
        }
    });

    let mut stdin = std::io::stdin();
    let mut read_buf = [0u8; 256];
    let mut acc: CobsAccumulator<512> = CobsAccumulator::new();
    // Rate-limit error logs (every 64th) to avoid livelock under corrupt-byte storms.
    let mut deser_error_count: u32 = 0;
    let mut over_full_count: u32 = 0;

    loop {
        match stdin.read(&mut read_buf) {
            Ok(0) => {
                FreeRtos::delay_ms(10);
                continue;
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                        FreeRtos::delay_ms(10);
                    }
                    kind => {
                        log::error!("stdin read error ({:?}): {:?}", kind, e);
                        FreeRtos::delay_ms(100);
                    }
                }
                continue;
            }
            Ok(n) => {
                let mut chunk = &read_buf[..n];
                loop {
                    match acc.feed::<Request>(chunk) {
                        FeedResult::Success { data, remaining } => {
                            chunk = remaining;
                            dispatch_request(data);
                        }
                        FeedResult::Consumed => break,
                        FeedResult::OverFull(r) => {
                            acc = CobsAccumulator::new();
                            if over_full_count.is_multiple_of(64) {
                                log::warn!(
                                    target: "protocol",
                                    "OverFull: accumulator reset (count={})",
                                    over_full_count
                                );
                            }
                            over_full_count = over_full_count.saturating_add(1);
                            chunk = r;
                        }
                        FeedResult::DeserError(r) => {
                            if deser_error_count.is_multiple_of(64) {
                                log::warn!(
                                    target: "protocol",
                                    "COBS DeserError: corrupt or unknown-discriminant frame skipped (count={})",
                                    deser_error_count
                                );
                            }
                            deser_error_count = deser_error_count.saturating_add(1);
                            chunk = r;
                        }
                    }
                    if chunk.is_empty() {
                        break;
                    }
                }
            }
        }
    }
}

/// Dispatch a decoded `Request` to the appropriate handler and send the response.
/// Falls back to `Status::Fail / Payload::Empty` if the primary response is too
/// large to encode.
#[cfg(target_os = "espidf")]
fn dispatch_request(req: Request) {
    let (status, payload) = match req.command {
        Command::RunTest(name) => run_handler(name),
        Command::ProvisionWifi { ssid, passphrase } => handle_provision_wifi(ssid, passphrase),
        Command::ProvisionPeer {
            host,
            udp_port,
            tcp_port,
            tls_host,
            tls_port,
            inbound_frames_port,
            backpressure_port,
            poll_readiness_port,
            rtd_port,
        } => handle_provision_peer(
            host,
            udp_port,
            tcp_port,
            tls_host,
            tls_port,
            inbound_frames_port,
            backpressure_port,
            poll_readiness_port,
            rtd_port,
        ),
        Command::ProvisionAudio { host, port } => handle_provision_audio(host, port),
        Command::SetVadThreshold { threshold } => handle_set_vad_threshold(threshold),
        Command::SetVadHangover { hangover_ms } => handle_set_vad_hangover(hangover_ms),
        Command::ClearWifiCredentials => handle_clear_wifi_credentials(),
        Command::SetTemporaryWifiConfig { ssid, passphrase } => {
            crate::wifi::handle_set_temporary_wifi_config(ssid, passphrase)
        }
        Command::ClearTemporaryWifiConfig => crate::wifi::handle_clear_temporary_wifi_config(),
    };
    let primary = DeviceFrame::Response(Response {
        id: req.id,
        status,
        payload,
    });
    let mut buf = [0u8; device_protocol::RESPONSE_FRAME_BUF];
    match device_protocol::framing::encode_device_frame(&primary, &mut buf) {
        Ok(len) => console::write_encoded_frame(&buf[..len]),
        Err(e) => {
            debug_assert!(false, "primary encode failed for id={}: {:?}", req.id, e);
            let fallback = DeviceFrame::Response(Response {
                id: req.id,
                status: Status::Fail,
                payload: Payload::Empty,
            });
            write_frame(&fallback);
        }
    }
}
