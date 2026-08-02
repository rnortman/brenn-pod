//! Audio streamer thread and its supporting seam.
//!
//! Owns the pod identity static, the telemetry→streamer channel endpoints, the
//! NVS provisioning reads, and the esp-tls answers to the shared streamer's
//! platform questions. The streamer itself — the idle/segment cycle, the
//! per-segment drain loop, reconnect pacing, socket lifecycle, segment
//! placement — is `pod_streamer::run::run_streamer_loop` and the modules under
//! it, shared with the Linux pod. What stays here is the ESP-side seams (the
//! `poll()` shim, the `esp_timer` clock, the heap-waypoint observers, the WiFi
//! link state) and the thread that hands them over.

use crate::netpoll::EspPoll;
use crate::nvs::{nvs_get_blob4, open_wifi_nvs};
use crate::tls_link::{PSK_LEN, TlsConnectParams, TlsStream, tls_connect_psk};
use crate::wifi::{jitter_seed, monotonic_secs, snapshot_wifi_state, wifi_is_up_nonblocking};
use crate::{CAPTURE_RING, build_inbound_stream_sink};
use audio_pipeline::ring::RingIndex;
use audio_pipeline::stream_send::{SendOutcome, WRITE_TIMEOUT_MS};
use pod_streamer::idle::{ParkOutcome, park_drain, should_log_provisioning_failure};
use pod_streamer::link::LinkStream;
use pod_streamer::run::{StreamerExit, StreamerPlatform, StreamerRuntime, run_streamer_loop};
use pod_streamer::segment::ObsEvent;
pub(crate) use pod_streamer::segment::StreamerMsg;
use std::sync::Mutex;
use std::time::Duration;
use wifi_diag::{fmt_ipv4, fmt_wifi_snapshot};

/// How often the streamer re-reads audio provisioning from NVS while it is absent.
///
/// An NVS open plus two key reads is negligible load, and 5 s keeps the
/// provision-to-first-stream latency short enough to feel immediate during
/// `podctl provision-audio`.
const REPROVISION_POLL: Duration = Duration::from_secs(5);

// ── Pod identity ──────────────────────────────────────────────────────────────

/// DHCP hostname of this pod, e.g. `"pod-aabbcc"`.
///
/// Set once at boot during WiFi stack initialization, from the STA MAC.
/// Read by the streamer thread to populate `Hello::pod_id`.
///
/// `heapless::String<32>` matches `Hello::pod_id` capacity.
pub(crate) static POD_ID: Mutex<heapless::String<32>> = Mutex::new(heapless::String::new());

/// A copy of [`POD_ID`], or `None` while it is still empty — i.e. before the
/// WiFi stack has derived it from the STA MAC.
///
/// Every consumer needs the same two decisions (a poisoned mutex is
/// unrecoverable; an empty id means "asked too early"), so they live here and
/// callers supply only their own error framing.
pub(crate) fn pod_id_snapshot() -> Option<heapless::String<32>> {
    let guard = POD_ID
        .lock()
        .unwrap_or_else(|_| panic!("POD_ID mutex poisoned"));
    if guard.is_empty() {
        return None;
    }
    let mut id = heapless::String::<32>::new();
    let _ = id.push_str(guard.as_str());
    Some(id)
}

// ── Streamer message channel ──────────────────────────────────────────────────

/// Channel capacity for `STREAMER_TX` / `STREAMER_RX` — the shared depth, so this pod
/// and the Linux pod present the same backpressure envelope to the shared engine.
pub(crate) use pod_streamer::segment::STREAMER_CHAN_CAPACITY;

/// Process-lifetime receiver half of the telemetry→streamer channel.
///
/// Initialized in `main()` before the telemetry/VAD thread is spawned.
/// The streamer thread takes the `Receiver` out of this static once at startup.
pub(crate) static STREAMER_RX: Mutex<Option<std::sync::mpsc::Receiver<StreamerMsg>>> =
    Mutex::new(None);

/// Lossless VAD-closed flag: set `true` by the telemetry thread on every `VadClosed`
/// event, cleared to `false` on `VadOpened`.
///
/// The `VadClosed` message through the bounded channel can be dropped when the channel
/// is full (telemetry backlog under a TCP stall). A dropped `VadClosed` would leave the
/// streamer streaming silence plus the next utterance as one long segment. This atomic
/// guarantees the streamer eventually sees the close even when the channel message is
/// lost — the streamer checks it once per `'stream` loop iteration after draining the
/// channel queue.
pub(crate) static VAD_CLOSED_FLAG: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Observational flag: `true` while the streamer loop is servicing a VAD onset,
/// from onset acceptance through segment teardown, cleared when the onset scope
/// exits. A HIL test reads it to confirm the production streamer has quiesced (no
/// live segment touching `CAPTURE_RING`) before borrowing the ring. Pure
/// observation — it gates no control flow.
///
/// Published by the shared loop, which takes it as a
/// [`StreamerRuntime::segment_active_flag`].
pub(crate) static SEGMENT_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// TCP connect timeout (ms). 300 ms fast-fails unreachable hosts (LAN handshake
/// is sub-10 ms). Bounded by the 1.0 s pre-roll budget, not the ring size.
const CONNECT_TIMEOUT_MS: u64 = 300;

// ── Streamer helpers ──────────────────────────────────────────────────────────

/// [`pod_streamer::segment::send_frame_bp`] with this platform's `poll()` shim
/// injected, so no call site names the shim.
pub(crate) fn send_frame_bp(
    stream: &mut dyn LinkStream,
    frame: &audio_pipeline::wire::StreamFrame,
    buf: &mut [u8],
) -> std::io::Result<SendOutcome> {
    pod_streamer::segment::send_frame_bp(&EspPoll, stream, frame, buf)
}

/// [`pod_streamer::segment::send_frame_bp_counted`] with this platform's `poll()`
/// shim injected. The resume-cycle count is what HIL self-tests use to tell a
/// frame that blocked and resumed from one the transport accepted outright.
pub(crate) fn send_frame_bp_counted(
    stream: &mut dyn LinkStream,
    frame: &audio_pipeline::wire::StreamFrame,
    buf: &mut [u8],
) -> (std::io::Result<SendOutcome>, u32) {
    pod_streamer::segment::send_frame_bp_counted(&EspPoll, stream, frame, buf)
}

/// The ESP pod's monotonic microsecond clock — the segment engine's and the
/// telemetry loop's `now_us` seam, and the origin of every `device_ts_us` this pod
/// puts on the wire.
pub(crate) fn esp_monotonic_us() -> u64 {
    // SAFETY: pure-read esp_timer query; no arguments, no allocation.
    unsafe { esp_idf_svc::sys::esp_timer_get_time() as u64 }
}

/// The ESP pod's segment observer: an intra-segment heap/stack reading per
/// waypoint, plus the telemetry-drop warning.
///
/// Each reading is three pure heap-registry queries (sub-µs) and one log line, so
/// the instrument is safe on the audio path; the engine's waypoint cadence bounds
/// it to a handful of lines per segment.
///
/// The shared loop hands the segment id in with each event, so this is a plain
/// function; [`segment_obs`] binds one id for a caller that drives a single
/// segment itself.
pub(crate) fn segment_obs_event(segment_id: u32, event: ObsEvent) {
    match event {
        ObsEvent::TelemetryDropped { cap } => log::warn!(
            "streamer: pending_telemetry at cap {} (outbound stalled, seg {}) — dropping oldest",
            cap,
            segment_id
        ),
        waypoint => {
            let (free, min, largest) = crate::health::heap_waypoint();
            log::info!(
                "streamer: heap wp seg={} {} heap_free={} min_heap={} largest_free={} alloc_fail={}",
                segment_id,
                waypoint.as_str(),
                free,
                min,
                largest,
                crate::alloc_probe::alloc_fail_count(),
            );
        }
    }
}

/// [`segment_obs_event`] bound to one segment id.
///
/// `pub(crate)` so the `StreamRealtimeDuplex` self-test drives this same body
/// on the bench rather than a log-only stand-in.
pub(crate) fn segment_obs(segment_id: u32) -> impl FnMut(ObsEvent) {
    move |event| segment_obs_event(segment_id, event)
}

/// This pod's answers to the shared streamer loop's platform questions: an
/// esp-tls connection, the WiFi supervisor's link state and diagnostics, the
/// `esp_timer` clock, and the `esp_idf_svc::sys::poll` shim.
///
/// Built once per streamer thread from the provisioning read, so a reconnect
/// reuses the same address and key without touching NVS again.
struct EspPlatform {
    /// Audio host to connect to.
    peer: std::net::SocketAddr,
    /// This pod's id — both the `Hello` field and the TLS PSK identity.
    pod_id: heapless::String<32>,
    /// The provisioned audio-link key.
    psk: [u8; PSK_LEN],
}

impl StreamerPlatform for EspPlatform {
    type Link = TlsStream;

    fn pod_id(&self) -> &str {
        self.pod_id.as_str()
    }

    fn peer(&self) -> std::net::SocketAddr {
        self.peer
    }

    /// The returned stream is already non-blocking: esp-tls owns the fd and the
    /// mode must be set before the handoff.
    fn connect(&self) -> std::io::Result<TlsStream> {
        tls_connect_psk(&TlsConnectParams {
            peer: &self.peer,
            pod_id: self.pod_id.as_str(),
            key: &self.psk,
            connect_timeout: Duration::from_millis(CONNECT_TIMEOUT_MS),
            write_timeout: Duration::from_millis(WRITE_TIMEOUT_MS),
        })
    }

    fn link_up(&self) -> Option<bool> {
        wifi_is_up_nonblocking()
    }

    fn link_diag(&self) -> impl std::fmt::Display {
        fmt_wifi_snapshot(&snapshot_wifi_state())
    }

    fn now_secs(&self) -> u64 {
        monotonic_secs()
    }

    fn poll(&self) -> &dyn pod_streamer::netpoll::NetPoll {
        &EspPoll
    }
}

// ── Streamer thread ───────────────────────────────────────────────────────────

/// One attempt to read audio provisioning from NVS.
///
/// Opens the NVS handle fresh per call and drops it on return, so each attempt
/// observes current flash contents and no handle is held across a park.
///
/// The audio-link PSK is part of provisioning, not an optional extra: without
/// it there is no way to reach the host at all, so a keyless pod parks on the
/// reprovision poll exactly as an addressless one does.
#[allow(clippy::result_large_err)] // device_protocol::TestResultMsg is the no-alloc error type on no_std
fn read_audio_provisioning() -> Result<([u8; 4], u16, [u8; PSK_LEN]), device_protocol::TestResultMsg>
{
    let nvs = open_wifi_nvs(false)
        .map_err(|msg| fmt_msg(format_args!("cannot open NVS — {}", msg.as_str())))?;
    let ip = nvs_get_blob4(&nvs, "audio_ip")
        .map_err(|msg| fmt_msg(format_args!("audio_ip unavailable: {}", msg.as_str())))?;
    let port = match nvs.get_u16("audio_port") {
        Ok(Some(p)) => p,
        Ok(None) => {
            return Err(lit_msg(
                "audio_port not provisioned (run podctl provision-audio)",
            ));
        }
        Err(e) => {
            return Err(fmt_msg(format_args!(
                "audio_port NVS read error: {e:?} (NVS may be corrupt)"
            )));
        }
    };
    let psk = crate::hil_session::effective_audio_psk(&nvs)
        .map_err(|msg| fmt_msg(format_args!("audio_psk unavailable: {}", msg.as_str())))?;
    Ok((ip, port, psk))
}

/// Format into the no-alloc message type, cutting at a UTF-8 char boundary on
/// overflow and marking the cut with `TRUNCATION_SENTINEL`.
fn fmt_msg(args: core::fmt::Arguments<'_>) -> device_protocol::TestResultMsg {
    device_protocol::format_truncating_marked::<{ device_protocol::TEST_RESULT_MSG_CAP }>(
        args,
        device_protocol::TRUNCATION_SENTINEL,
    )
}

/// Build the no-alloc message type from a fixed literal.
fn lit_msg(msg: &str) -> device_protocol::TestResultMsg {
    let mut s = device_protocol::TestResultMsg::new();
    let _ = s.push_str(msg);
    s
}

/// Spawn the audio streamer thread.
///
/// State machine:
/// - **Idle:** tick every `IDLE_TICK` — maintain connection, drain inbound audio.
/// - **VAD onset:** reuse held socket (or one cold-connect attempt); send `SegmentStart`;
///   enter streaming. Failure → drop segment, return to idle (real-time-or-drop).
/// - **Streaming:** drain ring into `AudioFrame`s, interleave `Telemetry`, drain inbound.
/// - **VAD release:** drain residual samples, send `SegmentEnd{VadRelease}`.
/// - **Overrun / write error:** `SegmentEnd{Overrun}` or drop socket; idle reconnects.
///
/// Polls NVS every `REPROVISION_POLL` until `audio_ip`/`audio_port` are
/// provisioned, then runs.
pub(crate) fn spawn_streamer_thread() {
    use audio_pipeline::ring::RING_CAPACITY_SAMPLES;

    // ESP-IDF's std::thread::Builder::name() does NOT propagate to the FreeRTOS
    // task name (the espidf target's set_name is a no-op). Without the workaround
    // below, xTaskGetHandle(c"streamer") returns NULL and the health-check HWM
    // gate reports streamer_hwm=0.
    //
    // Workaround: set esp_pthread_set_cfg(thread_name) before spawn, then restore
    // to NULL afterward. The cfg is in the *calling* task's TLS, so the restore
    // prevents later spawns from inheriting "streamer".
    //
    // SAFETY: esp_pthread_set_cfg deep-copies the cfg; the 'static C string is
    // valid for the spawn duration. A failed spawn panics (unrecoverable).
    // TODO(supervisor-spawn-tls-restore): if panic="unwind" is adopted, the TLS
    // restore would be skipped on panic. Use a scopeguard in that scenario.
    {
        let mut cfg = unsafe { esp_idf_svc::sys::esp_pthread_get_default_config() };
        // 15 chars = CONFIG_FREERTOS_MAX_TASK_NAME_LEN - 1 (NUL). Do not lengthen.
        cfg.thread_name = c"streamer".as_ptr();
        let set_rc = unsafe { esp_idf_svc::sys::esp_pthread_set_cfg(&cfg) };
        if set_rc != esp_idf_svc::sys::ESP_OK {
            log::warn!(
                "streamer: esp_pthread_set_cfg failed (rc={set_rc:#x}) — task name will be 'pthread', DeviceHealthCheck will report streamer_hwm=0"
            );
        }

        std::thread::Builder::new()
        .name("streamer".into())
        // The ECDHE-PSK handshake runs on this thread and mbedTLS ECC wants
        // several KB of stack beyond what the segment loop needs. Sized to err
        // safe: an overflow trips the armed end-of-stack watchpoint, and the
        // health report's stack HWM is what says how much of this is used.
        // TODO(tls-link-bench-measure): confirm or tune against a bench run.
        .stack_size(28672)
        .spawn(move || {
            // ── Take the channel receiver ────────────────────────────────────
            let rx = {
                let mut guard = STREAMER_RX
                    .lock()
                    .unwrap_or_else(|_| panic!("STREAMER_RX mutex poisoned in streamer thread"));
                guard.take().expect("STREAMER_RX is None — telemetry thread not yet spawned or already taken")
            };

            // ── Read provisioning, polling until it appears ──────────────────
            // These values are captured for the life of this thread on the first
            // success, so a HIL audio-PSK override is observed only if it lands
            // before then — a not-yet-provisioned streamer can pick it up mid-run,
            // a boot-provisioned one keeps its boot-time key until reboot. Reconnects
            // reuse these values too, so a connection loss does not re-read provisioning.
            let (audio_ip, audio_port, audio_psk): ([u8; 4], u16, [u8; PSK_LEN]) = {
                let mut last_err: Option<device_protocol::TestResultMsg> = None;
                loop {
                    match read_audio_provisioning() {
                        Ok(v) => {
                            if last_err.is_some() {
                                log::info!("streamer: audio provisioning appeared, resuming");
                            }
                            break v;
                        }
                        Err(msg) => {
                            // Log the first failure and every change of cause; identical
                            // repeats stay silent so a long wait does not spam the log.
                            if should_log_provisioning_failure(
                                last_err.as_ref().map(|p| p.as_str()),
                                msg.as_str(),
                            ) {
                                log::warn!(
                                    "streamer: {} — waiting for provisioning, retrying every {}s",
                                    msg.as_str(),
                                    REPROVISION_POLL.as_secs()
                                );
                                last_err = Some(msg);
                            }
                            match park_drain(&rx, REPROVISION_POLL) {
                                ParkOutcome::TimedOut => continue,
                                ParkOutcome::Disconnected => {
                                    log::error!(
                                        "streamer: channel disconnected; streamer thread exiting"
                                    );
                                    return;
                                }
                            }
                        }
                    }
                }
            };

            // Empty is not reachable here — the streamer thread starts after WiFi
            // init — and an empty identity simply fails the handshake if it ever were.
            let pod_id: heapless::String<32> = pod_id_snapshot().unwrap_or_default();

            log::info!(
                "streamer: audio receiver {}:{} pod_id={}",
                fmt_ipv4(audio_ip), audio_port, pod_id.as_str()
            );

            let platform = EspPlatform {
                peer: std::net::SocketAddr::from((audio_ip, audio_port)),
                pod_id,
                psk: audio_psk,
            };
            let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
            let mut inbound_sink = build_inbound_stream_sink();
            let mut obs = segment_obs_event;
            let mut runtime = StreamerRuntime {
                rx: &rx,
                ring: &CAPTURE_RING,
                ridx: &ridx,
                vad_closed_flag: &VAD_CLOSED_FLAG,
                segment_active_flag: &SEGMENT_ACTIVE,
                inbound_sink: &mut inbound_sink,
                now_us: &esp_monotonic_us,
                now_instant: &std::time::Instant::now,
                obs: &mut obs,
                inbound_obs: &mut crate::inbound::HeapWaypointObs,
                jitter_seed: jitter_seed(),
            };

            // The shared idle/segment loop runs for the life of the pod; it
            // returns only once the telemetry thread's sender is gone.
            match run_streamer_loop(&platform, &mut runtime) {
                StreamerExit::ChannelDisconnected => {}
            }
        })
        .expect("streamer: thread spawn failed — heap exhausted?");

        // Restore main's TLS thread_name to NULL (see workaround comment above).
        cfg.thread_name = core::ptr::null();
        let restore_rc = unsafe { esp_idf_svc::sys::esp_pthread_set_cfg(&cfg) };
        if restore_rc != esp_idf_svc::sys::ESP_OK {
            log::warn!(
                "streamer: esp_pthread_set_cfg restore failed (rc={restore_rc:#x}) — subsequent thread spawns from main may inherit task name 'streamer'"
            );
        }
    }
}
