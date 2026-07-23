//! Audio streamer thread and its supporting seam.
//!
//! Owns the pod identity static, the telemetry→streamer message channel, the
//! reconnect/backoff helpers, the frame-send helpers, and the streamer event
//! loop (`spawn_streamer_thread`). Moved verbatim from `main.rs` during the
//! module split; no logic, message, or name changes.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use crate::inbound::{FrameAccumulator, InboundConnectionState, inbound_has_room, pump_inbound};
#[cfg(target_os = "espidf")]
use crate::netpoll::poll_timeout;
#[cfg(target_os = "espidf")]
use crate::netpoll::{
    IDLE_TICK, INBOUND_STEPS_PER_WAKE, OUTBOUND_FRAMES_PER_WAKE, Readiness, poll_readiness,
    poll_writable,
};
#[cfg(target_os = "espidf")]
use crate::nvs::{nvs_get_blob4, open_wifi_nvs};
#[cfg(target_os = "espidf")]
use crate::tls_link::{LinkStream, PSK_LEN, TlsConnectParams, TlsStream, tls_connect_psk};
#[cfg(target_os = "espidf")]
use crate::wifi::{jitter_seed, monotonic_secs, snapshot_wifi_state, wifi_is_up_nonblocking};
#[cfg(target_os = "espidf")]
use crate::{CAPTURE_RING, CaptureRing, DEVICE_PLAYBACK_FORMAT, build_inbound_stream_sink};
#[cfg(target_os = "espidf")]
use audio_pipeline::playback::{I2sStreamSink, PlaybackSink};
#[cfg(target_os = "espidf")]
use audio_pipeline::ring::RingIndex;
#[cfg(target_os = "espidf")]
use audio_pipeline::stream_send::{SendOutcome, WRITE_TIMEOUT_MS, write_frame_classified};
use audio_pipeline::wire::Telemetry as WireTelemetry;
#[cfg(target_os = "espidf")]
use std::sync::Mutex;
use std::time::Duration;
#[cfg(target_os = "espidf")]
use wifi_diag::{fmt_ipv4, fmt_wifi_snapshot};
use wifi_reconnect::Backoff;

/// How often the streamer re-reads audio provisioning from NVS while it is absent.
///
/// An NVS open plus two key reads is negligible load, and 5 s keeps the
/// provision-to-first-stream latency short enough to feel immediate during
/// `podctl provision-audio`.
#[cfg(target_os = "espidf")]
const REPROVISION_POLL: Duration = Duration::from_secs(5);

// ── Pod identity ──────────────────────────────────────────────────────────────

/// DHCP hostname of this pod, e.g. `"pod-aabbcc"`.
///
/// Set once at boot during WiFi stack initialization, from the STA MAC.
/// Read by the streamer thread to populate `Hello::pod_id`.
///
/// `heapless::String<32>` matches `Hello::pod_id` capacity.
#[cfg(target_os = "espidf")]
pub(crate) static POD_ID: Mutex<heapless::String<32>> = Mutex::new(heapless::String::new());

/// A copy of [`POD_ID`], or `None` while it is still empty — i.e. before the
/// WiFi stack has derived it from the STA MAC.
///
/// Every consumer needs the same two decisions (a poisoned mutex is
/// unrecoverable; an empty id means "asked too early"), so they live here and
/// callers supply only their own error framing.
#[cfg(target_os = "espidf")]
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

/// Messages from the telemetry/VAD thread to the streamer thread.
///
/// Channel capacity: 64 (bounded `sync_channel`). On full, the telemetry/VAD
/// thread drops the oldest telemetry frame and increments a drop counter —
/// audio frames have priority and are never dropped to make room for telemetry.
pub enum StreamerMsg {
    /// VAD gate just opened; carry the write-head sample index at onset time so
    /// the streamer can place the pre-roll cursor.
    VadOpened { write_head: u64 },
    /// VAD gate just closed (hangover expired).
    VadClosed,
    /// XVF3800 telemetry frame, to be forwarded in-band while a segment is open.
    Telemetry(WireTelemetry),
}

/// Channel capacity for `STREAMER_TX` / `STREAMER_RX`.
#[cfg(target_os = "espidf")]
pub(crate) const STREAMER_CHAN_CAPACITY: usize = 64;

/// Process-lifetime receiver half of the telemetry→streamer channel.
///
/// Initialized in `main()` before the telemetry/VAD thread is spawned.
/// The streamer thread takes the `Receiver` out of this static once at startup.
#[cfg(target_os = "espidf")]
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
#[cfg(target_os = "espidf")]
pub(crate) static VAD_CLOSED_FLAG: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Observational flag: `true` while `spawn_streamer_thread` is servicing a VAD
/// onset, from onset acceptance through segment teardown, cleared when the onset
/// scope exits. A HIL test reads it to confirm the production streamer has
/// quiesced (no live segment touching `CAPTURE_RING`) before borrowing the ring.
/// Pure observation — it gates no control flow.
#[cfg(target_os = "espidf")]
pub(crate) static SEGMENT_ACTIVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// RAII guard that publishes [`SEGMENT_ACTIVE`] for the duration of one onset
/// span: set on construction, cleared on drop — so every `continue 'outer` drop
/// path and the `run_segment` return all clear it.
#[cfg(target_os = "espidf")]
struct SegmentActiveGuard;

#[cfg(target_os = "espidf")]
impl SegmentActiveGuard {
    fn new() -> Self {
        SEGMENT_ACTIVE.store(true, std::sync::atomic::Ordering::Release);
        SegmentActiveGuard
    }
}

#[cfg(target_os = "espidf")]
impl Drop for SegmentActiveGuard {
    fn drop(&mut self) {
        SEGMENT_ACTIVE.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// TCP connect timeout (ms). 300 ms fast-fails unreachable hosts (LAN handshake
/// is sub-10 ms). Bounded by the 1.0 s pre-roll budget, not the ring size.
#[cfg(target_os = "espidf")]
const CONNECT_TIMEOUT_MS: u64 = 300;

/// How long POLLOUT stays de-armed once the write spin guard trips. Long enough to hand
/// the TCP stack a real scheduling window, short enough that a handful of backoffs cost
/// only a slice of the 750 ms write budget — the budget, not this pause, remains the
/// terminal bound.
#[cfg(target_os = "espidf")]
const SPIN_BACKOFF_MS: u64 = 10;

/// Cap on the per-segment `pending_telemetry` queue. Telemetry is advisory, so
/// at the cap the oldest is dropped rather than risking heap exhaustion.
#[cfg(target_os = "espidf")]
const PENDING_TELEMETRY_CAP: usize = 8;

// ── Streamer helpers ──────────────────────────────────────────────────────────

/// Encode `frame` and write it to `stream` with bounded backpressure via
/// [`write_frame_classified`] + [`poll_writable`]. Discards the resume-cycle
/// count (production path); see [`send_frame_bp_counted`] for the HIL variant.
#[cfg(target_os = "espidf")]
pub(crate) fn send_frame_bp(
    stream: &mut dyn LinkStream,
    frame: &audio_pipeline::wire::StreamFrame,
    buf: &mut [u8],
) -> std::io::Result<SendOutcome> {
    send_frame_bp_counted(stream, frame, buf).0
}

/// Like [`send_frame_bp`] but also returns the resume-cycle count (completed
/// writability waits that were followed by forward progress). Used by HIL
/// self-tests to distinguish a frame that blocked and resumed from one the
/// transport accepted outright.
#[cfg(target_os = "espidf")]
pub(crate) fn send_frame_bp_counted(
    stream: &mut dyn LinkStream,
    frame: &audio_pipeline::wire::StreamFrame,
    buf: &mut [u8],
) -> (std::io::Result<SendOutcome>, u32) {
    let fd = stream.link_fd();
    write_frame_classified(stream.as_write(), frame, buf, |deadline| {
        poll_writable(fd, deadline)
    })
}

/// The per-connection inputs of [`connect_and_hello`], bundled to stay inside
/// the Xtensa realign-miscompile guard's argument-word budget.
#[cfg(target_os = "espidf")]
struct ConnectInputs<'a> {
    /// Audio host to connect to.
    peer_addr: &'a std::net::SocketAddr,
    /// This pod's id — both the `Hello` field and the TLS PSK identity.
    pod_id: &'a str,
    /// The provisioned audio-link key.
    psk: &'a [u8; PSK_LEN],
}

/// Open a fresh TLS-PSK connection to the audio host and send the `Hello` frame
/// identifying this pod.
///
/// Returns the ready session, or an `io::Error` if connect, handshake, or
/// `Hello` fails. The returned stream is already non-blocking: esp-tls owns
/// the fd and the mode must be set before the handoff.
#[cfg(target_os = "espidf")]
fn connect_and_hello(inputs: &ConnectInputs, encode_buf: &mut [u8]) -> std::io::Result<TlsStream> {
    use audio_pipeline::wire::{AUDIO_PROTOCOL_VERSION, ChannelSource, Hello, StreamFrame};
    let mut stream = tls_connect_psk(&TlsConnectParams {
        peer: inputs.peer_addr,
        pod_id: inputs.pod_id,
        key: inputs.psk,
        connect_timeout: std::time::Duration::from_millis(CONNECT_TIMEOUT_MS),
        write_timeout: std::time::Duration::from_millis(WRITE_TIMEOUT_MS),
    })?;
    let hello = StreamFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::try_from(inputs.pod_id)
            .unwrap_or_else(|_| heapless::String::new()),
        sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
        bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
        channels: DEVICE_PLAYBACK_FORMAT.channels,
        codec: DEVICE_PLAYBACK_FORMAT.codec,
        channel_source: ChannelSource::CommunicationBeam,
    });
    // The socket is non-blocking from the handoff on, so `Hello` goes out
    // through the same bounded-backpressure path as every other frame rather
    // than a blocking `write_all`.
    match send_frame_bp(&mut stream, &hello, encode_buf)? {
        SendOutcome::Sent => Ok(stream),
        SendOutcome::BackpressureAligned => Err(std::io::Error::other(
            "Hello stalled on write backpressure — dropping the fresh connection",
        )),
    }
}

/// Tags the single in-flight outbound frame so post-send bookkeeping knows what to
/// do: `Audio` bumps delivered counters, `SegmentEnd` exits the segment loop,
/// `Telemetry` completes silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundKind {
    Audio { samples: u32 },
    Telemetry,
    SegmentEnd,
}

// ── Streamer thread ───────────────────────────────────────────────────────────

/// Whether to attempt a connect on this idle tick.
#[derive(Debug, PartialEq, Eq)]
enum IdleConnectAction {
    /// Socket is down, link is up, and backoff deadline has elapsed.
    Attempt,
    /// Already connected, link down, or backoff not yet elapsed.
    Skip,
}

/// Should the idle loop attempt a connect this tick?
///
/// Link-down is not an audio-server failure and must not charge a backoff.
/// Takes the pre-computed deadline (not the `Backoff`) so it never redraws
/// jitter on each 10 ms tick.
fn should_attempt_idle_connect(
    socket_is_some: bool,
    link_up: Option<bool>,
    now_secs: u64,
    reconnect_deadline_secs: u64,
) -> IdleConnectAction {
    if !socket_is_some && link_up == Some(true) && now_secs >= reconnect_deadline_secs {
        IdleConnectAction::Attempt
    } else {
        IdleConnectAction::Skip
    }
}

/// Arm the next reconnect deadline after a failed connect or drain error.
///
/// Draws the jittered wait once and returns the absolute deadline; subsequent
/// idle ticks compare against this fixed value.
fn arm_reconnect_deadline(
    now_secs: u64,
    backoff: &mut Backoff,
    attempt_counter: &mut u32,
    jitter_seed_base: u32,
) -> u64 {
    backoff.record_failure();
    *attempt_counter = attempt_counter.wrapping_add(1);
    now_secs.saturating_add(backoff.next_wait_secs(jitter_seed_base ^ *attempt_counter))
}

/// Reset backoff state after a successful connect.
///
/// Every connect-success path must call this — zeroes the deadline so a later
/// socket-clear reconnects immediately rather than waiting out a stale deadline.
fn note_connect_success(backoff: &mut Backoff, reconnect_deadline_secs: &mut u64) {
    backoff.record_success();
    *reconnect_deadline_secs = 0;
}

/// Route every socket-teardown site through one place: clear the socket, reset
/// the inbound framing state so stale partial bytes cannot corrupt the next
/// connection's first frame, and signal end-of-audio to playback so the banked
/// tail plays out and *then* the DAC mutes.
///
/// A subsequent reconnect bumps the ring generation (`send_stream_reset`),
/// which discards any un-played tail and the just-pushed EOA mark — so a drop
/// followed by an immediate reconnect is seamless (no spurious mute), while a
/// drop that stays down plays the banked tail out and mutes. Marks are
/// generation-tagged, so the reconnect's `apply_reset` discards this
/// dead-generation mark even if the tail had not yet drained.
#[cfg(target_os = "espidf")]
fn note_socket_lost(
    held_socket: &mut Option<TlsStream>,
    inbound_accum: &mut FrameAccumulator,
    inbound_state: &mut InboundConnectionState,
    inbound_sink: &mut I2sStreamSink,
) {
    *held_socket = None;
    inbound_accum.reset();
    inbound_state.reset();
    inbound_sink.end_of_audio();
}

/// Connection-established mirror of `note_socket_lost`: reset per-connection inbound
/// state, install the socket, and emit the stream-boundary signal — in that order.
/// A fresh socket is a fresh inbound stream (see `InboundConnectionState`).
#[cfg(target_os = "espidf")]
fn note_socket_established(
    held_socket: &mut Option<TlsStream>,
    stream: TlsStream,
    inbound_accum: &mut FrameAccumulator,
    inbound_state: &mut InboundConnectionState,
    inbound_sink: &mut I2sStreamSink,
) {
    inbound_accum.reset();
    inbound_state.reset();
    *held_socket = Some(stream);
    // Infallible boundary signal (generation bump, never `Full`).
    inbound_sink.send_stream_reset();
}

/// Idle-tick connection maintenance. Run once per `'outer` iteration.
///
/// No-op when socket is already up. Link-down skips silently (no backoff charged —
/// radio recovery is the WiFi supervisor's job).
#[cfg(target_os = "espidf")]
#[allow(clippy::too_many_arguments)]
fn ensure_connected(
    held_socket: &mut Option<TlsStream>,
    backoff: &mut Backoff,
    reconnect_deadline_secs: &mut u64,
    attempt_counter: &mut u32,
    now_secs: impl Fn() -> u64,
    jitter_seed_base: u32,
    connect: &ConnectInputs,
    inbound_accum: &mut FrameAccumulator,
    inbound_state: &mut InboundConnectionState,
    inbound_sink: &mut I2sStreamSink,
    encode_buf: &mut [u8],
) {
    if held_socket.is_some() {
        return;
    }

    let link_up = wifi_is_up_nonblocking();
    let now = now_secs();
    if should_attempt_idle_connect(false, link_up, now, *reconnect_deadline_secs)
        == IdleConnectAction::Skip
    {
        return;
    }

    match connect_and_hello(connect, encode_buf) {
        Ok(stream) => {
            // Fresh socket = fresh inbound stream.
            note_socket_established(
                held_socket,
                stream,
                inbound_accum,
                inbound_state,
                inbound_sink,
            );
            note_connect_success(backoff, reconnect_deadline_secs);
            log::info!(
                "streamer: idle connect established to {}",
                connect.peer_addr
            );
        }
        Err(e) => {
            let snap = snapshot_wifi_state();
            log::warn!(
                "streamer: idle connect/Hello failed: dst={} {} err={:?} — backing off",
                connect.peer_addr,
                fmt_wifi_snapshot(&snap),
                e
            );
            *reconnect_deadline_secs =
                arm_reconnect_deadline(now, backoff, attempt_counter, jitter_seed_base);
        }
    }
}

/// How the bounded provisioning park ended.
#[derive(Debug, PartialEq, Eq)]
enum ParkOutcome {
    /// The park interval elapsed; the caller re-checks provisioning.
    TimedOut,
    /// The sender side was dropped; the caller exits the thread.
    Disconnected,
}

/// Drain and discard streamer messages for up to `timeout`.
///
/// Used by `spawn_streamer_thread`'s provisioning retry loop: while audio
/// provisioning is missing the streamer has nothing to do, so it parks here,
/// discarding any messages so the channel never wedges. Returns `TimedOut`
/// when the interval elapses, `Disconnected` when the sender side is dropped.
fn park_drain(rx: &std::sync::mpsc::Receiver<StreamerMsg>, timeout: Duration) -> ParkOutcome {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(_) => {
                // A steady message stream always yields `Ok`, so the deadline is
                // checked here rather than relying on the channel going quiet.
                if std::time::Instant::now() >= deadline {
                    return ParkOutcome::TimedOut;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return ParkOutcome::TimedOut,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return ParkOutcome::Disconnected;
            }
        }
    }
}

/// Whether a provisioning failure should be logged, given the last logged cause.
///
/// The first failure logs, identical repeats stay silent, and any change of
/// cause logs again — including a return to a cause seen before the current one.
fn should_log_provisioning_failure(last: Option<&str>, current: &str) -> bool {
    last != Some(current)
}

/// One attempt to read audio provisioning from NVS.
///
/// Opens the NVS handle fresh per call and drops it on return, so each attempt
/// observes current flash contents and no handle is held across a park.
///
/// The audio-link PSK is part of provisioning, not an optional extra: without
/// it there is no way to reach the host at all, so a keyless pod parks on the
/// reprovision poll exactly as an addressless one does.
#[cfg(target_os = "espidf")]
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
#[cfg(target_os = "espidf")]
fn fmt_msg(args: core::fmt::Arguments<'_>) -> device_protocol::TestResultMsg {
    device_protocol::format_truncating_marked::<{ device_protocol::TEST_RESULT_MSG_CAP }>(
        args,
        device_protocol::TRUNCATION_SENTINEL,
    )
}

/// Build the no-alloc message type from a fixed literal.
#[cfg(target_os = "espidf")]
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
#[cfg(target_os = "espidf")]
pub(crate) fn spawn_streamer_thread() {
    use audio_pipeline::ring::{PREROLL_SAMPLES, RING_CAPACITY_SAMPLES};
    use audio_pipeline::wire::{SegmentStart, StreamFrame};

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
            // a boot-provisioned one keeps its boot-time key until reboot.
            // TODO(hil-streamer-psk-quiesce): observe overrides after this point.
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

            let peer_addr = std::net::SocketAddr::from((audio_ip, audio_port));
            let connect = ConnectInputs {
                peer_addr: &peer_addr,
                pod_id: pod_id.as_str(),
                psk: &audio_psk,
            };
            let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);

            // ── Reconnect pacing ─────────────────────────────────────────────
            let mut backoff = Backoff::new();
            let now_secs = monotonic_secs;
            let mut reconnect_deadline_secs: u64 = 0; // 0 → connect immediately on boot
            let jitter_seed_base: u32 = jitter_seed(); // fleet jitter seed
            let mut attempt_counter: u32 = 0;

            // ── Persistent state across segments ─────────────────────────────
            let mut held_socket: Option<TlsStream> = None;
            let mut segment_counter: u32 = 0;
            // Carries "the idle inbound pump stopped at its cap" into the next idle poll
            // so a backlog drains with timeout-0 re-polls instead of one frame per tick.
            let mut idle_work_pending = false;
            use audio_pipeline::wire::MAX_FRAME_BYTES;
            let mut encode_buf = vec![0u8; MAX_FRAME_BYTES + 2];
            // Hoisted to thread lifetime to avoid per-onset alloc; write-before-read every frame.
            let mut outbound_buf = vec![0u8; MAX_FRAME_BYTES + 2];

            // ── Inbound state (reset on socket replacement) ─────────────────
            let mut inbound_accum = FrameAccumulator::new();
            let mut inbound_sink = build_inbound_stream_sink();
            let mut inbound_state = InboundConnectionState::new();

            // ── Main event loop ───────────────────────────────────────────────
            'outer: loop {
                ensure_connected(
                    &mut held_socket,
                    &mut backoff,
                    &mut reconnect_deadline_secs,
                    &mut attempt_counter,
                    now_secs,
                    jitter_seed_base,
                    &connect,
                    &mut inbound_accum,
                    &mut inbound_state,
                    &mut inbound_sink,
                    &mut encode_buf,
                );

                // ── Idle readiness wait ──────────────────────────────────────
                // POLLIN de-armed while accumulator is full (backpressure) to avoid spinning.
                // TODO(tls-link-run-segment-hil-coverage): no test drives this loop over a
                // `TlsStream` — the direction-substitution and buffered-plaintext branches
                // are only exercised against a plain `TcpStream`.
                let inbound_armed = held_socket.is_some() && inbound_has_room(&inbound_accum);

                let mut readable = false;
                let vad_write_head = if let Some((fd, events)) = held_socket
                    .as_ref()
                    .map(|s| (s.link_fd(), s.poll_events(inbound_armed, false)))
                {
                    let now = std::time::Instant::now();
                    let timeout = poll_timeout(now, None, idle_work_pending);
                    match poll_readiness(fd, events, timeout) {
                        Readiness::Fault(e) => {
                            log::warn!("streamer: idle poll fault — clearing socket, backing off: {:?}", e);
                            note_socket_lost(&mut held_socket, &mut inbound_accum, &mut inbound_state, &mut inbound_sink);
                            reconnect_deadline_secs =
                                arm_reconnect_deadline(now_secs(), &mut backoff, &mut attempt_counter, jitter_seed_base);
                            continue 'outer;
                        }
                        ready => {
                            readable = ready.readable();
                        }
                    }
                    match rx.try_recv() {
                        Ok(StreamerMsg::VadOpened { write_head }) => Some(write_head),
                        Ok(StreamerMsg::VadClosed) | Ok(StreamerMsg::Telemetry(_)) => {
                            None // stale before segment open
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => None,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            log::error!("streamer: channel disconnected; streamer thread exiting");
                            return;
                        }
                    }
                } else {
                    // No socket → no fd to poll; block on channel for IDLE_TICK.
                    match rx.recv_timeout(IDLE_TICK) {
                        Ok(StreamerMsg::VadOpened { write_head }) => Some(write_head),
                        Ok(StreamerMsg::VadClosed) | Ok(StreamerMsg::Telemetry(_)) => None,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            log::error!("streamer: channel disconnected; streamer thread exiting");
                            return;
                        }
                    }
                };

                // ── Idle inbound drain ────────────────────────────────────────
                // Pumps until the socket blocks (or the cap) rather than one frame per
                // tick — the common TTS-playback path. Also runs when POLLIN is de-armed
                // — re-offers held frame so a freed slot re-arms POLLIN next iteration.
                // Poll discipline rule 1: a TLS session can hold decrypted
                // plaintext that no POLLIN will ever reveal, so on that
                // transport a read is attempted every wake rather than only on
                // readiness. `idle_work_pending` carries a cap-stopped pump
                // into the next wake for the same reason.
                let must_drain = readable
                    || !inbound_armed
                    || idle_work_pending
                    || held_socket.as_ref().is_some_and(|s| s.buffers_plaintext());
                idle_work_pending = false;
                if must_drain && let Some(ref mut s) = held_socket {
                    match pump_inbound(
                        s.as_read(),
                        &mut inbound_accum,
                        &mut inbound_sink,
                        &mut inbound_state,
                        INBOUND_STEPS_PER_WAKE,
                    ) {
                        Ok(p) => idle_work_pending = p.hit_cap,
                        Err(e) => {
                            log::warn!("streamer: idle inbound drain error — clearing socket, backing off: {:?}", e);
                            // Blind-window coverage: a post-Hello idle-drain exit
                            // is inside the post-Hello window too; gated on
                            // seen_hello inside the helper (silent pre-Hello).
                            crate::inbound::log_inbound_exit_wp(&inbound_state);
                            // Stale partial bytes would corrupt the next connection's first frame.
                            note_socket_lost(&mut held_socket, &mut inbound_accum, &mut inbound_state, &mut inbound_sink);
                            reconnect_deadline_secs =
                                arm_reconnect_deadline(now_secs(), &mut backoff, &mut attempt_counter, jitter_seed_base);
                        }
                    }
                }

                let vad_write_head = match vad_write_head {
                    Some(wh) => wh,
                    None => continue 'outer,
                };

                // Publish the onset span so a HIL test can wait for it to end before
                // borrowing CAPTURE_RING. Drops (clears) at the end of this 'outer
                // iteration, covering every reconnect-drop path and the segment return.
                let _segment_active = SegmentActiveGuard::new();

                // ── Ensure TCP connection (real-time-or-drop) ─────────────────
                // `fresh_connect` gates the reconnect path below — no point retrying
                // on a socket we just opened.
                let mut fresh_connect = false;
                if held_socket.is_none() {
                    log::info!("streamer: connecting to {}:{}", fmt_ipv4(audio_ip), audio_port);
                    match connect_and_hello(&connect, &mut encode_buf) {
                        Ok(stream) => {
                            // The `inbound_state` reset inside the helper is a no-op here today:
                            // state is provably clean whenever `held_socket` is `None`. Routed
                            // through the helper so the "fresh socket = fresh inbound stream"
                            // invariant holds by construction, not by which path preceded us.
                            note_socket_established(
                                &mut held_socket,
                                stream,
                                &mut inbound_accum,
                                &mut inbound_state,
                                &mut inbound_sink,
                            );
                            fresh_connect = true;
                            note_connect_success(&mut backoff, &mut reconnect_deadline_secs);
                        }
                        Err(e) => {
                            let snap = snapshot_wifi_state();
                            log::warn!(
                                "streamer: connect/Hello failed: dst={}:{} {} err={:?} — dropping segment",
                                fmt_ipv4(audio_ip), audio_port, fmt_wifi_snapshot(&snap), e
                            );
                            continue 'outer;
                        }
                    }
                }

                // ── Send SegmentStart ────────────────────────────────────────
                let segment_id = segment_counter;
                segment_counter = segment_counter.wrapping_add(1);

                let (cursor, base_ts_us) = {
                    let guard = CAPTURE_RING
                        .lock()
                        .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned in streamer"));
                    let ring = guard.as_ref().expect("CAPTURE_RING not initialized");
                    let c = ridx.preroll_cursor(vad_write_head, PREROLL_SAMPLES);
                    let base_ts = if ring.anchor_sample >= c {
                        let delta_samples = ring.anchor_sample - c;
                        ring.anchor_ts_us.saturating_sub(delta_samples * 1_000_000 / 16_000)
                    } else {
                        ring.anchor_ts_us
                    };
                    (c, base_ts)
                };
                let preroll_count = vad_write_head.saturating_sub(cursor) as u32;

                let seg_start = StreamFrame::SegmentStart(SegmentStart {
                    segment_id,
                    base_sample_index: cursor,
                    base_device_ts_us: base_ts_us,
                    preroll_samples: preroll_count,
                });

                let seg_start_err = match send_frame_bp(
                    held_socket.as_mut().unwrap(),
                    &seg_start,
                    &mut encode_buf,
                ) {
                    Ok(SendOutcome::Sent) => None,
                    Ok(SendOutcome::BackpressureAligned) => {
                        log::warn!(
                            "streamer: SegmentStart backpressure (seg {}) — dropping segment, keeping socket",
                            segment_id
                        );
                        continue 'outer;
                    }
                    Err(e) => Some(e),
                };
                if let Some(e) = seg_start_err {
                    note_socket_lost(&mut held_socket, &mut inbound_accum, &mut inbound_state, &mut inbound_sink);
                    if fresh_connect {
                        let snap = snapshot_wifi_state();
                        log::warn!(
                            "streamer: SegmentStart send failed on fresh connect: dst={}:{} {} err={:?} — dropping segment",
                            fmt_ipv4(audio_ip), audio_port, fmt_wifi_snapshot(&snap), e
                        );
                        continue 'outer;
                    }
                    let snap = snapshot_wifi_state();
                    log::warn!(
                        "streamer: SegmentStart send failed on held socket: dst={}:{} {} err={:?} — one reconnect attempt",
                        fmt_ipv4(audio_ip), audio_port, fmt_wifi_snapshot(&snap), e
                    );
                    match connect_and_hello(&connect, &mut encode_buf) {
                        Ok(mut stream) => {
                            let resend = send_frame_bp(&mut stream, &seg_start, &mut encode_buf);
                            match resend {
                                Ok(outcome @ (SendOutcome::Sent | SendOutcome::BackpressureAligned)) => {
                                    // Both keep the socket: install it, then Sent falls
                                    // through to run the segment while BackpressureAligned
                                    // drops the segment (host still stalled).
                                    note_socket_established(
                                        &mut held_socket,
                                        stream,
                                        &mut inbound_accum,
                                        &mut inbound_state,
                                        &mut inbound_sink,
                                    );
                                    note_connect_success(&mut backoff, &mut reconnect_deadline_secs);
                                    if matches!(outcome, SendOutcome::BackpressureAligned) {
                                        log::warn!(
                                            "streamer: SegmentStart re-send backpressure after reconnect (seg {}) — dropping segment, keeping socket",
                                            segment_id
                                        );
                                        continue 'outer;
                                    }
                                }
                                Err(_) => {
                                    let snap = snapshot_wifi_state();
                                    log::warn!(
                                        "streamer: SegmentStart re-send failed after reconnect: dst={}:{} {} outcome={:?} — dropping segment",
                                        fmt_ipv4(audio_ip), audio_port, fmt_wifi_snapshot(&snap), resend
                                    );
                                    continue 'outer;
                                }
                            }
                        }
                        Err(e) => {
                            let snap = snapshot_wifi_state();
                            log::warn!(
                                "streamer: reconnect failed (SegmentStart): dst={}:{} {} err={:?} — dropping segment",
                                fmt_ipv4(audio_ip), audio_port, fmt_wifi_snapshot(&snap), e
                            );
                            continue 'outer;
                        }
                    }
                }

                log::info!(
                    "streamer: segment {} started cursor={} preroll={}",
                    segment_id, cursor, preroll_count
                );

                // ── Run the extracted segment loop ───────────────────────────
                // Onset, pre-roll cursor, and SegmentStart stay above; the drain
                // discipline lives in `run_segment` so a test harness can drive it.
                let exit = {
                    let mut deps = SegmentDeps {
                        socket: held_socket.as_mut().unwrap(),
                        rx: &rx,
                        ring: &CAPTURE_RING,
                        vad_closed_flag: &VAD_CLOSED_FLAG,
                        ridx: &ridx,
                        inbound_accum: &mut inbound_accum,
                        inbound_sink: &mut inbound_sink,
                        inbound_state: &mut inbound_state,
                        outbound_buf: &mut outbound_buf,
                    };
                    run_segment(&mut deps, segment_id, cursor)
                };
                if exit == SegmentExit::SocketLost {
                    // Mid-segment socket loss keeps today's policy: clear the socket
                    // but do NOT arm backoff — the zeroed reconnect deadline makes the
                    // next `ensure_connected` reconnect immediately.
                    note_socket_lost(
                        &mut held_socket,
                        &mut inbound_accum,
                        &mut inbound_state,
                        &mut inbound_sink,
                    );
                }

                // Per-segment state drops here; stale mid-frame tails never carry over.
            } // 'outer
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

/// The full mutable dependency set of the post-SegmentStart streaming span.
///
/// Groups everything the `'stream` loop touches so [`run_segment`] can be driven
/// with test-supplied inputs (a HIL harness passes its own ring, channel, flag,
/// and sink) without inheriting the production connect/backoff state. The socket
/// is already connected with `SegmentStart` sent; the caller owns all reconnect
/// policy and reacts to the returned [`SegmentExit`].
#[cfg(target_os = "espidf")]
pub(crate) struct SegmentDeps<'a> {
    /// Connected, non-blocking stream with `SegmentStart` already sent.
    pub(crate) socket: &'a mut dyn LinkStream,
    /// Telemetry/VAD → streamer channel.
    pub(crate) rx: &'a std::sync::mpsc::Receiver<StreamerMsg>,
    /// Capture ring; production passes `&CAPTURE_RING`.
    pub(crate) ring: &'a Mutex<Option<CaptureRing>>,
    /// Lossless VAD-closed flag; production passes `&VAD_CLOSED_FLAG`.
    pub(crate) vad_closed_flag: &'a std::sync::atomic::AtomicBool,
    /// Ring index geometry.
    pub(crate) ridx: &'a RingIndex,
    /// Inbound reassembly buffer.
    pub(crate) inbound_accum: &'a mut FrameAccumulator,
    /// Playback sink for decoded inbound PCM.
    pub(crate) inbound_sink: &'a mut dyn PlaybackSink,
    /// Per-connection inbound framing state.
    pub(crate) inbound_state: &'a mut InboundConnectionState,
    /// Reused encode scratch for outbound frames.
    pub(crate) outbound_buf: &'a mut Vec<u8>,
}

/// How a [`run_segment`] call terminated, formalizing the old `break 'stream`
/// plus `held_socket` Option. The caller maps each onto reconnect policy.
#[cfg(target_os = "espidf")]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SegmentExit {
    /// `SegmentEnd` was sent; the socket is healthy and kept.
    Completed,
    /// The segment was dropped (write backpressure or channel disconnect) but
    /// the socket is still usable and kept.
    SegmentDroppedSocketKept,
    /// The socket faulted or must be torn down; the caller runs `note_socket_lost`.
    SocketLost,
}

/// Run one capture segment: drain the ring into `AudioFrame`s, interleave
/// `Telemetry`, drain inbound playback, and close with `SegmentEnd`.
///
/// The socket in `deps` is connected with `SegmentStart` already sent, and
/// `read_cursor` is the pre-roll cursor. Returns a [`SegmentExit`] the caller
/// translates into reconnect policy — all connect/backoff state stays outside.
#[cfg(target_os = "espidf")]
pub(crate) fn run_segment(
    deps: &mut SegmentDeps,
    segment_id: u32,
    read_cursor: u64,
) -> SegmentExit {
    use audio_pipeline::pace::{advance_pace_us, pace_wait_us};
    use audio_pipeline::stream_send::{FrameWriteState, StepOutcome};
    use audio_pipeline::wire::{
        AUDIO_SAMPLES_PER_FRAME, AudioFrame, EndReason, MAX_AUDIO_PAYLOAD, SegmentEnd, StreamFrame,
    };
    let mut read_cursor = read_cursor;
    let mut frames_sent: u32 = 0;
    let mut samples_sent: u64 = 0;
    let mut pace_resyncs: u32 = 0;
    let mut vad_closed = false;
    let mut outbound: Option<(FrameWriteState, OutboundKind)> = None;
    let mut segment_end: Option<EndReason> = None;
    // Set when the telemetry channel disconnected mid-segment. The sender is gone, so
    // the loop stops draining the channel and only pushes the closing SegmentEnd out;
    // exit paths then keep the socket only if that frame left the wire frame-aligned.
    let mut channel_lost = false;
    // POLLOUT is armed only while a write actually blocked; otherwise writes are
    // attempted optimistically each wake. `write_blocked` implies an in-flight frame.
    let mut write_blocked = false;
    // Carries "a pump stopped with work remaining" from this wake into the next
    // wake's poll timeout: while true the loop re-polls with 0 rather than sleeping on
    // the tick. Seeded true so the segment's opening pre-roll backlog begins draining
    // immediately; the pace gate below then bounds the drain rate.
    let mut work_pending = true;
    // Earliest instant (monotonic esp_timer µs; None before the segment's first frame)
    // the next outbound audio frame may be emitted, bounding the catch-up drain to
    // CATCH_UP_PACE_MULTIPLIER × real time. Steady-state production is slower than the
    // paced cadence, so the gate binds only while a backlog is draining.
    let mut audio_pace_schedule: Option<u64> = None;
    // Set when the pace gate defers a ready audio frame; carried into the next poll so
    // the loop sleeps until the frame is due instead of busy-repolling.
    let mut pace_deadline: Option<std::time::Instant> = None;
    // Set when the write spin guard trips (poll says POLLOUT, write says WouldBlock, over
    // and over): POLLOUT stays de-armed until this instant so the loop sleeps in `poll`
    // instead of spinning, leaving the TCP stack the CPU it needs to clear the stall.
    let mut spin_backoff_deadline: Option<std::time::Instant> = None;
    let mut pending_telemetry: std::collections::VecDeque<WireTelemetry> =
        std::collections::VecDeque::new();

    // ── Intra-segment heap waypoints ─────────────────────────────────
    // Time-bracket the transient heap dive within a segment so a gradual aggregate
    // fill-to-floor (elastic consumers expanding into headroom) is distinguishable
    // from a single-instant spike. Waypoints: segment start, the moment the pre-roll
    // backlog first drains to steady state, the first write-block of the segment, and
    // a ~1 s cadence during production. No per-frame logging; the budget is a handful
    // of lines per segment. Each reads three pure heap-registry queries (sub-µs) and
    // emits one log line — the instrument's own heap and latency cost is negligible.
    let seg_start_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
    let log_heap_wp = |label: &str| {
        let (free, min, largest) = crate::health::heap_waypoint();
        log::info!(
            "streamer: heap wp seg={} {} heap_free={} min_heap={} largest_free={} alloc_fail={}",
            segment_id,
            label,
            free,
            min,
            largest,
            crate::alloc_probe::alloc_fail_count(),
        );
    };
    log_heap_wp("start");
    let mut preroll_drain_logged = false;
    let mut first_write_blocked_logged = false;
    let mut last_periodic_wp_us = seg_start_us;

    loop {
        // Periodic production waypoint on a ~1 s wall-clock cadence (bounds line
        // count regardless of wake frequency).
        {
            let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
            if now_us.saturating_sub(last_periodic_wp_us) >= 1_000_000 {
                last_periodic_wp_us = now_us;
                log_heap_wp("prod");
            }
        }
        // The loop never sleeps while either direction has actionable work: each
        // direction drains until WouldBlock / exhaustion / a fairness cap, and the poll
        // timeout is 0 whenever a pump stopped at its cap (`work_pending`).

        // ── Poll ─────────────────────────────────────────────────────
        // POLLIN while the accumulator has room; POLLOUT only while a write blocked —
        // writes are attempted optimistically, so arming POLLOUT while writable would
        // just busy-wake.
        let inbound_armed = inbound_has_room(deps.inbound_accum);
        let now = std::time::Instant::now();
        // Backoff expired: re-arm POLLOUT and let a fresh run of disagreement re-trip.
        if spin_backoff_deadline.is_some_and(|d| now >= d) {
            spin_backoff_deadline = None;
            if let Some((state, _)) = outbound.as_mut() {
                state.reset_spin_guard();
            }
        }
        let events = deps.socket.poll_events(
            inbound_armed,
            write_blocked && spin_backoff_deadline.is_none(),
        );
        let fd = deps.socket.link_fd();
        // The write deadline bounds the wait only while blocked on POLLOUT; the pace
        // deadline (set when a ready audio frame was deferred for rate-limiting) bounds
        // it while the catch-up drain is throttled. The earlier of the two wins.
        let write_deadline = if write_blocked {
            outbound.as_ref().map(|(st, _)| st.next_deadline())
        } else {
            None
        };
        // The spin backoff joins the min so the loop wakes to re-arm POLLOUT the moment it
        // expires; it can only shorten the wait, never delay a write budget/ceiling firing.
        let deadline = write_deadline
            .into_iter()
            .chain(pace_deadline)
            .chain(spin_backoff_deadline)
            .min();
        let timeout = poll_timeout(now, deadline, work_pending);
        // Consumed by this poll; the outbound pump re-arms it below if the gate still
        // defers a frame.
        pace_deadline = None;
        let ready = poll_readiness(fd, events, timeout);
        if let Readiness::Fault(e) = ready {
            log::warn!(
                "streamer: poll fault mid-segment (seg {}): {:?} — clearing socket",
                segment_id,
                e
            );
            return SegmentExit::SocketLost;
        }

        // ── Channel + VAD-flag drain ─────────────────────────────────
        while !channel_lost {
            match deps.rx.try_recv() {
                Ok(StreamerMsg::Telemetry(tel)) => {
                    if pending_telemetry.len() >= PENDING_TELEMETRY_CAP {
                        pending_telemetry.pop_front();
                        log::warn!(
                            "streamer: pending_telemetry at cap {} (outbound stalled, seg {}) — dropping oldest",
                            PENDING_TELEMETRY_CAP,
                            segment_id
                        );
                    }
                    pending_telemetry.push_back(tel);
                }
                Ok(StreamerMsg::VadClosed) => {
                    vad_closed = true;
                }
                Ok(StreamerMsg::VadOpened { .. }) => {
                    // Re-onset during hangover — ignored (FSM-handled).
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    // Best-effort close: route SegmentEnd(InternalError) through the
                    // normal selector + pump so the in-flight frame drains first and the
                    // write budget / POLLOUT handling is not duplicated here.
                    log::error!("streamer: channel disconnected mid-segment");
                    channel_lost = true;
                    if segment_end.is_none() {
                        segment_end = Some(EndReason::InternalError);
                    }
                    break;
                }
            }
        }
        // Atomic fallback: channel message can be dropped under TCP stall.
        if !vad_closed
            && deps
                .vad_closed_flag
                .load(std::sync::atomic::Ordering::Acquire)
        {
            vad_closed = true;
        }

        // ── Inbound pump ─────────────────────────────────────────────
        // Drain inbound until WouldBlock or the per-wake cap; the gate on the first
        // call keeps the load-bearing re-offer-under-backpressure guard (`!inbound_armed`).
        let mut inbound_work = false;
        // Poll discipline rule 1: on a transport that buffers decrypted
        // plaintext, readiness under-reports what is available, so read every
        // wake instead of only when POLLIN fired.
        if ready.readable() || !inbound_armed || deps.socket.buffers_plaintext() {
            match pump_inbound(
                deps.socket.as_read(),
                deps.inbound_accum,
                deps.inbound_sink,
                deps.inbound_state,
                INBOUND_STEPS_PER_WAKE,
            ) {
                Ok(p) => inbound_work = p.hit_cap,
                Err(e) => {
                    log::warn!("streamer: inbound drain error — clearing socket: {:?}", e);
                    // Blind-window coverage: one last heap reading on the inbound
                    // error/exit path, before the socket is torn down. Gated on
                    // seen_hello inside the helper.
                    crate::inbound::log_inbound_exit_wp(deps.inbound_state);
                    return SegmentExit::SocketLost;
                }
            }
        }

        // ── Outbound pump ────────────────────────────────────────────
        // A poll that reported writable clears the blocked flag; writes then resume.
        if ready.writable() {
            write_blocked = false;
        }
        let mut outbound_work = false;
        let mut frames_this_wake: u32 = 0;
        'outbound: loop {
            if frames_this_wake >= OUTBOUND_FRAMES_PER_WAKE {
                // Stopped for fairness with more to send → re-poll with timeout 0.
                outbound_work = true;
                break 'outbound;
            }

            if outbound.is_none() {
                // ── Selector: SegmentEnd → telemetry → mic AudioFrame → partial-at-close ──
                if let Some(reason) = segment_end {
                    let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
                    let seg_end = StreamFrame::SegmentEnd(SegmentEnd {
                        segment_id,
                        device_ts_us: now_us,
                        frames_sent,
                        samples_sent,
                        reason,
                    });
                    match FrameWriteState::begin(
                        &seg_end,
                        deps.outbound_buf.as_mut_slice(),
                        std::time::Instant::now,
                    ) {
                        Ok(state) => outbound = Some((state, OutboundKind::SegmentEnd)),
                        Err(e) => {
                            log::warn!(
                                "streamer: SegmentEnd encode failed (seg {}): {:?} — clearing socket",
                                segment_id,
                                e
                            );
                            return SegmentExit::SocketLost;
                        }
                    }
                } else if let Some(tel) = pending_telemetry.pop_front() {
                    let tel_frame = StreamFrame::Telemetry(tel);
                    match FrameWriteState::begin(
                        &tel_frame,
                        deps.outbound_buf.as_mut_slice(),
                        std::time::Instant::now,
                    ) {
                        Ok(state) => outbound = Some((state, OutboundKind::Telemetry)),
                        Err(e) => {
                            log::warn!(
                                "streamer: Telemetry encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                segment_id,
                                e
                            );
                            // Local fault: no bytes written, the stream stays
                            // frame-aligned, so the connection remains usable.
                            return SegmentExit::SegmentDroppedSocketKept;
                        }
                    }
                } else {
                    let (write_head, anchor_sample, anchor_ts_us) = {
                        let guard = deps
                            .ring
                            .lock()
                            .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned in streamer"));
                        let ring = guard.as_ref().expect("CAPTURE_RING not initialized");
                        (ring.write_head, ring.anchor_sample, ring.anchor_ts_us)
                    };

                    // Lapped cursor → close segment.
                    if deps.ridx.is_overrun(write_head, read_cursor) {
                        log::warn!("streamer: ring overrun in segment {}", segment_id);
                        segment_end = Some(EndReason::Overrun);
                        continue 'outbound;
                    }

                    let avail = deps.ridx.available(write_head, read_cursor);

                    if avail >= AUDIO_SAMPLES_PER_FRAME as u64 {
                        // ── Real-time pace gate ──────────────────────────────
                        // A full frame of backlog is ready. Release it no faster than
                        // the paced cadence so the pre-roll catch-up does not blast the
                        // whole backlog into the TX pool + TCP send queue at once —
                        // bounding transient heap consumption during the drain window.
                        let now_us = unsafe { esp_idf_svc::sys::esp_timer_get_time() } as u64;
                        if let Some(wait_us) = pace_wait_us(audio_pace_schedule, now_us) {
                            pace_deadline = Some(
                                std::time::Instant::now()
                                    + std::time::Duration::from_micros(wait_us),
                            );
                            break 'outbound;
                        }
                        let adv = advance_pace_us(audio_pace_schedule, now_us);
                        if adv.resynced {
                            pace_resyncs = pace_resyncs.saturating_add(1);
                        }
                        audio_pace_schedule = Some(adv.next_schedule_us);

                        let frame_first_index = read_cursor;
                        let frame_ts_us = if anchor_sample >= frame_first_index {
                            let delta = anchor_sample - frame_first_index;
                            anchor_ts_us.saturating_sub(delta * 1_000_000 / 16_000)
                        } else {
                            let delta = frame_first_index - anchor_sample;
                            anchor_ts_us + delta * 1_000_000 / 16_000
                        };

                        let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
                        {
                            let guard = deps.ring.lock().unwrap_or_else(|_| {
                                panic!("CAPTURE_RING mutex poisoned in streamer")
                            });
                            let ring = guard.as_ref().expect("CAPTURE_RING not initialized");
                            // Re-check overrun under the copy lock.
                            let live_head = ring.write_head;
                            if deps.ridx.is_overrun(live_head, read_cursor) {
                                drop(guard);
                                segment_end = Some(EndReason::Overrun);
                                continue 'outbound;
                            }
                            for i in 0..AUDIO_SAMPLES_PER_FRAME {
                                let slot = deps.ridx.slot(read_cursor + i as u64);
                                let bytes = ring.samples[slot].to_le_bytes();
                                pcm.push(bytes[0]).expect("pcm push overflow");
                                pcm.push(bytes[1]).expect("pcm push overflow");
                            }
                        }

                        let audio_frame = StreamFrame::Audio(AudioFrame {
                            segment_id,
                            first_sample_index: frame_first_index,
                            device_ts_us: frame_ts_us,
                            pcm,
                        });
                        match FrameWriteState::begin(
                            &audio_frame,
                            deps.outbound_buf.as_mut_slice(),
                            std::time::Instant::now,
                        ) {
                            Ok(state) => {
                                read_cursor += AUDIO_SAMPLES_PER_FRAME as u64;
                                outbound = Some((
                                    state,
                                    OutboundKind::Audio {
                                        samples: AUDIO_SAMPLES_PER_FRAME as u32,
                                    },
                                ));
                            }
                            Err(e) => {
                                log::warn!(
                                    "streamer: AudioFrame encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                    segment_id,
                                    e
                                );
                                // Local size/serialization fault; socket untouched and
                                // frame-aligned.
                                return SegmentExit::SegmentDroppedSocketKept;
                            }
                        }
                    } else if vad_closed && avail < AUDIO_SAMPLES_PER_FRAME as u64 {
                        // VAD released with < full frame residual → drain partial, then close.
                        let partial = avail as usize;
                        if partial > 0 {
                            let frame_first_index = read_cursor;
                            let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> =
                                heapless::Vec::new();
                            let frame_ts_us;
                            {
                                let guard = deps
                                    .ring
                                    .lock()
                                    .unwrap_or_else(|_| panic!("CAPTURE_RING mutex poisoned"));
                                let ring = guard.as_ref().expect("CAPTURE_RING not initialized");
                                let live_head = ring.write_head;
                                if deps.ridx.is_overrun(live_head, read_cursor) {
                                    drop(guard);
                                    log::warn!(
                                        "streamer: ring overrun in partial-frame copy (seg {})",
                                        segment_id
                                    );
                                    segment_end = Some(EndReason::Overrun);
                                    continue 'outbound;
                                }
                                frame_ts_us = if ring.anchor_sample >= frame_first_index {
                                    let delta = ring.anchor_sample - frame_first_index;
                                    ring.anchor_ts_us.saturating_sub(delta * 1_000_000 / 16_000)
                                } else {
                                    let delta = frame_first_index - ring.anchor_sample;
                                    ring.anchor_ts_us + delta * 1_000_000 / 16_000
                                };
                                for i in 0..partial {
                                    let slot = deps.ridx.slot(read_cursor + i as u64);
                                    let bytes = ring.samples[slot].to_le_bytes();
                                    pcm.push(bytes[0]).expect("pcm push overflow");
                                    pcm.push(bytes[1]).expect("pcm push overflow");
                                }
                            }
                            let audio_frame = StreamFrame::Audio(AudioFrame {
                                segment_id,
                                first_sample_index: frame_first_index,
                                device_ts_us: frame_ts_us,
                                pcm,
                            });
                            match FrameWriteState::begin(
                                &audio_frame,
                                deps.outbound_buf.as_mut_slice(),
                                std::time::Instant::now,
                            ) {
                                Ok(state) => {
                                    read_cursor += partial as u64;
                                    segment_end = Some(EndReason::VadRelease);
                                    outbound = Some((
                                        state,
                                        OutboundKind::Audio {
                                            samples: partial as u32,
                                        },
                                    ));
                                }
                                Err(e) => {
                                    log::warn!(
                                        "streamer: partial AudioFrame encode failed (seg {}): {:?} — dropping segment, keeping socket",
                                        segment_id,
                                        e
                                    );
                                    // Local fault; socket untouched and frame-aligned.
                                    return SegmentExit::SegmentDroppedSocketKept;
                                }
                            }
                        } else {
                            segment_end = Some(EndReason::VadRelease);
                            continue 'outbound;
                        }
                    } else {
                        // Caught up, VAD still open — nothing to send this wake. The
                        // first arrival here marks the pre-roll backlog fully drained to
                        // steady state (the end of the catch-up window).
                        if !preroll_drain_logged {
                            preroll_drain_logged = true;
                            log_heap_wp("preroll-drained");
                        }
                        break 'outbound;
                    }
                }
            }

            // ── In-flight frame: optimistic non-blocking write ──
            if write_blocked {
                // Kernel send buffer full; wait for POLLOUT (armed at the next poll).
                break 'outbound;
            }
            let Some((state, kind)) = outbound.as_mut() else {
                unreachable!("outbound is Some after the selector built a frame");
            };
            match state.step_writable(
                deps.socket.as_write(),
                deps.outbound_buf.as_slice(),
                std::time::Instant::now,
            ) {
                Ok(StepOutcome::WroteWhole) => {
                    match *kind {
                        OutboundKind::Audio { samples } => {
                            frames_sent += 1;
                            samples_sent += samples as u64;
                            outbound = None;
                        }
                        OutboundKind::Telemetry => {
                            outbound = None;
                        }
                        OutboundKind::SegmentEnd => {
                            log::info!(
                                "streamer: segment {} ended frames={} samples={} pace_resyncs={}",
                                segment_id,
                                frames_sent,
                                samples_sent,
                                pace_resyncs
                            );
                            // A channel-loss close is a dropped segment, not a normal
                            // completion; the socket stays only because the closing
                            // frame went out whole.
                            return if channel_lost {
                                SegmentExit::SegmentDroppedSocketKept
                            } else {
                                SegmentExit::Completed
                            };
                        }
                    }
                    frames_this_wake += 1;
                }
                // Kernel took bytes but the frame is not done — retry immediately, no poll.
                Ok(StepOutcome::WrotePartial) => {}
                Ok(StepOutcome::WouldBlock) => {
                    if !first_write_blocked_logged {
                        first_write_blocked_logged = true;
                        log_heap_wp("write-blocked");
                    }
                    write_blocked = true;
                    if state.spin_guard_tripped() && spin_backoff_deadline.is_none() {
                        spin_backoff_deadline = Some(
                            std::time::Instant::now()
                                + std::time::Duration::from_millis(SPIN_BACKOFF_MS),
                        );
                    }
                    break 'outbound;
                }
                Err(e) => {
                    // Partial write leaves receiver mid-frame; can't resume.
                    log::warn!(
                        "streamer: outbound send failed mid-segment (seg {}): {:?} — dropping segment, clearing socket",
                        segment_id,
                        e
                    );
                    return SegmentExit::SocketLost;
                }
            }
        }

        // ── Write watchdog: enforce budget/ceiling once per wake for an in-flight frame ──
        if let Some((state, _)) = outbound.as_mut() {
            match state.check_deadlines(std::time::Instant::now) {
                None => {}
                Some(Ok(SendOutcome::BackpressureAligned)) => {
                    log::warn!(
                        "streamer: outbound backpressure mid-segment (seg {}) — dropping segment, keeping socket",
                        segment_id
                    );
                    return SegmentExit::SegmentDroppedSocketKept;
                }
                Some(Ok(SendOutcome::Sent)) => {
                    unreachable!(
                        "check_deadlines returned Sent, which its contract forbids — invariant violated"
                    );
                }
                Some(Err(e)) => {
                    log::warn!(
                        "streamer: outbound write ceiling/budget elapsed mid-tail (seg {}): {:?} — dropping segment, clearing socket",
                        segment_id,
                        e
                    );
                    return SegmentExit::SocketLost;
                }
            }
        }

        work_pending = inbound_work || outbound_work;
    }
}

#[cfg(test)]
mod tests {
    // ── Provisioning park ─────────────────────────────────────────────────

    use super::{ParkOutcome, StreamerMsg, park_drain, should_log_provisioning_failure};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// An idle channel parks for the full interval, then reports the timeout.
    #[test]
    fn park_drain_times_out_when_idle() {
        let (_tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        let timeout = Duration::from_millis(120);
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "park_drain returned early: {:?}",
            start.elapsed()
        );
    }

    /// Messages arriving mid-park are drained — not merely slept through — and
    /// do not cut the park short. The channel holds one message, so a sender
    /// pushing eight only completes if the park actually consumes them.
    #[test]
    fn park_drain_discards_messages_and_still_waits() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(1);
        let timeout = Duration::from_millis(200);
        let sent = Arc::new(AtomicUsize::new(0));
        let sent_tx = Arc::clone(&sent);
        std::thread::spawn(move || {
            for _ in 0..8 {
                std::thread::sleep(Duration::from_millis(10));
                if tx.send(StreamerMsg::VadClosed).is_err() {
                    return;
                }
                sent_tx.fetch_add(1, Ordering::SeqCst);
            }
            // Hold the sender so disconnect does not race the deadline.
            std::thread::sleep(Duration::from_millis(400));
        });
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "messages shortened the park: {:?}",
            start.elapsed()
        );
        assert_eq!(
            sent.load(Ordering::SeqCst),
            8,
            "sender blocked: park did not drain the channel"
        );
    }

    /// Dropping the sender ends the park promptly with `Disconnected`, well
    /// before the park deadline.
    #[test]
    fn park_drain_reports_disconnect() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            drop(tx);
        });
        let start = Instant::now();
        assert_eq!(
            park_drain(&rx, Duration::from_secs(10)),
            ParkOutcome::Disconnected
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "disconnect was not reported promptly: {:?}",
            start.elapsed()
        );
    }

    /// A sustained message flood still lets the park reach its deadline.
    #[test]
    fn park_drain_times_out_under_message_flood() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StreamerMsg>(4);
        std::thread::spawn(move || while tx.send(StreamerMsg::VadClosed).is_ok() {});
        let timeout = Duration::from_millis(150);
        let start = Instant::now();
        assert_eq!(park_drain(&rx, timeout), ParkOutcome::TimedOut);
        assert!(
            start.elapsed() >= timeout,
            "park ended early: {:?}",
            start.elapsed()
        );
    }

    // ── Provisioning failure log throttle ─────────────────────────────────

    #[test]
    fn provisioning_failure_logs_first_and_on_cause_change() {
        // First failure always logs.
        assert!(should_log_provisioning_failure(
            None,
            "audio_port not provisioned"
        ));
        // An identical repeat stays silent.
        assert!(!should_log_provisioning_failure(
            Some("audio_port not provisioned"),
            "audio_port not provisioned"
        ));
        // A changed cause logs again.
        assert!(should_log_provisioning_failure(
            Some("audio_port not provisioned"),
            "cannot open NVS — no such namespace"
        ));
        // Returning to an earlier cause logs again — this is last-cause
        // comparison, not a seen-set.
        assert!(should_log_provisioning_failure(
            Some("cannot open NVS — no such namespace"),
            "audio_port not provisioned"
        ));
    }

    // ── Idle reconnect pacing ─────────────────────────────────────────────

    use super::{
        Backoff, IdleConnectAction, arm_reconnect_deadline, note_connect_success,
        should_attempt_idle_connect,
    };
    use wifi_reconnect::{BACKOFF_CAP_SECS, BACKOFF_FLOOR_SECS};

    /// An established socket always skips reconnect regardless of link state.
    #[test]
    fn idle_gate_skips_when_socket_present() {
        for link in [Some(true), Some(false), None] {
            assert_eq!(
                should_attempt_idle_connect(true, link, 1_000, 0),
                IdleConnectAction::Skip,
                "socket Some must always skip (link={link:?})"
            );
        }
    }

    /// Link down/unknown → skip, regardless of deadline (no point trying without radio).
    #[test]
    fn idle_gate_skips_when_link_down_or_unknown() {
        for link in [Some(false), None] {
            assert_eq!(
                should_attempt_idle_connect(false, link, 1_000, 0),
                IdleConnectAction::Skip,
                "link down/unknown must skip even with a past deadline (link={link:?})"
            );
        }
    }

    /// Socket down + link up, but backoff not elapsed → skip.
    #[test]
    fn idle_gate_skips_before_deadline() {
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 99, 100),
            IdleConnectAction::Skip,
            "now < deadline must skip"
        );
    }

    /// Socket down + link up + deadline elapsed → attempt.
    #[test]
    fn idle_gate_attempts_when_due() {
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 100, 100),
            IdleConnectAction::Attempt,
            "now == deadline must attempt"
        );
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 101, 100),
            IdleConnectAction::Attempt,
            "now > deadline must attempt"
        );
        // Zeroed deadline (fresh boot / post-success) → immediate attempt.
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 0, 0),
            IdleConnectAction::Attempt,
            "zero deadline must attempt immediately"
        );
    }

    /// Arming advances the attempt counter and returns a deadline in the ±25% jitter band.
    #[test]
    fn arm_deadline_advances_counter_and_lands_in_jitter_band() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 1_000;

        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 0xABCD);

        assert_eq!(attempt_counter, 1);
        // After one record_failure, backoff doubled from floor (2 → 4); jitter band ±25%.
        let base = BACKOFF_FLOOR_SECS * 2;
        let low = now + (base * 75 / 100).max(1);
        let high = now + base * 125 / 100;
        assert!(
            deadline >= low && deadline <= high,
            "deadline {deadline} not in jitter band [{low}, {high}]"
        );
    }

    /// The stored deadline is evaluated by value — repeated gate polls never re-jitter.
    #[test]
    fn stored_deadline_is_stable_across_repeated_gate_polls() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 1_000;

        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 0x1234);

        let just_before = deadline - 1;
        for _ in 0..1_000 {
            assert_eq!(
                should_attempt_idle_connect(false, Some(true), just_before, deadline),
                IdleConnectAction::Skip,
                "gate must read the fixed stored deadline, never re-jitter"
            );
        }
        assert_eq!(attempt_counter, 1, "polling must not advance the counter");

        // Fires exactly at the armed deadline (composed arm→wait→fire boundary).
        assert_eq!(
            should_attempt_idle_connect(false, Some(true), deadline, deadline),
            IdleConnectAction::Attempt,
        );
    }

    /// After a successful connect (which zeroes the deadline), a mid-segment socket
    /// drop reconnects immediately — the zeroed deadline is already in the past.
    #[test]
    fn mid_segment_drop_reconnects_immediately_after_success_clear() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;

        let armed = arm_reconnect_deadline(1_000, &mut backoff, &mut attempt_counter, 0x55);
        assert!(armed > 1_000);

        let mut reconnect_deadline_secs = armed;
        note_connect_success(&mut backoff, &mut reconnect_deadline_secs);
        assert_eq!(
            reconnect_deadline_secs, 0,
            "note_connect_success must zero the deadline"
        );
        assert_eq!(
            backoff.current_secs(),
            BACKOFF_FLOOR_SECS,
            "note_connect_success must reset backoff to the floor"
        );

        assert_eq!(
            should_attempt_idle_connect(false, Some(true), 0, reconnect_deadline_secs),
            IdleConnectAction::Attempt,
            "post-success-clear mid-segment drop must reconnect on the next idle tick"
        );
    }

    /// Repeated failures climb the backoff; `record_success` resets to floor so
    /// the next re-arm draws from the floor band again.
    #[test]
    fn record_success_resets_rearm_to_floor_band() {
        let mut backoff = Backoff::new();
        let mut attempt_counter: u32 = 0;
        let now: u64 = 500;

        let mut last_base = BACKOFF_FLOOR_SECS;
        for _ in 0..5 {
            let _ = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 7);
            assert!(
                backoff.current_secs() >= last_base,
                "backoff must not shrink across failures"
            );
            last_base = backoff.current_secs();
        }
        assert!(
            backoff.current_secs() > BACKOFF_FLOOR_SECS,
            "backoff must have climbed above the floor after repeated failures"
        );
        assert!(
            backoff.current_secs() <= BACKOFF_CAP_SECS,
            "backoff must stay at or below the cap"
        );

        backoff.record_success();
        assert_eq!(
            backoff.current_secs(),
            BACKOFF_FLOOR_SECS,
            "record_success must reset backoff to the floor"
        );

        // Re-arm must draw from the floor band again.
        let deadline = arm_reconnect_deadline(now, &mut backoff, &mut attempt_counter, 7);
        let base = BACKOFF_FLOOR_SECS * 2;
        let low = now + (base * 75 / 100).max(1);
        let high = now + base * 125 / 100;
        assert!(
            deadline >= low && deadline <= high,
            "post-reset deadline {deadline} not in floor band [{low}, {high}]"
        );
    }
}
