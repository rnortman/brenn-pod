//! Network self-test handlers for the respeaker-pod HIL suite.
//!
//! UDP echo round-trips, TLS reachability, inbound-frame drain, TLS
//! send-backpressure (blocked-write resume), and bidirectional `poll()`
//! readiness. Each `run_*` handler is dispatched from `hil::run_handler`.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use crate::hil::test_report_fail_msg;
#[cfg(target_os = "espidf")]
use crate::inbound::{
    consume_frames, drain_inbound, CountingSink, FrameAccumulator, InboundConnectionState,
    StallCountingSink,
};
#[cfg(target_os = "espidf")]
use crate::netpoll::poll_one;
#[cfg(target_os = "espidf")]
use crate::nvs::open_wifi_nvs;
#[cfg(target_os = "espidf")]
use crate::tls_link::LinkStream;
#[cfg(target_os = "espidf")]
use crate::{build_inbound_stream_sink, send_frame_bp, send_frame_bp_counted};
#[cfg(target_os = "espidf")]
use audio_pipeline::stream_send::SendOutcome;
#[cfg(target_os = "espidf")]
use device_protocol::{
    test_report_fail, test_report_fail_detail, test_report_fail_fmt, test_report_ok,
    test_report_ok_detail, Payload, Status, TestData, TLS_PSK_CONNECT_TIMEOUT_SECS,
    TLS_PSK_ECHO_TIMEOUT_SECS,
};
#[cfg(target_os = "espidf")]
use esp_idf_svc::tls::{self, EspTls};
#[cfg(target_os = "espidf")]
use wifi_diag::fmt_ipv4;

// ── Network test helpers ──────────────────────────────────────────────────────

/// UDP echo round-trip self-test.
///
/// Sends a 16-byte nonce to the HIL-host echo server and asserts the reply
/// matches. Reads peer IP and port from NVS.
#[cfg(target_os = "espidf")]
pub(crate) fn run_udp_roundtrip() -> (Status, Payload) {
    use std::net::UdpSocket;

    let peer = match crate::hil_session::peer_config() {
        Some(p) => p,
        None => {
            return test_report_fail("no session peer config — run SetTemporaryPeerConfig first");
        }
    };
    let peer_ip = peer.host;
    let udp_port = peer.udp_port;

    let nonce: [u8; 16] = [
        0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD,
        0xEF,
    ];

    let sock = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => return test_report_fail_detail("udp bind failed", &e),
    };

    let peer_addr = std::net::SocketAddr::from((peer_ip, udp_port));
    if let Err(e) = sock.connect(peer_addr) {
        return test_report_fail_detail("udp connect failed", &e);
    }

    if let Err(e) = sock.send(&nonce) {
        return test_report_fail_detail("udp send failed", &e);
    }

    if let Err(e) = sock.set_read_timeout(Some(std::time::Duration::from_secs(10))) {
        return test_report_fail_detail("udp set_read_timeout failed", &e);
    }

    let mut reply = [0u8; 32];
    let n = match sock.recv(&mut reply) {
        Ok(n) => n,
        Err(e) => return test_report_fail_detail("udp recv timeout/error", &e),
    };

    if n != nonce.len() || reply[..n] != nonce[..] {
        return test_report_fail_fmt(format_args!(
            "FAIL echo mismatch len={} expected={}",
            n,
            nonce.len()
        ));
    }

    test_report_ok(TestData::UdpEcho {
        bytes: n as u32,
        peer_ip,
        peer_port: udp_port,
    })
}

/// TLS handshake reachability self-test.
///
/// Connects via `EspTls` to a host from the session peer config, verifying
/// against the bundled CA store. A successful handshake proves the TLS stack
/// works over live WiFi.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_reachability() -> (Status, Payload) {
    let peer = match crate::hil_session::peer_config() {
        Some(p) => p,
        None => {
            return test_report_fail("no session peer config — run SetTemporaryPeerConfig first");
        }
    };
    let tls_host = peer.tls_host;
    let tls_port = peer.tls_port;

    let host_str = fmt_ipv4(tls_host);

    let mut tls_conn = match EspTls::new() {
        Ok(t) => t,
        Err(e) => return test_report_fail_detail("EspTls::new failed", &e),
    };

    match tls_conn.connect(
        host_str.as_str(),
        tls_port,
        &tls::Config {
            use_crt_bundle_attach: true,
            // Target is a literal IP so CN matching is skipped. CA chain
            // validation still occurs. Do not copy for hostname-based connections.
            skip_common_name: true,
            ..Default::default()
        },
    ) {
        Ok(_) => test_report_ok(TestData::TlsHandshake {
            peer_ip: tls_host,
            peer_port: tls_port,
        }),
        Err(e) => test_report_fail_detail("tls handshake failed", &e),
    }
}

/// In-band selector byte written first (inside the tunnel) on an inbound-frames
/// connection to select the happy-path profile (fixed `INBOUND_FRAMES_COUNT`
/// frames). `run_tls_inbound_backpressure` writes `INBOUND_SELECTOR_FLOOD`
/// instead to select the unpaced flood profile. The host's short-timeout
/// selector read falls back to this happy-path profile on a stray/no-selector
/// caller (e.g. an old device build), so omitting the write only costs latency,
/// not correctness.
#[cfg(target_os = "espidf")]
const INBOUND_SELECTOR_HAPPY_PATH: u8 = b'N';

/// In-band selector byte written first on an inbound-frames connection to select the
/// unpaced flood profile the `TlsInboundBackpressure` self-test needs.
#[cfg(target_os = "espidf")]
const INBOUND_SELECTOR_FLOOD: u8 = b'F';

/// Shared preamble for both inbound-source self-tests: pull the session peer
/// config + audio PSK, open a TLS-PSK connection to `inbound_frames_port` with
/// `INBOUND_CONNECT_TIMEOUT_SECS`, then write `selector` inside the tunnel
/// (after the handshake, before the server's Hello — a stray old device/server
/// that omits or ignores it falls back to the happy-path profile after a short
/// server-side timeout). The returned stream is non-blocking; callers drive it
/// with the `tls_psk_wait` poll discipline.
///
/// `err_prefix` (e.g. `"tls inbound"` / `"tls inbound-bp"`) distinguishes the two
/// callers' connect failure messages; session-lookup failures use their own
/// fixed messages, identical for both callers.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn connect_inbound_source(
    selector: u8,
    err_prefix: &str,
) -> Result<(crate::tls_link::TlsStream, [u8; 4], u16), (Status, Payload)> {
    let TlsPskInputs {
        peer_ip,
        peer_port: inbound_port,
        psk,
        pod_id,
    } = tls_psk_inputs(|p| p.inbound_frames_port)?;

    let peer_addr = std::net::SocketAddr::from((peer_ip, inbound_port));
    let mut stream = crate::tls_link::tls_connect_psk(&crate::tls_link::TlsConnectParams {
        peer: &peer_addr,
        pod_id: pod_id.as_str(),
        key: &psk,
        connect_timeout: std::time::Duration::from_secs(
            device_protocol::INBOUND_CONNECT_TIMEOUT_SECS,
        ),
        write_timeout: std::time::Duration::from_secs(device_protocol::INBOUND_READ_TIMEOUT_SECS),
    })
    .map_err(|e| {
        test_report_fail_detail(&format!("{err_prefix} tls connect/handshake failed"), &e)
    })?;

    // Selector byte goes inside the tunnel, after the handshake.
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(device_protocol::INBOUND_READ_TIMEOUT_SECS);
    tls_psk_write_all(&mut stream, &[selector], deadline)?;

    Ok((stream, peer_ip, inbound_port))
}

/// TLS-PSK inbound-frames self-test.
///
/// Opens a TLS-PSK connection to the HIL-host frame source, reads
/// `StreamFrame::Audio` frames inside the tunnel through `drain_inbound` until
/// EOF, and reports the count. Exercises the inbound framing/decode path on a
/// dedicated connection.
///
/// Requires prior `WifiAssociate` and `SetTemporaryPeerConfig`.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_inbound_frames() -> (Status, Payload) {
    let (mut stream, peer_ip, inbound_port) =
        match connect_inbound_source(INBOUND_SELECTOR_HAPPY_PATH, "tls inbound") {
            Ok(t) => t,
            Err(fail) => return fail,
        };

    let mut accum = FrameAccumulator::new();
    let mut sink = CountingSink::new();
    let mut inbound_state = InboundConnectionState::new();
    let mut total_frames: u32 = 0;
    // Fail fast if the server stalls (MAX_IDLE_RETRIES consecutive idle poll waits
    // with no frames).
    let mut idle_count: u32 = 0;

    loop {
        match drain_inbound(&mut stream, &mut accum, &mut sink, &mut inbound_state) {
            Ok(outcome) if outcome.frames_routed > 0 => {
                total_frames += outcome.frames_routed;
                idle_count = 0; // reset on progress
            }
            // Buffered plaintext (poll discipline rule 1): a drain that read bytes but
            // routed no frame keeps draining without a poll wait, because decrypted bytes
            // can sit in the session buffer with no POLLIN to reveal them.
            Ok(outcome) if outcome.made_progress() => {}
            Ok(_) => {
                // Nothing pending: poll for readable up to the read-timeout budget rather
                // than busy-spinning on the non-blocking tunnel. The wait happens before
                // the limit check so `idle_count` counts waits actually performed and the
                // total patience is MAX_IDLE_RETRIES × INBOUND_READ_TIMEOUT_SECS.
                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_secs(device_protocol::INBOUND_READ_TIMEOUT_SECS);
                if let Err(fail) = tls_psk_wait(&stream, true, deadline) {
                    return fail;
                }
                idle_count += 1;
                if idle_count >= device_protocol::MAX_IDLE_RETRIES {
                    log::info!(
                        "TlsInboundFrames: idle fail-fast after {} waits, frames so far={}",
                        idle_count,
                        total_frames,
                    );
                    return test_report_fail_fmt(format_args!(
                        "tls inbound: server stalled after {} waits, frames={}",
                        idle_count, total_frames,
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF — server closed after sending all frames. A genuine peer
                // RST surfaces separately as ConnectionReset and falls to the arm below.
                break;
            }
            Err(e) => {
                return test_report_fail_detail("tls inbound read/decode error", &e);
            }
        }
    }

    log::info!("TlsInboundFrames: received {} frames", total_frames);
    test_report_ok(TestData::TlsInboundFrames {
        inbound_frames: total_frames,
        peer_ip,
        peer_port: inbound_port,
    })
}

/// TCP inbound-backpressure self-test.
///
/// Connects to the HIL-host inbound-frames source, selects the flood profile with the
/// in-band `INBOUND_SELECTOR_FLOOD` byte, and drains an unpaced over-capacity Audio
/// flood through the **production** socket → `drain_inbound` → ring path — a
/// `StallCountingSink` wrapping `build_inbound_stream_sink()`'s `I2sStreamSink` — while
/// the real capture thread drains at real time. This is the socket-path counterpart
/// to `TcpInboundFrames`: the flood overruns the ring + accumulator + lwIP buffering
/// so the accumulator-full read-skip and the held-frame retry that reopens the TCP
/// window (`drain_inbound`'s livelock guard) both actually run on real lwIP.
///
/// Reports `TestData::TcpInboundBackpressure` on a clean EOF (flood-complete FIN);
/// any other error drops the connection and fails typed, since "connection stays up"
/// is asserted by construction — only the flood-complete path reports Ok. The host
/// eval owns the `sink_full_events > 0` / exact-count / connect assertions.
///
/// Decision for one post-EOF `consume_frames` tick in `run_tcp_inbound_backpressure`,
/// factored out as a `cfg`-free pure function so the classification logic is
/// unit-testable on the host without a real socket or ring.
#[derive(Debug, PartialEq, Eq)]
enum PostEofTick {
    /// This tick routed at least one frame — caller resets its idle streak.
    Progress,
    /// No frames routed, but the no-progress streak has not yet hit the limit — keep
    /// waiting for the ring to free up.
    Continue,
    /// The no-progress streak hit the limit with nothing left buffered — clean finish.
    Done,
    /// The no-progress streak hit the limit with residual buffered bytes — a genuine
    /// failure, with a caller-ready message that already names which of the two possible
    /// causes (held complete frame vs. partial tail) applies.
    Fail(String),
}

/// Classify one post-EOF tick from `consume_frames`'s result plus accumulator state.
/// `frames_routed` is this tick's `consume_frames` return value; `idle_ticks` is the
/// no-progress streak *after* this tick (already incremented by the caller when
/// `frames_routed == 0`).
fn post_eof_tick(
    frames_routed: u32,
    idle_ticks: u32,
    idle_limit: u32,
    valid_len: usize,
    has_complete_frame_held: bool,
    total_frames: u32,
    full_stalls: u32,
) -> PostEofTick {
    if frames_routed > 0 {
        return PostEofTick::Progress;
    }
    if idle_ticks < idle_limit {
        return PostEofTick::Continue;
    }
    if valid_len == 0 {
        return PostEofTick::Done;
    }
    let cause = if has_complete_frame_held {
        "ring never freed a slot in 500ms — held complete frame, capture drain stalled"
    } else {
        "genuine truncated tail — partial frame, peer closed mid-write"
    };
    PostEofTick::Fail(format!(
        "tcp inbound-bp: EOF left {valid_len} undecodable bytes buffered ({cause}), \
         frames={total_frames}, full_stalls={full_stalls}",
    ))
}

/// Requires prior `WifiAssociate` and `SetTemporaryPeerConfig`.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_inbound_backpressure() -> (Status, Payload) {
    use std::time::{Duration, Instant};

    let (mut stream, peer_ip, inbound_port) =
        match connect_inbound_source(INBOUND_SELECTOR_FLOOD, "tls inbound-bp") {
            Ok(t) => t,
            Err(fail) => return fail,
        };

    let mut accum = FrameAccumulator::new();
    let mut inner_sink = build_inbound_stream_sink();
    let mut sink = StallCountingSink::new(&mut inner_sink);
    let mut inbound_state = InboundConnectionState::new();
    let mut total_frames: u32 = 0;

    // Wall-clock deadline, not an idle-retry counter: under sustained backpressure the
    // read is skipped (accumulator full) so the 2 s read timeout never fires, and
    // no-progress ticks recur on a ~10 ms yield cadence rather than the happy path's
    // 2 s-per-timeout scale.
    let deadline = Instant::now() + Duration::from_secs(device_protocol::INBOUND_BP_DEADLINE_SECS);

    // Set once the socket reports EOF. The flood's unpaced writes can outrun this
    // handler's own decode pace: the host can finish writing and close (FIN) while
    // several already-received frames still sit queued in `accum` behind a
    // backpressure-held head frame. `drain_inbound`'s EOF arm returns before calling
    // consume_frames (production semantics: an EOF with buffered bytes is normally a
    // genuine truncation), so once EOF is seen this loop switches to calling
    // `consume_frames` directly — no more socket reads — to finish routing whatever was
    // already fully received. The bounded no-progress streak below just bounds how long
    // this post-EOF drain waits for the ring to free up before giving up; a clean drain
    // leaves the accumulator empty once the streak expires. Residual bytes at that point
    // are either a held complete frame (capture-thread drain stalled, ring never freed a
    // slot) or a genuine partial trailing frame (truncated tail); `has_complete_frame_held`
    // tells them apart for the fail message below.
    let mut eof = false;
    let mut post_eof_idle_ticks: u32 = 0;
    const POST_EOF_IDLE_LIMIT: u32 = 50; // 50 × 10 ms = 500 ms of no ring movement

    loop {
        if Instant::now() >= deadline {
            log::info!(
                "TlsInboundBackpressure: deadline exceeded, frames={} full={}",
                total_frames,
                sink.full,
            );
            return test_report_fail_fmt(format_args!(
                "tls inbound-bp: {}s deadline exceeded, frames={}, full_stalls={}",
                device_protocol::INBOUND_BP_DEADLINE_SECS,
                total_frames,
                sink.full,
            ));
        }

        if eof {
            match consume_frames(&mut accum, &mut sink, &mut inbound_state) {
                Ok(n) => {
                    if n > 0 {
                        total_frames += n;
                        post_eof_idle_ticks = 0;
                    } else {
                        post_eof_idle_ticks += 1;
                    }
                    match post_eof_tick(
                        n,
                        post_eof_idle_ticks,
                        POST_EOF_IDLE_LIMIT,
                        accum.valid_len(),
                        accum.has_complete_frame_held(),
                        total_frames,
                        sink.full,
                    ) {
                        PostEofTick::Progress => {}
                        PostEofTick::Continue => {
                            esp_idf_svc::hal::delay::FreeRtos::delay_ms(10);
                        }
                        PostEofTick::Done => break,
                        PostEofTick::Fail(msg) => {
                            return test_report_fail_fmt(format_args!("{msg}"));
                        }
                    }
                }
                Err(e) => {
                    return test_report_fail_detail("tls inbound-bp post-eof decode error", &e);
                }
            }
            continue;
        }

        match drain_inbound(&mut stream, &mut accum, &mut sink, &mut inbound_state) {
            Ok(outcome) => {
                total_frames += outcome.frames_routed;
                if !outcome.made_progress() {
                    // Accumulator-full read-skip (or a quiet timeout with nothing held):
                    // yield one full FreeRTOS tick so the capture thread can drain and
                    // idle can run. A busy-spin here starves core 0's idle task and trips
                    // the Task WDT (same rationale as PLAYBACK_DRAIN_RATE_FULL_YIELD_MS).
                    esp_idf_svc::hal::delay::FreeRtos::delay_ms(10);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF — server closed after writing the whole flood. Switch to
                // post-EOF draining above rather than treating this as terminal:
                // "connection stays up" is asserted by construction, but bytes already
                // received are not necessarily decoded yet. A genuine peer RST (e.g. the
                // host aborting mid-flood on its write timeout) surfaces separately as
                // ConnectionReset and falls to the typed-fail arm below, rather than
                // being laundered through this clean-EOF path.
                eof = true;
            }
            Err(e) => {
                return test_report_fail_detail("tls inbound-bp read/decode error", &e);
            }
        }
    }

    log::info!(
        "TlsInboundBackpressure: received {} frames, full_stalls={}",
        total_frames,
        sink.full,
    );
    test_report_ok(TestData::TlsInboundBackpressure {
        inbound_frames: total_frames,
        sink_full_events: sink.full,
        peer_ip,
        peer_port: inbound_port,
    })
}

/// TLS-PSK send-backpressure self-test.
///
/// Proves the `poll(POLLOUT)` backpressure + resume path through the TLS record
/// layer on real lwIP. Opens a TLS-PSK connection to the HIL-host adversary
/// server (selected by an in-band selector byte inside the tunnel), which closes
/// its receive window and then drains, forcing a write boundary that must resume
/// to `Sent`.
///
/// Uses the production `send_frame_bp` path so the tested code is exactly the
/// path the streamer uses. The boundary frame (first send with `resume_cycles > 0`)
/// proves the write blocked with `WANT_WRITE`, waited on `poll(POLLOUT)`, and
/// completed on the same-bytes retry — the discipline the streamer depends on.
///
/// Returns `TestData::TlsSendBackpressure`.
/// Requires prior `WifiAssociate` and `SetTemporaryPeerConfig`.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_send_backpressure() -> (Status, Payload) {
    use audio_pipeline::wire::{
        AudioFrame, StreamFrame, AUDIO_SAMPLES_PER_FRAME, MAX_AUDIO_PAYLOAD,
    };
    use std::time::Duration;

    let TlsPskInputs {
        peer_ip,
        peer_port: bp_port,
        psk,
        pod_id,
    } = match tls_psk_inputs(|p| p.backpressure_port) {
        Ok(v) => v,
        Err(fail) => return fail,
    };

    let peer_addr = std::net::SocketAddr::from((peer_ip, bp_port));

    // Build a single silence Audio frame, reused for every send.
    let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
    for _ in 0..AUDIO_SAMPLES_PER_FRAME {
        let _ = pcm.push(0u8); // S16_LE silence: two zero bytes per sample
        let _ = pcm.push(0u8);
    }
    let frame = StreamFrame::Audio(AudioFrame {
        segment_id: 0,
        first_sample_index: 0,
        device_ts_us: 0,
        pcm,
    });

    // Run the saturate-then-drain adversary on a fresh TLS-PSK connection.
    let params = crate::tls_link::TlsConnectParams {
        peer: &peer_addr,
        pod_id: pod_id.as_str(),
        key: &psk,
        connect_timeout: Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
        write_timeout: Duration::from_secs(10),
    };
    let a = match run_bp_subcase(&params, &frame) {
        Ok(r) => r,
        Err(fail) => return fail,
    };

    log::info!("TlsSendBackpressure: A resumed cycles={}", a.resume_cycles,);

    test_report_ok(TestData::TlsSendBackpressure {
        a_resumed: true,
        a_rc: a.resume_cycles,
        a_ru: a.reusable,
    })
}

/// In-band selector byte written first on a backpressure connection. The host reads
/// and discards it. Duplicated here because device and host are separate crates.
#[cfg(target_os = "espidf")]
const BP_SUBCASE_A: u8 = b'A';

/// Backpressure sub-case A verdict.
#[cfg(target_os = "espidf")]
struct BpSubcaseResult {
    /// Resume cycles on the boundary frame: completed `poll(POLLOUT)` waits that
    /// were followed by forward progress. ≥1 proves the write genuinely blocked on
    /// real lwIP and resumed rather than being accepted outright.
    resume_cycles: u32,
    /// Whether a post-backpressure send succeeded on the kept socket.
    reusable: bool,
}

/// Cap on warm-up sends. The lwIP send buffer (`CONFIG_LWIP_TCP_SND_BUF_DEFAULT`,
/// 2880 B) plus the host's clamped receive window hold only a handful of ~664 B
/// TLS records, so the boundary must appear well within this bound.
#[cfg(target_os = "espidf")]
const BP_MAX_WARMUP_FRAMES: u32 = 200;

/// Drive the backpressure adversary end-to-end: open the TLS-PSK session, write
/// the selector byte inside the tunnel, send frames until the boundary frame
/// resumes to `Sent`, and confirm the socket is still usable.
///
/// The boundary frame is the first `Sent` whose `resume_cycles > 0` (from
/// `send_frame_bp_counted`) — i.e. the first frame whose write returned
/// `WANT_WRITE`, waited on `poll(POLLOUT)`, and then completed on retry. A
/// `BackpressureAligned` outcome (the wait budget elapsed with nothing written)
/// is a FAIL — the resume path was never exercised.
///
/// The session is already non-blocking (esp-tls owns the adopted socket), so no
/// explicit `set_nonblocking` is needed — it mirrors the streamer's own config.
// Err variant is the test's (Status, Payload) FAIL — propagated via `?`.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn run_bp_subcase(
    connect: &crate::tls_link::TlsConnectParams,
    frame: &audio_pipeline::wire::StreamFrame,
) -> Result<BpSubcaseResult, (Status, Payload)> {
    use std::time::{Duration, Instant};

    let mut stream = crate::tls_link::tls_connect_psk(connect).map_err(|e| {
        test_report_fail_detail("FAIL backpressure[A] tls connect/handshake failed", &e)
    })?;
    // Selector byte inside the tunnel, before any frame.
    let deadline = Instant::now() + Duration::from_secs(10);
    tls_psk_write_all(&mut stream, &[BP_SUBCASE_A], deadline)?;

    let mut encode_buf = vec![0u8; 4100];
    let mut sent_count: u32 = 0;

    for _ in 0..BP_MAX_WARMUP_FRAMES {
        let t0 = Instant::now();
        let (result, resume_cycles) = send_frame_bp_counted(&mut stream, frame, &mut encode_buf);
        match result {
            Ok(SendOutcome::Sent) => {
                sent_count += 1;
                if resume_cycles > 0 {
                    // Boundary frame: the write blocked, waited, and resumed to Sent.
                    if resume_cycles < BACKPRESSURE_A_MIN_RESUME_CYCLES {
                        return Err(test_report_fail_fmt(format_args!(
                            "FAIL backpressure[A] resumed but resume_cycles={resume_cycles} < \
                             {BACKPRESSURE_A_MIN_RESUME_CYCLES} — no WANT_WRITE/poll/retry \
                             cycle on real lwIP",
                        )));
                    }
                    let reusable = bp_confirm_reusable(&mut stream, frame, &mut encode_buf)?;
                    return Ok(BpSubcaseResult {
                        resume_cycles,
                        reusable,
                    });
                }
                // Immediately-fitting frame — keep warming up.
            }
            Ok(SendOutcome::BackpressureAligned) => {
                let wait_ms = t0.elapsed().as_millis();
                return Err(test_report_fail_fmt(format_args!(
                    "FAIL backpressure[A] aligned (written==0, wait_ms={wait_ms}) — \
                     the blocked write never became writable, so resume was not exercised",
                )));
            }
            Err(e) => {
                return Err(test_report_fail_detail(
                    "FAIL backpressure[A] fatal Err on boundary frame (expected resumed)",
                    &e,
                ));
            }
        }
    }

    // Never reached the boundary outcome within the bound — the server drained
    // everything (no withhold), so the resume path was never exercised.
    Err(test_report_fail_fmt(format_args!(
        "FAIL backpressure[A] no boundary outcome after {sent_count} sends \
         (BP_MAX_WARMUP_FRAMES={BP_MAX_WARMUP_FRAMES}) — server did not withhold reads",
    )))
}

/// Minimum resume cycles for a valid boundary frame. A cycle is a completed
/// `poll(POLLOUT)` wait followed by forward progress — over TLS, a `WANT_WRITE`
/// and the same-bytes retry that completes the record. Set to 1: proving one such
/// cycle on real lwIP is sufficient. Forcing ≥2 is unreliable on hardware (a
/// single host read typically frees enough window for everything queued);
/// multi-cycle repeatability is proven by off-target unit tests.
///
/// Must match the host-side `BACKPRESSURE_A_MIN_RESUME_CYCLES`, including its
/// `u32` type — the two crates share no constant module, so this is a manual-sync
/// contract.
#[cfg(target_os = "espidf")]
const BACKPRESSURE_A_MIN_RESUME_CYCLES: u32 = 1;

/// Confirm the socket is still usable after backpressure by retrying sends until
/// one completes or retries are exhausted. A fatal error is a FAIL.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn bp_confirm_reusable(
    stream: &mut crate::tls_link::TlsStream,
    frame: &audio_pipeline::wire::StreamFrame,
    encode_buf: &mut [u8],
) -> Result<bool, (Status, Payload)> {
    // 10 retries × the per-wait budget covers the server's drain-resume latency; the
    // host drain-to-EOF timeout (15 s) is sized to outlast this window.
    const MAX_REUSE_RETRIES: u32 = 10;
    for _ in 0..MAX_REUSE_RETRIES {
        match send_frame_bp(stream, frame, encode_buf) {
            Ok(SendOutcome::Sent) => return Ok(true),
            Ok(SendOutcome::BackpressureAligned) => {
                // Still backpressured (server not yet draining); retry.
            }
            Err(e) => {
                return Err(test_report_fail_detail(
                    "FAIL backpressure post-backpressure send error",
                    &e,
                ));
            }
        }
    }
    // Retries exhausted — socket never drained within the window.
    Err(test_report_fail_fmt(format_args!(
        "FAIL backpressure post-backpressure send did not complete in {MAX_REUSE_RETRIES} retries",
    )))
}

// ── PollReadinessBidir HIL self-test ─────────────────────────────────────

/// Bidirectional `poll()` readiness self-test.
///
/// Proves that `poll(POLLIN|POLLOUT)` reports per-direction readiness correctly
/// on this lwIP/VFS firmware — the platform fact the audio I/O event loop
/// depends on. `poll(POLLOUT)` is already proven; `poll(POLLIN)` has never been
/// exercised in any production path and is the key assertion here.
///
/// Opens a TLS-PSK connection to the HIL-host poll-readiness adversary, writes a
/// trigger byte inside the tunnel, then asserts:
///   1. `poll(POLLOUT)` reports ready on a fresh empty TX buffer.
///   2. `poll(POLLIN)` reports ready once the host queues inbound bytes as TLS
///      records.
///   3. Both bits are set in one `revents` (single-fd multiplex proof).
///
/// The poll runs against the adopted socket fd (`link_fd`), which reports
/// *ciphertext* readiness: a read after `POLLIN` can return `WouldBlock` when only
/// part of a record has landed, so poll and read share one wait budget and retry.
///
/// Returns `TestData::PollReadiness`.
/// Requires prior `WifiAssociate` and `SetTemporaryPeerConfig`.
#[cfg(target_os = "espidf")]
pub(crate) fn run_poll_readiness_bidir() -> (Status, Payload) {
    use esp_idf_svc::sys::{POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT};
    use std::io::Read as _;
    use std::time::{Duration, Instant};

    /// Trigger byte written first; the host responds by queuing inbound bytes back.
    const POLL_TRIGGER_BYTE: u8 = b'P';
    /// Per-poll timeout — generous headroom over LAN RTT.
    const POLL_TIMEOUT_MS: std::os::raw::c_int = 200;
    /// Total budget to observe POLLIN before declaring the path dead.
    const POLLIN_WAIT_BUDGET: Duration = Duration::from_secs(5);

    let TlsPskInputs {
        peer_ip,
        peer_port: poll_port,
        psk,
        pod_id,
    } = match tls_psk_inputs(|p| p.poll_readiness_port) {
        Ok(v) => v,
        Err(fail) => return fail,
    };

    let peer_addr = std::net::SocketAddr::from((peer_ip, poll_port));
    let mut stream = match crate::tls_link::tls_connect_psk(&crate::tls_link::TlsConnectParams {
        peer: &peer_addr,
        pod_id: pod_id.as_str(),
        key: &psk,
        connect_timeout: Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
        write_timeout: Duration::from_secs(10),
    }) {
        Ok(s) => s,
        Err(e) => {
            return test_report_fail_detail("FAIL poll-readiness tls connect/handshake failed", &e);
        }
    };

    // Trigger byte inside the tunnel, before the readiness polls.
    let trigger_deadline = Instant::now() + Duration::from_secs(10);
    if let Err(fail) = tls_psk_write_all(&mut stream, &[POLL_TRIGGER_BYTE], trigger_deadline) {
        return fail;
    }

    let fd = stream.link_fd();

    // ── Assertion 1: POLLOUT on a fresh, empty TX buffer ──────────────────────
    let pollout_ready = match poll_one(fd, POLLOUT, POLL_TIMEOUT_MS) {
        Ok(revents) => {
            if revents & (POLLERR | POLLHUP | POLLNVAL) != 0 {
                return test_report_fail_fmt(format_args!(
                    "FAIL poll-readiness POLLOUT poll reported socket fault (revents={revents:#x})"
                ));
            }
            revents & POLLOUT != 0
        }
        Err(e) => return test_report_fail_detail("FAIL poll-readiness POLLOUT poll() errno", &e),
    };
    if !pollout_ready {
        return test_report_fail(
            "FAIL poll-readiness POLLOUT not reported on a fresh empty TX buffer — \
             poll(POLLOUT) does not work on this lwIP/VFS build",
        );
    }

    // ── Assertion 2 + 3: POLLIN + both-direction multiplex ───────────────────
    // Ciphertext-readiness can yield WouldBlock on a healthy connection (partial TLS
    // record). Only budget exhaustion is a poll-readiness failure.
    let deadline = Instant::now() + POLLIN_WAIT_BUDGET;
    let mut rbuf = [0u8; 64];
    let (both_ready, read_bytes) = loop {
        let revents = match poll_one(fd, POLLIN | POLLOUT, POLL_TIMEOUT_MS) {
            Ok(r) => r,
            Err(e) => {
                return test_report_fail_detail("FAIL poll-readiness POLLIN poll() errno", &e);
            }
        };
        if revents & (POLLERR | POLLHUP | POLLNVAL) != 0 {
            return test_report_fail_fmt(format_args!(
                "FAIL poll-readiness POLLIN|POLLOUT poll reported socket fault \
                 (revents={revents:#x}) — peer may have closed before queuing bytes"
            ));
        }
        if revents & POLLIN != 0 {
            // The multiplex assertion: POLLIN and POLLOUT reported together in one syscall.
            let both = revents & POLLOUT != 0;
            // Confirm POLLIN is backed by actual readable plaintext.
            match stream.read(&mut rbuf) {
                Ok(0) => {
                    return test_report_fail(
                        "FAIL poll-readiness POLLIN reported ready but read returned EOF \
                         (0 bytes) — peer closed; readiness did not back real data",
                    );
                }
                Ok(n) => break (both, n),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Partial record: more ciphertext is still in flight. Fall through to
                    // the budget check and poll again.
                }
                Err(e) => {
                    return test_report_fail_detail(
                        "FAIL poll-readiness read after POLLIN failed",
                        &e,
                    );
                }
            }
        }
        if Instant::now() >= deadline {
            return test_report_fail(
                "FAIL poll-readiness no readable plaintext within budget — poll(POLLIN) never \
                 reported read-readiness backed by data on this lwIP/VFS build",
            );
        }
    };

    log::info!(
        "PollReadinessBidir: pollin=true pollout={pollout_ready} both={both_ready} \
         read_bytes={read_bytes} peer={}:{poll_port}",
        fmt_ipv4(peer_ip),
    );
    test_report_ok(TestData::PollReadiness {
        pollin: true,
        pollout: pollout_ready,
        both: both_ready,
        read_bytes: read_bytes as u32,
    })
}

// ── StreamRealtimeDuplex HIL self-test ───────────────────────────────────

/// Number of 20 ms audio frames the synthetic producer commits after the
/// pre-roll (250 × 20 ms = 5 s of real-time capture).
#[cfg(target_os = "espidf")]
const RTD_PRODUCER_FRAMES: u64 = 250;

/// Real-time frame cadence for the synthetic producer (320 samples @ 16 kHz).
#[cfg(target_os = "espidf")]
const RTD_FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Monotonic device time in microseconds (ESP-IDF high-resolution timer).
#[cfg(target_os = "espidf")]
fn now_us() -> u64 {
    // SAFETY: pure-read ESP-IDF query; no aliasing or lifetime concerns.
    unsafe { esp_idf_svc::sys::esp_timer_get_time() as u64 }
}

/// Log the current STA RSSI once at test entry so post-run heap and link behavior can
/// be correlated against signal strength. Observability only: an unavailable read logs a
/// sentinel and never affects the test outcome. Reads through the crate's lock-guarded
/// `snapshot_wifi_state` accessor so this shares the one WiFi-state read path.
#[cfg(target_os = "espidf")]
fn log_test_start_rssi() {
    match crate::wifi::snapshot_wifi_state().rssi {
        Some(rssi) => log::info!("rtd: test start rssi={rssi}"),
        None => log::warn!("rtd: test start rssi=unavailable"),
    }
}

/// Stack for the dedicated thread that runs the whole `StreamRealtimeDuplex` handler.
///
/// The extracted `run_segment` drain loop carries a per-frame `heapless::Vec` PCM
/// scratch (`MAX_AUDIO_PAYLOAD`) plus the wire codec; running the handler inline on the
/// HIL main task's 16 KB stack, already carrying the command-dispatch and `run_handler`
/// chain, overflows and faults on-device — hence the dedicated thread.
///
/// Sizing (measured 2026-07-11): the worst-case used depth is 9_980 bytes, derived from the
/// post-segment result-line `min(shwm)` (`RTD_TEST_STACK_BYTES − min(shwm)`); the run reached
/// Scenario B, so HWM monotonicity — a FreeRTOS HWM is a task-lifetime minimum — makes that
/// post-segment sample cover the inbound-Hello-decode window where the peak lives. Budget =
/// round_up_1KiB(used + 4_096) = round_up_1KiB(14_076) = 14_336.
///
/// The 4_096-byte fixed floor is the whole margin. The 9_980 value is a single post-slim cold
/// measurement; the expectation that it is stable rests on the pre-slim determinism evidence
/// (constant stack HWM across all instrumented runs, warm and cold) plus the permanent
/// post-segment stack-HWM assertion (`RTD_STACK_HWM_FLOOR`) that guards any regression. So no
/// proportional (`used/2`) transient branch is carried — the assertion, not a padding
/// multiplier, catches a depth regression.
#[cfg(target_os = "espidf")]
const RTD_TEST_STACK_BYTES: usize = 14_336;

/// Stack for the synthetic `rtd-producer` thread. Its body is a lock-write-sleep loop plus
/// one heap sample per tick — no logging, no deep call chain — so the 3072-byte FreeRTOS
/// pthread default is ample. Made explicit (not defaulted) so the per-thread footprint the
/// heap budget accounts for is visible at the spawn site.
#[cfg(target_os = "espidf")]
const RTD_PRODUCER_STACK_BYTES: usize = 3_072;

/// Per-scenario free-heap floor for the RTD run, asserted device-side against
/// `min(hla, hlb)` — the lower of the two in-window free-heap samples taken during
/// the run.
///
/// Observed `min(hla, hlb)` with duplex TLS-PSK: 28_824. The floor must not
/// exceed that (or the run fails) and must stay `>=`
/// `device_protocol::HEAP_MIN_EVER_FLOOR` (24_576) to preserve the compile-time
/// ordering below. The standard "largest 4 KiB multiple ≤ 0.75 × observed"
/// formula gives 20_480, violating the ordering, so the floor is set to the
/// invariant minimum (24_576), with ~4.2 KiB of margin.
#[cfg(target_os = "espidf")]
const RTD_HEAP_LOW_FLOOR: u32 = 24_576;

// Hardware-baked; a silent edit gets no host-test coverage (this module is
// espidf-only), so pin the literal here and re-verify the design's required
// ordering against `device_protocol::HEAP_MIN_EVER_FLOOR` at compile time. A move
// forces a deliberate re-bake with fresh provenance.
#[cfg(target_os = "espidf")]
const _: () = assert!(RTD_HEAP_LOW_FLOOR == 24_576);
#[cfg(target_os = "espidf")]
const _: () = assert!(device_protocol::HEAP_MIN_EVER_FLOOR <= RTD_HEAP_LOW_FLOOR);

/// Near-overflow floor for the rtd-test thread's own stack HWM (bytes), mirroring
/// `device_protocol::STACK_HWM_FLOOR`, sampled device-side after each segment and asserted once after
/// both scenarios complete (the min-accumulator carries a self-healed Scenario-A breach into
/// the final check). FreeRTOS HWM is a task-lifetime minimum, so a post-segment sample
/// necessarily covers the inbound-decode window inside the segment. With the `used + 4_096` budget the expected remaining margin
/// at the deterministic peak is ~4 KB, so 1_024 trips only after a >3 KB depth regression —
/// a real signal, not noise.
#[cfg(target_os = "espidf")]
const RTD_STACK_HWM_FLOOR: u32 = 1_024;

/// Per-scenario resource measurements returned by `rtd_run_one_segment`.
#[derive(Clone, Copy, Default)]
#[cfg(target_os = "espidf")]
struct SegMeas {
    /// Minimum internal-RAM free heap (`heap_caps_get_free_size(MALLOC_CAP_INTERNAL)`)
    /// observed across the producer's real-time window (the residual-drain/`SegmentEnd`
    /// tail — which allocates nothing new — is not sampled).
    heap_low: u32,
    /// TCB/pthread bookkeeping overhead of the `rtd-producer` spawn: the spawn heap delta
    /// minus the 3072-byte stack. Approximate — concurrent lwIP noise is not excluded.
    producer_tcb: u32,
    /// The `rtd-producer` thread's own stack high-water mark (bytes remaining), sampled just
    /// before it exits. Observability for re-deriving `RTD_PRODUCER_STACK_BYTES` if a
    /// producer-thread overflow is ever implicated.
    producer_hwm: u32,
}

/// Handler-wide resource measurements, accumulated across both scenarios. Every RTD result
/// line — PASS and every FAIL variant — carries the fields collected so far, so the
/// (expected-to-fail) measurement run still yields the numbers it exists to produce.
#[derive(Clone, Copy)]
#[cfg(target_os = "espidf")]
struct RtdMeasure {
    min_heap_after: u32,
    rtd_stack_hwm: u32,
    heap_low_a: u32,
    heap_low_b: u32,
    producer_tcb: u32,
    producer_hwm: u32,
}

/// Map the `u32::MAX` "never sampled" sentinel (used by the stack-HWM and heap-low fields)
/// to 0 for reporting.
#[cfg(target_os = "espidf")]
fn sampled_or_zero(v: u32) -> u32 {
    if v == u32::MAX {
        0
    } else {
        v
    }
}

/// Full-heap integrity walk over every region; `true` = clean. On corruption ESP-IDF dumps
/// the offending block to the console (`print_errors=true`).
#[cfg(target_os = "espidf")]
fn heap_integrity_ok() -> bool {
    // SAFETY: pure-read integrity walk over every heap region.
    unsafe { esp_idf_svc::sys::heap_caps_check_integrity_all(true) }
}

#[cfg(target_os = "espidf")]
impl RtdMeasure {
    fn new() -> Self {
        RtdMeasure {
            min_heap_after: 0,
            rtd_stack_hwm: u32::MAX,
            heap_low_a: 0,
            heap_low_b: 0,
            producer_tcb: 0,
            producer_hwm: u32::MAX,
        }
    }

    /// Sample the boot-wide minimum free heap and the rtd-test thread's own stack HWM
    /// (keeping the smaller HWM across scenarios). Called immediately after each
    /// `rtd_run_one_segment` return, before any exit-status match, so a Scenario A failure
    /// still reports them.
    fn sample_after_segment(&mut self) {
        // Boot-wide minimum-ever free heap, scoped to internal RAM so `mh_post` and the
        // `HEAP_MIN_EVER_FLOOR` check it feeds measure the internal pool the floor was
        // derived under, not the PSRAM-inflated whole-heap total.
        self.min_heap_after = crate::health::heap_free_min().1;
        // SAFETY: pure-read FreeRTOS query; NULL handle queries the calling (rtd-test) task.
        let hwm = unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(core::ptr::null_mut()) };
        self.rtd_stack_hwm = self.rtd_stack_hwm.min(hwm);
    }

    /// The sampled stack HWM, mapping the unsampled sentinel to 0.
    fn stack_hwm(&self) -> u32 {
        sampled_or_zero(self.rtd_stack_hwm)
    }

    /// The sampled `rtd-producer` stack HWM, mapping the unsampled sentinel to 0.
    fn producer_hwm(&self) -> u32 {
        sampled_or_zero(self.producer_hwm)
    }

    /// The trailing measurement fields appended to every result line. Compact token names
    /// keep the whole line under the `TestReport` detail (`TestResultMsg`) budget: `mh_post` =
    /// min-heap-ever after the run (the boot-wide-minimum assertion), `hla`/`hlb` =
    /// per-scenario free-heap low, `shwm` = rtd-test stack HWM, `ptcb` = rtd-producer
    /// TCB/pthread overhead, `phwm` = rtd-producer stack HWM. Assertion-critical fields lead so
    /// a graceful truncation on a FAIL line drops observability, not a floor check's value;
    /// `phwm` (pure observability) is last so it is the first token dropped. Per-scenario wall
    /// times are host-observed (obs.catch_up_ms) and are not repeated here.
    fn suffix(&self) -> String {
        format!(
            "mh_post={} hla={} hlb={} shwm={} ptcb={} phwm={}",
            self.min_heap_after,
            self.heap_low_a,
            self.heap_low_b,
            self.stack_hwm(),
            self.producer_tcb,
            self.producer_hwm(),
        )
    }

    /// Build a `FAIL src=rtd <detail> <fields>` result carrying all fields collected so far.
    fn fail(&self, detail: core::fmt::Arguments) -> (Status, Payload) {
        test_report_fail_fmt(format_args!("FAIL src=rtd {detail} {}", self.suffix()))
    }
}

/// Streamer real-time duplex drain self-test (Scenario A — outbound catch-up).
///
/// Drives the extracted `run_segment` drain loop against a test-owned capture ring
/// fed by a synthetic real-time producer, streaming a full segment to the HIL-host
/// `StreamRealtimeDuplex` listener. The host times the pre-roll burst drain and the
/// catch-up wall clock and owns the throughput assertions (device→host observation);
/// this handler reports only the loop exit and its own wall time.
///
/// Per CLAUDE.md bring-up doctrine the host asserts the expected keep-up behavior and
/// is allowed to FAIL first against the current one-action-per-wake loop. Requires
/// prior `WifiAssociate` and `SetTemporaryPeerConfig` (session `rtd_port`).
///
/// Runs the handler body on a dedicated large-stack thread (see `RTD_TEST_STACK_BYTES`);
/// the main task's stack is too small for the `run_segment` frame.
#[cfg(target_os = "espidf")]
pub(crate) fn run_stream_realtime_duplex() -> (Status, Payload) {
    log_test_start_rssi();
    // SAFETY: pure-read ESP-IDF query.
    let heap_before = unsafe { esp_idf_svc::sys::esp_get_free_heap_size() };
    let spawn = std::thread::Builder::new()
        .name("rtd-test".to_string())
        .stack_size(RTD_TEST_STACK_BYTES)
        .spawn(run_stream_realtime_duplex_inner);
    // SAFETY: pure-read ESP-IDF query. This spawn delta is only an upper bound on the
    // rtd-test TCB/stack overhead — the inner body begins allocating (NVS open, scratch) as
    // soon as it is scheduled, racing this sample; after_join confirms full release.
    let heap_after_spawn = unsafe { esp_idf_svc::sys::esp_get_free_heap_size() };
    let result = match spawn {
        Ok(handle) => handle
            .join()
            .unwrap_or_else(|_| test_report_fail("FAIL src=rtd test thread panicked")),
        Err(e) => test_report_fail_detail("rtd test thread spawn failed", &e),
    };
    // SAFETY: pure-read ESP-IDF query.
    let heap_after_join = unsafe { esp_idf_svc::sys::esp_get_free_heap_size() };
    log::info!(
        "rtd-test thread heap: before_spawn={heap_before} after_spawn={heap_after_spawn} \
         after_join={heap_after_join} (spawn delta is an upper bound; after_join confirms release)"
    );
    result
}

#[cfg(target_os = "espidf")]
fn run_stream_realtime_duplex_inner() -> (Status, Payload) {
    use crate::inbound::CountingSink;
    use crate::streamer::{SegmentExit, SEGMENT_ACTIVE};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let TlsPskInputs {
        peer_ip,
        peer_port: rtd_port,
        psk,
        pod_id,
    } = match tls_psk_inputs(|p| p.rtd_port) {
        Ok(v) => v,
        Err(fail) => return fail,
    };

    // Quiesce the production capture pathway and borrow CAPTURE_RING: the capture
    // thread stops committing mic chunks and the telemetry thread feeds the VAD
    // silence while this guard is held. It is held across both scenarios and drops
    // only after each scenario's producer thread has been joined (inside the helper).
    let _quiesce = crate::capture::CaptureQuiesceGuard::new();

    // Barrier: wait for any in-flight production segment to tear down before the test
    // writes the ring. SEGMENT_ACTIVE must read false continuously for >=100 ms
    // (absorbing a VadOpened already queued when the flag went up) within a 10 s
    // deadline (800 ms hangover + segment drain + margin).
    {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut quiet_since: Option<Instant> = None;
        loop {
            let now = Instant::now();
            if SEGMENT_ACTIVE.load(Ordering::Acquire) {
                quiet_since = None;
            } else {
                let since = *quiet_since.get_or_insert(now);
                if now.duration_since(since) >= Duration::from_millis(100) {
                    break;
                }
            }
            if now >= deadline {
                return test_report_fail(
                    "FAIL src=rtd production segment did not quiesce within 10 s",
                );
            }
            esp_idf_svc::hal::delay::FreeRtos::delay_ms(10);
        }
    }

    let mut meas = RtdMeasure::new();

    // Shared scratch/accumulator/handshake state, allocated once and reused across both
    // scenarios. Each scenario opens its own TCP connection; the helper resets the
    // per-connection state at entry.
    let mut scratch = vec![0u8; audio_pipeline::wire::MAX_FRAME_BYTES + 2];
    let mut inbound_accum = FrameAccumulator::new();
    let mut inbound_state = InboundConnectionState::new();

    // Heap-integrity tripwire bracketing the scenarios (before A / between A and B / after B):
    // a clean sequence time-brackets any corruption to a single scenario's window and catches
    // quiet corruption on an otherwise-passing run. Permanent RTD assertions.
    if !heap_integrity_ok() {
        return meas.fail(format_args!(
            "heap integrity check failed before scenario A"
        ));
    }

    // Scenario A — outbound catch-up with no inbound co-traffic: an idle counting sink
    // (the host reads only, never paces playback on this connection).
    let mut sink_a = CountingSink::new();
    let a_seg = rtd_run_one_segment(
        &RtdConnect {
            peer_ip,
            rtd_port,
            psk: &psk,
            pod_id: pod_id.as_str(),
        },
        'A',
        &mut RtdSegmentIo {
            sink: &mut sink_a,
            scratch: &mut scratch,
            accum: &mut inbound_accum,
            state: &mut inbound_state,
        },
    );
    meas.sample_after_segment();
    // Heap state at the A/B boundary as a standalone serial log line, so it survives even
    // if Scenario B panics moments later (the assertion suffix would not be emitted then).
    let (free, min) = crate::health::heap_free_min();
    log::info!("rtd: heap waypoint after-A heap_free={free} min_heap={min}");
    let (a_exit, a_wall, a_meas) = match a_seg {
        Ok(v) => v,
        Err(reason) => return meas.fail(format_args!("scenario=A {reason}")),
    };
    meas.heap_low_a = a_meas.heap_low;
    meas.producer_tcb = a_meas.producer_tcb;
    meas.producer_hwm = meas.producer_hwm.min(a_meas.producer_hwm);
    if !matches!(a_exit, SegmentExit::Completed) {
        return meas.fail(format_args!("scenario=A exit={a_exit:?} wall_ms={a_wall}"));
    }

    if !heap_integrity_ok() {
        return meas.fail(format_args!(
            "heap integrity check failed between scenarios A and B"
        ));
    }

    // Scenario B — duplex under paced-playback backpressure: the fake-DAC sink models a
    // real speaker's real-time playout while the host paces inbound playback frames on the
    // same connection. Zero fake-DAC underruns is the field-symptom keep-up assertion.
    // Heap state at Scenario B entry as a standalone serial log line, so the immediately
    // pre-B heap headroom survives even if Scenario B panics before the assertion suffix.
    let (free, min) = crate::health::heap_free_min();
    log::info!("rtd: heap waypoint before-B heap_free={free} min_heap={min}");
    let mut sink_b = crate::inbound::FakeDacSink::new();
    let b_seg = rtd_run_one_segment(
        &RtdConnect {
            peer_ip,
            rtd_port,
            psk: &psk,
            pod_id: pod_id.as_str(),
        },
        'B',
        &mut RtdSegmentIo {
            sink: &mut sink_b,
            scratch: &mut scratch,
            accum: &mut inbound_accum,
            state: &mut inbound_state,
        },
    );
    meas.sample_after_segment();

    let (b_exit, b_wall, b_meas) = match b_seg {
        Ok(v) => v,
        Err(reason) => return meas.fail(format_args!("scenario=B {reason}")),
    };
    meas.heap_low_b = b_meas.heap_low;
    meas.producer_hwm = meas.producer_hwm.min(b_meas.producer_hwm);
    if !matches!(b_exit, SegmentExit::Completed) {
        return meas.fail(format_args!("scenario=B exit={b_exit:?} wall_ms={b_wall}"));
    }

    if !heap_integrity_ok() {
        return meas.fail(format_args!("heap integrity check failed after scenario B"));
    }

    // Heap/stack assertions, ordered after a successful Scenario B (all fields already
    // sampled above, so a Scenario A/B failure above still carried them). These make the RTD
    // test itself the permanent Defect-1 regression guard. The assertions presume a fresh
    // boot: a boot whose watermark already sits below the floor fails here by design (and
    // would have failed DeviceHealthCheck earlier in the suite). heap_low covers the
    // production window only; min_heap_after bounds the true boot-wide minimum regardless.
    if meas.min_heap_after < device_protocol::HEAP_MIN_EVER_FLOOR {
        return meas.fail(format_args!(
            "min_heap_after={}<{}",
            meas.min_heap_after,
            device_protocol::HEAP_MIN_EVER_FLOOR
        ));
    }
    let heap_low = meas.heap_low_a.min(meas.heap_low_b);
    if heap_low < RTD_HEAP_LOW_FLOOR {
        return meas.fail(format_args!("heap_low={heap_low}<{RTD_HEAP_LOW_FLOOR}"));
    }
    if meas.stack_hwm() < RTD_STACK_HWM_FLOOR {
        return meas.fail(format_args!(
            "rtd_stack_hwm={}<{RTD_STACK_HWM_FLOOR}",
            meas.stack_hwm()
        ));
    }

    test_report_ok_detail(
        TestData::Rtd {
            underruns: u64::from(sink_b.underruns()),
            gap_ms: sink_b.total_gap_ms(),
            consumed: u64::from(sink_b.consumed()),
        },
        format_args!("src=rtd {}", meas.suffix()),
    )
}

/// Caller-owned per-run I/O state shared across both RTD scenarios: the playback sink,
/// the reused encode scratch, and the per-connection inbound reassembly and framing
/// state. Bundled behind one `&mut` so every incoming argument word of
/// `rtd_run_one_segment` rides in a register.
///
/// TODO(xtensa-realign-stack-args): this bundling is load-bearing, not style.
/// `rtd_run_one_segment` must keep <= 6 incoming argument words (all in registers) while
/// its body holds an align-64 stack temporary (the `mpsc` channel), or the stock esp
/// Xtensa backend miscompiles it (mechanism in the slug entry).
/// `firmware/tools/check-realign-args.sh` enforces the invariant image-wide on every
/// `make check-realign` / `make flash`. Do not unbundle or add argument words until the
/// upstream fix is released and the gate retired.
#[cfg(target_os = "espidf")]
struct RtdSegmentIo<'a> {
    sink: &'a mut dyn audio_pipeline::playback::PlaybackSink,
    scratch: &'a mut Vec<u8>,
    accum: &'a mut crate::inbound::FrameAccumulator,
    state: &'a mut crate::inbound::InboundConnectionState,
}

/// The per-connection TLS-PSK dial inputs for one RTD segment, bundled to stay
/// inside the Xtensa realign-miscompile guard's argument-word budget.
#[cfg(target_os = "espidf")]
struct RtdConnect<'a> {
    /// RTD listener host.
    peer_ip: [u8; 4],
    /// RTD listener port.
    rtd_port: u16,
    /// The session audio-link key.
    psk: &'a [u8; crate::tls_link::PSK_LEN],
    /// This pod's id — the TLS PSK identity.
    pod_id: &'a str,
}

/// Run one `StreamRealtimeDuplex` segment against the rtd listener: borrow the
/// boot-allocated `CAPTURE_RING` (the caller has quiesced production), pre-roll it,
/// open a TLS-PSK session and send the outbound `Hello`/`SegmentStart` via poll-driven
/// backpressure, then drive the extracted `run_segment` drain loop against a synthetic
/// real-time producer, feeding decoded inbound frames to `io.sink`. Returns the loop exit
/// and the `SegmentStart`→exit wall time on success.
///
/// The `io` bundle (sink/scratch/accum/state) is owned by the caller and reused across
/// scenarios; the per-connection framing state is reset at entry. The producer commits an
/// exact frame count so the host's received-sample total is deterministic (integrity
/// assertion). Both scenarios share this shape; only the sink and whether the host paces
/// inbound playback differ.
#[cfg(target_os = "espidf")]
fn rtd_run_one_segment(
    conn: &RtdConnect<'_>,
    scenario: char,
    io: &mut RtdSegmentIo<'_>,
) -> Result<(crate::streamer::SegmentExit, u128, SegMeas), String> {
    use crate::capture::CAPTURE_RING;
    use crate::streamer::{run_segment, SegmentDeps, StreamerMsg};
    use audio_pipeline::ring::{RingIndex, PREROLL_SAMPLES, RING_CAPACITY_SAMPLES};
    use audio_pipeline::wire::{
        ChannelSource, Hello, SegmentStart, StreamFrame, AUDIO_PROTOCOL_VERSION,
        AUDIO_SAMPLES_PER_FRAME,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::{Duration, Instant};

    let peer_ip = conn.peer_ip;
    let rtd_port = conn.rtd_port;

    // Fresh per-connection inbound state — each scenario opens its own TLS session.
    io.state.reset();
    io.accum.reset();

    let frame_samples = AUDIO_SAMPLES_PER_FRAME as u64;
    let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);

    // Borrow the boot-allocated CAPTURE_RING (no per-scenario allocation). Pre-fill
    // PREROLL_SAMPLES of synthetic history from the live head so the segment opens with a
    // full pre-roll burst; write_head is never reset (it is a cross-thread monotone
    // invariant and the host asserts sample counts, not absolute indices).
    let (read_cursor, preroll_count) = {
        let mut guard = CAPTURE_RING.lock().expect("CAPTURE_RING mutex poisoned");
        let r = guard.as_mut().expect("CAPTURE_RING not initialized");
        let onset_write_head = r.write_head + PREROLL_SAMPLES;
        for _ in 0..PREROLL_SAMPLES {
            let slot = ridx.slot(r.write_head);
            r.samples[slot] = (r.write_head % 4096) as i16;
            r.write_head += 1;
        }
        r.anchor_sample = r.write_head.saturating_sub(1);
        r.anchor_ts_us = now_us();
        let cursor = ridx.preroll_cursor(onset_write_head, PREROLL_SAMPLES);
        (cursor, onset_write_head.saturating_sub(cursor) as u32)
    };

    // Open the TLS-PSK session, then send the outbound Hello + SegmentStart through
    // the tunnel (buffer empty at onset). The adopted socket is non-blocking from the
    // handshake on, so `send_frame_bp`'s poll-driven backpressure carries the onset
    // sends rather than a blocking write.
    let peer_addr = std::net::SocketAddr::from((peer_ip, rtd_port));
    // Prove per-run which port the device dialed from the session config, and for which
    // scenario, so a misrouted connection is a transcript fact rather than an inference.
    log::info!(
        "rtd: connecting to {}.{}.{}.{}:{} (scenario {})",
        peer_ip[0],
        peer_ip[1],
        peer_ip[2],
        peer_ip[3],
        rtd_port,
        scenario
    );
    let mut socket = match crate::tls_link::tls_connect_psk(&crate::tls_link::TlsConnectParams {
        peer: &peer_addr,
        pod_id: conn.pod_id,
        key: conn.psk,
        connect_timeout: Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
        write_timeout: Duration::from_secs(10),
    }) {
        Ok(s) => s,
        Err(e) => return Err(format!("tls connect/handshake failed: {e:?}")),
    };

    let hello = StreamFrame::Hello(Hello {
        version: AUDIO_PROTOCOL_VERSION,
        pod_id: heapless::String::new(),
        sample_rate_hz: crate::DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
        bits_per_sample: crate::DEVICE_PLAYBACK_FORMAT.bits_per_sample,
        channels: crate::DEVICE_PLAYBACK_FORMAT.channels,
        codec: crate::DEVICE_PLAYBACK_FORMAT.codec,
        channel_source: ChannelSource::CommunicationBeam,
    });
    rtd_send_blocking(&mut socket, &hello, &mut *io.scratch, "Hello")?;
    let seg_start = StreamFrame::SegmentStart(SegmentStart {
        segment_id: 0,
        base_sample_index: read_cursor,
        base_device_ts_us: now_us(),
        preroll_samples: preroll_count,
    });
    rtd_send_blocking(&mut socket, &seg_start, &mut *io.scratch, "SegmentStart")?;

    // Synthetic real-time producer: commit synthetic 320-sample frames on an absolute
    // 20 ms schedule directly into CAPTURE_RING, then raise the vad-closed flag. The exact
    // frame count makes the received-sample total deterministic. Each wake also samples
    // free heap and keeps the minimum across the production window (the tail after the last
    // frame allocates nothing).
    //
    // TCB/pthread overhead of this spawn is the free-heap delta across the `spawn()` call
    // minus the 3072-byte stack; the producer's first action is a 20 ms sleep, so the
    // after-sample is clean of its own loop allocations (subject to concurrent lwIP noise).
    let vad_closed_flag = Arc::new(AtomicBool::new(false));
    let producer_flag = Arc::clone(&vad_closed_flag);
    // SAFETY: pure-read ESP-IDF query, immediately before the spawn() call.
    let heap_before_spawn = unsafe { esp_idf_svc::sys::esp_get_free_heap_size() };
    let producer = std::thread::Builder::new()
        .name("rtd-producer".to_string())
        .stack_size(RTD_PRODUCER_STACK_BYTES)
        .spawn(move || {
            let mut heap_low = u32::MAX;
            // Absolute-schedule producer: each frame's deadline is anchored to the
            // producer's start (deadline_k = start + k*interval), and each wake commits
            // every frame due from elapsed time. A sleep that rounds up to the 10 ms
            // FreeRTOS tick, or a heavy wake, is absorbed by the next absolute deadline
            // instead of accumulating, so all RTD_PRODUCER_FRAMES frames complete in
            // frames × RTD_FRAME_INTERVAL of wall time.
            let frame_interval_us = RTD_FRAME_INTERVAL.as_micros() as u64;
            let start_us = now_us();
            let mut frames_committed: u64 = 0;
            while frames_committed < RTD_PRODUCER_FRAMES {
                let wake_us = now_us();
                let next_deadline_us = start_us + (frames_committed + 1) * frame_interval_us;
                if next_deadline_us > wake_us {
                    std::thread::sleep(std::time::Duration::from_micros(
                        next_deadline_us - wake_us,
                    ));
                }
                // Internal-RAM free heap so `hla`/`hlb` and the `RTD_HEAP_LOW_FLOOR` check
                // measure the internal pool, not the PSRAM-inflated whole-heap total.
                // SAFETY: pure-read ESP-IDF heap-registry query with no side effects.
                let free = unsafe {
                    esp_idf_svc::sys::heap_caps_get_free_size(esp_idf_svc::sys::MALLOC_CAP_INTERNAL)
                        as u32
                };
                heap_low = heap_low.min(free);
                let ts = now_us();
                // Frames due from elapsed time, capped at the total and floored at one
                // past the last commit so every wake makes progress; the shortfall
                // (frames_due - frames_committed) is committed to catch up any drift.
                let frames_due = audio_pipeline::pace::absolute_frames_due(
                    ts,
                    start_us,
                    frame_interval_us,
                    frames_committed,
                    RTD_PRODUCER_FRAMES,
                );
                let mut guard = CAPTURE_RING.lock().expect("CAPTURE_RING mutex poisoned");
                let r = guard.as_mut().expect("CAPTURE_RING not initialized");
                while frames_committed < frames_due {
                    for _ in 0..frame_samples {
                        let slot = ridx.slot(r.write_head);
                        r.samples[slot] = (r.write_head % 4096) as i16;
                        r.write_head += 1;
                    }
                    frames_committed += 1;
                }
                r.anchor_sample = r.write_head.saturating_sub(1);
                r.anchor_ts_us = ts;
            }
            producer_flag.store(true, Ordering::Release);
            // Sample this thread's own stack HWM (bytes remaining) just before exiting, so an
            // under-budgeted producer stack is quantifiable rather than only a silent trap.
            // SAFETY: pure-read FreeRTOS query; NULL handle queries the calling task.
            let producer_hwm =
                unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(core::ptr::null_mut()) };
            (heap_low, producer_hwm)
        });
    let producer = match producer {
        Ok(h) => h,
        Err(e) => return Err(format!("producer spawn failed: {e:?}")),
    };
    // SAFETY: pure-read ESP-IDF query.
    let heap_after_spawn = unsafe { esp_idf_svc::sys::esp_get_free_heap_size() };
    let producer_tcb = heap_before_spawn
        .saturating_sub(heap_after_spawn)
        .saturating_sub(RTD_PRODUCER_STACK_BYTES as u32);

    let (_tx, rx) = mpsc::channel::<StreamerMsg>();

    let started = Instant::now();
    let exit = {
        let mut deps = SegmentDeps {
            socket: &mut socket,
            rx: &rx,
            ring: &CAPTURE_RING,
            vad_closed_flag: &vad_closed_flag,
            ridx: &ridx,
            inbound_accum: &mut *io.accum,
            inbound_sink: &mut *io.sink,
            inbound_state: &mut *io.state,
            outbound_buf: &mut *io.scratch,
        };
        run_segment(&mut deps, 0, read_cursor)
    };
    let wall_ms = started.elapsed().as_millis();
    // A panicked producer must not masquerade as an unsampled heap: `unwrap_or` would
    // substitute `u32::MAX`, which `sampled_or_zero` then collapses to 0 — the exact
    // "never sampled" encoding — hiding a mid-run crash. Surface the panic instead.
    let (heap_low, producer_hwm) = match producer.join() {
        Ok(v) => v,
        Err(e) => return Err(format!("producer thread panicked: {e:?}")),
    };
    let heap_low = sampled_or_zero(heap_low);
    Ok((
        exit,
        wall_ms,
        SegMeas {
            heap_low,
            producer_tcb,
            producer_hwm,
        },
    ))
}

/// Send one onset frame through the tunnel with `send_frame_bp`'s poll-driven
/// backpressure, mapping any error to a bare failure reason (the caller wraps it
/// with the measurement fields). At onset the send buffer is empty, so a
/// `BackpressureAligned` outcome is a fault.
#[cfg(target_os = "espidf")]
fn rtd_send_blocking(
    socket: &mut crate::tls_link::TlsStream,
    frame: &audio_pipeline::wire::StreamFrame,
    encode_buf: &mut [u8],
    what: &str,
) -> Result<(), String> {
    match send_frame_bp(socket, frame, encode_buf) {
        Ok(SendOutcome::Sent) => Ok(()),
        Ok(SendOutcome::BackpressureAligned) => Err(format!(
            "{what} backpressured at onset (buffer should be empty)"
        )),
        Err(e) => Err(format!("onset send {what} failed: {e:?}")),
    }
}

// ── TLS-PSK audio-link self-tests ─────────────────────────────────────────────

/// Payload echoed through the tunnel by `TlsPskHandshake`.
#[cfg(target_os = "espidf")]
const TLS_PSK_ECHO_NONCE: [u8; 16] = *b"pod-tls-psk-echo";

/// Everything one TLS-PSK self-test needs to open its session.
#[cfg(target_os = "espidf")]
struct TlsPskInputs {
    /// HIL host address, shared by both listeners.
    peer_ip: [u8; 4],
    /// Port of the listener this test connects to.
    peer_port: u16,
    /// The provisioned audio-link key.
    psk: [u8; crate::tls_link::PSK_LEN],
    /// This pod's id, which is also the PSK identity.
    pod_id: heapless::String<32>,
}

/// Gather the peer endpoint, audio-link key, and pod identity for a TLS-PSK
/// self-test. `port` selects which listener port to read from the session peer
/// config.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn tls_psk_inputs(
    port: impl Fn(&crate::hil_session::PeerConfig) -> u16,
) -> Result<TlsPskInputs, (Status, Payload)> {
    let peer = crate::hil_session::peer_config().ok_or_else(|| {
        test_report_fail("no session peer config — run SetTemporaryPeerConfig first")
    })?;
    let peer_port = port(&peer);
    let psk = match crate::hil_session::audio_psk_override() {
        Some(k) => k,
        None => {
            let nvs = open_wifi_nvs(false).map_err(test_report_fail_msg)?;
            crate::hil_session::effective_audio_psk(&nvs).map_err(test_report_fail_msg)?
        }
    };

    let Some(pod_id) = crate::streamer::pod_id_snapshot() else {
        return Err(test_report_fail(
            "pod identity not yet initialized — run WifiAssociate first",
        ));
    };
    Ok(TlsPskInputs {
        peer_ip: peer.host,
        peer_port,
        psk,
        pod_id,
    })
}

/// Wait for the tunnel to be ready in `direction` (a `poll()` event mask), or
/// report the fault/timeout as a failed report.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn tls_psk_wait(
    stream: &crate::tls_link::TlsStream,
    readable: bool,
    deadline: std::time::Instant,
) -> Result<(), (Status, Payload)> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let timeout_ms =
        remaining.as_millis().min(std::os::raw::c_int::MAX as u128) as std::os::raw::c_int;
    let events = stream.poll_events(readable, !readable);
    match crate::netpoll::poll_readiness(stream.link_fd(), events, timeout_ms) {
        crate::netpoll::Readiness::Fault(e) => {
            Err(test_report_fail_detail("tls-psk socket fault", &e))
        }
        crate::netpoll::Readiness::TimedOut | crate::netpoll::Readiness::Ready { .. } => Ok(()),
    }
}

/// Write every byte of `buf` through the tunnel under `deadline`.
///
/// Poll discipline rule 2: a `WouldBlock` retry re-presents the same unsent
/// slice, never a differently-sliced buffer.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn tls_psk_write_all(
    stream: &mut crate::tls_link::TlsStream,
    buf: &[u8],
    deadline: std::time::Instant,
) -> Result<(), (Status, Payload)> {
    use std::io::Write as _;
    let mut sent = 0usize;
    while sent < buf.len() {
        match stream.write(&buf[sent..]) {
            Ok(0) => return Err(test_report_fail("tls-psk write made no progress")),
            Ok(n) => sent += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tls_psk_wait(stream, false, deadline)?
            }
            Err(e) => return Err(test_report_fail_detail("tls-psk write failed", &e)),
        }
        if sent < buf.len() && std::time::Instant::now() >= deadline {
            return Err(test_report_fail_fmt(format_args!(
                "tls-psk write timed out after {sent}/{} bytes",
                buf.len()
            )));
        }
    }
    Ok(())
}

/// Fill `buf` from the tunnel under `deadline`.
///
/// Poll discipline rule 1: reads are attempted until `WouldBlock`, because
/// decrypted plaintext can sit in the session buffer with no `POLLIN` to reveal
/// it.
#[allow(clippy::result_large_err)]
#[cfg(target_os = "espidf")]
fn tls_psk_read_exact(
    stream: &mut crate::tls_link::TlsStream,
    buf: &mut [u8],
    deadline: std::time::Instant,
) -> Result<(), (Status, Payload)> {
    use std::io::Read as _;
    let want = buf.len();
    let mut got = 0usize;
    while got < want {
        match stream.read(&mut buf[got..]) {
            Ok(0) => {
                return Err(test_report_fail_fmt(format_args!(
                    "tls-psk peer closed after {got}/{want} echoed bytes"
                )));
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                tls_psk_wait(stream, true, deadline)?
            }
            Err(e) => return Err(test_report_fail_detail("tls-psk read failed", &e)),
        }
        if got < want && std::time::Instant::now() >= deadline {
            return Err(test_report_fail_fmt(format_args!(
                "tls-psk read timed out after {got}/{want} bytes"
            )));
        }
    }
    Ok(())
}

/// TLS-PSK handshake proof over the production audio-link client.
///
/// Connects to the HIL host's TLS-PSK listener with `tls_connect_psk` — the same
/// call the streamer makes — using the effective audio-link key (the session
/// override set by `SetTemporaryAudioPsk`, else the NVS `audio_psk`) and this
/// pod's id as the PSK identity, then echoes one payload through the tunnel.
/// Reports the negotiated version and ciphersuite so the host asserts them
/// against the pinned expectation. The reported `handshake_ms` and the echo
/// deadline are both charged from after the TCP connect, so a connect that spent
/// its budget on SYN retransmits fails as a connect, not as a slow handshake or
/// an expired echo.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_psk_handshake() -> (Status, Payload) {
    use device_protocol::{TlsSuiteStr, TlsVersionStr};

    let TlsPskInputs {
        peer_ip,
        peer_port,
        psk,
        pod_id,
    } = match tls_psk_inputs(|p| p.tls_psk_port) {
        Ok(v) => v,
        Err(report) => return report,
    };
    let peer = std::net::SocketAddr::from((peer_ip, peer_port));

    let opened = match crate::tls_link::tls_connect_psk_staged(&crate::tls_link::TlsConnectParams {
        peer: &peer,
        pod_id: pod_id.as_str(),
        key: &psk,
        connect_timeout: std::time::Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
        write_timeout: std::time::Duration::from_secs(2),
    }) {
        Ok(c) => c,
        Err(f) => {
            return test_report_fail_fmt(format_args!(
                "tls-psk {} failed after {} ms: {:?}",
                f.stage.label(),
                f.elapsed.as_millis(),
                f.error
            ));
        }
    };
    let mut stream = opened.stream;
    let handshake_ms = opened.handshake.as_millis().min(u32::MAX as u128) as u32;
    log::info!(
        "tls-psk: handshake with {peer} took {handshake_ms} ms after a {} ms connect",
        opened.connect.as_millis()
    );

    let (version, ciphersuite) = {
        let (v, c) = stream.negotiated();
        let mut vs = TlsVersionStr::new();
        let _ = vs.push_str(v);
        let mut cs = TlsSuiteStr::new();
        let _ = cs.push_str(c);
        (vs, cs)
    };

    // Anchored after the handshake: the echo budget bounds the round-trip, and a
    // connect that spent its own budget on SYN retransmits must not consume it.
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(TLS_PSK_ECHO_TIMEOUT_SECS);
    if let Err(report) = tls_psk_write_all(&mut stream, &TLS_PSK_ECHO_NONCE, deadline) {
        return report;
    }
    let mut reply = [0u8; TLS_PSK_ECHO_NONCE.len()];
    if let Err(report) = tls_psk_read_exact(&mut stream, &mut reply, deadline) {
        return report;
    }
    if reply != TLS_PSK_ECHO_NONCE {
        return test_report_fail("tls-psk echo mismatch through the tunnel");
    }

    test_report_ok(TestData::TlsPskHandshake {
        peer_ip,
        peer_port,
        handshake_ms,
        version,
        ciphersuite,
        echo_bytes: TLS_PSK_ECHO_NONCE.len() as u32,
    })
}

/// TLS-PSK identity-negative self-test: the wrong key must not open the tunnel.
///
/// Connects to the listener that holds a *different* key for this pod's identity
/// and asserts the handshake fails. Nothing is ever written, so no application
/// byte can cross a link that was not authenticated; a completed handshake here
/// means the key is not what gates the link and is a hard failure.
///
/// The refusal only means something if TLS was actually spoken, so the ways a
/// broken fixture or a flaky link would otherwise look like a pass are failures
/// here: an unreachable listener (proved reachable by a TCP probe first — a
/// firewalled or absent port times out rather than refusing), a failure from any
/// stage before the handshake (connect or esp-tls setup), and a handshake that
/// ends by consuming its deadline instead of by an alert. `reject_ms` measures
/// the handshake stage alone, so the host bound on refusal latency is not
/// polluted by connect time.
#[cfg(target_os = "espidf")]
pub(crate) fn run_tls_psk_wrong_key_rejected() -> (Status, Payload) {
    let TlsPskInputs {
        peer_ip,
        peer_port,
        psk,
        pod_id,
    } = match tls_psk_inputs(|p| p.tls_psk_bad_port) {
        Ok(v) => v,
        Err(report) => return report,
    };
    let peer = std::net::SocketAddr::from((peer_ip, peer_port));

    // Reachability first, as its own phase: a listener that is not there cannot
    // refuse anything, and its connect error must not be read as a rejection.
    let probe_started = std::time::Instant::now();
    match std::net::TcpStream::connect_timeout(
        &peer,
        std::time::Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
    ) {
        Ok(probe) => {
            drop(probe);
            log::info!(
                "tls-psk: wrong-key reachability probe to {peer} took {} ms",
                probe_started.elapsed().as_millis()
            );
        }
        Err(e) => {
            return test_report_fail_fmt(format_args!(
                "tls-psk wrong-key listener unreachable after {} ms: {e:?}",
                probe_started.elapsed().as_millis()
            ));
        }
    }

    let outcome = crate::tls_link::tls_connect_psk_staged(&crate::tls_link::TlsConnectParams {
        peer: &peer,
        pod_id: pod_id.as_str(),
        key: &psk,
        connect_timeout: std::time::Duration::from_secs(TLS_PSK_CONNECT_TIMEOUT_SECS),
        write_timeout: std::time::Duration::from_secs(2),
    });

    match outcome {
        Ok(_) => test_report_fail(
            "tls-psk handshake COMPLETED against a peer holding a different key — \
             the key does not gate the link",
        ),
        // Anything short of the handshake stage means TLS was never spoken (a lost
        // link, a refused connect, an esp-tls setup fault), so the run proves
        // nothing about the key and must not pass.
        Err(f) if f.stage != crate::tls_link::TlsConnectStage::Handshake => {
            test_report_fail_fmt(format_args!(
                "tls-psk wrong-key run never reached the handshake: {} failed after {} ms: {:?}",
                f.stage.label(),
                f.elapsed.as_millis(),
                f.error
            ))
        }
        // Deadline, not alert: the peer never refused, so this run proves nothing
        // about the key.
        Err(f) if f.error.kind() == std::io::ErrorKind::TimedOut => test_report_fail_detail(
            "tls-psk wrong-key handshake hit the deadline instead of being refused",
            &f.error,
        ),
        // Refusal latency only — the connect that preceded it is excluded, so a SYN
        // retransmit cannot masquerade as a slow rejection.
        Err(f) => test_report_ok(TestData::TlsPskRejected {
            peer_ip,
            peer_port,
            reject_ms: f.elapsed.as_millis().min(u32::MAX as u128) as u32,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{post_eof_tick, PostEofTick};

    const LIMIT: u32 = 50;

    /// A tick that routes frames resets the idle streak regardless of buffered state —
    /// the "still draining" case.
    #[test]
    fn post_eof_tick_progress_when_frames_routed() {
        assert_eq!(
            post_eof_tick(3, 0, LIMIT, 10, false, 40, 5),
            PostEofTick::Progress
        );
    }

    /// A no-progress tick short of the idle limit just asks the caller to keep polling.
    #[test]
    fn post_eof_tick_continue_before_limit() {
        assert_eq!(
            post_eof_tick(0, LIMIT - 1, LIMIT, 10, false, 40, 5),
            PostEofTick::Continue
        );
    }

    /// Streak expiry with an empty accumulator is a clean finish.
    #[test]
    fn post_eof_tick_done_when_streak_expires_empty() {
        assert_eq!(
            post_eof_tick(0, LIMIT, LIMIT, 0, false, 300, 12),
            PostEofTick::Done
        );
    }

    /// Streak expiry with a held complete frame names the capture-drain-stalled cause,
    /// not truncation.
    #[test]
    fn post_eof_tick_fail_names_stalled_drain_for_held_complete_frame() {
        match post_eof_tick(0, LIMIT, LIMIT, 64, true, 250, 30) {
            PostEofTick::Fail(msg) => {
                assert!(
                    msg.contains("capture drain stalled"),
                    "message must name the held-complete-frame cause: {msg:?}"
                );
                assert!(
                    msg.contains("64"),
                    "message must include the byte count: {msg:?}"
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// Streak expiry with a genuine partial frame names truncation, not a stalled drain.
    #[test]
    fn post_eof_tick_fail_names_truncated_tail_for_partial_frame() {
        match post_eof_tick(0, LIMIT, LIMIT, 3, false, 299, 30) {
            PostEofTick::Fail(msg) => {
                assert!(
                    msg.contains("truncated tail"),
                    "message must name the partial-frame cause: {msg:?}"
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// The RTD measurement suffix rides in `TestReport::detail`, which truncates on
    /// overflow; truncation would drop the trailing observability tokens, so its worst-case
    /// width stays budgeted. Per-field caps are upper bounds tied to device
    /// characteristics: heap free-byte fields fit 6 digits on the internal-RAM heap; the
    /// stack fields are bounded by the KB-scale RTD/producer stacks (`shwm`/`ptcb`/`phwm`).
    #[test]
    fn rtd_detail_length_budget() {
        let worst_case = format!(
            "src=rtd mh_post={} hla={} hlb={} shwm={} ptcb={} phwm={}",
            999_999u32, 999_999u32, 999_999u32, 99_999u32, 99_999u32, 99_999u32,
        );
        assert!(
            worst_case.len() <= 127,
            "worst-case RTD detail ({} bytes) exceeds 127-byte budget \
             (conservative, well under TestResultMsg cap): {:?}",
            worst_case.len(),
            worst_case
        );
    }
}
