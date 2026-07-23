//! TLS-PSK transport for the pod↔host audio link.
//!
//! One long-lived TLS 1.2 session, `ECDHE-PSK-CHACHA20-POLY1305`, carrying the
//! whole audio protocol in both directions. The PSK identity is the pod id; the
//! host looks the key up by that identity, so completing the handshake *is* the
//! pod's authentication.
//!
//! # Why raw `sys` calls instead of `esp_idf_svc::tls::EspTls`
//!
//! The streamer's event loop is non-blocking fds plus `poll`. The esp-tls C API
//! supports exactly that shape; the safe wrapper does not. Two defects in
//! `esp-idf-svc-0.52.1/src/tls.rs` rule it out for this connection:
//!
//! 1. `Config::try_into_raw` (tls.rs:245-263) points `rcfg.psk_hint_key` at
//!    `raw_psk`, a stack local of that function, and then returns the cfg by
//!    value. esp-tls dereferences the pointer later, inside
//!    `esp_tls_conn_new_*`, from a dead frame. Whether the key bytes survive is
//!    frame-layout luck, which a green test cannot distinguish from soundness.
//!    (Same pattern for the keep-alive cfg.) The crate's own heap-owned
//!    `TlsPsk` helper (tls.rs:31-70) shows the lifetime requirement is known.
//! 2. `EspTls::negotiate` passes `cfg.non_block` as both the `asynch` selector
//!    and `rcfg.non_block` (tls.rs:620). For an adopted socket the correct call
//!    — per `EspAsyncTls::negotiate` (tls.rs:866-886) — is
//!    `esp_tls_conn_new_async` with `rcfg.non_block = false`, because adoption
//!    jumps straight to `ESP_TLS_CONNECTING` and leaves the `non_block`
//!    connectivity `select()` uninitialized. The sync safe API cannot express
//!    that combination, and `non_block = false` through `negotiate` means a
//!    blocking handshake, which kills the poll loop.
//!
//! [`TlsSessionCfg`] fixes (1) locally by heap-pinning everything the C side
//! holds a pointer to; the handshake driver below expresses (2) directly.
//! If upstream fixes them this module can be re-hosted on `EspTls` without
//! touching callers.
//! TODO(esp-idf-svc-psk-wrapper-upstream): report both to `esp-rs/esp-idf-svc`.
//!
//! The esp-tls surface used here is `esp_tls_init`, `esp_tls_set_conn_sockfd`,
//! `esp_tls_set_conn_state`, `esp_tls_conn_new_async`, `esp_tls_conn_read`,
//! `esp_tls_conn_write`, `esp_tls_conn_destroy` — the same narrow set
//! `esp_idf_svc::tls` wraps internally — plus `esp_tls_get_ssl_context` and the
//! two mbedTLS session getters [`TlsStream::negotiated`] uses. An IDF bump
//! reviews those nine calls.
//!
//! # Poll discipline (correctness requirements, not style)
//!
//! 1. **Drain until `WouldBlock`.** TLS decrypts whole records into an internal
//!    buffer, so the fd can show no `POLLIN` while plaintext is still pending.
//!    A caller must keep reading until `WouldBlock` rather than trusting
//!    readiness; [`LinkStream::buffers_plaintext`] is how the streamer's event
//!    loop learns it must attempt a read every wake.
//! 2. **Retry writes with the same bytes.** After `WANT_WRITE` mbedTLS requires
//!    the next write to present the same buffer contents; partial-write
//!    bookkeeping must not re-slice differently on retry.
//! 3. **Poll the direction TLS asked for.** A read can want write and vice
//!    versa — constant during the handshake, rare afterwards. [`Want`] is
//!    tracked per direction (a read's outstanding request and a write's are
//!    independent) and [`LinkStream::poll_events`] *substitutes* it for the
//!    direction the caller armed. Substitution, not addition: a caller that
//!    de-armed `POLLOUT` (write backoff) or `POLLIN` (inbound backpressure) must
//!    not have it reinstated by the other direction's outstanding request, or
//!    the level-triggered fd wakes the loop immediately and the de-arm becomes a
//!    busy spin. The one exception is the self-contained `POLLOUT` wait inside
//!    `send_frame_bp`, which cannot consult [`Want`] while it holds the stream
//!    mutably: a write blocked on `WANT_READ` there waits out its write budget
//!    and the caller reconnects. That needs renegotiation to happen at all,
//!    which this configuration never initiates.
//!
//! # Threading
//!
//! An mbedTLS session is not thread-safe without `CONFIG_MBEDTLS_THREADING_C`,
//! which is not enabled. [`TlsStream`] holds a raw pointer and is therefore not
//! `Send`; the streamer thread creates and uses the session entirely by itself.

// Host view: these items exist for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use esp_idf_svc::sys;
#[cfg(target_os = "espidf")]
use std::io;
#[cfg(target_os = "espidf")]
use std::os::fd::RawFd;
#[cfg(target_os = "espidf")]
use std::time::{Duration, Instant};

/// Length of the audio-link pre-shared key, in bytes.
pub(crate) const PSK_LEN: usize = 32;

/// Overall wall-clock bound on the TLS handshake, from adopted socket to
/// completed session.
///
/// The ECDHE-PSK exchange costs on the order of 100–300 ms on this silicon; a
/// 3 s ceiling absorbs a retransmit or two on a bad radio while still failing
/// fast enough that the caller's reconnect backoff, not this wait, dominates.
///
/// Sourced from `device_protocol` because the host's HIL timeout-budget
/// invariants charge the same ceiling as a term.
#[cfg(target_os = "espidf")]
const HANDSHAKE_TIMEOUT: Duration =
    Duration::from_secs(device_protocol::TLS_HANDSHAKE_TIMEOUT_SECS);

/// Which direction TLS last said it was waiting on, for one operation
/// (a read's outstanding request and a write's are tracked separately).
#[cfg(target_os = "espidf")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Want {
    /// The last call returned `WANT_READ`: poll `POLLIN` before retrying.
    Read,
    /// The last call returned `WANT_WRITE`: poll `POLLOUT` before retrying.
    Write,
    /// The last call completed; no direction is outstanding.
    None,
}

/// A byte stream the streamer's `poll`-driven event loop can drive.
///
/// Bundles the pollable fd, event mask, and readiness-trust signal with
/// `Read`/`Write` so TLS poll discipline lives in the impl rather than at
/// every call site.
#[cfg(target_os = "espidf")]
pub(crate) trait LinkStream: io::Read + io::Write {
    /// The fd to hand `poll()`.
    fn link_fd(&self) -> RawFd;

    /// `poll()` event mask for a wake in which the caller is interested in
    /// reading (`readable`) and/or writing (`writable`). A direction the caller
    /// did not arm contributes nothing; an armed one contributes whichever
    /// event the transport needs to make that operation progress.
    fn poll_events(&self, readable: bool, writable: bool) -> u32;

    /// Whether decrypted bytes can sit in a transport-internal buffer that
    /// `POLLIN` cannot reveal. `true` obliges the caller to attempt a read
    /// every wake instead of only on readiness (poll discipline rule 1).
    fn buffers_plaintext(&self) -> bool;

    /// Reborrow as a plain reader, for helpers that need only `Read`.
    fn as_read(&mut self) -> &mut dyn io::Read;

    /// Reborrow as a plain writer, for helpers that need only `Write`.
    fn as_write(&mut self) -> &mut dyn io::Write;
}

#[cfg(target_os = "espidf")]
impl LinkStream for std::net::TcpStream {
    fn link_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd as _;
        self.as_raw_fd()
    }

    fn poll_events(&self, readable: bool, writable: bool) -> u32 {
        let mut events = 0;
        if readable {
            events |= sys::POLLIN;
        }
        if writable {
            events |= sys::POLLOUT;
        }
        events
    }

    fn buffers_plaintext(&self) -> bool {
        false
    }

    fn as_read(&mut self) -> &mut dyn io::Read {
        self
    }

    fn as_write(&mut self) -> &mut dyn io::Write {
        self
    }
}

/// Everything the C side holds a pointer to for the life of one session.
///
/// Heap-pinned behind a `Box` and never moved, so `cfg.psk_hint_key` →
/// `psk` → `key`/`identity` is a chain of pointers that provably outlives every
/// esp-tls call. This is the local fix for the dangling-pointer defect in the
/// safe wrapper (module docs, defect 1).
#[cfg(target_os = "espidf")]
struct TlsSessionCfg {
    /// Config handed to `esp_tls_conn_new_async` on every handshake step.
    cfg: sys::esp_tls_cfg,
    /// PSK descriptor `cfg.psk_hint_key` points at.
    psk: sys::psk_key_hint,
    /// Key bytes `psk.key` points at.
    key: [u8; PSK_LEN],
    /// NUL-terminated PSK identity (the pod id) `psk.hint` points at.
    identity: std::ffi::CString,
}

#[cfg(target_os = "espidf")]
impl TlsSessionCfg {
    /// Build the pinned session config for `identity`/`key`.
    ///
    /// Fails only if the pod id is not NUL-encodable, which genuine firmware
    /// never produces (the id is MAC-derived ASCII).
    fn new(identity: &str, key: &[u8; PSK_LEN]) -> io::Result<Box<Self>> {
        let identity = std::ffi::CString::new(identity)
            .map_err(|_| io::Error::other("pod id contains an interior NUL"))?;
        let mut boxed = Box::new(TlsSessionCfg {
            cfg: Default::default(),
            psk: sys::psk_key_hint {
                key: core::ptr::null(),
                key_size: PSK_LEN,
                hint: core::ptr::null(),
            },
            key: *key,
            identity,
        });

        // Pointers are taken after boxing so they name the final addresses.
        boxed.psk.key = boxed.key.as_ptr();
        boxed.psk.hint = boxed.identity.as_ptr();
        let psk_ptr: *mut sys::psk_key_hint = &mut boxed.psk;
        boxed.cfg.psk_hint_key = psk_ptr as _;
        // No cert bundle, no CA store: PSK carries the authentication, and a
        // cert path would drag in X.509 parsing and a trusted clock.
        // `non_block` stays false even though the socket is non-blocking —
        // esp-tls only uses it for a `select()` connectivity probe that is
        // invalid for an adopted socket (module docs, defect 2). The async
        // handshake entry point is what makes the calls non-blocking.
        boxed.cfg.non_block = false;
        boxed.cfg.is_plain_tcp = false;
        Ok(boxed)
    }
}

/// A TLS-PSK session over an adopted, non-blocking socket.
///
/// `read`/`write` carry `std::io` semantics with `WANT_READ`/`WANT_WRITE`
/// reported as [`io::ErrorKind::WouldBlock`], so the streamer's existing
/// non-blocking send/drain machinery works unchanged. Not `Send` (module docs,
/// threading).
#[cfg(target_os = "espidf")]
pub(crate) struct TlsStream {
    /// esp-tls session handle; owns the fd once adopted.
    tls: *mut sys::esp_tls,
    /// Pinned config the C side dereferences; dropped only with the session.
    _cfg: Box<TlsSessionCfg>,
    /// The adopted socket fd, kept for `poll()`. Closed by
    /// `esp_tls_conn_destroy`, never by this type.
    fd: RawFd,
    /// Direction the last `read` asked for.
    read_want: Want,
    /// Direction the last `write` asked for.
    write_want: Want,
}

#[cfg(target_os = "espidf")]
impl TlsStream {
    /// Negotiated protocol version and ciphersuite, as mbedTLS names them
    /// (e.g. `("TLSv1.2", "TLS-ECDHE-PSK-WITH-CHACHA20-POLY1305-SHA256")`).
    ///
    /// Reaches through `esp_tls_get_ssl_context` to the session mbedTLS owns;
    /// esp-tls exposes no accessor of its own for either fact. Either component
    /// is `""` if mbedTLS reports none, which happens only before the handshake
    /// completes. The eighth and ninth esp-tls/mbedTLS calls this module makes —
    /// an IDF bump reviews them with the other seven.
    pub(crate) fn negotiated(&self) -> (&str, &str) {
        // SAFETY: `self.tls` is a live session handle; `esp_tls_get_ssl_context`
        // returns the mbedTLS context esp-tls owns (or null), and both getters
        // take it by const pointer and return a static or session-owned
        // NUL-terminated string that outlives this borrow.
        unsafe {
            let ssl = sys::esp_tls_get_ssl_context(self.tls) as *const sys::mbedtls_ssl_context;
            if ssl.is_null() {
                return ("", "");
            }
            (
                cstr_or_empty(sys::mbedtls_ssl_get_version(ssl)),
                cstr_or_empty(sys::mbedtls_ssl_get_ciphersuite(ssl)),
            )
        }
    }

    /// Classify an esp-tls return code into the `std::io` world, returning the
    /// direction the failing operation is now waiting on. The caller stores it
    /// in that operation's own `Want` slot.
    fn classify(rc: i32) -> (Want, io::Error) {
        if rc == sys::ESP_TLS_ERR_SSL_WANT_READ {
            return (Want::Read, io::Error::from(io::ErrorKind::WouldBlock));
        }
        if rc == sys::ESP_TLS_ERR_SSL_WANT_WRITE {
            return (Want::Write, io::Error::from(io::ErrorKind::WouldBlock));
        }
        (
            Want::None,
            io::Error::other(format!("esp-tls error {rc:#x}")),
        )
    }
}

#[cfg(target_os = "espidf")]
impl io::Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // SAFETY: `self.tls` is a live session handle (non-null since
        // construction, destroyed only in `Drop`), and `buf` is a valid
        // writable slice of the length passed.
        let rc = unsafe {
            sys::esp_tls_conn_read(
                self.tls,
                buf.as_mut_ptr() as *mut core::ffi::c_void,
                buf.len(),
            )
        };
        if rc >= 0 {
            self.read_want = Want::None;
            // 0 is peer close, which `Read` reports as EOF.
            Ok(rc as usize)
        } else {
            let (want, err) = Self::classify(rc as i32);
            self.read_want = want;
            Err(err)
        }
    }
}

#[cfg(target_os = "espidf")]
impl io::Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // SAFETY: as in `read`; `buf` is a valid readable slice of the length
        // passed.
        let rc = unsafe {
            sys::esp_tls_conn_write(
                self.tls,
                buf.as_ptr() as *const core::ffi::c_void,
                buf.len(),
            )
        };
        if rc >= 0 {
            self.write_want = Want::None;
            Ok(rc as usize)
        } else {
            let (want, err) = Self::classify(rc as i32);
            self.write_want = want;
            Err(err)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // esp-tls writes straight through to the socket; nothing is buffered on
        // the way out.
        Ok(())
    }
}

#[cfg(target_os = "espidf")]
impl LinkStream for TlsStream {
    fn link_fd(&self) -> RawFd {
        self.fd
    }

    fn poll_events(&self, readable: bool, writable: bool) -> u32 {
        let mut events = 0;
        if readable {
            events |= match self.read_want {
                Want::Write => sys::POLLOUT,
                _ => sys::POLLIN,
            };
        }
        if writable {
            events |= match self.write_want {
                Want::Read => sys::POLLIN,
                _ => sys::POLLOUT,
            };
        }
        events
    }

    fn buffers_plaintext(&self) -> bool {
        true
    }

    fn as_read(&mut self) -> &mut dyn io::Read {
        self
    }

    fn as_write(&mut self) -> &mut dyn io::Write {
        self
    }
}

#[cfg(target_os = "espidf")]
impl Drop for TlsStream {
    fn drop(&mut self) {
        // SAFETY: `self.tls` is a live handle and this is the only place it is
        // destroyed. `esp_tls_conn_destroy` also closes the adopted fd, which
        // is why `fd` is a bare `RawFd` and not an owned socket.
        unsafe {
            sys::esp_tls_conn_destroy(self.tls);
        }
    }
}

/// Borrow a C string as `&str`, mapping null and non-UTF-8 to `""`.
///
/// # Safety
///
/// `p` is null or points at a NUL-terminated string that outlives `'a`.
#[cfg(target_os = "espidf")]
unsafe fn cstr_or_empty<'a>(p: *const core::ffi::c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    unsafe { core::ffi::CStr::from_ptr(p) }
        .to_str()
        .unwrap_or("")
}

/// Milliseconds remaining until `deadline`, clamped to a non-negative `c_int`.
#[cfg(target_os = "espidf")]
fn poll_timeout_ms(deadline: Instant) -> std::os::raw::c_int {
    let remaining = deadline.saturating_duration_since(Instant::now());
    remaining.as_millis().min(std::os::raw::c_int::MAX as u128) as std::os::raw::c_int
}

/// Inputs for [`tls_connect_psk`], bundled to keep the argument-word count
/// inside the Xtensa realign-miscompile guard's budget.
#[cfg(target_os = "espidf")]
pub(crate) struct TlsConnectParams<'a> {
    /// Audio host to connect to.
    pub(crate) peer: &'a std::net::SocketAddr,
    /// PSK identity — this pod's id, which the host keys the table by.
    pub(crate) pod_id: &'a str,
    /// The per-link pre-shared key.
    pub(crate) key: &'a [u8; PSK_LEN],
    /// TCP connect timeout, before any TLS is spoken.
    pub(crate) connect_timeout: Duration,
    /// Write timeout applied to the socket before it is handed to esp-tls.
    pub(crate) write_timeout: Duration,
}

/// Stage of [`tls_connect_psk_staged`] a failure came from.
///
/// Only [`TlsConnectStage::Handshake`] means TLS was actually spoken; the
/// negative self-test keys its verdict on that distinction.
#[cfg(target_os = "espidf")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TlsConnectStage {
    /// TCP connect, before any TLS byte crosses.
    TcpConnect,
    /// esp-tls session construction and fd adoption.
    Setup,
    /// The handshake exchange itself, including a peer's refusal alert.
    Handshake,
}

#[cfg(target_os = "espidf")]
impl TlsConnectStage {
    /// Short name for diagnostics.
    pub(crate) fn label(self) -> &'static str {
        match self {
            TlsConnectStage::TcpConnect => "tcp connect",
            TlsConnectStage::Setup => "tls setup",
            TlsConnectStage::Handshake => "tls handshake",
        }
    }
}

/// A ready TLS-PSK session plus the per-stage timings measured opening it.
#[cfg(target_os = "espidf")]
pub(crate) struct TlsConnected {
    /// The established session.
    pub(crate) stream: TlsStream,
    /// Time spent in `connect_timeout`, before any TLS byte.
    pub(crate) connect: Duration,
    /// Time spent driving the handshake to completion.
    pub(crate) handshake: Duration,
}

/// A failed TLS-PSK connect: which stage produced it, how long that stage ran,
/// and the error itself (original `ErrorKind` preserved).
#[cfg(target_os = "espidf")]
pub(crate) struct TlsConnectFailed {
    /// Stage that failed.
    pub(crate) stage: TlsConnectStage,
    /// Wall-clock time spent inside that stage.
    pub(crate) elapsed: Duration,
    /// The underlying error.
    pub(crate) error: io::Error,
}

/// Open a TCP connection to `params.peer` and complete a TLS-PSK handshake over
/// it, returning the ready session.
///
/// The socket is put in non-blocking mode *before* the fd is handed to esp-tls
/// (esp-tls owns it from that point on) and the handshake is driven by
/// `poll()`, so the caller's event loop is never blocked for longer than
/// [`HANDSHAKE_TIMEOUT`]. On any failure the fd is closed and nothing leaks.
///
/// Errors name the stage that produced them and preserve the original
/// `ErrorKind`, so callers that classify by kind are unaffected. Callers that
/// need the stage as data, or the connect/handshake split as a measurement, use
/// [`tls_connect_psk_staged`].
#[cfg(target_os = "espidf")]
pub(crate) fn tls_connect_psk(params: &TlsConnectParams) -> io::Result<TlsStream> {
    tls_connect_psk_staged(params)
        .map(|c| c.stream)
        .map_err(|f| f.error)
}

/// [`tls_connect_psk`] with the failing stage and the per-stage elapsed times
/// returned as data rather than encoded in prose.
#[cfg(target_os = "espidf")]
pub(crate) fn tls_connect_psk_staged(
    params: &TlsConnectParams,
) -> Result<TlsConnected, TlsConnectFailed> {
    use std::os::fd::IntoRawFd as _;

    let connect_started = Instant::now();
    let sock =
        std::net::TcpStream::connect_timeout(params.peer, params.connect_timeout).map_err(|e| {
            let elapsed = connect_started.elapsed();
            TlsConnectFailed {
                stage: TlsConnectStage::TcpConnect,
                elapsed,
                error: io::Error::new(
                    e.kind(),
                    format!(
                        "tcp connect to {} after {} ms: {e}",
                        params.peer,
                        elapsed.as_millis()
                    ),
                ),
            }
        })?;
    let connect = connect_started.elapsed();
    log::info!(
        "tls-psk: tcp connect to {} took {} ms",
        params.peer,
        connect.as_millis()
    );
    let setup_started = Instant::now();
    let setup_failed = |error: io::Error| TlsConnectFailed {
        stage: TlsConnectStage::Setup,
        elapsed: setup_started.elapsed(),
        error,
    };
    if let Err(e) = sock.set_nodelay(true) {
        log::warn!("tls_link: set_nodelay failed: {e:?}");
    }
    if let Err(e) = sock.set_write_timeout(Some(params.write_timeout)) {
        log::warn!("tls_link: set_write_timeout failed: {e:?}");
    }
    // Must precede the handoff: esp-tls owns the fd afterwards, and a blocking
    // socket would stall the streamer's poll loop inside the handshake.
    sock.set_nonblocking(true).map_err(setup_failed)?;

    let cfg = TlsSessionCfg::new(params.pod_id, params.key).map_err(setup_failed)?;
    let fd = sock.into_raw_fd();

    // SAFETY: no arguments; returns a heap handle or null.
    let tls = unsafe { sys::esp_tls_init() };
    if tls.is_null() {
        // SAFETY: `fd` is a live fd this function still owns — the adopt
        // sequence below has not run, so nothing else will close it.
        unsafe { sys::close(fd) };
        return Err(setup_failed(io::Error::other(
            "esp_tls_init failed (out of memory)",
        )));
    }

    // From here on `stream`'s `Drop` owns both the session and the fd.
    let mut stream = TlsStream {
        tls,
        _cfg: cfg,
        fd,
        read_want: Want::None,
        write_want: Want::None,
    };

    // Adopt sequence: hand esp-tls the connected socket and place the session
    // directly in CONNECTING, skipping esp-tls's own connect path.
    // SAFETY: `tls` is a live handle and `fd` is a connected socket.
    let rc = unsafe { sys::esp_tls_set_conn_sockfd(stream.tls, fd) };
    if rc != sys::ESP_OK {
        return Err(setup_failed(io::Error::other(format!(
            "esp_tls_set_conn_sockfd failed ({rc:#x})"
        ))));
    }
    // SAFETY: `tls` is a live handle; the state enum value is from the bindings.
    let rc = unsafe {
        sys::esp_tls_set_conn_state(stream.tls, sys::esp_tls_conn_state_ESP_TLS_CONNECTING)
    };
    if rc != sys::ESP_OK {
        return Err(setup_failed(io::Error::other(format!(
            "esp_tls_set_conn_state failed ({rc:#x})"
        ))));
    }

    // Hostname is irrelevant under PSK (no certificate, no SNI need); the peer
    // IP keeps esp-tls's logging meaningful.
    let host = params.peer.ip().to_string();
    let handshake_started = Instant::now();
    let deadline = handshake_started + HANDSHAKE_TIMEOUT;
    handshake(&mut stream, &host, deadline).map_err(|error| TlsConnectFailed {
        stage: TlsConnectStage::Handshake,
        elapsed: handshake_started.elapsed(),
        error,
    })?;
    Ok(TlsConnected {
        stream,
        connect,
        handshake: handshake_started.elapsed(),
    })
}

/// Drive `esp_tls_conn_new_async` to completion under `deadline`.
///
/// The C function returns exactly three values: `1` (handshake complete), `0`
/// (in progress — retry after polling), or a negative code (fatal, commonly a
/// PSK the peer does not know). It never surfaces `WANT_READ`/`WANT_WRITE`:
/// `esp_mbedtls_handshake` collapses both into `0`, so the direction mbedTLS is
/// waiting on is not observable here. This loop maps those into `Ok(())` or an
/// `io::Error`.
#[cfg(target_os = "espidf")]
fn handshake(stream: &mut TlsStream, host: &str, deadline: Instant) -> io::Result<()> {
    loop {
        // SAFETY: `stream.tls` is a live handle, `host` is a valid slice
        // described by the pointer/length pair, and the cfg is pinned in
        // `stream._cfg` for the whole call.
        let rc = unsafe {
            sys::esp_tls_conn_new_async(
                host.as_bytes().as_ptr() as *const core::ffi::c_char,
                host.len() as i32,
                0,
                &stream._cfg.cfg,
                stream.tls,
            )
        };
        match rc {
            1 => return Ok(()),
            // In progress, direction unknown (see this function's doc comment).
            0 => {}
            other => {
                return Err(io::Error::other(format!(
                    "TLS handshake failed ({other:#x}) — wrong or unknown PSK?"
                )));
            }
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS handshake did not complete within the deadline",
            ));
        }
        // Both directions, because the step's blocking direction is unobservable:
        // a client flight blocked on a full send buffer produces no `POLLIN`, and
        // waiting only on `POLLIN` would burn the whole deadline. A wake in the
        // wrong direction just re-calls `esp_tls_conn_new_async`, which is cheap.
        let events = sys::POLLIN | sys::POLLOUT;
        match crate::netpoll::poll_readiness(stream.fd, events, poll_timeout_ms(deadline)) {
            crate::netpoll::Readiness::Fault(e) => {
                return Err(io::Error::other(format!(
                    "socket fault during TLS handshake: {e:?}"
                )));
            }
            crate::netpoll::Readiness::TimedOut | crate::netpoll::Readiness::Ready { .. } => {}
        }
    }
}
