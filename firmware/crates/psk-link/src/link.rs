//! The Linux client transport for the pod↔host audio link.
//!
//! The streamer engine in `pod_streamer` is written against three seams: a
//! pollable byte stream ([`pod_streamer::link::LinkStream`]), a `poll()` shim
//! ([`pod_streamer::netpoll::NetPoll`]), and the platform's answers about its
//! identity, peer and clock ([`pod_streamer::run::StreamerPlatform`]). This
//! module is those three for a Linux pod: openssl over a non-blocking
//! `TcpStream`, `libc::poll`, and [`LinkPlatform`].
//!
//! Nothing here is reachy-specific — it is the transport any Linux pod on this
//! link speaks — which is why it lives beside the link's TLS parameters rather
//! than inside a device binary. The host-side integration test drives these
//! exact types, so what the test proves is what the pod ships.
//!
//! # Poll discipline
//!
//! The same three rules the ESP32 transport documents, for the same reasons, and
//! here the session tells us which direction it wants rather than leaving it to
//! be guessed:
//!
//! 1. **Drain until `WouldBlock`.** TLS decrypts whole records into an internal
//!    buffer, so the fd can show nothing readable while plaintext is already
//!    decoded and waiting. [`LinkStream::buffers_plaintext`] is `true` here,
//!    which obliges the streamer loop to attempt a read every wake.
//! 2. **Retry writes with the same bytes.** After `WANT_WRITE`, OpenSSL requires
//!    the next `SSL_write` to present the same buffer contents; partial-write
//!    bookkeeping must not re-slice differently on retry.
//! 3. **Poll the direction TLS asked for.** A read can be blocked on
//!    writability and a write on readability. Each direction's outstanding
//!    request is tracked separately and
//!    [`pod_streamer::link::plan_poll_interest`] *substitutes* it for the
//!    direction the caller armed — never adds to it, so a de-armed direction
//!    stays de-armed and a backpressure de-arm cannot become a busy spin.

use std::cell::Cell;
use std::fmt::Display;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd as _, RawFd};
use std::time::{Duration, Instant};

use openssl::ssl::{ErrorCode, Ssl, SslStream};
use pod_streamer::link::{LinkStream, PollInterest, Want, plan_poll_interest};
use pod_streamer::netpoll::{NetPoll, Readiness, classify_wake};
use pod_streamer::run::StreamerPlatform;

use crate::{PSK_LEN, client_context};

/// TCP connect timeout. 300 ms fast-fails an unreachable host — a LAN connect is
/// sub-millisecond — and the bound that matters is the pre-roll budget: an onset
/// that spends its ring history dialling has nothing left to send.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(300);

/// Wall-clock bound on the TLS handshake, from connected socket to ready
/// session. The ECDHE-PSK exchange is a millisecond of arithmetic on this class
/// of CPU, so a second is all retransmit headroom; failing inside it keeps the
/// caller's reconnect backoff, not this wait, in charge of retry pacing.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);

// ── The poll shim ─────────────────────────────────────────────────────────────

/// The Linux poll shim: `libc::poll` over one fd.
///
/// Zero-sized — every wake's state travels in the arguments — so callers can
/// name it inline wherever the shared engine wants a `&dyn NetPoll`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LibcPoll;

impl NetPoll for LibcPoll {
    fn poll_readiness(
        &self,
        fd: RawFd,
        interest: PollInterest,
        timeout: Duration,
    ) -> io::Result<Readiness> {
        let mut events = 0;
        if interest.read {
            events |= libc::POLLIN;
        }
        if interest.write {
            events |= libc::POLLOUT;
        }
        // The wait is resumable, so the budget is a deadline rather than a
        // per-call timeout: a signal that interrupts `poll` must cost the caller
        // the elapsed time and nothing else.
        let deadline = Instant::now() + timeout;
        let mut remaining = timeout;
        let revents = loop {
            // `poll` takes whole milliseconds as a `c_int`; the clamp only guards
            // the cast, since every caller's budget is far below `c_int::MAX` ms.
            // A sub-millisecond budget truncates to a non-blocking check; callers
            // that need a blocking wait must pass at least 1 ms.
            let timeout_ms = remaining.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;
            let mut pfd = libc::pollfd {
                fd,
                events,
                revents: 0,
            };
            // SAFETY: `pfd` is a single valid, initialized `pollfd` and `nfds = 1`
            // matches; `poll` only reads `fd`/`events` and writes `revents`.
            let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            // rc == 0 → timeout, revents is 0; rc > 0 → revents carries the bits.
            if rc >= 0 {
                break pfd.revents;
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
            // `poll` is not restarted for us on any signal disposition, so an
            // uncaught `EINTR` here would reach the streamer as a dead socket and
            // tear down a healthy session. Resume on what is left of the budget;
            // a budget already spent is the same answer as a timeout.
            remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break 0;
            }
        };
        let fault =
            (revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0).then_some(revents);
        Ok(classify_wake(
            revents & libc::POLLIN != 0,
            revents & libc::POLLOUT != 0,
            fault.map(|bits| bits as u32),
        ))
    }
}

// ── The TLS link ──────────────────────────────────────────────────────────────

/// One TLS-PSK session over a non-blocking socket, as the streamer's link.
///
/// Holds no fd of its own: the socket is owned by the `SslStream`, so
/// [`LinkStream::link_fd`] reads it back rather than caching a value that a
/// moved stream could invalidate.
#[derive(Debug)]
pub struct TlsLink {
    /// The session. Non-blocking underneath, so every call can report
    /// `WouldBlock` with a direction.
    stream: SslStream<TcpStream>,
    read_want: Want,
    write_want: Want,
    /// How often a poll wake was armed for the opposite direction to the one the
    /// caller asked for. Diagnostics only: a nonzero count on a settled session
    /// means TLS is doing work the loop did not ask for.
    want_substitutions: Cell<u32>,
}

impl TlsLink {
    /// Wrap a session whose handshake has completed.
    fn new(stream: SslStream<TcpStream>) -> Self {
        Self {
            stream,
            read_want: Want::None,
            write_want: Want::None,
            want_substitutions: Cell::new(0),
        }
    }

    /// Negotiated protocol version and ciphersuite. Empty strings if the session
    /// cannot report them.
    pub fn negotiated(&self) -> (&str, &str) {
        let ssl = self.stream.ssl();
        (
            ssl.version_str(),
            ssl.current_cipher().map(|c| c.name()).unwrap_or(""),
        )
    }

    /// Poll wakes so far whose direction was substituted (see
    /// [`plan_poll_interest`]).
    pub fn want_substitutions(&self) -> u32 {
        self.want_substitutions.get()
    }

    /// One line of session-end diagnostics: the peer, and how many poll wakes
    /// this session inverted a direction on — the "TLS did work the loop did not
    /// ask for" signal, which is only worth anything as a per-session total.
    fn teardown_facts(&self) -> String {
        let peer = match self.stream.get_ref().peer_addr() {
            Ok(addr) => addr.to_string(),
            // A socket already torn down cannot name its peer any more.
            Err(_) => "unknown peer".to_string(),
        };
        format!(
            "session down to {peer} — wsub={}",
            self.want_substitutions.get()
        )
    }
}

impl Drop for TlsLink {
    /// Report the session's teardown facts — the one exit path every ending
    /// passes through. Without it the substitution count would be paid for on
    /// every wake and never read.
    fn drop(&mut self) {
        log::info!("tls-psk: {}", self.teardown_facts());
    }
}

/// Classify an OpenSSL error into the `std::io` world, returning the direction
/// the failing operation is now waiting on. The caller stores it in that
/// operation's own [`Want`] slot.
///
/// `WANT_READ`/`WANT_WRITE` become `WouldBlock`; everything else preserves its
/// underlying `ErrorKind`, so callers that classify by kind (an unexpected EOF,
/// a reset) still see it.
fn classify(e: openssl::ssl::Error) -> (Want, io::Error) {
    match e.code() {
        ErrorCode::WANT_READ => (Want::Read, io::Error::from(io::ErrorKind::WouldBlock)),
        ErrorCode::WANT_WRITE => (Want::Write, io::Error::from(io::ErrorKind::WouldBlock)),
        _ => (
            Want::None,
            e.into_io_error()
                .unwrap_or_else(|e| io::Error::other(format!("tls error: {e}"))),
        ),
    }
}

impl io::Read for TlsLink {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        match self.stream.ssl_read(buf) {
            Ok(n) => {
                self.read_want = Want::None;
                Ok(n)
            }
            // A `close_notify` is the peer's orderly end of stream, which `Read`
            // reports as EOF rather than as an error.
            Err(e) if e.code() == ErrorCode::ZERO_RETURN => {
                self.read_want = Want::None;
                Ok(0)
            }
            Err(e) => {
                let (want, err) = classify(e);
                self.read_want = want;
                Err(err)
            }
        }
    }
}

impl io::Write for TlsLink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        match self.stream.ssl_write(buf) {
            Ok(n) => {
                self.write_want = Want::None;
                Ok(n)
            }
            Err(e) => {
                let (want, err) = classify(e);
                self.write_want = want;
                Err(err)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // OpenSSL writes each record straight through to the socket; only the
        // socket itself could hold anything back.
        self.stream.get_mut().flush()
    }
}

impl LinkStream for TlsLink {
    fn link_fd(&self) -> RawFd {
        self.stream.get_ref().as_raw_fd()
    }

    fn poll_interest(&self, readable: bool, writable: bool) -> PollInterest {
        let plan = plan_poll_interest(readable, writable, self.read_want, self.write_want);
        self.want_substitutions.set(
            self.want_substitutions
                .get()
                .saturating_add(plan.substituted),
        );
        plan.interest
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

// ── Connect ───────────────────────────────────────────────────────────────────

/// Inputs for [`connect_psk`].
pub struct TlsConnectParams<'a> {
    /// Audio host to connect to.
    pub peer: &'a SocketAddr,
    /// PSK identity — this pod's id, which the host keys its table by.
    pub pod_id: &'a str,
    /// The per-link pre-shared key.
    pub key: &'a [u8; PSK_LEN],
    /// TCP connect timeout, before any TLS byte.
    pub connect_timeout: Duration,
    /// Wall-clock bound on the handshake once the socket is up.
    pub handshake_timeout: Duration,
}

/// Open a TCP connection to `params.peer` and complete a TLS-PSK handshake over
/// it, returning the ready session.
///
/// The socket is switched to non-blocking *before* the first TLS byte and stays
/// that way, so the caller's event loop is never blocked for longer than
/// `params.handshake_timeout` and every later read/write can report
/// `WouldBlock`. The handshake itself is driven by [`LibcPoll`] on exactly the
/// direction OpenSSL asked for.
///
/// Errors name the stage that produced them and preserve the underlying
/// `ErrorKind`, so a caller classifying by kind (unreachable host, timeout) is
/// unaffected by the wrapping.
pub fn connect_psk(params: &TlsConnectParams) -> io::Result<TlsLink> {
    let started = Instant::now();
    let tcp = open_link_socket(params.peer, params.connect_timeout)?;

    let ctx = client_context(params.pod_id, *params.key)
        .map_err(|e| io::Error::other(format!("tls client context: {e}")))?;
    let ssl = Ssl::new(&ctx).map_err(|e| io::Error::other(format!("ssl session: {e}")))?;
    let mut stream =
        SslStream::new(ssl, tcp).map_err(|e| io::Error::other(format!("ssl stream: {e}")))?;

    // The handshake budget starts at the connected socket, not at `started`:
    // folding in the unused part of `connect_timeout` would hand a fast LAN
    // connect's whole slack to the handshake, and a bound that varies with
    // connect speed is not a bound the caller's reconnect pacing can be tuned
    // against.
    drive_handshake(
        &mut stream,
        &LibcPoll,
        params.peer,
        params.handshake_timeout,
    )?;

    let link = TlsLink::new(stream);
    let (version, suite) = link.negotiated();
    log::info!(
        "tls-psk: session up to {} as {:?} ({version} {suite}) — connect+handshake {} ms",
        params.peer,
        params.pod_id,
        started.elapsed().as_millis()
    );
    Ok(link)
}

/// Connect to `peer` and leave the socket in the shape a link needs.
///
/// Nagle is off because frames are written one at a time and read as they
/// arrive; batching would hold a 30-byte Hello until the next frame. Failing to
/// set it costs latency, not correctness, so it is logged rather than fatal. The
/// socket is non-blocking: both the handshake and the session are driven by
/// polls on the direction TLS names.
fn open_link_socket(peer: &SocketAddr, connect_timeout: Duration) -> io::Result<TcpStream> {
    let started = Instant::now();
    let tcp = TcpStream::connect_timeout(peer, connect_timeout).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "tcp connect to {peer} after {} ms: {e}",
                started.elapsed().as_millis()
            ),
        )
    })?;
    if let Err(e) = tcp.set_nodelay(true) {
        log::warn!("tls-psk: TCP_NODELAY not set on {peer}: {e}");
    }
    tcp.set_nonblocking(true)
        .map_err(|e| io::Error::new(e.kind(), format!("set non-blocking: {e}")))?;
    Ok(tcp)
}

/// Drive `stream`'s TLS handshake to completion, waiting on `poll` for the one
/// direction OpenSSL asks for, bounded by `timeout` from the moment of entry.
///
/// `poll` is a parameter rather than a hardcoded [`LibcPoll`] so tests can order
/// the wakes this function has to survive; production passes [`LibcPoll`].
///
/// # A fault wake is one more attempt, not a verdict
///
/// A socket can be readable *and* dead in the same wake — `POLLIN|POLLERR|POLLHUP`
/// is exactly what a peer that refuses the PSK produces, since it writes a fatal
/// alert and then closes over the client's still-unread flight, which makes the
/// close an RST. Reporting that wake as a socket fault throws away the alert
/// sitting in the receive buffer, and with it the only thing that separates a
/// provisioning error (fix the key table) from a network fault (go debug the
/// LAN).
///
/// So a fault is remembered, not returned: `connect()` runs once more and does
/// its own reads. It finds the alert (a named protocol error), or the socket's
/// pending error (a reset whose `ErrorKind` survives), or nothing at all — and
/// only that last case returns the socket fault. The retry is one non-blocking
/// call and the fault arm is reachable at most once per handshake, so a dead
/// socket whose polls return instantly cannot spin here.
fn drive_handshake(
    stream: &mut SslStream<TcpStream>,
    poll: &dyn NetPoll,
    peer: &SocketAddr,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut pending_fault: Option<io::Error> = None;
    loop {
        let e = match stream.connect() {
            // Succeeding right after a fault wake is still success: the peer
            // completed the handshake and then died, and that death surfaces on
            // the session's first I/O exactly as it would have had the poll run a
            // moment earlier.
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        // Wait on the one direction the session named. Anything else is a real
        // failure — a refused PSK identity or a wrong key arrives here as an
        // alert, not as a want.
        let interest = match e.code() {
            ErrorCode::WANT_READ => PollInterest::READ,
            ErrorCode::WANT_WRITE => PollInterest::WRITE,
            _ => return Err(handshake_error(e)),
        };
        // The backstop: the retry above had its chance to read and still wants
        // I/O, so the socket is dead with nothing left to say. The raw mask is
        // all the diagnostic there is.
        if let Some(fault) = pending_fault {
            return Err(io::Error::other(format!(
                "socket fault during TLS handshake to {peer}: {fault:?}"
            )));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "TLS handshake to {peer} did not complete within {} ms",
                    timeout.as_millis()
                ),
            ));
        }
        // A fault consumes no wait budget — the retry is immediate.
        if let Readiness::Fault(e) =
            poll.readiness(stream.get_ref().as_raw_fd(), interest, remaining)
        {
            pending_fault = Some(e);
        }
    }
}

/// Wrap a handshake failure, keeping OpenSSL's `ErrorKind` when it supplied one.
fn handshake_error(e: openssl::ssl::Error) -> io::Error {
    match e.into_io_error() {
        Ok(io) => io::Error::new(io.kind(), format!("tls handshake: {io}")),
        Err(e) => io::Error::other(format!("tls handshake: {e}")),
    }
}

// ── The platform seam ─────────────────────────────────────────────────────────

/// A Linux pod's answers to [`StreamerPlatform`].
///
/// Built once per streamer thread from configuration, so a reconnect re-reads
/// nothing. A connect must not depend on re-reading provisioning.
pub struct LinkPlatform {
    /// PSK identity and `Hello.pod_id`.
    pod_id: String,
    /// The audio host.
    peer: SocketAddr,
    /// Per-link pre-shared key.
    key: [u8; PSK_LEN],
    connect_timeout: Duration,
    handshake_timeout: Duration,
    poll: LibcPoll,
    /// `Instant` is `CLOCK_MONOTONIC`, so reconnect deadlines are immune to the
    /// wall clock jumping when NTP first lands — which on a CM4 with no RTC it
    /// always does.
    started: Instant,
}

impl LinkPlatform {
    /// A platform streaming to `peer` as `pod_id`, with the default timeouts.
    pub fn new(pod_id: String, peer: SocketAddr, key: [u8; PSK_LEN]) -> Self {
        Self {
            pod_id,
            peer,
            key,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            poll: LibcPoll,
            started: Instant::now(),
        }
    }

    /// Override the connect and handshake bounds.
    pub fn with_timeouts(mut self, connect: Duration, handshake: Duration) -> Self {
        self.connect_timeout = connect;
        self.handshake_timeout = handshake;
        self
    }
}

impl StreamerPlatform for LinkPlatform {
    type Link = TlsLink;

    fn pod_id(&self) -> &str {
        &self.pod_id
    }

    fn peer(&self) -> SocketAddr {
        self.peer
    }

    fn connect(&self) -> io::Result<Self::Link> {
        connect_psk(&TlsConnectParams {
            peer: &self.peer,
            pod_id: &self.pod_id,
            key: &self.key,
            connect_timeout: self.connect_timeout,
            handshake_timeout: self.handshake_timeout,
        })
    }

    fn link_up(&self) -> Option<bool> {
        // Always `Some(true)`: unlike the ESP32's own radio, a Linux pod's link
        // is the kernel's business and there is no cheap query whose answer
        // would be more current than a connect attempt. A carrier that is down
        // surfaces as a connect error and is paced by the same backoff — which
        // does mean a down link charges backoff here where it does not on the
        // ESP32. That is the honest trade: guessing `false` would strand a pod
        // whose route came back.
        Some(true)
    }

    fn link_diag(&self) -> impl Display {
        "link=os-managed"
    }

    fn now_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn poll(&self) -> &dyn NetPoll {
        &self.poll
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PSK_CIPHERSUITE, test_server_context};
    use openssl::ssl::SslContext;
    use std::cell::RefCell;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::mpsc;
    use std::thread::JoinHandle;

    const POD_ID: &str = "pod-test";
    const KEY: [u8; PSK_LEN] = [0x5a; PSK_LEN];

    /// Longest any test waits for a peer thread to do its part. Generous: these
    /// are localhost round trips, so reaching it means something is wedged.
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Bind a listener on an ephemeral port and run `serve` on the one accepted
    /// TLS session. Returns the address to connect to and the peer's handle.
    fn spawn_tls_peer<F>(ctx: SslContext, serve: F) -> (SocketAddr, JoinHandle<()>)
    where
        F: FnOnce(&mut SslStream<TcpStream>) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            let ssl = Ssl::new(&ctx).expect("server ssl");
            let mut stream = SslStream::new(ssl, sock).expect("wrap server socket");
            // The peer's socket stays blocking: it is a test thread with nothing
            // else to do, so an event loop here would only add a way to hang.
            if stream.accept().is_ok() {
                serve(&mut stream);
            }
        });
        (addr, handle)
    }

    /// A client-side `SslStream` over a production-configured socket (via
    /// [`open_link_socket`]), valid as [`drive_handshake`] input.
    fn client_stream(addr: &SocketAddr, pod_id: &str, key: [u8; PSK_LEN]) -> SslStream<TcpStream> {
        let tcp = open_link_socket(addr, DEFAULT_CONNECT_TIMEOUT).expect("tcp connect");
        let ctx = client_context(pod_id, key).expect("client context");
        let ssl = Ssl::new(&ctx).expect("client ssl");
        SslStream::new(ssl, tcp).expect("wrap client socket")
    }

    fn connect(addr: &SocketAddr, key: [u8; PSK_LEN]) -> io::Result<TlsLink> {
        connect_psk(&TlsConnectParams {
            peer: addr,
            pod_id: POD_ID,
            key: &key,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        })
    }

    /// Write every byte of `payload`, polling on the direction TLS asks for
    /// between `WouldBlock` retries. Returns the number of waits.
    fn write_all_polled(link: &mut TlsLink, payload: &[u8]) -> u32 {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut sent = 0;
        let mut waits = 0;
        while sent < payload.len() {
            match link.write(&payload[sent..]) {
                Ok(n) => sent += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    waits += 1;
                    let interest = link.poll_interest(false, true);
                    assert!(
                        Instant::now() < deadline,
                        "write of {} bytes never completed",
                        payload.len()
                    );
                    LibcPoll.readiness(link.link_fd(), interest, Duration::from_millis(50));
                }
                Err(e) => panic!("write failed: {e}"),
            }
        }
        waits
    }

    /// Read exactly `want` bytes, polling on the direction TLS asks for between
    /// `WouldBlock` retries. Returns the bytes and the number of waits.
    fn read_exact_polled(link: &mut TlsLink, want: usize) -> (Vec<u8>, u32) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut got = Vec::with_capacity(want);
        let mut waits = 0;
        let mut chunk = [0u8; 256];
        while got.len() < want {
            let room = (want - got.len()).min(chunk.len());
            match link.read(&mut chunk[..room]) {
                Ok(0) => panic!("peer closed after {} of {want} bytes", got.len()),
                Ok(n) => got.extend_from_slice(&chunk[..n]),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    waits += 1;
                    let interest = link.poll_interest(true, false);
                    assert!(
                        Instant::now() < deadline,
                        "read of {want} bytes never completed"
                    );
                    LibcPoll.readiness(link.link_fd(), interest, Duration::from_millis(50));
                }
                Err(e) => panic!("read failed: {e}"),
            }
        }
        (got, waits)
    }

    fn loopback_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let client = TcpStream::connect(addr).expect("connect");
        let (server, _) = listener.accept().expect("accept");
        (client, server)
    }

    /// Set one socket-buffer size on `fd`. Naming a size also switches off the
    /// kernel's auto-tuning, which is what makes filling a stalled peer bounded
    /// and quick rather than dependent on the host's `wmem_max`.
    fn set_socket_buf(fd: RawFd, option: libc::c_int, bytes: libc::c_int) {
        // SAFETY: `bytes` is a live, initialized `c_int` and the length passed is
        // its own size; `setsockopt` only reads through the pointer.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                option,
                (&raw const bytes).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "setsockopt: {}", io::Error::last_os_error());
    }

    // ── The handshake ─────────────────────────────────────────────────────

    /// The session both ends agree on is the one the link pins: TLS 1.2 and the
    /// single ECDHE-PSK suite. Asserted on a live handshake, so a parameter that
    /// silently fails to apply cannot pass here.
    #[test]
    fn a_psk_handshake_pins_tls12_and_the_one_suite() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            // Hold the session open until the client drops it.
            let mut sink = [0u8; 16];
            let _ = stream.read(&mut sink);
        });
        let link = connect(&addr, KEY).expect("handshake");
        assert_eq!(
            link.negotiated(),
            ("TLSv1.2", PSK_CIPHERSUITE),
            "negotiated parameters must be the pinned ones"
        );
        assert!(
            link.buffers_plaintext(),
            "a TLS link can hold decoded plaintext readiness cannot reveal"
        );
        assert!(link.link_fd() >= 0, "a live session has a real fd");
        assert_eq!(
            link.want_substitutions(),
            0,
            "no direction was substituted before any I/O"
        );
        drop(link);
        peer.join().expect("peer thread");
    }

    /// The session's end reports the substitution count — `Drop` is the only
    /// consumer of a counter the transport pays for on every wake; one that
    /// stopped reporting would make the bookkeeping write-only.
    #[test]
    fn a_sessions_end_reports_its_substitution_count() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            let mut sink = [0u8; 16];
            let _ = stream.read(&mut sink);
        });
        let link = connect(&addr, KEY).expect("handshake");
        let facts = link.teardown_facts();
        assert!(
            facts.contains(&addr.to_string()),
            "the peer must be named: {facts}"
        );
        assert!(
            facts.contains("wsub=0"),
            "a settled session inverted nothing: {facts}"
        );
        // Not a reachable state on a loopback session, so set it directly: the
        // point is that the reported number is the counter rather than a literal.
        link.want_substitutions.set(3);
        assert!(
            link.teardown_facts().contains("wsub=3"),
            "the line must report the live counter"
        );
        drop(link);
        peer.join().expect("peer thread");
    }

    /// A key the host does not hold fails the handshake as an error, not as a
    /// session that appears up and then cannot carry audio.
    #[test]
    fn a_key_the_server_does_not_hold_fails_the_handshake() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |_| {});
        let err = connect(&addr, [0xa5; PSK_LEN]).expect_err("wrong key must not connect");
        assert!(
            err.to_string().contains("tls handshake"),
            "the failing stage must be named: {err}"
        );
        // The alert the peer sent, not the socket-fault backstop: the two classes
        // must stay distinguishable by more than the casing of "TLS handshake".
        assert!(
            !err.to_string().contains("socket fault"),
            "a rejected key is a protocol error, not a socket fault: {err}"
        );
        peer.join().expect("peer thread");
    }

    /// An identity the host's table does not know is refused the same way — the
    /// PSK callback declines, and no session comes up.
    #[test]
    fn an_identity_the_server_does_not_know_fails_the_handshake() {
        let (addr, peer) = spawn_tls_peer(test_server_context("some-other-pod", KEY), |_| {});
        let err = connect(&addr, KEY).expect_err("unknown identity must not connect");
        assert!(
            err.to_string().contains("tls handshake"),
            "the failing stage must be named: {err}"
        );
        // As above: a refusal must not be reported as the socket-fault class.
        assert!(
            !err.to_string().contains("socket fault"),
            "a declined identity is a protocol error, not a socket fault: {err}"
        );
        peer.join().expect("peer thread");
    }

    /// A poll shim that holds one wait back until the peer signals, then either
    /// delegates to the real shim or answers with a synthetic fault.
    ///
    /// This is how the CI race becomes an ordering instead of a coincidence: the
    /// peer reaches the state the wake is about — an alert plus an RST on the
    /// wire, or a completed server flight — before the wait is allowed to
    /// proceed, so what the handshake sees is fixed rather than raced.
    struct DelayedPoll {
        /// Signalled once the peer has reached the state the held wait is about.
        signal: mpsc::Receiver<()>,
        /// Waits served so far.
        waits: Cell<u32>,
        /// Which wait to hold back — the one whose wake the test is about.
        hold_at: u32,
        /// Answer the held wait with a synthetic readable-and-hung-up fault
        /// instead of delegating, for a socket that is healthy and only the wake
        /// claims otherwise.
        synthesize_fault: bool,
        /// Whether the real shim ever answered with a fault.
        saw_fault: Cell<bool>,
        /// The interest each wait was armed with, in order.
        interests: RefCell<Vec<PollInterest>>,
    }

    impl NetPoll for DelayedPoll {
        fn poll_readiness(
            &self,
            fd: RawFd,
            interest: PollInterest,
            timeout: Duration,
        ) -> io::Result<Readiness> {
            let wait = self.waits.get() + 1;
            self.waits.set(wait);
            self.interests.borrow_mut().push(interest);
            if wait == self.hold_at {
                self.signal.recv_timeout(TEST_TIMEOUT).expect(
                    "the peer never reached the state the held wait is about — wait 1 is the \
                     server's own flight and wait 2 the held one, so a handshake whose wake \
                     shape changed parks here until this timeout",
                );
                if self.synthesize_fault {
                    return Ok(classify_wake(true, false, Some(libc::POLLHUP as u32)));
                }
            }
            let readiness = LibcPoll.poll_readiness(fd, interest, timeout)?;
            if matches!(readiness, Readiness::Fault(_)) {
                self.saw_fault.set(true);
            }
            Ok(readiness)
        }
    }

    /// A poll shim that reports a socket fault on every wake, counting them.
    struct AlwaysFaultPoll {
        waits: Cell<u32>,
    }

    impl NetPoll for AlwaysFaultPoll {
        fn poll_readiness(
            &self,
            _fd: RawFd,
            _interest: PollInterest,
            _timeout: Duration,
        ) -> io::Result<Readiness> {
            self.waits.set(self.waits.get() + 1);
            Ok(classify_wake(true, false, Some(libc::POLLHUP as u32)))
        }
    }

    /// A refusal that lands together with the peer's reset still names the TLS
    /// stage — the CI flake, pinned.
    ///
    /// The peer declines the identity, so `accept()` fails after writing a fatal
    /// alert, and dropping the session closes a socket with the client's unread
    /// CCS+Finished still queued, which makes the close an RST. Waking only after
    /// both have landed is the wake that used to be reported as a bare socket
    /// fault, discarding the alert that says *why* the peer refused.
    #[test]
    fn a_refusal_arriving_with_a_reset_still_names_the_tls_stage() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        // Not `spawn_tls_peer`: its `serve` closure runs only when `accept()`
        // succeeds, so a declined handshake could never produce the signal.
        let ctx = test_server_context("some-other-pod", KEY);
        let (closed_tx, closed_rx) = mpsc::channel::<()>();
        let peer = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            let ssl = Ssl::new(&ctx).expect("server ssl");
            let mut stream = SslStream::new(ssl, sock).expect("wrap server socket");
            stream
                .accept()
                .expect_err("an identity the table does not hold must be refused");
            // Explicit, and strictly before the signal: this drop is what emits
            // the RST. Signalling first would let the client's poll beat the
            // reset and see a plain readable wake — the test itself would flake.
            drop(stream);
            closed_tx.send(()).expect("the client is waiting");
        });

        let poll = DelayedPoll {
            signal: closed_rx,
            waits: Cell::new(0),
            // Wait 1 is the server's own flight; wait 2 is the one the refusal
            // arrives on, after the client has sent CKE+CCS+Finished.
            hold_at: 2,
            synthesize_fault: false,
            saw_fault: Cell::new(false),
            interests: RefCell::new(Vec::new()),
        };
        let mut stream = client_stream(&addr, POD_ID, KEY);
        let err = drive_handshake(&mut stream, &poll, &addr, DEFAULT_HANDSHAKE_TIMEOUT)
            .expect_err("an unknown identity must not connect");
        assert!(
            poll.saw_fault.get(),
            "the real shim never reported a fault, so the race this test exists for \
             did not happen and the assertion below proves nothing"
        );
        assert!(
            err.to_string().contains("tls handshake"),
            "the failing stage must be named: {err}"
        );
        // The whole point of the fault retry: the alert was read. Asserting the
        // absence of the backstop's message keeps the two classes apart
        // structurally, so normalizing either message's casing cannot make a
        // discarded alert look like a read one.
        assert!(
            !err.to_string().contains("socket fault"),
            "the alert was discarded — the readable-and-dead wake lost the refusal: {err}"
        );
        // `hold_at: 2` is only the refusal's wake while the handshake keeps this
        // shape. Asserting the count makes a changed shape fail here, naming the
        // cause, instead of failing as a wedged peer thread five seconds later.
        assert_eq!(
            poll.waits.get(),
            2,
            "wait 1 is the server's flight and wait 2 the refusal; a different count \
             means the wake shape changed and `hold_at` no longer points at the refusal"
        );
        // Both handshake waits here are a `WANT_READ`: the loop must arm the
        // direction TLS named, since arming writability on a connected socket
        // returns immediately and spins the wake loop.
        assert_eq!(
            poll.interests.borrow().as_slice(),
            [PollInterest::READ, PollInterest::READ].as_slice(),
            "the handshake must wait on the direction TLS asked for"
        );
        peer.join().expect("peer thread");
    }

    /// A handshake that completes on the wake that reported a fault is a
    /// session, not a failure.
    ///
    /// The mirror image of the flake: turning this wake away would send a pod
    /// whose handshake *did* finish around the reconnect-and-backoff loop. The
    /// peer's death, if it died, is the session's first I/O to report — which
    /// `an_aborted_peer_reads_as_a_fault_rather_than_end_of_stream` pins.
    ///
    /// The fault is synthetic because a socket that is genuinely dead cannot also
    /// complete a handshake; what is real is the ordering — the held wait is
    /// released only once the server's `accept()` has returned, so its
    /// CCS+Finished is on the wire and the retried `connect()` has something to
    /// finish with.
    #[test]
    fn a_handshake_completing_after_a_fault_wake_still_connects() {
        let (flight_tx, flight_rx) = mpsc::channel::<()>();
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), move |stream| {
            // `serve` runs only after `accept()` returned, so reaching here is
            // the proof that the server's last flight was written.
            flight_tx.send(()).expect("the client is waiting");
            let mut sink = [0u8; 16];
            let _ = stream.read(&mut sink);
        });

        let poll = DelayedPoll {
            signal: flight_rx,
            waits: Cell::new(0),
            // Wait 1 is the server's first flight; wait 2 is the wait for its
            // Finished, which is the one answered with the fault.
            hold_at: 2,
            synthesize_fault: true,
            saw_fault: Cell::new(false),
            interests: RefCell::new(Vec::new()),
        };
        let mut stream = client_stream(&addr, POD_ID, KEY);
        drive_handshake(&mut stream, &poll, &addr, DEFAULT_HANDSHAKE_TIMEOUT)
            .expect("a fault wake must not turn away a handshake that then completes");
        assert_eq!(
            poll.waits.get(),
            2,
            "wait 2 is the fault wake this test is about; a different count means the \
             handshake completed without ever being handed one"
        );
        drop(stream);
        peer.join().expect("peer thread");
    }

    /// A fault over a socket with nothing left to say still reports the socket
    /// fault, and reports it without waiting again.
    ///
    /// The peer here is healthy and silent, so the retry the fault path grants
    /// finds neither an alert nor a pending socket error; the raw mask is then
    /// the whole truth and must survive. That the retry happens at all is pinned
    /// by `a_refusal_arriving_with_a_reset_still_names_the_tls_stage` — a
    /// retried `connect()` over a socket with nothing readable is not observable
    /// from here. What this test holds is the message, the mask, and that the
    /// fault path never waits a second time.
    #[test]
    fn a_fault_with_nothing_readable_reports_the_socket_fault_without_waiting_again() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let peer = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            // Held open and silent until the client has given up, so nothing the
            // client could read ever appears.
            release_rx.recv_timeout(TEST_TIMEOUT).expect("release");
            drop(sock);
        });

        let poll = AlwaysFaultPoll {
            waits: Cell::new(0),
        };
        let mut stream = client_stream(&addr, POD_ID, KEY);
        let err = drive_handshake(&mut stream, &poll, &addr, DEFAULT_HANDSHAKE_TIMEOUT)
            .expect_err("a faulting socket must not connect");
        assert!(
            err.to_string()
                .contains("socket fault during TLS handshake"),
            "a fault with nothing to read is still a socket fault: {err}"
        );
        assert!(
            err.to_string().contains(&format!("{:#x}", libc::POLLHUP)),
            "the raw mask is the diagnostic that survives: {err}"
        );
        assert_eq!(
            poll.waits.get(),
            1,
            "the fault path reports without waiting again; a second wait would let a \
             socket whose polls return instantly spin to the deadline"
        );
        release_tx.send(()).expect("peer thread is alive");
        peer.join().expect("peer thread");
    }

    /// A peer that accepts the TCP connection and then says nothing is bounded
    /// by the handshake timeout rather than parking the streamer thread.
    #[test]
    fn a_peer_that_never_speaks_tls_is_bounded_by_the_handshake_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let peer = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            // Hold the connection open, silent, until the client gives up.
            std::thread::sleep(Duration::from_millis(600));
            drop(sock);
        });
        let started = Instant::now();
        let err = connect_psk(&TlsConnectParams {
            peer: &addr,
            pod_id: POD_ID,
            key: &KEY,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            handshake_timeout: Duration::from_millis(150),
        })
        .expect_err("a silent peer must not connect");
        assert_eq!(
            err.kind(),
            io::ErrorKind::TimedOut,
            "a silent peer is a timeout, not another error: {err}"
        );
        assert!(
            started.elapsed() < TEST_TIMEOUT,
            "the wait must be bounded by the handshake timeout, took {:?}",
            started.elapsed()
        );
        peer.join().expect("peer thread");
    }

    // ── Reads and writes ──────────────────────────────────────────────────

    /// Bytes cross both ways over the non-blocking session, and every block is a
    /// `WouldBlock` the caller can wait out.
    #[test]
    fn bytes_round_trip_over_the_non_blocking_session() {
        // Long enough to be several TLS records, so a partial write is likely.
        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), move |stream| {
            let mut got = vec![0u8; expected.len()];
            stream.read_exact(&mut got).expect("peer read");
            assert_eq!(got, expected, "peer received different bytes");
            stream.write_all(&got).expect("peer echo");
            stream.flush().expect("peer flush");
        });

        let mut link = connect(&addr, KEY).expect("handshake");
        write_all_polled(&mut link, &payload);
        link.flush().expect("flush");
        let (echoed, read_waits) = read_exact_polled(&mut link, payload.len());
        assert_eq!(echoed, payload, "the echoed bytes must come back intact");
        assert!(
            read_waits > 0,
            "an 8 KiB echo cannot all be there on the first read; the link must have \
             reported WouldBlock at least once"
        );
        assert_eq!(
            link.want_substitutions(),
            0,
            "a settled session should not have inverted a direction"
        );
        peer.join().expect("peer thread");
    }

    /// A read with nothing to read reports `WouldBlock` and arms readability
    /// alone; a wake the caller armed nothing for asks for nothing.
    #[test]
    fn a_blocked_read_arms_readability_alone() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            let mut sink = [0u8; 16];
            let _ = stream.read(&mut sink);
        });
        let mut link = connect(&addr, KEY).expect("handshake");
        let mut buf = [0u8; 64];
        let err = link
            .read(&mut buf)
            .expect_err("a silent peer has nothing to read");
        assert_eq!(
            err.kind(),
            io::ErrorKind::WouldBlock,
            "an empty session must block, not fail: {err}"
        );
        assert_eq!(
            link.poll_interest(true, false),
            PollInterest::READ,
            "a read wanting read polls in"
        );
        assert_eq!(
            link.poll_interest(false, false),
            PollInterest::NONE,
            "an unarmed wake asks for nothing"
        );
        assert_eq!(
            link.want_substitutions(),
            0,
            "wanting read while armed for read substitutes nothing"
        );
        drop(link);
        peer.join().expect("peer thread");
    }

    /// A peer's `close_notify` is end of stream, not an error: callers
    /// distinguish EOF from a fault by the `Ok(0)`.
    #[test]
    fn an_orderly_peer_close_reads_as_end_of_stream() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            stream.shutdown().expect("peer close_notify");
        });
        let mut link = connect(&addr, KEY).expect("handshake");
        let mut buf = [0u8; 64];
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            match link.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => panic!("a closing peer sent {n} bytes"),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "close_notify never arrived");
                    LibcPoll.readiness(
                        link.link_fd(),
                        link.poll_interest(true, false),
                        Duration::from_millis(50),
                    );
                }
                Err(e) => panic!("an orderly close must not be an error: {e}"),
            }
        }
        peer.join().expect("peer thread");
    }

    /// A write the socket cannot take blocks, arms writability, and completes by
    /// re-presenting the same bytes from the same place — the poll-discipline
    /// rules for the write half, over a live session rather than a truth table.
    #[test]
    fn a_write_the_peer_will_not_read_blocks_and_resumes_with_the_same_bytes() {
        /// Larger than both ends' buffers below, so the write cannot finish until
        /// the peer reads.
        const PAYLOAD_LEN: usize = 256 * 1024;
        /// Small enough that filling the path is immediate, and named explicitly
        /// so the kernel stops resizing it underneath the test.
        const SOCKET_BUF: libc::c_int = 8 * 1024;

        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i % 251) as u8).collect();
        let expected_len = payload.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        // Set on the listener, so the accepted socket carries it from its first
        // byte rather than after a window has already been advertised.
        set_socket_buf(listener.as_raw_fd(), libc::SO_RCVBUF, SOCKET_BUF);
        let addr = listener.local_addr().expect("local addr");
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (got_tx, got_rx) = mpsc::channel::<Vec<u8>>();
        let ctx = test_server_context(POD_ID, KEY);
        let peer = std::thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            let ssl = Ssl::new(&ctx).expect("server ssl");
            let mut stream = SslStream::new(ssl, sock).expect("wrap server socket");
            stream.accept().expect("server handshake");
            // Read nothing until the client has seen its write blocked; an
            // attentive peer would never let the send path fill.
            release_rx.recv().expect("release");
            let mut got = vec![0u8; expected_len];
            stream.read_exact(&mut got).expect("peer read");
            got_tx.send(got).expect("report what arrived");
        });

        let mut link = connect(&addr, KEY).expect("handshake");
        set_socket_buf(link.link_fd(), libc::SO_SNDBUF, SOCKET_BUF);

        let mut sent = 0;
        loop {
            match link.write(&payload[sent..]) {
                Ok(n) => {
                    sent += n;
                    assert!(
                        sent < PAYLOAD_LEN,
                        "the whole payload went out against a peer that is not reading and a \
                         {SOCKET_BUF}-byte send buffer, so the block this test is about never \
                         happened"
                    );
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("a stalled write must block, not fail: {e}"),
            }
        }
        assert_eq!(
            link.poll_interest(false, true),
            PollInterest::WRITE,
            "a write blocked on the socket asks for writability"
        );

        release_tx.send(()).expect("peer thread is alive");
        // The same slice from the same start: `sent` advances only on a write that
        // took bytes, which is the buffer-stability rule OpenSSL enforces across a
        // `WANT_WRITE` retry.
        let waits = write_all_polled(&mut link, &payload[sent..]);
        assert!(
            waits > 0,
            "the payload was waited out through the poll shim at least once"
        );
        link.flush().expect("flush");
        assert_eq!(
            got_rx.recv_timeout(TEST_TIMEOUT).expect("peer report"),
            payload,
            "every byte must reach the peer, in order, across the retries"
        );
        assert_eq!(
            link.want_substitutions(),
            0,
            "a write blocked on writability inverts no direction"
        );
        peer.join().expect("peer thread");
    }

    /// A peer that dies rather than closing politely is an error whose kind
    /// survives, not the `Ok(0)` an orderly `close_notify` produces. Callers
    /// rely on the distinction: end of stream is a host that went away, a fault
    /// is a socket to replace.
    #[test]
    fn an_aborted_peer_reads_as_a_fault_rather_than_end_of_stream() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            // Wait for a byte first: the client's handshake is then provably
            // complete, so the reset cannot discard a flight it still needed.
            let mut ping = [0u8; 1];
            stream.read_exact(&mut ping).expect("peer read");
            let linger = libc::linger {
                l_onoff: 1,
                l_linger: 0,
            };
            // SAFETY: `linger` is a live, initialized `linger` and the length
            // passed is its own size; `setsockopt` only reads through the pointer.
            let rc = unsafe {
                libc::setsockopt(
                    stream.get_ref().as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_LINGER,
                    (&raw const linger).cast(),
                    std::mem::size_of::<libc::linger>() as libc::socklen_t,
                )
            };
            assert_eq!(rc, 0, "setsockopt: {}", io::Error::last_os_error());
            // Returning drops the session, which closes the socket with an RST and
            // no `close_notify` — a killed daemon rather than a shutdown.
        });
        let mut link = connect(&addr, KEY).expect("handshake");
        write_all_polled(&mut link, b"x");

        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut buf = [0u8; 64];
        let err = loop {
            match link.read(&mut buf) {
                Ok(0) => panic!("a reset peer must not read as an orderly end of stream"),
                Ok(n) => panic!("a reset peer sent {n} bytes"),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "the reset never arrived");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => break e,
            }
        };
        // Either shape is a faithful report: a reset the socket named keeps its
        // `ErrorKind`, and one OpenSSL reports as a protocol-level error arrives
        // named as such. What must not happen is `WouldBlock` (a wait on a socket
        // that will never be readable again) or `Ok(0)`.
        assert!(
            matches!(
                err.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::BrokenPipe
            ) || err.to_string().contains("tls error"),
            "a torn session must arrive as a classifiable fault, got {:?}: {err}",
            err.kind()
        );
        peer.join().expect("peer thread");
    }

    // ── The poll shim ─────────────────────────────────────────────────────

    /// A connected socket with an empty send buffer is writable and not
    /// readable, and a zero timeout is a non-blocking check rather than a wait.
    #[test]
    fn a_connected_socket_is_writable_now() {
        let (client, _server) = loopback_pair();
        let started = Instant::now();
        let readiness = LibcPoll
            .poll_readiness(client.as_raw_fd(), PollInterest::BOTH, Duration::ZERO)
            .expect("poll");
        assert!(readiness.writable(), "an idle socket has send room");
        assert!(!readiness.readable(), "nothing has been written to it");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "a zero timeout must not wait"
        );
    }

    /// Readability appears once the peer writes, and the wake reports only the
    /// direction that is actually ready.
    #[test]
    fn readability_arrives_once_the_peer_writes() {
        let (client, mut server) = loopback_pair();
        server.write_all(b"hi").expect("peer write");
        server.flush().expect("peer flush");
        let readiness = LibcPoll
            .poll_readiness(
                client.as_raw_fd(),
                PollInterest::READ,
                Duration::from_millis(500),
            )
            .expect("poll");
        assert!(readiness.readable(), "the peer's bytes are waiting");
        assert!(
            !readiness.writable(),
            "writability was not armed, so it must not be reported"
        );
    }

    /// Nothing ready inside the budget is a timeout, distinct from a fault.
    #[test]
    fn an_idle_socket_times_out_rather_than_faulting() {
        let (client, _server) = loopback_pair();
        let readiness = LibcPoll
            .poll_readiness(
                client.as_raw_fd(),
                PollInterest::READ,
                Duration::from_millis(20),
            )
            .expect("poll");
        assert!(
            matches!(readiness, Readiness::TimedOut),
            "an idle socket must time out, got {readiness:?}"
        );
    }

    /// An fd that is not open faults, with the raw mask in the message.
    ///
    /// The number is one no process can hold rather than one just closed: the
    /// harness runs this module's tests concurrently and its siblings open sockets
    /// continuously, so a recycled descriptor number could be handed to another
    /// thread between the close and the poll and report no `POLLNVAL` at all. A
    /// *negative* fd would not do either — `poll` skips those and reports nothing.
    #[test]
    fn a_dead_fd_faults_with_its_revents_in_the_message() {
        let readiness = LibcPoll
            .poll_readiness(
                libc::c_int::MAX,
                PollInterest::BOTH,
                Duration::from_millis(20),
            )
            .expect("poll itself succeeds; the fault is in revents");
        let Readiness::Fault(e) = readiness else {
            panic!("a closed fd must classify as Fault");
        };
        let msg = e.to_string();
        assert!(
            msg.contains(&format!("{:#x}", libc::POLLNVAL)),
            "the raw mask is the diagnostic: {msg}"
        );
    }

    /// A signal caught while the shim is parked in `poll` is not a dead socket.
    ///
    /// `poll` is restarted for nobody — `SA_RESTART` does not cover it — so an
    /// `EINTR` passed through would reach the streamer as `Readiness::Fault` and
    /// tear down a healthy TLS session. Any process embedding this transport that
    /// installs a handler (a `SIGTERM` shutdown handler is table stakes for a
    /// daemon) would arm that.
    #[test]
    fn a_signal_during_a_wait_resumes_instead_of_faulting() {
        /// The signal this test delivers to its own thread. Nothing else in the
        /// tree sends it, and the disposition is restored before returning.
        const SIG: libc::c_int = libc::SIGUSR1;
        const BUDGET: Duration = Duration::from_millis(300);
        static DELIVERED: AtomicU32 = AtomicU32::new(0);

        extern "C" fn on_signal(_: libc::c_int) {
            DELIVERED.fetch_add(1, Ordering::Relaxed);
        }

        let (client, _server) = loopback_pair();
        // No `SA_RESTART`, an empty mask: the plainest handler that interrupts a
        // wait. The previous disposition is put back below.
        // SAFETY: both structs are fully initialized (zeroed is an empty mask and
        // no flags) and outlive the calls; `on_signal` only touches an atomic, so
        // it is safe to run in a signal context.
        let previous = unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as *const () as libc::sighandler_t;
            let mut previous: libc::sigaction = std::mem::zeroed();
            assert_eq!(
                libc::sigaction(SIG, &action, &mut previous),
                0,
                "install handler: {}",
                io::Error::last_os_error()
            );
            previous
        };
        // SAFETY: `pthread_self` takes no arguments and cannot fail.
        let target = unsafe { libc::pthread_self() };
        let waker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            // SAFETY: `target` names this process's still-running test thread,
            // which is joined with this one below.
            unsafe { libc::pthread_kill(target, SIG) }
        });

        let started = Instant::now();
        let readiness = LibcPoll
            .poll_readiness(client.as_raw_fd(), PollInterest::READ, BUDGET)
            .expect("an interrupted wait is not a failure of the poll call");
        let elapsed = started.elapsed();

        assert_eq!(waker.join().expect("waker thread"), 0, "pthread_kill");
        // SAFETY: `previous` is the disposition `sigaction` just filled in.
        unsafe { libc::sigaction(SIG, &previous, std::ptr::null_mut()) };

        assert!(
            DELIVERED.load(Ordering::Relaxed) >= 1,
            "the signal never arrived, so nothing was interrupted and this test \
             asserted nothing"
        );
        assert!(
            matches!(readiness, Readiness::TimedOut),
            "an interrupted wait on a healthy socket is a timeout, not a fault: {readiness:?}"
        );
        assert!(
            elapsed >= BUDGET - Duration::from_millis(50),
            "the wait must resume on what is left of its budget, took {elapsed:?}"
        );
    }

    // ── The platform ──────────────────────────────────────────────────────

    /// The platform answers the streamer's queries from its configuration and
    /// connects a real session — the whole client stack below the loop.
    #[test]
    fn the_platform_answers_the_streamer_seam_and_connects() {
        let (addr, peer) = spawn_tls_peer(test_server_context(POD_ID, KEY), |stream| {
            let mut sink = [0u8; 16];
            let _ = stream.read(&mut sink);
        });
        let platform = LinkPlatform::new(POD_ID.to_string(), addr, KEY);
        assert_eq!(platform.pod_id(), POD_ID, "the id is the PSK identity");
        assert_eq!(platform.peer(), addr);
        assert_eq!(
            platform.link_up(),
            Some(true),
            "a Linux pod cannot cheaply say its link is down, and guessing false \
             would strand it"
        );
        assert!(
            platform.link_diag().to_string().starts_with("link="),
            "the diagnostic is one labelled field"
        );
        let first = platform.now_secs();
        assert!(
            platform.now_secs() >= first,
            "the reconnect clock must be monotonic"
        );

        let link = platform.connect().expect("platform connect");
        assert_eq!(link.negotiated().0, "TLSv1.2");
        drop(link);
        peer.join().expect("peer thread");
    }

    /// A refused connection is an error the idle loop can pace, not a panic and
    /// not a wait: nothing is listening, so `connect` fails at the TCP stage
    /// with its own `ErrorKind` intact.
    #[test]
    fn a_refused_connect_reports_the_tcp_stage_and_its_kind() {
        // Bind and drop, so the port is almost certainly unused and nothing is
        // listening on it.
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.local_addr().expect("local addr")
        };
        let platform = LinkPlatform::new(POD_ID.to_string(), addr, KEY)
            .with_timeouts(Duration::from_millis(200), Duration::from_millis(200));
        let err = platform.connect().expect_err("nothing is listening");
        assert!(
            err.to_string().contains("tcp connect"),
            "the failing stage must be named: {err}"
        );
        assert!(
            matches!(
                err.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
            ),
            "the underlying kind must survive the wrapping: {:?}",
            err.kind()
        );
    }
}
