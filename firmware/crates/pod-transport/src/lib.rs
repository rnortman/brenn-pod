//! Shared host-side transport for respeaker pod communication.
//!
//! Provides the USB-serial-JTAG transport primitives consumed by both `hil-host`
//! and `podctl`: port enumeration, port opening, and the framed request/response
//! harness (COBS demux, correlation by id, bounded timeout).
//!
//! This crate is host-only and must not be added to workspace default-members.

use device_protocol::{DeviceFrame, LogFrame, Request, Response};
use postcard::accumulator::{CobsAccumulator, FeedResult};
use std::{
    io::{Read as _, Write as _},
    time::{Duration, Instant},
};

// ── Transport trait ───────────────────────────────────────────────────────────

/// Minimal trait the harness needs from a serial connection.
///
/// Decouples `Harness` from the full `serialport::SerialPort` trait surface so
/// `FakePort` in tests only implements `Read + Write` rather than 20+ boilerplate
/// methods.
pub trait Transport: std::io::Read + std::io::Write + Send + 'static {}
impl<T: std::io::Read + std::io::Write + Send + 'static> Transport for T {}

// ── Device USB IDs ────────────────────────────────────────────────────────────

pub const ESP32S3_VID: u16 = 0x303a;
pub const ESP32S3_APP_PID: u16 = 0x1001;
pub const ESP32S3_DFU_PID: u16 = 0x0002;

// ── Timeouts ──────────────────────────────────────────────────────────────────

/// Per-request response timeout. Never hang; distinct message on expiry.
/// Set to 10 s to accommodate the I2S waveform sanity test, which has a 3 s
/// device-side capture timeout (I2S_READ_TIMEOUT_TICKS=300 ticks @ 100 Hz).
/// All other tests complete in milliseconds; the longer timeout is harmless.
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serial port read chunk size. Crate-internal; only `send_command_timeout` uses it.
const READ_BUF: usize = 256;

// ── SerialPortTransport ───────────────────────────────────────────────────────

/// Newtype wrapping `Box<dyn serialport::SerialPort>` so it satisfies
/// `Box<dyn Transport>`.
pub struct SerialPortTransport(Box<dyn serialport::SerialPort>);

impl SerialPortTransport {
    /// Wrap an already-open serial port.
    pub fn new(port: Box<dyn serialport::SerialPort>) -> Self {
        Self(port)
    }
}

impl From<Box<dyn serialport::SerialPort>> for SerialPortTransport {
    fn from(port: Box<dyn serialport::SerialPort>) -> Self {
        Self::new(port)
    }
}

impl std::io::Read for SerialPortTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl std::io::Write for SerialPortTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

// ── Port enumeration ──────────────────────────────────────────────────────────

/// The operating mode of an enumerated respeaker pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodMode {
    /// Running app firmware; accepts provisioning commands.
    App,
    /// DFU/bootloader mode; cannot accept provisioning commands.
    Dfu,
}

/// A respeaker pod enumerated on a USB-serial port.
#[derive(Debug, Clone)]
pub struct PodPort {
    /// Serial port path (e.g. `/dev/ttyACM0`).
    pub port_name: String,
    /// USB serial number, if the device exposes one.
    pub serial_number: Option<String>,
    /// Whether the pod is running app firmware or is in DFU/bootloader mode.
    pub mode: PodMode,
}

impl PodPort {
    /// Operator-facing identity string.
    ///
    /// Format: `"{port_name} (SN {sn})"` when a serial number is present,
    /// otherwise just `"{port_name}"`.
    pub fn identity(&self) -> String {
        match &self.serial_number {
            Some(sn) => format!("{} (SN {})", self.port_name, sn),
            None => self.port_name.clone(),
        }
    }
}

/// Enumerate all respeaker pods (app + DFU) by VID:PID.
///
/// Returns both app-mode and DFU-mode pods with their mode tagged. No policy
/// is applied here; callers own the selection logic (e.g. hil-host aborts on
/// DFU; podctl errors only when DFU is the selected device).
pub fn enumerate_pods() -> Result<Vec<PodPort>, serialport::Error> {
    let all = serialport::available_ports()?;
    let pods = all
        .into_iter()
        .filter_map(|p| {
            if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                if info.vid == ESP32S3_VID {
                    let mode = if info.pid == ESP32S3_APP_PID {
                        PodMode::App
                    } else if info.pid == ESP32S3_DFU_PID {
                        PodMode::Dfu
                    } else {
                        return None;
                    };
                    return Some(PodPort {
                        port_name: p.port_name.clone(),
                        serial_number: info.serial_number.clone(),
                        mode,
                    });
                }
            }
            None
        })
        .collect();
    Ok(pods)
}

/// Open an app-mode port at the fixed serial-JTAG settings (115_200, 50 ms read
/// timeout) and wrap it as a boxed `Transport` ready for `Harness::new`.
///
/// Baud rate is irrelevant for USB-CDC/serial-JTAG but `serialport` requires one.
pub fn open_port(port_name: &str) -> Result<Box<dyn Transport>, serialport::Error> {
    let port = serialport::new(port_name, 115_200)
        .timeout(Duration::from_millis(50))
        .open()?;
    Ok(Box::new(SerialPortTransport::new(port)))
}

// ── FrameReader ───────────────────────────────────────────────────────────────

/// Format a `LogFrame` as the canonical `[device <Level>] <target>: <message>` string.
///
/// Both `Harness` (which surfaces log frames to stderr while waiting for a response)
/// and the `podctl logs` monitor use this function so the format stays in one place.
///
/// Control characters in `target` and `message` (including `\n`, `\r`, and ESC)
/// are escaped via [`char::escape_default`] to prevent terminal escape-sequence
/// injection when the output is printed to an operator's terminal.  Printable
/// ASCII and valid UTF-8 text pass through unchanged.
pub fn format_log(log: &LogFrame) -> String {
    format!(
        "[device {:?}] {}: {}",
        log.level,
        escape_device_str(&log.target),
        escape_device_str(&log.message)
    )
}

/// True for chars that render as nothing yet alter how surrounding text is displayed:
/// the Unicode `Cf` format category plus the `Zl`/`Zp` separators. `char::is_control`
/// covers only `Cc`, so these would otherwise reach the terminal verbatim and let a
/// device reorder or line-break operator-facing text.
fn is_invisible_format(c: char) -> bool {
    matches!(c,
        '\u{00ad}'
        | '\u{0600}'..='\u{0605}' | '\u{061c}' | '\u{06dd}' | '\u{070f}'
        | '\u{08e2}' | '\u{110bd}' | '\u{110cd}'
        | '\u{180e}'
        | '\u{200b}'..='\u{200f}'
        | '\u{2028}'..='\u{202e}'
        | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{206f}'
        | '\u{feff}'
        | '\u{fff9}'..='\u{fffb}'
        | '\u{1d173}'..='\u{1d17a}'
        | '\u{e0001}' | '\u{e0020}'..='\u{e007f}'
    )
}

/// Escape control characters in a device-originated string for terminal-safe display.
///
/// Every `char` for which [`char::is_control`] holds — including ESC, `\n`, `\r`, and
/// `\t` — is replaced by its [`char::escape_default`] form, as is every invisible
/// formatting char (Unicode `Cf`, plus `U+2028`/`U+2029`): bidi overrides and isolates,
/// zero-width spaces and joiners, and the line/paragraph separators. Every other char
/// (printable ASCII and printable non-ASCII alike) passes through verbatim. This closes
/// both the terminal escape-sequence injection vector and the visual-reordering vector
/// for any string a device can influence.
///
/// Device strings arrive as `heapless::String`, i.e. already valid UTF-8, so escaping is
/// defined over `char`s rather than raw bytes. Round-trip fidelity is not a goal: a
/// literal backslash in the input is not doubled, so the output is display text, not a
/// reversible encoding.
pub fn escape_device_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_control() || is_invisible_format(c) {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

/// Owns a `Transport` and a `CobsAccumulator`, providing a single `pump` method
/// that reads one chunk from the port and dispatches every complete `DeviceFrame`
/// it yields to a caller-supplied closure.
///
/// This is the shared read→feed→dispatch→resync loop used by both `Harness` and
/// the `podctl logs` monitor. Keeping one copy here prevents the two consumers
/// from diverging on COBS resync behavior.
pub struct FrameReader {
    port: Box<dyn Transport>,
    acc: CobsAccumulator<ACC_CAP>,
    /// Optional label (e.g. port path) included in `OverFull`/`DeserError` warnings
    /// so log messages are attributable to a specific device when multiple ports are
    /// open.
    label: Option<String>,
    /// Bounded shadow of the bytes fed into `acc` since the last frame boundary.
    ///
    /// The postcard `CobsAccumulator` keeps its internal buffer private and resets
    /// its write index to 0 *before* returning `OverFull`/`DeserError`, so the bytes
    /// it just discarded are unrecoverable from `acc` itself. We keep our own copy of
    /// the same byte run so the error paths can dump it (e.g. an un-COBS-framed ESP32
    /// "Guru Meditation" panic backtrace, which would otherwise be silently dropped).
    ///
    /// Capacity is bounded to `ACC_CAP` — the accumulator's own capacity — so this is
    /// never an unbounded buffer; the overflow that triggers the dump is the same
    /// overflow that bounds it.
    shadow: Vec<u8>,
}

/// Capacity of the COBS accumulator (and the bounded shadow buffer used for error-path
/// dumps). Bounds both buffers; a frame larger than this triggers `OverFull`.
const ACC_CAP: usize = 1024;

/// Render `bytes` as a side-by-side hex + lossy-ASCII view for human inspection of an
/// undecodable byte run (e.g. a raw ESP32 panic dump that is not COBS-framed).
///
/// Each line covers 16 bytes: an offset, the hex octets, then the ASCII gutter where
/// printable bytes (0x20..=0x7e) appear literally and everything else as `.`. This makes
/// lines like `Guru Meditation Error: Core 0 panic'ed` and `Backtrace: 0x40012345:...`
/// legible while still preserving the exact bytes in hex.
fn render_dump(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4 + 64);
    for (row, chunk) in bytes.chunks(16).enumerate() {
        let offset = row * 16;
        let mut hex = String::with_capacity(16 * 3);
        let mut ascii = String::with_capacity(16);
        for &b in chunk {
            hex.push_str(&format!("{b:02x} "));
            ascii.push(if (0x20..=0x7e).contains(&b) {
                b as char
            } else {
                '.'
            });
        }
        // Pad the hex column so the ASCII gutter aligns on the final, short row.
        out.push_str(&format!("  {offset:04x}: {hex:<48}|{ascii}|\n"));
    }
    out
}

impl FrameReader {
    /// Wrap an already-open transport in a `FrameReader`.
    pub fn new(transport: Box<dyn Transport>) -> Self {
        Self {
            port: transport,
            acc: CobsAccumulator::new(),
            label: None,
            shadow: Vec::with_capacity(ACC_CAP),
        }
    }

    /// Wrap an already-open transport in a `FrameReader` with a port label.
    ///
    /// The label is included in `OverFull` and `DeserError` warning messages so
    /// an operator can identify which device triggered the warning.
    pub fn with_label(transport: Box<dyn Transport>, label: impl Into<String>) -> Self {
        Self {
            port: transport,
            acc: CobsAccumulator::new(),
            label: Some(label.into()),
            shadow: Vec::with_capacity(ACC_CAP),
        }
    }

    /// Read at most one chunk from the port and dispatch every complete
    /// `DeviceFrame` it yields to `on_frame`.
    ///
    /// Returns `Ok(false)` on a read timeout (no bytes), `Ok(true)` if at least
    /// one byte was read (regardless of whether any complete frames were decoded),
    /// and `Err(io::Error)` on a non-timeout read error (e.g. device disconnect).
    ///
    /// `OverFull` and `DeserError` accumulator states are handled internally:
    /// `OverFull` resets the accumulator and emits a warning to stderr;
    /// `DeserError` emits a warning and resyncs on the next COBS zero delimiter.
    /// Neither causes an `Err` return — the stream continues.
    pub fn pump<F: FnMut(DeviceFrame)>(
        &mut self,
        on_frame: &mut F,
    ) -> Result<bool, std::io::Error> {
        let mut read_buf = [0u8; READ_BUF];
        let n = match self.port.read(&mut read_buf) {
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Ok(false);
            }
            Err(e) => return Err(e),
        };

        if n == 0 {
            return Ok(false);
        }

        let mut chunk = &read_buf[..n];
        loop {
            // Length of `chunk` before this feed, so we can compute how many bytes the
            // accumulator consumed (chunk.len() - remaining.len()) and mirror exactly
            // that run into the bounded `shadow` buffer.
            let pre_len = chunk.len();
            match self.acc.feed::<DeviceFrame>(chunk) {
                FeedResult::Success { data, remaining } => {
                    self.extend_shadow(&chunk[..pre_len - remaining.len()]);
                    // Frame boundary: the accumulator reset its index; drop the mirror.
                    self.shadow.clear();
                    chunk = remaining;
                    on_frame(data);
                    if chunk.is_empty() {
                        break;
                    }
                }
                FeedResult::Consumed => {
                    self.extend_shadow(chunk);
                    break;
                }
                FeedResult::OverFull(r) => {
                    self.extend_shadow(&chunk[..pre_len - r.len()]);
                    match &self.label {
                        Some(lbl) => {
                            eprintln!("WARN [demux] {lbl}: accumulator overflowed; resetting")
                        }
                        None => eprintln!("WARN [demux]: accumulator overflowed; resetting"),
                    }
                    self.dump_shadow();
                    self.acc = CobsAccumulator::new();
                    chunk = r;
                }
                FeedResult::DeserError(r) => {
                    self.extend_shadow(&chunk[..pre_len - r.len()]);
                    match &self.label {
                        Some(lbl) => {
                            eprintln!("WARN [demux] {lbl}: corrupt frame; skipping (COBS resync)")
                        }
                        None => eprintln!("WARN [demux]: corrupt frame; skipping (COBS resync)"),
                    }
                    self.dump_shadow();
                    chunk = r;
                }
            }
            if chunk.is_empty() {
                break;
            }
        }

        Ok(true)
    }

    /// Append `bytes` to the bounded `shadow` mirror, never exceeding `ACC_CAP`.
    ///
    /// Mirrors the accumulator's own bound: once the accumulator is full it returns
    /// `OverFull` rather than growing, so the shadow stops growing at the same point.
    /// Excess bytes are dropped (the dump shows up to the accumulator's capacity).
    fn extend_shadow(&mut self, bytes: &[u8]) {
        let room = ACC_CAP - self.shadow.len();
        let take = bytes.len().min(room);
        self.shadow.extend_from_slice(&bytes[..take]);
    }

    /// Emit the buffered undecoded bytes (hex + lossy-ASCII) on the same stderr
    /// `WARN [demux]` channel as the surrounding error message, then clear the buffer.
    ///
    /// Called on the `OverFull`/`DeserError` paths just before the bytes are discarded,
    /// so an un-COBS-framed ESP32 panic dump ("Guru Meditation …" + backtrace) survives
    /// into `hil-host` / `podctl logs` output instead of vanishing.
    fn dump_shadow(&mut self) {
        if self.shadow.is_empty() {
            return;
        }
        let dump = render_dump(&self.shadow);
        match &self.label {
            Some(lbl) => eprintln!(
                "WARN [demux] {lbl}: discarded {} undecoded byte(s):\n{dump}",
                self.shadow.len()
            ),
            None => eprintln!(
                "WARN [demux]: discarded {} undecoded byte(s):\n{dump}",
                self.shadow.len()
            ),
        }
        self.shadow.clear();
    }

    /// Write all bytes in `buf` to the underlying transport.
    ///
    /// Provided so callers (e.g. `Harness`) can send data through `FrameReader`
    /// without bypassing its encapsulation boundary.
    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.port.write_all(buf)
    }
}

// ── Harness ───────────────────────────────────────────────────────────────────

/// Host-side request/response harness over a `Transport`.
///
/// Frames requests with COBS encoding, demuxes the response stream (skipping
/// log and heartbeat frames), correlates by request id, and enforces a bounded
/// timeout. Delegates all COBS read/feed/resync work to `FrameReader::pump`.
pub struct Harness {
    reader: FrameReader,
    next_id: u32,
}

/// Errors from `Harness` send operations.
///
/// Note: the original `hil-host` `HarnessError` included an `UnexpectedKind` variant
/// (with `#[allow(dead_code)]`) as a forward-compatibility slot for future `DeviceFrame`
/// variants that warrant an error path. It was never produced by the demux loop and was
/// dead code; it was intentionally omitted during extraction to keep the public API clean.
/// If a new `DeviceFrame` variant is added that needs an error arm, a new variant should
/// be added here at that time.
#[derive(Debug)]
pub enum HarnessError {
    /// Device present and open but no well-formed response within timeout.
    Timeout,
    /// Write error.
    Write(std::io::Error),
    /// Non-timeout read error (e.g. device disconnected mid-run).
    Read(std::io::Error),
    /// Request encoding failed (buffer too small for the command).
    Encode(postcard::Error),
}

impl std::fmt::Display for HarnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HarnessError::Timeout => write!(f, "response timeout"),
            HarnessError::Write(e) => write!(f, "write error: {e}"),
            HarnessError::Read(e) => write!(f, "serial read error: {e}"),
            HarnessError::Encode(e) => write!(f, "request encode failed: {e}"),
        }
    }
}

impl Harness {
    /// Create a harness over an already-open transport.
    pub fn new(port: Box<dyn Transport>) -> Self {
        Self {
            reader: FrameReader::new(port),
            next_id: 1,
        }
    }

    /// Write raw bytes to the device, bypassing request encoding.
    ///
    /// HIL-only: used to inject malformed protocol frames so fault-recovery
    /// checks can exercise the device's DeserError path. Not for normal traffic —
    /// use `send_command*` for that.
    pub fn write_raw(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.reader.write_all(bytes)
    }

    /// Send a command and wait for the correlated response using `RESPONSE_TIMEOUT`.
    ///
    /// Surfaces (prints to stderr) any log or heartbeat frames encountered while
    /// waiting.
    pub fn send_command(
        &mut self,
        command: device_protocol::Command,
    ) -> Result<Response, HarnessError> {
        self.send_command_timeout(command, RESPONSE_TIMEOUT)
    }

    /// Send a command and wait for the correlated response within `timeout`.
    ///
    /// Surfaces (prints to stderr) any log or heartbeat frames encountered while
    /// waiting. Returns `Err(Timeout)` if no well-formed matching response arrives
    /// within the given timeout.
    ///
    /// Log and heartbeat frames that arrive in the same read as the matching
    /// response are dispatched (not dropped) — the pump drains all complete frames
    /// from each read before returning.
    ///
    /// Log lines are printed to stderr but not collected.  To also capture log lines,
    /// use [`send_command_timeout_collect_logs`](Self::send_command_timeout_collect_logs).
    pub fn send_command_timeout(
        &mut self,
        command: device_protocol::Command,
        timeout: Duration,
    ) -> Result<Response, HarnessError> {
        // Thin wrapper: delegate to the collecting variant with a scratch Vec that is
        // immediately discarded.  All demux / deadline / multiple-same-id edge-case
        // logic lives in one place (send_command_timeout_collect_logs).
        self.send_command_timeout_collect_logs(command, timeout, &mut Vec::new())
    }

    /// Send a command and wait for the correlated response within `timeout`,
    /// collecting all log lines seen while waiting into `logs_out`.
    ///
    /// Each collected log line has the same format as `format_log` produces
    /// (i.e. `"[device <Level>] <target>: <message>"`); lines are also printed
    /// to stderr as in `send_command_timeout`. This lets callers assert specific
    /// log events that arrive as `DeviceFrame::Log` frames during a long-running
    /// test (e.g. `wifi: disconnected reason=…`).
    ///
    /// Returns `Err(Timeout)` if no well-formed matching response arrives within
    /// the given timeout.
    pub fn send_command_timeout_collect_logs(
        &mut self,
        command: device_protocol::Command,
        timeout: Duration,
        logs_out: &mut Vec<String>,
    ) -> Result<Response, HarnessError> {
        let id = self.next_id;
        self.next_id += 1;

        let req = Request { id, command };
        let mut req_buf = [0u8; 512];
        let req_len = device_protocol::framing::encode_request(&req, &mut req_buf)
            .map_err(HarnessError::Encode)?;
        self.reader
            .write_all(&req_buf[..req_len])
            .map_err(HarnessError::Write)?;

        let deadline = Instant::now() + timeout;
        let mut matched_response: Option<Response> = None;
        // `pump` returns Ok(false) when the serial port read times out (no data yet).
        // The loop then re-checks the deadline and calls `pump` again.  This relies on the
        // transport always being opened with a non-zero read timeout (which the FakePort
        // simulates by returning TimedOut from an empty rx buffer) so that Ok(false) incurs
        // at least one OS-level sleep rather than a tight CPU spin.

        loop {
            if Instant::now() >= deadline {
                return Err(HarnessError::Timeout);
            }

            self.reader
                .pump(&mut |frame| match frame {
                    DeviceFrame::Response(resp) => {
                        if resp.id == id {
                            matched_response = Some(resp);
                        } else {
                            eprintln!(
                                "WARN [demux]: received response id={} while waiting for id={id}; skipping",
                                resp.id
                            );
                        }
                    }
                    DeviceFrame::Log(log) => {
                        let line = format_log(&log);
                        eprintln!("{line}");
                        logs_out.push(line);
                    }
                    DeviceFrame::Heartbeat => {
                        // Tolerate; ignore.
                    }
                })
                .map_err(HarnessError::Read)?;

            if let Some(resp) = matched_response {
                return Ok(resp);
            }
        }
    }

    /// Idle-drain unsolicited `Log` frames until `max` elapses or (if given) a line
    /// containing `stop_token` is received, with no command in flight.
    ///
    /// Unlike [`send_command_timeout_collect_logs`](Self::send_command_timeout_collect_logs),
    /// this issues no request — it only pumps the read loop until the deadline (or the stop
    /// token), recording a harness-side receipt `Instant` alongside each formatted log line.
    /// Used by behavioral steps that must observe supervisor logging over a wall-clock
    /// window uncorrelated with any single request/response (e.g. timing retry-with-backoff
    /// spacing, or waiting for a known recovery event without burning the full window).
    ///
    /// A `Response` frame arriving during the window (unexpected — no request is in flight)
    /// is printed and skipped, mirroring the mismatched-id handling above. Heartbeats are
    /// ignored. Returns `Err` only on a hard I/O read error; running to the deadline (or
    /// seeing the stop token) with no errors is the normal, successful outcome.
    fn drain_logs_impl(
        &mut self,
        stop_token: Option<&str>,
        max: Duration,
        logs_out: &mut Vec<(Instant, String)>,
    ) -> Result<(), HarnessError> {
        let deadline = Instant::now() + max;
        loop {
            if Instant::now() >= deadline {
                return Ok(());
            }

            let mut stop = false;
            self.reader
                .pump(&mut |frame| match frame {
                    DeviceFrame::Response(resp) => {
                        eprintln!(
                            "WARN [demux]: received response id={} during an idle log-drain \
                             window (no command in flight); skipping",
                            resp.id
                        );
                    }
                    DeviceFrame::Log(log) => {
                        let line = format_log(&log);
                        eprintln!("{line}");
                        if let Some(token) = stop_token {
                            if line.contains(token) {
                                stop = true;
                            }
                        }
                        logs_out.push((Instant::now(), line));
                    }
                    DeviceFrame::Heartbeat => {
                        // Tolerate; ignore.
                    }
                })
                .map_err(HarnessError::Read)?;

            if stop {
                return Ok(());
            }
        }
    }

    /// Idle-drain unsolicited `Log` frames for `duration`, with no command in flight. See
    /// [`drain_logs_impl`](Self::drain_logs_impl) for the full contract.
    pub fn drain_logs_for(
        &mut self,
        duration: Duration,
        logs_out: &mut Vec<(Instant, String)>,
    ) -> Result<(), HarnessError> {
        self.drain_logs_impl(None, duration, logs_out)
    }

    /// Idle-drain unsolicited `Log` frames like [`drain_logs_for`](Self::drain_logs_for),
    /// but return as soon as a line containing `stop_token` is received, rather than
    /// always running to `max`. For windows that only need to observe a known event —
    /// e.g. recovery latency, where the event typically arrives well before the window's
    /// upper bound — this avoids burning the full window's wall-clock time for no gained
    /// coverage. `logs_out` includes the matching line and everything collected up to it;
    /// a caller that also needs a negative assertion over the window (e.g. "no reboot")
    /// evaluates it over whatever was collected, same as `drain_logs_for`.
    ///
    /// Returns `Ok` whether the stop token was seen or `max` elapsed first — the caller
    /// distinguishes the two via `logs_out` (e.g. does the stop line appear).
    pub fn drain_logs_until(
        &mut self,
        stop_token: &str,
        max: Duration,
        logs_out: &mut Vec<(Instant, String)>,
    ) -> Result<(), HarnessError> {
        self.drain_logs_impl(Some(stop_token), max, logs_out)
    }
}

// ── Unit tests (hardware-free) ────────────────────────────────────────────────

#[cfg(any(test, feature = "test-helpers"))]
pub mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{make_frame_reader, make_harness, FakePort};
    use device_protocol::{
        log_tokens, Command, DeviceFrame, LogFrame, LogLevel, Payload, Response, Status, TestName,
    };

    /// A `FakePort` that returns a hard I/O error on any read.
    struct ErrorPort;

    impl std::io::Read for ErrorPort {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "simulated disconnect",
            ))
        }
    }

    impl std::io::Write for ErrorPort {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // ── Demux tests ───────────────────────────────────────────────────────────

    /// `Harness::send_command_timeout` propagates a hard read error as `HarnessError::Read`.
    #[test]
    fn harness_returns_read_error_on_disconnect() {
        let mut harness = Harness::new(Box::new(ErrorPort));
        let result = harness
            .send_command_timeout(Command::RunTest(TestName::Ping), Duration::from_millis(5));
        assert!(
            matches!(result, Err(HarnessError::Read(_))),
            "expected HarnessError::Read; got {:?}",
            result
        );
    }

    /// A stream mixing Response + Log + Heartbeat — harness correlates by id,
    /// does not let log/heartbeat frames corrupt parsing.
    #[test]
    fn demux_correlates_response_by_id_ignoring_log_and_heartbeat() {
        let mut port = FakePort::new();
        // Queue: log, heartbeat, then the expected response.
        port.queue_frame(&DeviceFrame::Log(LogFrame {
            level: LogLevel::Info,
            target: {
                let mut s = heapless::String::new();
                s.push_str("test").unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str("boot").unwrap();
                s
            },
        }));
        port.queue_frame(&DeviceFrame::Heartbeat);
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        let mut harness = make_harness(port);
        // send_command will use id=1 (first command).
        let resp = harness
            .send_command(Command::RunTest(TestName::Ping))
            .unwrap();
        assert_eq!(resp.id, 1);
        assert_eq!(resp.status, Status::Ok);
    }

    /// A response with a mismatched id must be skipped; the next matching one wins.
    #[test]
    fn demux_skips_mismatched_id_response() {
        let mut port = FakePort::new();
        // Stale response from a previous exchange (id=99), then the one we want (id=1).
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 99,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Pong({
                let mut s = heapless::String::new();
                s.push_str("pong").unwrap();
                s
            }),
        }));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Ping))
            .unwrap();
        assert_eq!(resp.id, 1);
        assert!(
            matches!(resp.payload, Payload::Pong(_)),
            "expected Pong payload; got {:?}",
            resp.payload
        );
    }

    /// Timeout: no response frame ever arrives — harness returns Err(Timeout).
    #[test]
    fn harness_timeout_when_no_response() {
        let port = FakePort::new(); // empty rx
        let mut harness = make_harness(port);
        let result = harness
            .send_command_timeout(Command::RunTest(TestName::Ping), Duration::from_millis(5));
        assert!(
            matches!(result, Err(HarnessError::Timeout)),
            "expected Timeout; got {:?}",
            result
        );
    }

    /// Corrupt COBS frame followed by a valid response — harness resyncs and
    /// delivers the valid response.
    #[test]
    fn harness_resyncs_after_corrupt_frame() {
        let mut port = FakePort::new();
        // Push junk bytes that will cause a COBS deserialization error.
        // A zero byte is the COBS frame delimiter; push a frame that decodes to garbage.
        port.rx.extend(&[0x01u8, 0x02, 0x03, 0x00]); // corrupt/unrecognized
                                                     // Then queue the valid response.
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command_timeout(Command::RunTest(TestName::Ping), Duration::from_millis(100))
            .unwrap();
        assert_eq!(resp.id, 1);
        assert_eq!(resp.status, Status::Ok);
    }

    // ── send_command_timeout_collect_logs tests ───────────────────────────────

    fn make_log_frame(target: &str, msg: &str) -> DeviceFrame {
        DeviceFrame::Log(LogFrame {
            level: LogLevel::Warn,
            target: {
                let mut s = heapless::String::new();
                s.push_str(target).unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str(msg).unwrap();
                s
            },
        })
    }

    /// Log frame + heartbeat + matching response → response returned and log captured.
    #[test]
    fn collect_logs_captures_log_lines_and_returns_response() {
        let mut port = FakePort::new();
        port.queue_frame(&make_log_frame(
            "wifi",
            &format!("{}8", log_tokens::WIFI_DISCONNECTED),
        ));
        port.queue_frame(&DeviceFrame::Heartbeat);
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        let mut harness = make_harness(port);
        let mut logs = Vec::new();
        let resp = harness
            .send_command_timeout_collect_logs(
                Command::RunTest(TestName::Ping),
                Duration::from_millis(100),
                &mut logs,
            )
            .unwrap();
        assert_eq!(resp.id, 1);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(logs.len(), 1, "expected exactly one log line captured");
        assert!(
            logs[0].contains("wifi: disconnected reason=8"),
            "captured log must contain the message; got: {:?}",
            logs[0]
        );
    }

    /// Timeout path: no response arrives — Err(Timeout) returned; any logs that
    /// arrived before the deadline are retained in logs_out.
    #[test]
    fn collect_logs_timeout_retains_captured_logs() {
        let mut port = FakePort::new();
        port.queue_frame(&make_log_frame("wifi", "wifi: some event"));
        // No response follows.
        let mut harness = make_harness(port);
        let mut logs = Vec::new();
        let result = harness.send_command_timeout_collect_logs(
            Command::RunTest(TestName::Ping),
            Duration::from_millis(5),
            &mut logs,
        );
        assert!(
            matches!(result, Err(HarnessError::Timeout)),
            "expected Timeout; got {:?}",
            result
        );
        // The log line that arrived before the deadline must be retained.
        assert_eq!(logs.len(), 1, "log line before timeout must be retained");
        assert!(
            logs[0].contains("wifi: some event"),
            "retained log must contain the message; got: {:?}",
            logs[0]
        );
    }

    // ── drain_logs_for tests ──────────────────────────────────────────────────

    /// Two queued log frames are both captured, each with a monotonically
    /// non-decreasing harness-side receipt `Instant`, and the call returns `Ok(())`
    /// once the deadline passes (no response ever "matches" — there's nothing to
    /// match against).
    #[test]
    fn drain_logs_for_captures_lines_with_receipt_timestamps() {
        let mut port = FakePort::new();
        port.queue_frame(&make_log_frame("wifi", "wifi: first event"));
        port.queue_frame(&make_log_frame("wifi", "wifi: second event"));
        let mut harness = make_harness(port);
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_for(Duration::from_millis(20), &mut logs);
        assert!(result.is_ok(), "expected Ok(()); got {result:?}");
        assert_eq!(logs.len(), 2, "expected both queued log lines captured");
        assert!(logs[0].1.contains("wifi: first event"));
        assert!(logs[1].1.contains("wifi: second event"));
        assert!(
            logs[1].0 >= logs[0].0,
            "receipt timestamps must be non-decreasing in arrival order"
        );
    }

    /// A stray `Response` during the drain window (no command in flight) is skipped,
    /// not treated as an error — only a hard I/O error should fail this method.
    #[test]
    fn drain_logs_for_skips_stray_response() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 99,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        port.queue_frame(&make_log_frame("wifi", "wifi: after stray response"));
        let mut harness = make_harness(port);
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_for(Duration::from_millis(20), &mut logs);
        assert!(result.is_ok(), "expected Ok(()); got {result:?}");
        assert_eq!(
            logs.len(),
            1,
            "stray response must not be captured as a log line"
        );
        assert!(logs[0].1.contains("wifi: after stray response"));
    }

    /// A hard I/O read error during the drain window propagates as `HarnessError::Read`.
    #[test]
    fn drain_logs_for_propagates_read_error() {
        let mut harness = Harness::new(Box::new(ErrorPort));
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_for(Duration::from_millis(5), &mut logs);
        assert!(
            matches!(result, Err(HarnessError::Read(_))),
            "expected HarnessError::Read; got {result:?}"
        );
    }

    /// Nothing ever arrives — the method still returns `Ok(())` once the deadline
    /// passes; an idle window is the expected/successful case, not a timeout error.
    #[test]
    fn drain_logs_for_returns_ok_on_empty_window() {
        let port = FakePort::new(); // empty rx
        let mut harness = make_harness(port);
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_for(Duration::from_millis(10), &mut logs);
        assert!(result.is_ok(), "expected Ok(()); got {result:?}");
        assert!(logs.is_empty());
    }

    /// A line containing the stop token ends the drain immediately, well before `max` —
    /// the whole point of the method over `drain_logs_for`.
    #[test]
    fn drain_logs_until_stops_on_matching_line() {
        let mut port = FakePort::new();
        port.queue_frame(&make_log_frame("wifi", "wifi: unrelated event"));
        port.queue_frame(&make_log_frame("wifi", "wifi-supervisor: re-associated"));
        let mut harness = make_harness(port);
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let start = Instant::now();
        let result = harness.drain_logs_until(
            "wifi-supervisor: re-associated",
            Duration::from_secs(30),
            &mut logs,
        );
        assert!(result.is_ok(), "expected Ok(()); got {result:?}");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "must return as soon as the stop token is seen, not wait for the 30s max"
        );
        assert_eq!(
            logs.len(),
            2,
            "both lines, including the matching one, are captured"
        );
        assert!(logs[1].1.contains("wifi-supervisor: re-associated"));
    }

    /// No matching line before `max` elapses still returns `Ok`, with whatever was
    /// collected — same "nothing to match, running out the clock is normal" contract as
    /// `drain_logs_for`.
    #[test]
    fn drain_logs_until_runs_to_max_when_stop_token_never_seen() {
        let mut port = FakePort::new();
        port.queue_frame(&make_log_frame("wifi", "wifi: unrelated event"));
        let mut harness = make_harness(port);
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_until(
            "wifi-supervisor: re-associated",
            Duration::from_millis(20),
            &mut logs,
        );
        assert!(result.is_ok(), "expected Ok(()); got {result:?}");
        assert_eq!(logs.len(), 1);
        assert!(logs[0].1.contains("wifi: unrelated event"));
    }

    /// A hard I/O read error during the drain window propagates as `HarnessError::Read`.
    #[test]
    fn drain_logs_until_propagates_read_error() {
        let mut harness = Harness::new(Box::new(ErrorPort));
        let mut logs: Vec<(Instant, String)> = Vec::new();
        let result = harness.drain_logs_until("stop", Duration::from_millis(5), &mut logs);
        assert!(
            matches!(result, Err(HarnessError::Read(_))),
            "expected HarnessError::Read; got {result:?}"
        );
    }

    // ── PodPort identity tests ────────────────────────────────────────────────

    #[test]
    fn pod_port_identity_with_serial_number() {
        let p = PodPort {
            port_name: "/dev/ttyACM0".to_string(),
            serial_number: Some("ABC123".to_string()),
            mode: PodMode::App,
        };
        assert_eq!(p.identity(), "/dev/ttyACM0 (SN ABC123)");
    }

    #[test]
    fn pod_port_identity_without_serial_number() {
        let p = PodPort {
            port_name: "/dev/ttyACM0".to_string(),
            serial_number: None,
            mode: PodMode::App,
        };
        assert_eq!(p.identity(), "/dev/ttyACM0");
    }

    // ── FrameReader / format_log tests ────────────────────────────────────────

    /// `format_log` produces the canonical `[device <Level>] <target>: <message>` string.
    #[test]
    fn format_log_output() {
        let log = LogFrame {
            level: LogLevel::Info,
            target: {
                let mut s = heapless::String::new();
                s.push_str("target").unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str("message").unwrap();
                s
            },
        };
        assert_eq!(format_log(&log), "[device Info] target: message");
    }

    /// `format_log` escapes control chars in both `target` and `message`, so a device
    /// cannot inject terminal escape sequences through either field. Pins the behavior
    /// across the refactor onto the shared `escape_device_str`.
    #[test]
    fn format_log_escapes_control_chars_in_both_fields() {
        let log = LogFrame {
            level: LogLevel::Warn,
            target: {
                let mut s = heapless::String::new();
                s.push_str("tar\x1bget").unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str("mes\nsage\r").unwrap();
                s
            },
        };
        let out = format_log(&log);
        assert_eq!(out, "[device Warn] tar\\u{1b}get: mes\\nsage\\r");
        assert!(!out.contains('\x1b'));
    }

    // ── escape_device_str ─────────────────────────────────────────────────────

    /// ESC — the terminal-injection vector this function exists to close — is escaped.
    #[test]
    fn escape_device_str_escapes_esc() {
        let out = escape_device_str("\x1b[31mred");
        assert!(!out.contains('\x1b'), "raw ESC survived: {out:?}");
        assert_eq!(out, "\\u{1b}[31mred");
    }

    /// The whitespace control chars are escaped too: a device cannot forge transcript
    /// line structure with embedded newlines or carriage returns.
    #[test]
    fn escape_device_str_escapes_whitespace_controls() {
        assert_eq!(escape_device_str("a\nb\rc\td"), "a\\nb\\rc\\td");
    }

    /// Printable ASCII and printable non-ASCII pass through verbatim, keeping device
    /// messages readable — including the `key=value` tokens host pass-predicates gate on.
    #[test]
    fn escape_device_str_passes_printable_through() {
        assert_eq!(
            escape_device_str("PASS src=amp gpo_write=inert x0d31=0x00"),
            "PASS src=amp gpo_write=inert x0d31=0x00"
        );
        assert_eq!(escape_device_str("café → ok ✓"), "café → ok ✓");
    }

    /// A literal backslash is not doubled: the output is display text, not a reversible
    /// encoding (see the function's contract).
    #[test]
    fn escape_device_str_leaves_literal_backslash_alone() {
        assert_eq!(escape_device_str("a\\nb"), "a\\nb");
    }

    /// Invisible formatting chars are not `Cc`, so `char::is_control` alone would let a
    /// device reverse or split the rendered text. They must escape too.
    #[test]
    fn escape_device_str_escapes_invisible_format_chars() {
        assert_eq!(
            escape_device_str("\u{202e}LIAF"),
            "\\u{202e}LIAF",
            "RTL override must not reach the terminal"
        );
        for c in ['\u{2066}', '\u{2069}', '\u{200b}', '\u{2028}', '\u{2029}'] {
            let out = escape_device_str(&format!("a{c}b"));
            assert!(
                !out.contains(c),
                "U+{:04X} survived escaping: {out:?}",
                c as u32
            );
        }
    }

    /// Empty in, empty out.
    #[test]
    fn escape_device_str_empty_stays_empty() {
        assert_eq!(escape_device_str(""), "");
    }

    /// `pump` invokes `on_frame` exactly once with a single queued Log frame.
    #[test]
    fn pump_dispatches_single_log_frame() {
        let mut port = FakePort::new();
        let log_frame = DeviceFrame::Log(LogFrame {
            level: LogLevel::Warn,
            target: {
                let mut s = heapless::String::new();
                s.push_str("mymod").unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str("hello").unwrap();
                s
            },
        });
        port.queue_frame(&log_frame);
        let mut reader = make_frame_reader(port);
        let mut dispatched: Vec<DeviceFrame> = Vec::new();
        let result = reader.pump(&mut |f| dispatched.push(f));
        assert!(
            matches!(result, Ok(true)),
            "expected Ok(true); got {:?}",
            result
        );
        assert_eq!(dispatched.len(), 1);
        assert!(
            matches!(&dispatched[0], DeviceFrame::Log(l) if l.message == "hello"),
            "unexpected frame: {:?}",
            dispatched[0]
        );
    }

    /// `pump` dispatches two frames queued in a single read.
    #[test]
    fn pump_dispatches_two_frames_from_one_read() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Heartbeat);
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 42,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        let mut reader = make_frame_reader(port);
        let mut dispatched: Vec<DeviceFrame> = Vec::new();
        let result = reader.pump(&mut |f| dispatched.push(f));
        assert!(
            matches!(result, Ok(true)),
            "expected Ok(true); got {:?}",
            result
        );
        assert_eq!(dispatched.len(), 2);
        assert!(matches!(dispatched[0], DeviceFrame::Heartbeat));
        assert!(matches!(dispatched[1], DeviceFrame::Response(ref r) if r.id == 42));
    }

    /// `pump` returns `Ok(false)` on a read timeout (no bytes available).
    #[test]
    fn pump_returns_ok_false_on_timeout() {
        let port = FakePort::new(); // empty rx → TimedOut
        let mut reader = make_frame_reader(port);
        let result = reader.pump(&mut |_| {});
        assert!(
            matches!(result, Ok(false)),
            "expected Ok(false); got {:?}",
            result
        );
    }

    /// `pump` returns `Err` on a non-timeout read error (e.g. device disconnect).
    #[test]
    fn pump_returns_err_on_hard_read_error() {
        let mut reader = FrameReader::new(Box::new(ErrorPort));
        let result = reader.pump(&mut |_| {});
        assert!(result.is_err(), "expected Err; got {:?}", result);
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    }

    /// An un-COBS-framed ESP32 panic dump fed through `pump` survives into the
    /// human-readable dump rather than being silently discarded.
    ///
    /// The ESP32 C panic handler writes raw ASCII ("Guru Meditation Error …" plus a
    /// backtrace) that is not COBS-framed. Such a byte run, once it hits a zero byte,
    /// drives the accumulator into `DeserError`; the resync path discards it. This test
    /// proves the bytes are rendered legibly (hex + lossy-ASCII) before the discard, so
    /// the next real HIL crash round-trip will actually capture the panic text.
    #[test]
    fn pump_dumps_undecoded_panic_text_on_resync() {
        // A panic-dump-shaped ASCII run. Contains no zero bytes itself, so it is fed
        // into the accumulator as `Consumed`; the trailing 0x00 then closes the "frame"
        // and forces a `DeserError` (it is not valid COBS-encoded postcard), triggering
        // the dump + resync path.
        let panic_text = b"Guru Meditation Error: Core 0 panic'ed (LoadProhibited).\n\
            Backtrace: 0x40012345:0x3ffb1234 0x400a9abc:0x3ffb1250\n";

        let mut port = FakePort::new();
        port.rx.extend(panic_text.iter().copied());
        port.rx.push_back(0x00); // close the "frame" → DeserError on the non-COBS run

        let mut reader = make_frame_reader(port);
        let mut dispatched: Vec<DeviceFrame> = Vec::new();

        // Drive pumps until the port drains. No frame should dispatch, and no Err should
        // be returned — the resync path swallows the garbage internally.
        for _ in 0..20 {
            match reader.pump(&mut |f| dispatched.push(f)) {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert_eq!(
            dispatched.len(),
            0,
            "panic garbage must not be decoded as a frame"
        );

        // The dump rendering is what reaches stderr on the resync path. Assert the same
        // renderer produces a view in which the panic text is legible. The ASCII gutter
        // wraps every 16 bytes, so reassemble the gutter contents across rows (joining
        // each row's `|...|` segment) before checking for the readable substrings.
        let dump = render_dump(panic_text);
        let gutter: String = dump
            .lines()
            .filter_map(|line| {
                let start = line.find('|')? + 1;
                let end = line.rfind('|')?;
                (end > start).then(|| line[start..end].to_string())
            })
            .collect();
        // `.` masks the embedded `\n` newlines, but the panic text is otherwise intact.
        assert!(
            gutter.contains("Guru Meditation Error: Core 0 panic'ed"),
            "panic headline not legible in dump gutter ({gutter:?}):\n{dump}"
        );
        assert!(
            gutter.contains("Backtrace: 0x40012345:0x3ffb1234 0x400a9abc:0x3ffb1250"),
            "backtrace not legible in dump gutter ({gutter:?}):\n{dump}"
        );
        // Hex view present too: the 'G' (0x47) of "Guru" appears in the first hex row.
        assert!(
            dump.contains("47 "),
            "hex view missing expected octet:\n{dump}"
        );
    }

    /// Non-printable bytes render as `.` in the ASCII gutter while staying exact in hex.
    #[test]
    fn render_dump_masks_nonprintable_bytes() {
        let bytes = [0x00u8, 0x47, 0x1f, 0x7f, 0x80, 0x41];
        let dump = render_dump(&bytes);
        // ASCII gutter: only 0x47 ('G') and 0x41 ('A') are printable.
        assert!(
            dump.contains("|.G...A|"),
            "unexpected ascii gutter:\n{dump}"
        );
        // Hex column preserves every byte exactly.
        assert!(
            dump.contains("00 47 1f 7f 80 41"),
            "hex column not exact:\n{dump}"
        );
    }

    /// `pump` resets the accumulator and continues dispatching after an `OverFull`.
    ///
    /// `FakePort` delivers data in `READ_BUF`-sized (256-byte) chunks, so the
    /// 1024-byte accumulator fills over four pumps.  The fifth pump feeds the 1025th
    /// non-zero byte, triggering `OverFull`; the accumulator is reset and the COBS
    /// zero delimiter in that same read is fed to the fresh accumulator.  A valid
    /// Heartbeat frame queued after the overflow bytes is then dispatched on a
    /// subsequent pump.
    #[test]
    fn pump_recovers_after_overflow() {
        // Build the port rx manually: 1025 non-zero bytes (fills + overflows the
        // 1024-byte accumulator) followed by a COBS zero delimiter, then a valid
        // Heartbeat frame.
        let mut port = FakePort::new();

        // 1025 non-zero bytes — more than the 1024-byte accumulator capacity.
        port.rx.extend(std::iter::repeat_n(0x01u8, 1025));
        // Zero delimiter: triggers OverFull on the next feed after the accumulator
        // fills, then the remainder (zero) is fed back as an empty COBS frame start.
        port.rx.push_back(0x00);

        // Valid Heartbeat frame follows; queued after the overflow bytes.
        port.queue_frame(&DeviceFrame::Heartbeat);

        let mut reader = make_frame_reader(port);
        let mut dispatched: Vec<DeviceFrame> = Vec::new();

        // Pump repeatedly until we either dispatch a frame or exhaust the port.
        // The OverFull reset happens internally; we keep pumping until we see the
        // Heartbeat or run out of data.
        let mut got_ok_true = false;
        for _ in 0..20 {
            match reader.pump(&mut |f| dispatched.push(f)) {
                Ok(true) => {
                    got_ok_true = true;
                }
                Ok(false) => break, // port drained
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(got_ok_true, "expected at least one Ok(true) pump");
        assert_eq!(
            dispatched.len(),
            1,
            "expected exactly one dispatched frame (Heartbeat)"
        );
        assert!(matches!(dispatched[0], DeviceFrame::Heartbeat));
    }

    /// Split frame: partial first read, remainder in second read.
    ///
    /// Delivers a frame as two separate reads — guards the `FeedResult::Consumed`
    /// partial-frame path.
    #[test]
    fn pump_assembles_frame_split_across_two_reads() {
        // Encode the target frame.
        let frame = DeviceFrame::Response(Response {
            id: 7,
            status: Status::Ok,
            payload: Payload::Empty,
        });
        let mut buf = [0u8; 512];
        let len = device_protocol::framing::encode_device_frame(&frame, &mut buf).unwrap();
        let encoded = buf[..len].to_vec();

        // Split at the midpoint; queue each half as a separate read.
        let mid = len / 2;
        assert!(mid > 0, "frame too short to split");
        let first_half = encoded[..mid].to_vec();
        let second_half = encoded[mid..].to_vec();

        // Use a SplitPort that delivers chunks sequentially, because FakePort
        // drains all available bytes in a single read call.
        struct SplitPort {
            chunks: std::collections::VecDeque<Vec<u8>>,
        }
        impl std::io::Read for SplitPort {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                match self.chunks.pop_front() {
                    None => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "no data")),
                    Some(chunk) => {
                        let n = buf.len().min(chunk.len());
                        buf[..n].copy_from_slice(&chunk[..n]);
                        Ok(n)
                    }
                }
            }
        }
        impl std::io::Write for SplitPort {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut reader = FrameReader::new(Box::new(SplitPort {
            chunks: [first_half, second_half].into_iter().collect(),
        }));

        // First pump: partial frame → Consumed, no dispatch.
        let mut dispatched: Vec<DeviceFrame> = Vec::new();
        let r1 = reader.pump(&mut |f| dispatched.push(f));
        assert!(
            matches!(r1, Ok(true)),
            "expected Ok(true) on first half; got {:?}",
            r1
        );
        assert_eq!(dispatched.len(), 0, "should not dispatch on partial frame");

        // Second pump: completes the frame.
        let r2 = reader.pump(&mut |f| dispatched.push(f));
        assert!(
            matches!(r2, Ok(true)),
            "expected Ok(true) on second half; got {:?}",
            r2
        );
        assert_eq!(dispatched.len(), 1);
        assert!(
            matches!(&dispatched[0], DeviceFrame::Response(r) if r.id == 7),
            "unexpected frame: {:?}",
            dispatched[0]
        );
    }

    /// All three `DeviceFrame` arms (Log, Heartbeat, Response) route through `on_frame`.
    #[test]
    fn pump_routes_all_frame_types_to_closure() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Log(LogFrame {
            level: LogLevel::Error,
            target: {
                let mut s = heapless::String::new();
                s.push_str("t").unwrap();
                s
            },
            message: {
                let mut s = heapless::String::new();
                s.push_str("m").unwrap();
                s
            },
        }));
        port.queue_frame(&DeviceFrame::Heartbeat);
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        }));
        let mut reader = make_frame_reader(port);
        let mut kinds: Vec<&'static str> = Vec::new();
        // FakePort delivers all available bytes in a single read call, so a single
        // pump dispatches all three frames at once.
        let mut record = |f: DeviceFrame| match f {
            DeviceFrame::Log(_) => kinds.push("Log"),
            DeviceFrame::Heartbeat => kinds.push("Heartbeat"),
            DeviceFrame::Response(_) => kinds.push("Response"),
        };
        reader.pump(&mut record).unwrap();
        assert_eq!(kinds, vec!["Log", "Heartbeat", "Response"]);
    }
}
