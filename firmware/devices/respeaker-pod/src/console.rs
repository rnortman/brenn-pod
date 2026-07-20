//! USB-serial-JTAG TX sink and framed `log::Log` backend.
//!
//! Every device→host byte leaves through the all-or-nothing [`UsbSerialTxSink`]
//! behind the [`WRITER`] mutex; [`write_frame`] serializes whole COBS frames so
//! log-frames and response-frames never interleave mid-bytes. [`FramedLogger`]
//! (installed as [`LOGGER`]) turns every `log::*` record into a `DeviceFrame::Log`.

use device_protocol::console::{classify_write, WriteOutcome};
use device_protocol::{DeviceFrame, LogFrame, LogLevel};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

// ── Writer ────────────────────────────────────────────────────────────────────

/// Maximum size of any single device→host frame, in bytes (COBS encoding with
/// 0x00 terminator). The encode buffer is 512 bytes; observed maximums are well
/// below that (e.g. the Identify response is ~55 bytes encoded); 275 is a
/// conservative upper bound that keeps the ring-size assert meaningful.
///
/// Compile-time invariant: every frame must fit in the TX ring when empty. If a
/// frame could exceed the ring, it would drop forever → build-time failure.
const MAX_DEVICE_FRAME_BYTES: usize = 275;
const _: () = assert!(
    MAX_DEVICE_FRAME_BYTES <= 2048,
    "MAX_DEVICE_FRAME_BYTES must fit in the USB-serial-JTAG TX ring (2048 bytes)"
);

/// Size of the stack buffer `write_frame` encodes each frame into. Named so the encode
/// buffer and the size assert below cannot drift apart.
const ENCODE_BUF_BYTES: usize = 512;
const _: () = assert!(
    MAX_DEVICE_FRAME_BYTES <= ENCODE_BUF_BYTES,
    "MAX_DEVICE_FRAME_BYTES must fit the write_frame encode buffer"
);

/// Count of whole frames dropped because `usb_serial_jtag_write_bytes` returned 0
/// (TX ring lacked room for the whole frame). Incremented atomically; never decremented.
/// Wraps after 2^32 drops (irrelevant in practice). Exposed via DeviceHealthCheck
/// `TestResult` as `tx_write_failures=N`. A non-zero value is environmental (ring fills
/// while host port is closed), not a device fault, so health checks assert its format
/// but not its magnitude.
pub(crate) static TX_WRITE_FAILURES: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Count of `DeviceFrame` values dropped because `encode_device_frame` returned `Err` in
/// `write_frame` (the encode buffer was too small for the frame). Incremented atomically;
/// never decremented. Wraps after 2^32 drops (irrelevant in practice — a device emitting
/// billions of encode failures has long since failed its health checks).
///
/// Unlike `TX_WRITE_FAILURES` (environmental — the host port closing fills the ring), a
/// non-zero value here is always a firmware bug: with the current schema no reachable frame
/// can overflow the encode buffer, so any increment means the schema outgrew the buffer or an
/// encoder regressed. It therefore follows the `WRITER_STATE_ANOMALIES` reporting semantics
/// (surfaced by `DeviceHealthCheck` as `encode_failures=N` and asserted zero), not the
/// `TX_WRITE_FAILURES` semantics (format only).
pub(crate) static ENCODE_FAILURES: AtomicU32 = AtomicU32::new(0);

/// All-or-nothing USB-serial-JTAG TX sink.
///
/// Calls `usb_serial_jtag_write_bytes` directly rather than going through
/// `std::io::Stdout` (whose VFS write path writes byte-by-byte and can split a
/// frame across two writes if the TX ring fills mid-write). The direct call is
/// an all-or-nothing primitive — it either pushes the entire frame or copies
/// nothing — so mid-frame truncation is structurally impossible.
pub(crate) struct UsbSerialTxSink;

impl UsbSerialTxSink {
    /// Write `bytes` to the USB-serial-JTAG TX ring in one atomic call.
    ///
    /// Returns `WriteOutcome::Sent` if the frame was pushed, `WriteOutcome::Dropped`
    /// if the ring lacked room. On drop, increments `TX_WRITE_FAILURES`.
    ///
    /// # Safety
    /// Caller must hold the `WRITER` mutex (serializes access; prevents concurrent
    /// callers from racing each other's writes on the ring).
    pub(crate) fn write_frame_bytes(&self, bytes: &[u8]) -> WriteOutcome {
        debug_assert!(
            !bytes.is_empty(),
            "write_frame_bytes called with empty slice (encode_device_frame returned Ok(0))"
        );
        debug_assert!(
            bytes.len() <= MAX_DEVICE_FRAME_BYTES,
            "write_frame_bytes: frame {} bytes exceeds MAX_DEVICE_FRAME_BYTES ({})",
            bytes.len(),
            MAX_DEVICE_FRAME_BYTES
        );
        // SAFETY: `bytes` is a valid slice for the duration of the call; ticks_to_wait=0
        // requests non-blocking behavior (returns immediately if ring is full).
        let n = unsafe {
            esp_idf_svc::sys::usb_serial_jtag_write_bytes(
                bytes.as_ptr() as *const core::ffi::c_void,
                bytes.len(),
                0,
            )
        } as usize;
        let outcome = classify_write(n, bytes.len());
        if outcome == WriteOutcome::Dropped {
            TX_WRITE_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        outcome
    }
}

/// Mutex-guarded USB-serial-JTAG sink. Serializes whole COBS frames so log-frames
/// and response-frames never interleave mid-bytes.
///
/// The `Option` is a boot-order gate over a zero-sized `UsbSerialTxSink`: it is `None`
/// until `main` installs `Some(UsbSerialTxSink)` (after the USB-serial-JTAG driver is
/// installed) and is never re-cleared. Because the sink is a ZST over an always-installed
/// driver, a `None` observed after boot does not mean the sink is unusable — it means the
/// discriminant byte was externally modified (memory corruption). `write_encoded_frame`
/// therefore records the anomaly and emits anyway rather than aborting the reporting channel.
pub(crate) static WRITER: Mutex<Option<UsbSerialTxSink>> = Mutex::new(None);

/// Count of times the `WRITER` `Option` was observed `None` after boot — an invariant
/// violation (the state-once gate never returns to `None` in program order), so a non-zero
/// value always means the discriminant byte was externally modified. Surfaced and asserted
/// zero by `DeviceHealthCheck`.
pub(crate) static WRITER_STATE_ANOMALIES: AtomicU32 = AtomicU32::new(0);

/// Push already-encoded frame bytes through the `WRITER` sink without ever panicking.
///
/// Both the log path (`write_frame`) and the response-emit path (`dispatch_request`) route
/// their encoded frames through here so the WRITER-access discipline lives in one place.
/// A poisoned lock is recovered (unreachable under panic=abort; defensive). A `None`
/// observation increments `WRITER_STATE_ANOMALIES` and emits through a fresh `UsbSerialTxSink`
/// (a ZST over the always-installed driver), so a corrupted state byte is recorded without
/// destroying the channel that must carry the diagnosis.
///
/// LOCK DISCIPLINE: no `log::*` inside the critical section — `std::sync::Mutex` is
/// non-reentrant, and a log call here would re-enter `write_frame` → `WRITER.lock()` →
/// self-deadlock. Callers encode outside the lock; only the write is guarded.
pub(crate) fn write_encoded_frame(bytes: &[u8]) {
    let mut guard = match WRITER.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match guard.as_mut() {
        // All-or-nothing write: either the whole frame is pushed to the TX ring or nothing
        // is. A drop increments TX_WRITE_FAILURES (diagnostic). No partial write is possible.
        Some(sink) => {
            let _ = sink.write_frame_bytes(bytes);
        }
        None => {
            WRITER_STATE_ANOMALIES.fetch_add(1, Ordering::Relaxed);
            let _ = UsbSerialTxSink.write_frame_bytes(bytes);
        }
    }
}

pub(crate) fn write_frame(frame: &DeviceFrame) {
    let mut buf = [0u8; ENCODE_BUF_BYTES];
    match device_protocol::framing::encode_device_frame(frame, &mut buf) {
        Ok(len) => write_encoded_frame(&buf[..len]),
        Err(e) => {
            // Encoding failed (e.g. buffer too small). Count it so release builds carry a
            // runtime signal (debug_assert compiles to nothing in release); callers that hold
            // a request id should send a structured failure response instead of calling
            // write_frame directly (see dispatch_request).
            ENCODE_FAILURES.fetch_add(1, Ordering::Relaxed);
            debug_assert!(false, "encode_device_frame failed: {:?}", e);
        }
    }
}

// ── Log backend ───────────────────────────────────────────────────────────────

pub(crate) struct FramedLogger;

impl log::Log for FramedLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let level = match record.level() {
            log::Level::Error => LogLevel::Error,
            log::Level::Warn => LogLevel::Warn,
            log::Level::Info => LogLevel::Info,
            log::Level::Debug => LogLevel::Debug,
            log::Level::Trace => LogLevel::Trace,
        };

        // format_truncating truncates at a UTF-8 char boundary instead of dropping
        // over-long output to empty. The cap N is inferred from each LogFrame
        // field's type, so the field declaration is the single source of truth
        // for the size.
        let target = device_protocol::format_truncating(format_args!("{}", record.target()));
        let message = device_protocol::format_truncating(*record.args());

        write_frame(&DeviceFrame::Log(LogFrame {
            level,
            target,
            message,
        }));
    }

    fn flush(&self) {}
}

pub(crate) static LOGGER: FramedLogger = FramedLogger;
