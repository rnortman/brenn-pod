//! XVF3800 (XMOS VocalFusion XU316) control protocol, independent of transport.
//!
//! The chip's control interface is one logical protocol — a resource ID (`resid`,
//! the servicer), a command ID (`cmd`, with bit 7 set for reads), and a status byte
//! returned ahead of the payload — reachable over I2C on-device and over USB vendor
//! control transfers from a host. This crate owns everything that is the same on
//! both: the resid/cmd/length constants, the status semantics, the retry policy,
//! the payload decoders, and the I2C header arithmetic.
//!
//! Transports implement [`ControlTransport`] (one attempt, returning the device's
//! status byte) and the shared drivers [`control_read`] / [`control_write`] apply
//! the retry policy on top. That keeps WAIT/RETRY handling — the part where the
//! two platforms must not diverge — in one place.
//!
//! `no_std` and dependency-free; every item here is host-testable.

#![cfg_attr(not(test), no_std)]

/// XVF3800 I2C control address (7-bit). Assumed to be exposed on the stock
/// l16k2ch firmware image; not separately confirmed for other firmware images.
pub const I2C_ADDR: u8 = 0x2C;

// ── Resource and command IDs ─────────────────────────────────────────────────

/// DFU controller resource ID.
pub const DFU_RESID: u8 = 240;

/// DFU GETVERSION command ID — the firmware version of the DFU controller.
pub const DFU_GETVERSION_CMD: u8 = 88;

/// Application servicer resource ID. Carries the application-level `VERSION`
/// register, which is the version the USB path reads at bring-up.
pub const APPLICATION_SERVICER_RESID: u8 = 48;

/// `VERSION` command ID on [`APPLICATION_SERVICER_RESID`].
pub const VERSION_CMD: u8 = 0;

/// Version payload length in bytes (major + minor + patch = 3; the I2C read totals
/// 4 with the status byte). Shared by [`DFU_GETVERSION_CMD`] and [`VERSION_CMD`],
/// which both return a three-byte triple.
pub const VERSION_READ_LEN: usize = 3;

/// `BLD_MSG` command ID on [`APPLICATION_SERVICER_RESID`] — the build string the
/// firmware was compiled with, `char × 50`.
pub const BLD_MSG_CMD: u8 = 1;

/// `BLD_MSG` payload length in bytes: 50 characters, NUL-padded by the firmware.
pub const BLD_MSG_READ_LEN: usize = 50;

/// `REBOOT` command ID on [`APPLICATION_SERVICER_RESID`] — write-only, `uint8 × 1`.
/// Any value reboots the chip and returns every parameter to its build default.
/// There is no read side and no acknowledgement: the board drops off the bus.
pub const REBOOT_CMD: u8 = 7;

/// `REBOOT` payload length in bytes. The value itself is ignored by the firmware.
pub const REBOOT_WRITE_LEN: usize = 1;

/// AEC servicer resource ID.
pub const AEC_RESID: u8 = 33;

/// `AEC_ASROUTONOFF` command ID (`int32 × 1`). 0 outputs the AEC residuals, one
/// channel per microphone; 1 outputs the ASR-processed signal, one channel per
/// beamformer beam. Off by default.
pub const AEC_ASROUTONOFF_CMD: u8 = 35;

/// `AEC_ASROUTGAIN` command ID (`float × 1`) — fixed gain on the ASR output, applied
/// only while [`AEC_ASROUTONOFF_CMD`] is 1. Valid range 0.0 to 1000.0.
pub const AEC_ASROUTGAIN_CMD: u8 = 36;

/// `AEC_AECCONVERGED` command ID (`int32 × 1`, read-only) — whether the linear echo
/// canceller has converged. Latching: once the firmware sets it, it never clears.
pub const AEC_AECCONVERGED_CMD: u8 = 3;

/// Post-processing servicer resource ID. Owns the adaptive stages — AGC, noise
/// suppression, echo suppression — that sit between the beamformer and the board's
/// post-processed output channel.
pub const PP_RESID: u8 = 17;

/// `PP_AGCONOFF` command ID (`int32 × 1`) — whether the automatic gain control is
/// permitted to adapt.
pub const PP_AGCONOFF_CMD: u8 = 10;

/// `PP_AGCGAIN` command ID (`float × 1`) — the AGC's current linear gain factor.
pub const PP_AGCGAIN_CMD: u8 = 13;

/// `PP_MIN_NS` command ID (`float × 1`) — gain floor for stationary noise
/// suppression.
pub const PP_MIN_NS_CMD: u8 = 21;

/// `PP_MIN_NN` command ID (`float × 1`) — gain floor for non-stationary noise
/// suppression.
pub const PP_MIN_NN_CMD: u8 = 22;

/// `PP_ECHOONOFF` command ID (`int32 × 1`) — whether echo suppression runs.
pub const PP_ECHOONOFF_CMD: u8 = 23;

/// `PP_DTSENSITIVE` command ID (`int32 × 1`) — the echo-suppression/double-talk
/// trade-off.
pub const PP_DTSENSITIVE_CMD: u8 = 31;

/// Payload length of every single-value AEC and PP register named here: one
/// `int32` or one `float`, four bytes either way (five on the wire with the status
/// byte).
pub const SCALAR_READ_LEN: usize = 4;

/// `AEC_AZIMUTH_VALUES` command ID. Read byte = 75 | 0x80 = 0xCB.
pub const AEC_AZIMUTH_VALUES_CMD: u8 = 75;

/// `AEC_AZIMUTH_VALUES` payload length in bytes: 4 × f32 = 16 payload + 1 status = 17 total.
pub const AEC_AZIMUTH_READ_LEN: usize = 16;

/// `AEC_SPENERGY_VALUES` command ID. Read byte = 80 | 0x80 = 0xD0.
pub const AEC_SPENERGY_VALUES_CMD: u8 = 80;

/// `AEC_SPENERGY_VALUES` payload length in bytes: 4 × f32 = 16 payload + 1 status = 17 total.
/// Same layout as [`AEC_AZIMUTH_READ_LEN`].
pub const AEC_SPENERGY_READ_LEN: usize = 16;

/// Audio manager servicer resource ID. Owns the output-routing registers: which
/// internal source each of the board's two output channels carries.
pub const AUDIO_MGR_RESID: u8 = 35;

/// `AUDIO_MGR_OP_L` command ID — the left output channel's routing.
pub const AUDIO_MGR_OP_L_CMD: u8 = 15;

/// `AUDIO_MGR_OP_R` command ID — the right output channel's routing.
pub const AUDIO_MGR_OP_R_CMD: u8 = 19;

/// Payload length of either output-routing read: two bytes, a category and a source
/// within it (3 total with the status byte). The bytes are reported raw — no code
/// here interprets them, because no reading of them from this firmware has been
/// reviewed yet.
pub const AUDIO_MGR_OP_READ_LEN: usize = 2;

/// GPO servicer resource ID. The general-purpose-output lines (amp enable, mute LED)
/// are read/written through this resid.
pub const GPO_RESID: u8 = 20;

/// GPO servicer command 0 — the GPO vector accessor. Read (with [`READ_BIT`])
/// returns the vector. **Writing to cmd 0 is accepted-and-DONE but inert** on the
/// respeaker-flex firmware image — cmd 0 is a read-only accessor and the write never
/// moves any GPO line. The respeaker-pod self-test issues the write precisely to
/// assert that inertness as a regression guard.
pub const GPO_CMD: u8 = 0;

/// GPO vector length in bytes for the flashed respeaker-flex firmware image: **6**,
/// bench-confirmed. A shorter length (5, matching some external documentation for a
/// different firmware variant) is rejected by this image with a wrong-command-length
/// status; only 6 works. X0D31 (the amp-enable line) stays at index 2 of the 6-byte
/// vector regardless.
pub const GPO_VECTOR_LEN: usize = 6;

/// Settle delay after a GPO write before the next transaction to the XVF3800 (~5 ms).
/// The device needs time to apply the change and will NAK transactions issued too soon.
pub const GPO_SETTLE_MS: u32 = 5;

/// Maximum payload bytes across all known XVF3800 control registers, used to size
/// transport-side staging buffers. The longest is [`BLD_MSG_READ_LEN`] at 50 bytes
/// (51 total with status); 64 leaves room for future registers and for the 3-byte
/// I2C header ahead of a written payload.
pub const CTRL_BUF_CAPACITY: usize = 64;

// ── Operator-facing register labels ──────────────────────────────────────────
//
// One label per register a caller names when it reports a read or a write: the
// name in the vendor's register table, with the address the driver issues. They
// live beside the constants they describe so a firmware revision that moves a
// command id moves the prose with it, and `every_label_states_its_own_address`
// holds them to it.

/// [`VERSION_CMD`] on [`APPLICATION_SERVICER_RESID`], as a reader names it.
pub const VERSION_LABEL: &str = "VERSION (resid 48 cmd 0)";

/// [`BLD_MSG_CMD`] on [`APPLICATION_SERVICER_RESID`], as a reader names it.
pub const BLD_MSG_LABEL: &str = "BLD_MSG (resid 48 cmd 1)";

/// [`REBOOT_CMD`] on [`APPLICATION_SERVICER_RESID`], as a reader names it.
pub const REBOOT_LABEL: &str = "REBOOT (resid 48 cmd 7)";

/// [`AEC_ASROUTONOFF_CMD`] on [`AEC_RESID`], as a reader names it.
pub const AEC_ASROUTONOFF_LABEL: &str = "AEC_ASROUTONOFF (resid 33 cmd 35)";

/// [`AEC_AZIMUTH_VALUES_CMD`] on [`AEC_RESID`], as a reader names it.
pub const AEC_AZIMUTH_VALUES_LABEL: &str = "AEC_AZIMUTH_VALUES (resid 33 cmd 75)";

/// [`AEC_SPENERGY_VALUES_CMD`] on [`AEC_RESID`], as a reader names it.
pub const AEC_SPENERGY_VALUES_LABEL: &str = "AEC_SPENERGY_VALUES (resid 33 cmd 80)";

/// [`AUDIO_MGR_OP_L_CMD`] on [`AUDIO_MGR_RESID`], as a reader names it.
pub const AUDIO_MGR_OP_L_LABEL: &str = "AUDIO_MGR_OP_L (resid 35 cmd 15)";

/// [`AUDIO_MGR_OP_R_CMD`] on [`AUDIO_MGR_RESID`], as a reader names it.
pub const AUDIO_MGR_OP_R_LABEL: &str = "AUDIO_MGR_OP_R (resid 35 cmd 19)";

// ── Status semantics ─────────────────────────────────────────────────────────

/// Read bit ORed into the command byte to make a command a read.
pub const READ_BIT: u8 = 0x80;

/// Status: the servicer completed the command.
pub const STATUS_DONE: u8 = 0;

/// Status: the servicer is not ready yet — re-issue the same command.
pub const STATUS_WAIT: u8 = 1;

/// Status: `SERVICER_COMMAND_RETRY` — re-issue the same command after a short delay.
pub const STATUS_RETRY: u8 = 0x40;

/// Whether `status` is transient (the command should be re-issued) rather than a
/// final answer. Any status that is neither [`STATUS_WAIT`] nor [`STATUS_RETRY`] is
/// final: [`STATUS_DONE`] is success and everything else is an error to report.
pub const fn should_retry(status: u8) -> bool {
    status == STATUS_WAIT || status == STATUS_RETRY
}

// ── Retry policy ─────────────────────────────────────────────────────────────

/// How persistently a transport re-issues a command that returned a transient status.
///
/// `max_retries` counts re-issues, so a call performs at most `max_retries + 1`
/// transactions. `delay_ms` is slept between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Re-issues after the initial attempt.
    pub max_retries: u32,
    /// Delay between attempts, milliseconds.
    pub delay_ms: u32,
}

/// I2C retry policy: 8 re-issues (9 transactions max) 1 ms apart. 1 ms is the
/// FreeRTOS `delay_ms` minimum granularity, and a bounded budget keeps the shared
/// I2C bus lock hold time to ~15 ms worst case.
pub const I2C_RETRY: RetryPolicy = RetryPolicy {
    max_retries: 8,
    delay_ms: 1,
};

/// USB retry policy: 100 re-issues 10 ms apart — modelled on the vendor host tool's
/// retry budget (~100 attempts × 10 ms) rather than copying it. Three deliberate
/// differences: this budget performs 101 transactions where the vendor caps at 100
/// evaluated ones; the delay falls before each re-issue, not after it; and
/// [`STATUS_WAIT`] is retried here, while the vendor's USB path treats it as fatal.
///
/// USB control transfers cross a kernel driver and a bus scheduler, so the chip is far
/// more likely to answer WAIT/RETRY here than on I2C, and no bus lock is held while we
/// wait.
pub const USB_RETRY: RetryPolicy = RetryPolicy {
    max_retries: 100,
    delay_ms: 10,
};

// ── Transport seam ───────────────────────────────────────────────────────────

/// One XVF3800 control transport: a single attempt per call, no retry.
///
/// Framing is the implementor's business; both return the device's status byte
/// and fill `payload` with exactly `payload.len()` bytes.
///
/// `attempt` is the retry driver's 1-based transaction number for the control call
/// in progress, supplied so a transport's error logs can distinguish a
/// first-attempt failure (wrong address, dead bus) from a retry-exhaustion failure
/// (marginal bus, clock-stretch) without keeping a counter of its own.
pub trait ControlTransport {
    /// Transport-level failure (bus fault, NAK, timeout, no device).
    type Error;

    /// Issue one control READ and return the status byte.
    ///
    /// `payload` is filled with `payload.len()` payload bytes whenever the device
    /// answered at all, regardless of status — callers log raw payloads next to
    /// failing statuses, so a transient status must not leave stale bytes behind.
    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error>;

    /// Issue one control WRITE and return the status byte.
    fn control_write_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &[u8],
        attempt: u32,
    ) -> Result<u8, Self::Error>;

    /// Sleep `ms` milliseconds between retries.
    fn delay_ms(&mut self, ms: u32);
}

/// Perform an XVF3800 control READ, retrying transient statuses per `policy`.
///
/// # Returns
/// - `Ok((status, attempts))` — the final status byte and the total transaction
///   count (≥ 1). `status == `[`STATUS_DONE`] is success; any other value is a
///   fatal status or an exhausted retry budget, and `attempts > 1` distinguishes
///   the two. `payload` holds the last response's bytes either way.
/// - `Err(_)` — the transport failed; `payload` contents are unspecified.
pub fn control_read<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    resid: u8,
    cmd: u8,
    payload: &mut [u8],
) -> Result<(u8, u32), T::Error> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let status = transport.control_read_once(resid, cmd, payload, attempt)?;
        if should_retry(status) && attempt <= policy.max_retries {
            transport.delay_ms(policy.delay_ms);
            continue;
        }
        return Ok((status, attempt));
    }
}

/// Perform an XVF3800 control WRITE, retrying transient statuses per `policy`.
///
/// The write counterpart to [`control_read`]; same return contract.
pub fn control_write<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    resid: u8,
    cmd: u8,
    payload: &[u8],
) -> Result<(u8, u32), T::Error> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let status = transport.control_write_once(resid, cmd, payload, attempt)?;
        if should_retry(status) && attempt <= policy.max_retries {
            transport.delay_ms(policy.delay_ms);
            continue;
        }
        return Ok((status, attempt));
    }
}

// ── I2C framing ──────────────────────────────────────────────────────────────

/// I2C control-read header: `[resid, cmd | READ_BIT, read_len + 1]`.
///
/// The length byte counts the status byte the device prepends, hence `+ 1`.
/// Panics if `read_len + 1` does not fit a byte — a caller contract violation,
/// not a runtime condition.
pub const fn i2c_read_header(resid: u8, cmd: u8, read_len: usize) -> [u8; 3] {
    let total = read_len + 1;
    assert!(
        total <= u8::MAX as usize,
        "i2c_read_header: read_len + 1 exceeds one byte"
    );
    [resid, cmd | READ_BIT, total as u8]
}

/// I2C control-write header: `[resid, cmd, payload_len]`, followed on the wire by
/// the payload itself. The command byte carries no [`READ_BIT`], and the length
/// byte counts payload bytes only — a write has no status byte on the way out.
///
/// Panics if `payload_len` does not fit a byte.
pub const fn i2c_write_header(resid: u8, cmd: u8, payload_len: usize) -> [u8; 3] {
    assert!(
        payload_len <= u8::MAX as usize,
        "i2c_write_header: payload_len exceeds one byte"
    );
    [resid, cmd, payload_len as u8]
}

// ── Payload decoders ─────────────────────────────────────────────────────────

/// Decode four consecutive IEEE-754 little-endian f32 values from a 16-byte payload.
pub fn decode_f32x4(p: &[u8; 16]) -> [f32; 4] {
    [
        f32::from_le_bytes([p[0], p[1], p[2], p[3]]),
        f32::from_le_bytes([p[4], p[5], p[6], p[7]]),
        f32::from_le_bytes([p[8], p[9], p[10], p[11]]),
        f32::from_le_bytes([p[12], p[13], p[14], p[15]]),
    ]
}

/// Decode one IEEE-754 little-endian f32 from a 4-byte payload.
pub fn decode_f32(p: &[u8; 4]) -> f32 {
    f32::from_le_bytes(*p)
}

/// Decode one little-endian i32 from a 4-byte payload.
pub fn decode_i32(p: &[u8; 4]) -> i32 {
    i32::from_le_bytes(*p)
}

/// Encode one i32 as the 4-byte little-endian payload a write carries.
pub fn encode_i32(v: i32) -> [u8; 4] {
    v.to_le_bytes()
}

/// The printable prefix of a NUL-padded character register, trimmed of trailing
/// whitespace.
///
/// Bytes outside printable ASCII end the string rather than being rendered: a
/// register that answered with the wrong length or with binary must not put control
/// characters into the journal.
pub fn decode_ascii(payload: &[u8]) -> &str {
    let end = payload
        .iter()
        .position(|b| !(0x20..0x7f).contains(b))
        .unwrap_or(payload.len());
    // Every byte below `end` is printable ASCII, so the slice is valid UTF-8.
    core::str::from_utf8(&payload[..end])
        .unwrap_or("")
        .trim_end()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every label ends in the address of the register it names. The label is what
    /// an operator reads to find the register in the vendor's table, so a command
    /// id that moves and leaves its prose behind is a label that lies; this is the
    /// check that fails instead.
    #[test]
    fn every_label_states_its_own_address() {
        for (name, label, resid, cmd) in [
            (
                "VERSION",
                VERSION_LABEL,
                APPLICATION_SERVICER_RESID,
                VERSION_CMD,
            ),
            (
                "BLD_MSG",
                BLD_MSG_LABEL,
                APPLICATION_SERVICER_RESID,
                BLD_MSG_CMD,
            ),
            (
                "REBOOT",
                REBOOT_LABEL,
                APPLICATION_SERVICER_RESID,
                REBOOT_CMD,
            ),
            (
                "AEC_ASROUTONOFF",
                AEC_ASROUTONOFF_LABEL,
                AEC_RESID,
                AEC_ASROUTONOFF_CMD,
            ),
            (
                "AEC_AZIMUTH_VALUES",
                AEC_AZIMUTH_VALUES_LABEL,
                AEC_RESID,
                AEC_AZIMUTH_VALUES_CMD,
            ),
            (
                "AEC_SPENERGY_VALUES",
                AEC_SPENERGY_VALUES_LABEL,
                AEC_RESID,
                AEC_SPENERGY_VALUES_CMD,
            ),
            (
                "AUDIO_MGR_OP_L",
                AUDIO_MGR_OP_L_LABEL,
                AUDIO_MGR_RESID,
                AUDIO_MGR_OP_L_CMD,
            ),
            (
                "AUDIO_MGR_OP_R",
                AUDIO_MGR_OP_R_LABEL,
                AUDIO_MGR_RESID,
                AUDIO_MGR_OP_R_CMD,
            ),
        ] {
            // The whole spelling, not only the address: the name is what a reader
            // greps the vendor's table with, so a right address under a wrong name
            // sends them to the wrong register.
            assert_eq!(label, format!("{name} (resid {resid} cmd {cmd})"));
        }
    }

    // ── Scripted fake transport ──────────────────────────────────────────────

    /// Transport error the fake returns; stands in for `EspError` / `rusb::Error`.
    #[derive(Debug, PartialEq, Eq)]
    struct FakeError;

    /// One recorded transport call.
    #[derive(Debug, PartialEq, Eq)]
    struct Call {
        read: bool,
        resid: u8,
        cmd: u8,
        len: usize,
        /// The 1-based attempt number the retry driver handed the transport.
        attempt: u32,
    }

    /// Replays a scripted status sequence, recording calls and delays.
    struct Scripted {
        /// Status per attempt, consumed front to back.
        statuses: Vec<u8>,
        /// Attempt index (0-based) at which to return `Err(FakeError)` instead.
        err_at: Option<usize>,
        /// Payload bytes the fake writes into every read's buffer.
        fill: u8,
        calls: Vec<Call>,
        delays: Vec<u32>,
    }

    impl Scripted {
        fn new(statuses: &[u8]) -> Self {
            Self {
                statuses: statuses.to_vec(),
                err_at: None,
                fill: 0xAB,
                calls: Vec::new(),
                delays: Vec::new(),
            }
        }

        /// A fake that answers `status` forever — for exhaustion tests, where the
        /// attempt count is exactly what is under test.
        fn repeating(status: u8, attempts: usize) -> Self {
            Self::new(&vec![status; attempts])
        }

        fn failing_at(mut self, attempt: usize) -> Self {
            self.err_at = Some(attempt);
            self
        }

        fn next_status(&mut self) -> Result<u8, FakeError> {
            if self.err_at == Some(self.calls.len() - 1) {
                return Err(FakeError);
            }
            assert!(
                !self.statuses.is_empty(),
                "Scripted: transport called more times than the script allows \
                 ({} calls so far)",
                self.calls.len()
            );
            Ok(self.statuses.remove(0))
        }
    }

    impl ControlTransport for Scripted {
        type Error = FakeError;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.calls.push(Call {
                read: true,
                resid,
                cmd,
                len: payload.len(),
                attempt,
            });
            let status = self.next_status()?;
            payload.fill(self.fill);
            Ok(status)
        }

        fn control_write_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &[u8],
            attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.calls.push(Call {
                read: false,
                resid,
                cmd,
                len: payload.len(),
                attempt,
            });
            self.next_status()
        }

        fn delay_ms(&mut self, ms: u32) {
            self.delays.push(ms);
        }
    }

    #[test]
    fn should_retry_only_on_wait_and_retry() {
        assert!(should_retry(STATUS_WAIT));
        assert!(should_retry(STATUS_RETRY));
        assert!(!should_retry(STATUS_DONE));
        // Everything else is a final (error) status, including neighbours of the
        // two transient codes — 0x41 must not be mistaken for 0x40.
        for status in [0x02u8, 0x03, 0x3F, 0x41, 0x80, 0xFF] {
            assert!(!should_retry(status), "status {status:#04x}");
        }
    }

    #[test]
    fn i2c_read_header_golden_bytes() {
        // AEC_AZIMUTH_VALUES: write [33, 0xCB, 17] then read 17 bytes.
        assert_eq!(
            i2c_read_header(AEC_RESID, AEC_AZIMUTH_VALUES_CMD, AEC_AZIMUTH_READ_LEN),
            [33, 0xCB, 17]
        );
        // AEC_SPENERGY_VALUES: same shape, cmd 80 → 0xD0.
        assert_eq!(
            i2c_read_header(AEC_RESID, AEC_SPENERGY_VALUES_CMD, AEC_SPENERGY_READ_LEN),
            [33, 0xD0, 17]
        );
        // DFU GETVERSION: 3 payload bytes → length byte 4; cmd 88 → 0xD8.
        assert_eq!(
            i2c_read_header(DFU_RESID, DFU_GETVERSION_CMD, VERSION_READ_LEN),
            [240, 0xD8, 4]
        );
        // Application servicer VERSION: cmd 0 still carries the read bit.
        assert_eq!(
            i2c_read_header(APPLICATION_SERVICER_RESID, VERSION_CMD, VERSION_READ_LEN),
            [48, 0x80, 4]
        );
        // GPO vector read: 6 payload bytes → length byte 7.
        assert_eq!(
            i2c_read_header(GPO_RESID, GPO_CMD, GPO_VECTOR_LEN),
            [20, 0x80, 7]
        );
    }

    #[test]
    fn i2c_write_header_golden_bytes() {
        // GPO vector write: no read bit, length counts payload bytes only.
        assert_eq!(
            i2c_write_header(GPO_RESID, GPO_CMD, GPO_VECTOR_LEN),
            [20, 0, 6]
        );
        // A command whose ID has bit 7 clear stays clear; the two headers differ in
        // exactly the read bit and the +1.
        let read = i2c_read_header(AEC_RESID, AEC_SPENERGY_VALUES_CMD, 16);
        let write = i2c_write_header(AEC_RESID, AEC_SPENERGY_VALUES_CMD, 16);
        assert_eq!(read[1] & !READ_BIT, write[1]);
        assert_eq!(read[2], write[2] + 1);
    }

    #[test]
    #[should_panic(expected = "read_len + 1 exceeds one byte")]
    fn i2c_read_header_rejects_oversized_length() {
        i2c_read_header(AEC_RESID, AEC_SPENERGY_VALUES_CMD, 255);
    }

    #[test]
    #[should_panic(expected = "payload_len exceeds one byte")]
    fn i2c_write_header_rejects_oversized_payload() {
        i2c_write_header(GPO_RESID, GPO_CMD, 256);
    }

    #[test]
    fn read_done_on_first_attempt() {
        let mut t = Scripted::new(&[STATUS_DONE]);
        let mut payload = [0u8; 16];
        let got = control_read(
            &mut t,
            I2C_RETRY,
            AEC_RESID,
            AEC_SPENERGY_VALUES_CMD,
            &mut payload,
        );
        assert_eq!(got, Ok((STATUS_DONE, 1)));
        assert!(t.delays.is_empty(), "no delay on a first-attempt success");
        assert_eq!(
            t.calls,
            vec![Call {
                read: true,
                resid: AEC_RESID,
                cmd: AEC_SPENERGY_VALUES_CMD,
                len: 16,
                attempt: 1,
            }]
        );
        assert_eq!(payload, [0xABu8; 16], "payload filled by the transport");
    }

    #[test]
    fn read_retries_wait_then_reports_done() {
        let mut t = Scripted::new(&[STATUS_WAIT, STATUS_WAIT, STATUS_DONE]);
        let mut payload = [0u8; 3];
        let got = control_read(
            &mut t,
            I2C_RETRY,
            DFU_RESID,
            DFU_GETVERSION_CMD,
            &mut payload,
        );
        assert_eq!(got, Ok((STATUS_DONE, 3)));
        // One delay per re-issue, none after the final answer.
        assert_eq!(t.delays, vec![1, 1]);
    }

    #[test]
    fn read_retries_retry_status_too() {
        let mut t = Scripted::new(&[STATUS_RETRY, STATUS_DONE]);
        let mut payload = [0u8; 3];
        assert_eq!(
            control_read(
                &mut t,
                I2C_RETRY,
                DFU_RESID,
                DFU_GETVERSION_CMD,
                &mut payload
            ),
            Ok((STATUS_DONE, 2))
        );
        assert_eq!(t.delays, vec![1]);
    }

    #[test]
    fn read_returns_fatal_status_without_retrying() {
        // 0x02 is not transient: answer immediately, one attempt, no delay.
        let mut t = Scripted::new(&[0x02]);
        let mut payload = [0u8; 3];
        assert_eq!(
            control_read(
                &mut t,
                I2C_RETRY,
                DFU_RESID,
                DFU_GETVERSION_CMD,
                &mut payload
            ),
            Ok((0x02, 1))
        );
        assert!(t.delays.is_empty());
        assert_eq!(t.calls.len(), 1);
    }

    #[test]
    fn read_exhausts_i2c_budget_at_nine_attempts() {
        // I2C budget: 1 initial + 8 retries = 9 transactions, 8 delays of 1 ms.
        let mut t = Scripted::repeating(STATUS_RETRY, 9);
        let mut payload = [0u8; 16];
        let got = control_read(
            &mut t,
            I2C_RETRY,
            AEC_RESID,
            AEC_AZIMUTH_VALUES_CMD,
            &mut payload,
        );
        assert_eq!(got, Ok((STATUS_RETRY, 9)));
        assert_eq!(t.calls.len(), 9);
        assert_eq!(t.delays, vec![1; 8]);
    }

    #[test]
    fn read_exhausts_usb_budget_at_hundred_and_one_attempts() {
        let mut t = Scripted::repeating(STATUS_RETRY, 101);
        let mut payload = [0u8; 3];
        let got = control_read(
            &mut t,
            USB_RETRY,
            APPLICATION_SERVICER_RESID,
            VERSION_CMD,
            &mut payload,
        );
        assert_eq!(got, Ok((STATUS_RETRY, 101)));
        assert_eq!(t.calls.len(), 101);
        assert_eq!(t.delays, vec![10; 100]);
    }

    #[test]
    fn read_propagates_transport_error_and_stops() {
        // Fails on the third attempt (index 2) after two WAITs.
        let mut t = Scripted::new(&[STATUS_WAIT, STATUS_WAIT, STATUS_DONE]).failing_at(2);
        let mut payload = [0u8; 3];
        assert_eq!(
            control_read(
                &mut t,
                I2C_RETRY,
                DFU_RESID,
                DFU_GETVERSION_CMD,
                &mut payload
            ),
            Err(FakeError)
        );
        assert_eq!(t.calls.len(), 3, "no attempts after the transport error");
        assert_eq!(t.delays, vec![1, 1]);
    }

    #[test]
    fn write_done_on_first_attempt() {
        let mut t = Scripted::new(&[STATUS_DONE]);
        let payload = [0u8; GPO_VECTOR_LEN];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Ok((STATUS_DONE, 1))
        );
        assert_eq!(
            t.calls,
            vec![Call {
                read: false,
                resid: GPO_RESID,
                cmd: GPO_CMD,
                len: GPO_VECTOR_LEN,
                attempt: 1,
            }]
        );
        assert!(t.delays.is_empty());
    }

    #[test]
    fn write_retries_wait_then_reports_done() {
        let mut t = Scripted::new(&[STATUS_WAIT, STATUS_DONE]);
        let payload = [1u8, 2, 3];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Ok((STATUS_DONE, 2))
        );
        assert_eq!(t.delays, vec![1]);
    }

    #[test]
    fn write_exhausts_i2c_budget_at_nine_attempts() {
        let mut t = Scripted::repeating(STATUS_WAIT, 9);
        let payload = [0u8; GPO_VECTOR_LEN];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Ok((STATUS_WAIT, 9))
        );
        assert_eq!(t.delays, vec![1; 8]);
    }

    /// The write driver classifies statuses exactly as the read driver does: a
    /// non-transient status is the final answer. A wrong-command-length status on a
    /// mis-sized GPO vector must fail on the first transaction rather than burning the
    /// whole budget with the I2C bus lock held.
    #[test]
    fn write_returns_fatal_status_without_retrying() {
        let mut t = Scripted::new(&[0x02]);
        let payload = [0u8; GPO_VECTOR_LEN];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Ok((0x02, 1))
        );
        assert_eq!(t.calls.len(), 1, "a fatal status is not re-issued");
        assert!(t.delays.is_empty());
    }

    #[test]
    fn write_exhausts_usb_budget_at_hundred_and_one_attempts() {
        let mut t = Scripted::repeating(STATUS_RETRY, 101);
        let payload = [0u8; 2];
        assert_eq!(
            control_write(
                &mut t,
                USB_RETRY,
                APPLICATION_SERVICER_RESID,
                VERSION_CMD,
                &payload
            ),
            Ok((STATUS_RETRY, 101))
        );
        assert_eq!(t.delays, vec![10; 100]);
    }

    #[test]
    fn write_propagates_transport_error() {
        let mut t = Scripted::new(&[STATUS_DONE]).failing_at(0);
        let payload = [0u8; GPO_VECTOR_LEN];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Err(FakeError)
        );
        assert_eq!(t.calls.len(), 1);
    }

    #[test]
    fn zero_retry_policy_makes_exactly_one_attempt() {
        let policy = RetryPolicy {
            max_retries: 0,
            delay_ms: 7,
        };
        let mut t = Scripted::repeating(STATUS_WAIT, 1);
        let mut payload = [0u8; 3];
        assert_eq!(
            control_read(&mut t, policy, DFU_RESID, DFU_GETVERSION_CMD, &mut payload),
            Ok((STATUS_WAIT, 1))
        );
        assert!(t.delays.is_empty());
    }

    #[test]
    fn transport_sees_ascending_one_based_attempt_numbers() {
        let mut t = Scripted::repeating(STATUS_RETRY, 9);
        let mut payload = [0u8; 16];
        let got = control_read(
            &mut t,
            I2C_RETRY,
            AEC_RESID,
            AEC_AZIMUTH_VALUES_CMD,
            &mut payload,
        );
        assert_eq!(got, Ok((STATUS_RETRY, 9)));
        let attempts: Vec<u32> = t.calls.iter().map(|c| c.attempt).collect();
        assert_eq!(attempts, (1..=9).collect::<Vec<u32>>());

        let mut t = Scripted::new(&[STATUS_WAIT, STATUS_DONE]);
        let payload = [0u8; GPO_VECTOR_LEN];
        assert_eq!(
            control_write(&mut t, I2C_RETRY, GPO_RESID, GPO_CMD, &payload),
            Ok((STATUS_DONE, 2))
        );
        assert_eq!(
            t.calls.iter().map(|c| c.attempt).collect::<Vec<u32>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn decode_f32x4_round_trips_le_bytes() {
        let values = [-1.25f32, 0.0, 3.5, 1.0e-8];
        let mut bytes = [0u8; 16];
        for (i, v) in values.iter().enumerate() {
            bytes[i * 4..i * 4 + 4].copy_from_slice(&v.to_le_bytes());
        }
        assert_eq!(decode_f32x4(&bytes), values);
    }

    #[test]
    fn decode_f32x4_preserves_nan_and_sign() {
        // The chip reports NaN azimuths for beams with no confirmed speech, so NaN
        // must survive the decode rather than being normalised away.
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&f32::NAN.to_le_bytes());
        bytes[4..8].copy_from_slice(&(-0.0f32).to_le_bytes());
        bytes[8..12].copy_from_slice(&f32::INFINITY.to_le_bytes());
        bytes[12..16].copy_from_slice(&core::f32::consts::PI.to_le_bytes());
        let got = decode_f32x4(&bytes);
        assert!(got[0].is_nan());
        assert!(got[1] == 0.0 && got[1].is_sign_negative());
        assert_eq!(got[2], f32::INFINITY);
        assert_eq!(got[3], core::f32::consts::PI);
    }

    #[test]
    fn the_routing_registers_frame_as_their_documented_resid_and_length() {
        // AEC_ASROUTONOFF: resid 33 cmd 35, one int32 → length byte 5 on a read.
        assert_eq!(
            i2c_read_header(AEC_RESID, AEC_ASROUTONOFF_CMD, SCALAR_READ_LEN),
            [33, 35 | 0x80, 5]
        );
        assert_eq!(
            i2c_write_header(AEC_RESID, AEC_ASROUTONOFF_CMD, SCALAR_READ_LEN),
            [33, 35, 4]
        );
        assert_eq!(
            i2c_read_header(AEC_RESID, AEC_ASROUTGAIN_CMD, SCALAR_READ_LEN),
            [33, 36 | 0x80, 5]
        );
        assert_eq!(
            i2c_read_header(AEC_RESID, AEC_AECCONVERGED_CMD, SCALAR_READ_LEN),
            [33, 0x83, 5]
        );
    }

    #[test]
    fn the_post_processing_readbacks_all_sit_on_resid_seventeen() {
        for cmd in [
            PP_AGCONOFF_CMD,
            PP_AGCGAIN_CMD,
            PP_MIN_NS_CMD,
            PP_MIN_NN_CMD,
            PP_ECHOONOFF_CMD,
            PP_DTSENSITIVE_CMD,
        ] {
            assert_eq!(
                i2c_read_header(PP_RESID, cmd, SCALAR_READ_LEN),
                [17, cmd | READ_BIT, 5],
                "cmd {cmd}"
            );
        }
        // The six are distinct commands; a duplicated id would read one register
        // twice and print it under two names.
        let mut ids = [
            PP_AGCONOFF_CMD,
            PP_AGCGAIN_CMD,
            PP_MIN_NS_CMD,
            PP_MIN_NN_CMD,
            PP_ECHOONOFF_CMD,
            PP_DTSENSITIVE_CMD,
        ];
        ids.sort_unstable();
        assert_eq!(ids, [10, 13, 21, 22, 23, 31]);
    }

    #[test]
    fn reboot_and_the_build_message_share_the_application_servicer() {
        // REBOOT is a one-byte write; it has no read side.
        assert_eq!(
            i2c_write_header(APPLICATION_SERVICER_RESID, REBOOT_CMD, REBOOT_WRITE_LEN),
            [48, 7, 1]
        );
        // BLD_MSG is the longest register here, and the staging buffers are sized
        // for it.
        assert_eq!(
            i2c_read_header(APPLICATION_SERVICER_RESID, BLD_MSG_CMD, BLD_MSG_READ_LEN),
            [48, 0x81, 51]
        );
        const { assert!(BLD_MSG_READ_LEN <= CTRL_BUF_CAPACITY) };
    }

    #[test]
    fn scalars_decode_little_endian_and_round_trip() {
        assert_eq!(decode_i32(&[1, 0, 0, 0]), 1);
        assert_eq!(decode_i32(&[0xFF, 0xFF, 0xFF, 0xFF]), -1);
        assert_eq!(decode_i32(&encode_i32(-2)), -2);
        // 0x3F800000 = 1.0f32.
        assert_eq!(decode_f32(&[0, 0, 0x80, 0x3F]), 1.0);
        assert!(decode_f32(&f32::NAN.to_le_bytes()).is_nan());
    }

    #[test]
    fn a_character_register_renders_its_printable_prefix_only() {
        let mut payload = [0u8; BLD_MSG_READ_LEN];
        let msg = b"XMOS XVF3800 v2.1.2 (ua-io16-lin)  ";
        payload[..msg.len()].copy_from_slice(msg);
        assert_eq!(decode_ascii(&payload), "XMOS XVF3800 v2.1.2 (ua-io16-lin)");
        // A register that answered with binary renders as nothing rather than as
        // control characters in the journal.
        assert_eq!(decode_ascii(&[0x01, 0x02, 0xFF]), "");
        assert_eq!(decode_ascii(&[]), "");
    }

    #[test]
    fn decode_f32x4_reads_little_endian() {
        // 0x3F800000 = 1.0f32; little-endian on the wire is 00 00 80 3F.
        let mut bytes = [0u8; 16];
        bytes[2] = 0x80;
        bytes[3] = 0x3F;
        assert_eq!(decode_f32x4(&bytes)[0], 1.0);
        assert_eq!(decode_f32x4(&bytes)[1], 0.0);
    }
}
