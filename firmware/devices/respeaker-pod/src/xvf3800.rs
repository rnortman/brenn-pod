//! XVF3800 XU316 voice-DSP control transport and self-tests.
//!
//! Holds the I2C control protocol (resid/cmd/status framing with WAIT/RETRY
//! retry) shared by every XVF3800 access, the GPO servicer used to prove the amp
//! is always-on, and the DFU-version / DoA / SPENERGY HIL self-tests. All I2C
//! transactions run against the shared `I2C_BUS`; callers hold the bus lock for
//! the duration of a control call.
//!
//! Host view: only the pure decoders are visible; every I2C item is device-gated, so
//! their host-unused pure helpers are covered by the file-level dead-code allow below.

#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use crate::hil::DebugF32;
#[cfg(target_os = "espidf")]
use crate::i2c::{I2C_BUS, I2C_CTRL_TIMEOUT_TICKS};
#[cfg(target_os = "espidf")]
use device_protocol::{
    doa_azimuth_ok, sp_energy_ok, test_report_fail, test_report_fail_fmt, test_report_ok, Payload,
    Status, TestData,
};
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::{delay::FreeRtos, i2c::I2cDriver};

/// XVF3800 I2C control address (7-bit). Assumed to be exposed on the stock
/// l16k2ch firmware image; not separately confirmed for other firmware images.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_ADDR: u8 = 0x2C;

// ── XVF3800 control-transport constants ──────────────────────────────────────
//
// Resource IDs, command IDs, and status codes for the XVF3800's I2C control
// protocol (resid/cmd/status framing used by `xvf3800_control_read` and
// `xvf3800_control_write`).

#[cfg(target_os = "espidf")]
const XVF3800_DFU_RESID: u8 = 240;
#[cfg(target_os = "espidf")]
const XVF3800_DFU_GETVERSION_CMD: u8 = 88;
#[cfg(target_os = "espidf")]
const XVF3800_READ_BIT: u8 = 0x80;
#[cfg(target_os = "espidf")]
const XVF3800_STATUS_DONE: u8 = 0;
#[cfg(target_os = "espidf")]
const XVF3800_STATUS_WAIT: u8 = 1;
#[cfg(target_os = "espidf")]
const XVF3800_STATUS_RETRY: u8 = 0x40;

/// Total I2C transactions per control-read call = 1 initial + up to XVF3800_MAX_RETRIES.
#[cfg(target_os = "espidf")]
const XVF3800_MAX_RETRIES: usize = 8;

/// Delay between retries; 1 ms is the FreeRTOS `delay_ms` minimum granularity.
#[cfg(target_os = "espidf")]
const XVF3800_RETRY_DELAY_MS: u32 = 1;

/// VERSION register payload length in bytes (major + minor + patch = 3; total I2C read = 4).
#[cfg(target_os = "espidf")]
const XVF3800_VERSION_READ_LEN: usize = 3;

/// AEC servicer resource ID.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_AEC_RESID: u8 = 33;

/// AEC_AZIMUTH_VALUES command ID. Read byte = 75 | 0x80 = 0xCB.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_AEC_AZIMUTH_VALUES_CMD: u8 = 75;

/// AEC_AZIMUTH_VALUES payload length in bytes: 4 × f32 = 16 payload + 1 status = 17 total.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_AEC_AZIMUTH_READ_LEN: usize = 16;

/// AEC_SPENERGY_VALUES command ID. Read byte = 80 | 0x80 = 0xD0.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_AEC_SPENERGY_VALUES_CMD: u8 = 80;

/// AEC_SPENERGY_VALUES payload length in bytes: 4 × f32 = 16 payload + 1 status = 17 total.
/// Same layout as AEC_AZIMUTH_VALUES.
#[cfg(target_os = "espidf")]
pub(crate) const XVF3800_AEC_SPENERGY_READ_LEN: usize = 16;

/// Maximum payload bytes across all known XVF3800 control registers accessed via
/// `xvf3800_control_read`. The DoA azimuth and SPENERGY registers each need 16 payload
/// bytes (read_len=16, total=17); round up to 32 to leave room for future registers.
#[cfg(target_os = "espidf")]
const XVF3800_CTRL_BUF_CAPACITY: usize = 32;

// ── XVF3800 GPO servicer (amp & carrier-board control) ───────────────────────

/// GPO servicer resource ID. The general-purpose-output lines (amp enable, mute LED)
/// are read/written through this resid.
#[cfg(target_os = "espidf")]
const XVF3800_GPO_RESID: u8 = 20;

/// GPO servicer command 0 — the GPO vector accessor. Read (with `XVF3800_READ_BIT`)
/// returns the 6-byte vector. **Writing to cmd 0 is accepted-and-DONE but inert** on this
/// firmware image — cmd 0 is a read-only accessor and the write never moves any GPO line.
/// The sole surviving caller (`run_amp_always_on_gpo_inert`) issues the write precisely to
/// assert this inertness as a regression guard.
#[cfg(target_os = "espidf")]
const XVF3800_GPO_CMD: u8 = 0;

/// GPO vector length in bytes for the flashed firmware image: **6**, bench-confirmed.
/// A shorter length (5, matching some external documentation for a different firmware
/// variant) is rejected by this image with a wrong-command-length status; only 6 works.
/// X0D31 (the amp-enable line) stays at index 2 of the 6-byte vector regardless.
#[cfg(target_os = "espidf")]
const XVF3800_GPO_VECTOR_LEN: usize = 6;

/// Settle delay after a GPO write before the next transaction to the XVF3800 (~5 ms).
/// The device needs time to apply the change and will NAK transactions issued too soon.
#[cfg(target_os = "espidf")]
const XVF3800_GPO_SETTLE_MS: u32 = 5;

/// Perform one XVF3800 control READ transaction over I2C, with retry on status 0x01/0x40.
///
/// Write header: `[resid, cmd | READ_BIT, read_len + 1]`.
/// Read: `read_len + 1` bytes where byte[0] = status and bytes[1..] = little-endian payload.
/// Up to `XVF3800_MAX_RETRIES + 1` total transactions (8 retries + 1 initial = 9 max).
///
/// # Returns
/// - `Ok((status, attempts))` — final status byte and total transaction count (≥1).
///   `status == XVF3800_STATUS_DONE (0x00)` = success; any other value = transient or fatal error.
///   The payload slice is always filled with `read_len` bytes regardless of status.
/// - `Err(EspError)` — I2C driver write or read error (NACK, bus fault, timeout).
///   The `attempts` field is not available on this path; the caller sees the attempt number
///   only via logging conventions.
///
/// # Safety note
/// The caller must hold the `I2C_BUS` mutex for the duration of this call.
/// Do not call from an interrupt context.
#[cfg(target_os = "espidf")]
pub(crate) fn xvf3800_control_read(
    driver: &mut I2cDriver<'_>,
    resid: u8,
    cmd: u8,
    read_len: usize,
    payload: &mut [u8],
) -> Result<(u8, usize), esp_idf_svc::sys::EspError> {
    // Wire header: [resid, cmd | READ_BIT, read_len + 1]
    let header = [resid, cmd | XVF3800_READ_BIT, (read_len + 1) as u8];

    // Validate buffer capacity unconditionally — this is a caller contract violation
    // if triggered, not a transient hardware error. Panic in all build modes.
    let total = read_len + 1;
    // buf holds status byte + payload; capacity must cover all known register sizes.
    let mut buf = [0u8; XVF3800_CTRL_BUF_CAPACITY];
    assert!(
        total <= buf.len(),
        "xvf3800_control_read: read_len {read_len} exceeds buf capacity ({cap}); \
         update XVF3800_CTRL_BUF_CAPACITY",
        cap = buf.len()
    );

    for attempt in 0..XVF3800_MAX_RETRIES + 1 {
        // Log attempt context before propagating errors so callers can distinguish
        // "first-attempt failure" (wrong address/bus) from "retry-exhaustion failure"
        // (marginal bus, clock-stretch) without changing the function signature.
        if let Err(e) = driver.write(XVF3800_ADDR, &header, I2C_CTRL_TIMEOUT_TICKS) {
            log::warn!(
                "xvf3800_control_read: attempt {} write error (resid={} cmd={}): {:?}",
                attempt + 1,
                header[0],
                header[1],
                e
            );
            return Err(e);
        }

        // Read status byte + payload.
        if let Err(e) = driver.read(XVF3800_ADDR, &mut buf[..total], I2C_CTRL_TIMEOUT_TICKS) {
            log::warn!(
                "xvf3800_control_read: attempt {} read error (resid={} cmd={}): {:?}",
                attempt + 1,
                header[0],
                header[1],
                e
            );
            return Err(e);
        }

        let status = buf[0];
        payload.copy_from_slice(&buf[1..total]);

        // Retry on transient statuses; return on done or fatal (including exhausted retries).
        if (status == XVF3800_STATUS_WAIT || status == XVF3800_STATUS_RETRY)
            && attempt < XVF3800_MAX_RETRIES
        {
            FreeRtos::delay_ms(XVF3800_RETRY_DELAY_MS);
            continue;
        }
        return Ok((status, attempt + 1));
    }
    // The loop (0..XVF3800_MAX_RETRIES+1) always executes at least once and every
    // iteration either continues or returns. The compiler does not always prove this
    // for range loops, so provide an explicit unreachable! to satisfy exhaustiveness.
    unreachable!("xvf3800_control_read: loop must return inside")
}

/// Perform one XVF3800 control WRITE transaction over I2C, with retry on status 0x01/0x40.
///
/// The write counterpart to `xvf3800_control_read`. The command byte is sent **without**
/// `XVF3800_READ_BIT`. Wire frame: `[resid, cmd, len]` followed by `payload`, where `len`
/// is the payload length.
///
/// After sending the header+payload, the device returns a single status byte. Transient
/// statuses (`XVF3800_STATUS_WAIT` / `XVF3800_STATUS_RETRY`) are retried up to
/// `XVF3800_MAX_RETRIES` times with `XVF3800_RETRY_DELAY_MS` between attempts, mirroring the
/// read path's retry budget. `XVF3800_STATUS_DONE (0x00)` = success; any other returned value
/// is a fatal/exhausted error reported via the status byte.
///
/// # Returns
/// - `Ok((status, attempts))` — final status byte and total transaction count (≥1).
/// - `Err(EspError)` — I2C driver write or read error (NACK, bus fault, timeout).
///
/// # Safety note
/// The caller must hold the `I2C_BUS` mutex for the duration of this call. Do not call from
/// an interrupt context.
#[cfg(target_os = "espidf")]
fn xvf3800_control_write(
    driver: &mut I2cDriver<'_>,
    resid: u8,
    cmd: u8,
    payload: &[u8],
) -> Result<(u8, usize), esp_idf_svc::sys::EspError> {
    // Wire frame: [resid, cmd, len] + payload. Command byte carries no READ_BIT.
    // Assemble header + payload into one buffer; capacity must cover the GPO vector (6 bytes)
    // plus the 3-byte header, with headroom for future registers.
    let total = payload.len() + 3;
    let mut buf = [0u8; XVF3800_CTRL_BUF_CAPACITY];
    assert!(
        total <= buf.len(),
        "xvf3800_control_write: payload {plen} + header exceeds buf capacity ({cap}); \
         update XVF3800_CTRL_BUF_CAPACITY",
        plen = payload.len(),
        cap = buf.len()
    );
    buf[0] = resid;
    buf[1] = cmd;
    buf[2] = payload.len() as u8;
    buf[3..total].copy_from_slice(payload);

    let mut status = [0u8; 1];
    for attempt in 0..XVF3800_MAX_RETRIES + 1 {
        // Send header + payload (STOP after, same as the read path's header write).
        if let Err(e) = driver.write(XVF3800_ADDR, &buf[..total], I2C_CTRL_TIMEOUT_TICKS) {
            log::warn!(
                "xvf3800_control_write: attempt {} write error (resid={} cmd={}): {:?}",
                attempt + 1,
                resid,
                cmd,
                e
            );
            return Err(e);
        }

        // Read the single status byte the servicer returns after a write.
        if let Err(e) = driver.read(XVF3800_ADDR, &mut status, I2C_CTRL_TIMEOUT_TICKS) {
            log::warn!(
                "xvf3800_control_write: attempt {} status read error (resid={} cmd={}): {:?}",
                attempt + 1,
                resid,
                cmd,
                e
            );
            return Err(e);
        }

        // Retry on transient statuses; return on done or fatal (including exhausted retries).
        if (status[0] == XVF3800_STATUS_WAIT || status[0] == XVF3800_STATUS_RETRY)
            && attempt < XVF3800_MAX_RETRIES
        {
            FreeRtos::delay_ms(XVF3800_RETRY_DELAY_MS);
            continue;
        }
        return Ok((status[0], attempt + 1));
    }
    // The loop (0..XVF3800_MAX_RETRIES+1) always executes at least once and every iteration
    // either continues or returns; the explicit unreachable! satisfies exhaustiveness.
    unreachable!("xvf3800_control_write: loop must return inside")
}

/// XVF3800 control register read self-test: read the DFU VERSION register.
///
/// Performs a write-then-read I2C control transaction to resid=240 (DFU controller),
/// cmd=88 (GETVERSION), read_len=3 (major, minor, patch). Reports raw status byte and
/// raw version payload bytes in the result message for human inspection.
///
/// PASS criterion (presence/transport level):
/// - status byte = 0x00 (CTRL_DONE)
/// - payload is plausible: not all-0x00 and not all-0xFF (would indicate read noise
///   or bus stuck, not a real version)
///
/// The exact version value is NOT asserted here — a FAIL is a hardware/firmware
/// discovery (wrong control framing for this firmware image), not a bug in this test.
///
/// FAIL message formats:
/// - I2C init failure:       `"XVF3800 reg read: I2C init failed: <EspError>"`
/// - Transport error (NACK, bus fault, timeout): `"FAIL I2C error v=[?] <EspError>"`
/// - Protocol error (bad status byte): `"FAIL status=0xNN attempts=N"`
#[cfg(target_os = "espidf")]
pub(crate) fn run_xvf3800_reg_read() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("I2C_BUS not initialized — firmware init bug"),
    };

    let mut version_payload = [0u8; XVF3800_VERSION_READ_LEN];
    let (status_byte, attempts) = match xvf3800_control_read(
        driver,
        XVF3800_DFU_RESID,
        XVF3800_DFU_GETVERSION_CMD,
        XVF3800_VERSION_READ_LEN,
        &mut version_payload,
    ) {
        Ok(result) => result,
        Err(e) => {
            // No status byte was received — the I2C transaction itself failed (NACK,
            // bus fault, or timeout). Do not emit status=0x00 here; that is the
            // CTRL_DONE success sentinel and would mislead any log reader.
            return test_report_fail_fmt(format_args!("FAIL I2C error v=[?] {:?}", e));
        }
    };

    let [v0, v1, v2] = version_payload;

    // Check PASS criterion: status must be DONE and payload must be plausible.
    if status_byte != XVF3800_STATUS_DONE {
        if attempts > 1 {
            return test_report_fail_fmt(format_args!(
                "FAIL retries_exhausted status={:#04x} attempts={} v=[{:#04x},{:#04x},{:#04x}]",
                status_byte, attempts, v0, v1, v2
            ));
        }
        return test_report_fail_fmt(format_args!(
            "FAIL status={:#04x} v=[{:#04x},{:#04x},{:#04x}]",
            status_byte, v0, v1, v2
        ));
    }

    // Plausibility: not all-zero and not all-0xFF.
    let all_zero = v0 == 0x00 && v1 == 0x00 && v2 == 0x00;
    let all_ff = v0 == 0xFF && v1 == 0xFF && v2 == 0xFF;
    if all_zero || all_ff {
        return test_report_fail_fmt(format_args!(
            "FAIL status={:#04x} implausible v=[{:#04x},{:#04x},{:#04x}]",
            status_byte, v0, v1, v2
        ));
    }

    // PASS: status=done, payload plausible.
    test_report_ok(TestData::Xvf3800RegRead {
        status: status_byte,
        version: [v0, v1, v2],
    })
}

/// HIL self-test for `TestName::AmpAlwaysOnGpoInert`.
///
/// Documents — as a durable regression guard — that the GPO cmd-0 write is **inert** on
/// this board: the TPA3139D2 amp is always-on hardware, and the cmd-0 vector accessor
/// (resid 20 / cmd 0) is read-only, so a write is accepted and reported DONE while X0D31
/// (the nominal amp-enable line, vector index 2) never moves.
///
/// This test does **not** toggle the amp (impossible); it asserts the *observable* inert
/// behavior so no future reader can reintroduce a software-amp-gate assumption.
///
/// Sequence:
/// 1. Read the GPO vector v0 (`XVF3800_GPO_VECTOR_LEN` = 6 bytes); record `x0d31_before = v0[2]`.
/// 2. Write v0 back via the read-only cmd 0 with index 2 flipped (`x0d31_before ^ 0x01`).
/// 3. Settle (`XVF3800_GPO_SETTLE_MS`), then re-read the vector v1.
/// 4. Assert `write_status == XVF3800_STATUS_DONE` **and** `v1[2] == x0d31_before` — the flip
///    did NOT take, proving the write is inert.
///
/// PASS data: `TestData::AmpGpoInert { x0d31, write_status }`.
///
/// This test encodes *expected* (proven) inert behavior, so it passes immediately and
/// stays as a guard. If a future firmware/hardware change ever makes the write actually
/// move X0D31, this test **FAILs** — the desired alarm that the always-on premise (and the
/// clean-shutdown design built on it) no longer holds, so it gets human review before
/// anyone "fixes" the test.
#[cfg(target_os = "espidf")]
pub(crate) fn run_amp_always_on_gpo_inert() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => {
            return test_report_fail("amp-gpo-inert: I2C_BUS not initialized — firmware init bug")
        }
    };

    // 1. Read the GPO vector v0; record X0D31 (vector index 2).
    let mut v0 = [0u8; XVF3800_GPO_VECTOR_LEN];
    let (read0_status, _) = match xvf3800_control_read(
        driver,
        XVF3800_GPO_RESID,
        XVF3800_GPO_CMD,
        XVF3800_GPO_VECTOR_LEN,
        &mut v0,
    ) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL src=amp gpo_read0 I2C error {:?}", e))
        }
    };
    if read0_status != XVF3800_STATUS_DONE {
        return test_report_fail_fmt(format_args!(
            "FAIL src=amp gpo_read0 status={:#04x}",
            read0_status
        ));
    }
    let x0d31_before = v0[2];

    // 2. Write the vector back with X0D31 flipped, via the read-only cmd 0.
    // X0D31 is expected to be 0 or 1 (a logic level). XOR-flip bit 0 so the write attempts
    // to change the line regardless of its value; XOR is safe on any u8 (no underflow if the
    // device ever returns a non-binary byte). If the byte is unexpected, the re-read assertion
    // below still correctly catches any actual movement of X0D31.
    let mut v_flip = v0;
    v_flip[2] = x0d31_before ^ 0x01;
    let (write_status, _) =
        match xvf3800_control_write(driver, XVF3800_GPO_RESID, XVF3800_GPO_CMD, &v_flip) {
            Ok(result) => result,
            Err(e) => {
                return test_report_fail_fmt(format_args!(
                    "FAIL src=amp gpo_write I2C error {:?}",
                    e
                ))
            }
        };

    // 3. Settle, then re-read so the device has had time to (not) apply the write.
    FreeRtos::delay_ms(XVF3800_GPO_SETTLE_MS);
    let mut v1 = [0u8; XVF3800_GPO_VECTOR_LEN];
    let (read1_status, _) = match xvf3800_control_read(
        driver,
        XVF3800_GPO_RESID,
        XVF3800_GPO_CMD,
        XVF3800_GPO_VECTOR_LEN,
        &mut v1,
    ) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL src=amp gpo_read1 I2C error {:?}", e))
        }
    };
    if read1_status != XVF3800_STATUS_DONE {
        return test_report_fail_fmt(format_args!(
            "FAIL src=amp gpo_read1 status={:#04x}",
            read1_status
        ));
    }

    // 4. Assert the write was accepted-DONE yet X0D31 did NOT change — the write is inert.
    if write_status != XVF3800_STATUS_DONE {
        // The cmd-0 write is expected to ACK with DONE even though it is inert; a non-DONE
        // status is a discovery (the servicer rejected the write outright) → human review.
        return test_report_fail_fmt(format_args!(
            "FAIL src=amp gpo_write status={:#04x} (expected DONE)",
            write_status
        ));
    }
    if v1[2] != x0d31_before {
        // X0D31 MOVED — the cmd-0 write is NOT inert. The always-on premise no longer holds;
        // this is the intended loud alarm for human review.
        return test_report_fail_fmt(format_args!(
            "FAIL src=amp gpo_write=took x0d31 {:#04x}->{:#04x} (write moved X0D31 — always-on premise broken)",
            x0d31_before, v1[2]
        ));
    }

    // PASS: write accepted-DONE, X0D31 unchanged → write is inert (always-on confirmed).
    test_report_ok(TestData::AmpGpoInert {
        x0d31: x0d31_before,
        write_status,
    })
}

/// Decode four consecutive IEEE-754 little-endian f32 values from a 16-byte payload.
///
/// `p` must be exactly 16 bytes: `[f0_b0..f0_b3, f1_b0..f1_b3, f2_b0..f2_b3, f3_b0..f3_b3]`.
/// Used for XVF3800 AEC_AZIMUTH_VALUES, AEC_SPENERGY_VALUES, and the telemetry-thread
/// inline SPENERGY / DoA reads — four identical sites consolidated here.
pub(crate) fn decode_f32x4(p: &[u8; 16]) -> [f32; 4] {
    [
        f32::from_le_bytes([p[0], p[1], p[2], p[3]]),
        f32::from_le_bytes([p[4], p[5], p[6], p[7]]),
        f32::from_le_bytes([p[8], p[9], p[10], p[11]]),
        f32::from_le_bytes([p[12], p[13], p[14], p[15]]),
    ]
}

/// XVF3800 DoA plausibility self-test: read AEC_AZIMUTH_VALUES (resid=33, cmd=75).
///
/// Transaction: write `[33, 0xCB, 17]`, read 17 bytes = `[status, f0_le, f1_le, f2_le, f3_le]`.
/// Parses 4 IEEE-754 little-endian f32 values. Reports all four raw values in the result message.
///
/// PASS criterion (plausibility, not exact value — azimuths depend on the room/orientation):
/// - Transaction succeeds: status=0x00 (CTRL_DONE), full 17 bytes received.
/// - Every NON-NaN value is finite (not Inf) and |x| ≤ π radians.
/// - Index 2 (free-running scanner) MUST be finite-and-in-range (not NaN).
///   Indices 0/1 (focused trackers A/B) and 3 (auto-select winner) MAY legitimately
///   be NaN in a quiet room — this is normal device behavior and is NOT a FAIL.
///
/// Azimuth convention: [-π, π] radians.
/// This is an assertion-as-probe test. A FAIL is a discovery.
#[cfg(target_os = "espidf")]
pub(crate) fn run_xvf3800_doa_plausibility() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("DoA: I2C_BUS not initialized — firmware init bug"),
    };

    let mut az_payload = [0u8; XVF3800_AEC_AZIMUTH_READ_LEN];
    let (status_byte, attempts) = match xvf3800_control_read(
        driver,
        XVF3800_AEC_RESID,
        XVF3800_AEC_AZIMUTH_VALUES_CMD,
        XVF3800_AEC_AZIMUTH_READ_LEN,
        &mut az_payload,
    ) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL DoA I2C error az=[?,?,?,?] {:?}", e));
        }
    };

    // Parse 4×f32 little-endian from the 16-byte payload.
    let [az0, az1, az2, az3] = decode_f32x4(&az_payload);

    // Check transport status first.
    if status_byte != XVF3800_STATUS_DONE {
        if attempts > 1 {
            return test_report_fail_fmt(format_args!(
                "FAIL DoA retries_exhausted status={:#04x} attempts={} az=[{},{},{},{}]",
                status_byte,
                attempts,
                DebugF32(az0),
                DebugF32(az1),
                DebugF32(az2),
                DebugF32(az3),
            ));
        }
        return test_report_fail_fmt(format_args!(
            "FAIL DoA status={:#04x} az=[{},{},{},{}]",
            status_byte,
            DebugF32(az0),
            DebugF32(az1),
            DebugF32(az2),
            DebugF32(az3),
        ));
    }

    // Validate each NON-NaN value: must be finite and |x| ≤ π.
    // idx 2 (free-running scanner) must additionally not be NaN.
    // `doa_azimuth_ok` accepts NaN (acceptable on indices 0/1/3; idx 2 checked
    // separately) and finite |x| ≤ π; on rejection classify the reason for the
    // per-index FAIL message.
    let check_az = |v: f32| -> Option<&'static str> {
        if doa_azimuth_ok(v) {
            None
        } else if !v.is_finite() {
            Some("infinite")
        } else {
            Some("out-of-range")
        }
    };

    // Check each tracker.
    for (v, idx) in [az0, az1, az2, az3].iter().zip(0usize..) {
        if let Some(reason) = check_az(*v) {
            return test_report_fail_fmt(format_args!(
                "FAIL DoA az[{idx}]={} {reason} (status={:#04x} az=[{},{},{},{}])",
                DebugF32(*v),
                status_byte,
                DebugF32(az0),
                DebugF32(az1),
                DebugF32(az2),
                DebugF32(az3),
            ));
        }
    }

    // idx 2 (free-running scanner) must be finite-and-in-range (not NaN).
    if az2.is_nan() {
        return test_report_fail_fmt(format_args!(
            "FAIL DoA az[2]=nan (scanner must be finite; focused/winner NaN ok) \
             status={:#04x} az=[{},{},{},{}]",
            status_byte,
            DebugF32(az0),
            DebugF32(az1),
            DebugF32(az2),
            DebugF32(az3),
        ));
    }

    // PASS: status=done, structurally plausible values, scanner finite.
    test_report_ok(TestData::Xvf3800Doa {
        status: status_byte,
        az: [az0, az1, az2, az3],
    })
}

/// XVF3800 SPENERGY plausibility self-test — assertion-as-probe.
///
/// Reads `AEC_SPENERGY_VALUES` (resid=33, cmd=80, 4×f32 LE, 17 bytes) via
/// `xvf3800_control_read` using the shared `I2C_BUS`.
///
/// PASS criterion:
/// - Transaction succeeds: status=0x00 (CTRL_DONE), full 17 bytes received.
/// - Every value is finite and ≥ 0.0 (NaN, Inf, or negative → FAIL).
///
/// All-zero is valid — SPENERGY is per-beam speech energy; 0.0 = no speech present.
/// An unattended HIL run cannot guarantee speech, so all-zero is expected and correct.
/// Magnitude/threshold proving is done via interactive full-system testing, not HIL.
#[cfg(target_os = "espidf")]
pub(crate) fn run_xvf3800_sp_energy() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("SpEnergy: I2C_BUS not initialized — firmware init bug"),
    };

    let mut sp_payload = [0u8; XVF3800_AEC_SPENERGY_READ_LEN];
    let (status_byte, attempts) = match xvf3800_control_read(
        driver,
        XVF3800_AEC_RESID,
        XVF3800_AEC_SPENERGY_VALUES_CMD,
        XVF3800_AEC_SPENERGY_READ_LEN,
        &mut sp_payload,
    ) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!(
                "FAIL SpEnergy I2C error sp=[?,?,?,?] {:?}",
                e
            ));
        }
    };

    // Parse 4×f32 little-endian from the 16-byte payload.
    let [sp0, sp1, sp2, sp3] = decode_f32x4(&sp_payload);

    // Check transport status first.
    if status_byte != XVF3800_STATUS_DONE {
        if attempts > 1 {
            return test_report_fail_fmt(format_args!(
                "FAIL SpEnergy retries_exhausted status={:#04x} attempts={} sp=[{},{},{},{}]",
                status_byte,
                attempts,
                DebugF32(sp0),
                DebugF32(sp1),
                DebugF32(sp2),
                DebugF32(sp3),
            ));
        }
        return test_report_fail_fmt(format_args!(
            "FAIL SpEnergy status={:#04x} sp=[{},{},{},{}]",
            status_byte,
            DebugF32(sp0),
            DebugF32(sp1),
            DebugF32(sp2),
            DebugF32(sp3),
        ));
    }

    // Validate: every value must be finite and ≥ 0.0 (energy is always non-negative).
    // `sp_energy_ok` is the shared accept predicate; on rejection classify the reason
    // for the per-index FAIL message.
    for (v, idx) in [sp0, sp1, sp2, sp3].iter().zip(0usize..) {
        if sp_energy_ok(*v) {
            continue;
        }
        if v.is_nan() {
            return test_report_fail_fmt(format_args!(
                "FAIL SpEnergy sp[{idx}]=nan (status={:#04x} sp=[{},{},{},{}])",
                status_byte,
                DebugF32(sp0),
                DebugF32(sp1),
                DebugF32(sp2),
                DebugF32(sp3),
            ));
        }
        if !v.is_finite() {
            return test_report_fail_fmt(format_args!(
                "FAIL SpEnergy sp[{idx}]={} infinite (status={:#04x} sp=[{},{},{},{}])",
                DebugF32(*v),
                status_byte,
                DebugF32(sp0),
                DebugF32(sp1),
                DebugF32(sp2),
                DebugF32(sp3),
            ));
        }
        // Negative is the only rejection cause `sp_energy_ok` has once NaN and non-finite
        // are handled. Label it explicitly; if `sp_energy_ok` later gains another reason
        // (e.g. an upper bound), report it honestly as "rejected" rather than mislabelling
        // it "negative" — this runs on hardware in release, where a debug assert would be
        // compiled out.
        let reason = if *v < 0.0 { "negative" } else { "rejected" };
        return test_report_fail_fmt(format_args!(
            "FAIL SpEnergy sp[{idx}]={} {reason} (status={:#04x} sp=[{},{},{},{}])",
            DebugF32(*v),
            status_byte,
            DebugF32(sp0),
            DebugF32(sp1),
            DebugF32(sp2),
            DebugF32(sp3),
        ));
    }

    // PASS: status=done, all values finite and non-negative.
    // Zero is valid — it means no speech present (SPENERGY is per-beam VAD energy;
    // any value > 0 indicates speech). An unattended HIL run cannot guarantee speech,
    // so all-zero is expected and correct. Magnitude/threshold proving is done via
    // interactive full-system testing, not HIL.
    test_report_ok(TestData::Xvf3800SpEnergy {
        status: status_byte,
        sp: [sp0, sp1, sp2, sp3],
    })
}
