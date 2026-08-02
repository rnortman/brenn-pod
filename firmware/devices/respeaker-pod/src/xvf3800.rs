//! XVF3800 XU316 voice-DSP I2C control transport and self-tests.
//!
//! Holds the [`ControlTransport`] implementation over the shared `I2C_BUS` driver
//! (resid/cmd/status framing on the wire, one attempt per call), the GPO servicer
//! self-test used to prove the amp is always-on, and the DFU-version / DoA /
//! SPENERGY HIL self-tests. The protocol itself — resource and command IDs, status
//! semantics, retry policy, payload decoders — lives in `xvf3800_ctrl` and is shared
//! with the USB transport on Linux. All I2C transactions run against `I2C_BUS`;
//! callers hold the bus lock for the duration of a control call.
//!
//! Device-only module: every item here touches the ESP-IDF I2C driver.

use crate::hil::DebugF32;
use crate::i2c::{I2C_BUS, I2C_CTRL_TIMEOUT_TICKS};
use device_protocol::{
    Payload, Status, TestData, doa_azimuth_ok, sp_energy_ok, test_report_fail,
    test_report_fail_fmt, test_report_ok,
};
use esp_idf_svc::hal::{delay::FreeRtos, i2c::I2cDriver};
use xvf3800_ctrl::{
    AEC_AZIMUTH_READ_LEN, AEC_AZIMUTH_VALUES_CMD, AEC_RESID, AEC_SPENERGY_READ_LEN,
    AEC_SPENERGY_VALUES_CMD, CTRL_BUF_CAPACITY, ControlTransport, DFU_GETVERSION_CMD, DFU_RESID,
    GPO_CMD, GPO_RESID, GPO_SETTLE_MS, GPO_VECTOR_LEN, I2C_RETRY, STATUS_DONE, VERSION_READ_LEN,
    control_read, control_write, decode_f32x4, i2c_read_header, i2c_write_header,
};

/// XVF3800 I2C control address (7-bit), as the rest of this crate names it.
pub(crate) use xvf3800_ctrl::I2C_ADDR as XVF3800_ADDR;

/// [`ControlTransport`] over the ESP32 I2C master: one transaction pair per call,
/// no retry (the shared driver in `xvf3800_ctrl` owns the retry budget).
pub(crate) struct I2cControl<'d, 'i> {
    driver: &'d mut I2cDriver<'i>,
}

impl<'d, 'i> I2cControl<'d, 'i> {
    pub(crate) fn new(driver: &'d mut I2cDriver<'i>) -> Self {
        Self { driver }
    }
}

impl ControlTransport for I2cControl<'_, '_> {
    type Error = esp_idf_svc::sys::EspError;

    /// Write `[resid, cmd | READ_BIT, len + 1]`, then read `len + 1` bytes where
    /// byte 0 is the status and bytes 1.. are the little-endian payload.
    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        let header = i2c_read_header(resid, cmd, payload.len());

        // buf holds the status byte + payload. Capacity must cover every register we
        // read; exceeding it is a caller contract violation, not a transient hardware
        // error, so assert in all build modes.
        let total = payload.len() + 1;
        let mut buf = [0u8; CTRL_BUF_CAPACITY];
        assert!(
            total <= buf.len(),
            "xvf3800 control read: payload len {plen} exceeds buf capacity ({cap}); \
             update CTRL_BUF_CAPACITY",
            plen = payload.len(),
            cap = buf.len()
        );

        if let Err(e) = self
            .driver
            .write(XVF3800_ADDR, &header, I2C_CTRL_TIMEOUT_TICKS)
        {
            log::warn!(
                "xvf3800_control_read: attempt {attempt} write error (resid={} cmd={}): {:?}",
                header[0],
                header[1],
                e
            );
            return Err(e);
        }

        if let Err(e) = self
            .driver
            .read(XVF3800_ADDR, &mut buf[..total], I2C_CTRL_TIMEOUT_TICKS)
        {
            log::warn!(
                "xvf3800_control_read: attempt {attempt} read error (resid={} cmd={}): {:?}",
                header[0],
                header[1],
                e
            );
            return Err(e);
        }

        payload.copy_from_slice(&buf[1..total]);
        Ok(buf[0])
    }

    /// Write `[resid, cmd, len]` followed by the payload in one transaction (the
    /// command byte carries no read bit), then read back the single status byte the
    /// servicer returns.
    fn control_write_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &[u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        let header = i2c_write_header(resid, cmd, payload.len());

        // Assemble header + payload into one buffer; capacity must cover the GPO
        // vector (6 bytes) plus the 3-byte header, with headroom for future registers.
        let total = payload.len() + header.len();
        let mut buf = [0u8; CTRL_BUF_CAPACITY];
        assert!(
            total <= buf.len(),
            "xvf3800 control write: payload {plen} + header exceeds buf capacity ({cap}); \
             update CTRL_BUF_CAPACITY",
            plen = payload.len(),
            cap = buf.len()
        );
        buf[..header.len()].copy_from_slice(&header);
        buf[header.len()..total].copy_from_slice(payload);

        if let Err(e) = self
            .driver
            .write(XVF3800_ADDR, &buf[..total], I2C_CTRL_TIMEOUT_TICKS)
        {
            log::warn!(
                "xvf3800_control_write: attempt {attempt} write error (resid={} cmd={}): {:?}",
                resid,
                cmd,
                e
            );
            return Err(e);
        }

        let mut status = [0u8; 1];
        if let Err(e) = self
            .driver
            .read(XVF3800_ADDR, &mut status, I2C_CTRL_TIMEOUT_TICKS)
        {
            log::warn!(
                "xvf3800_control_write: attempt {attempt} status read error (resid={} cmd={}): {:?}",
                resid,
                cmd,
                e
            );
            return Err(e);
        }

        Ok(status[0])
    }

    fn delay_ms(&mut self, ms: u32) {
        FreeRtos::delay_ms(ms);
    }
}

/// Perform an XVF3800 control READ over I2C, retrying transient statuses per
/// [`I2C_RETRY`].
///
/// # Returns
/// - `Ok((status, attempts))` — final status byte and total transaction count (≥1).
///   `status == STATUS_DONE (0x00)` = success; any other value = transient
///   (retry-exhausted) or fatal error. `payload` is filled with `payload.len()`
///   bytes regardless of status.
/// - `Err(EspError)` — I2C driver write or read error (NACK, bus fault, timeout).
///   The failing attempt number appears in the warning log, not in the return value.
///
/// # Safety note
/// The caller must hold the `I2C_BUS` mutex for the duration of this call.
/// Do not call from an interrupt context.
fn xvf3800_control_read(
    driver: &mut I2cDriver<'_>,
    resid: u8,
    cmd: u8,
    payload: &mut [u8],
) -> Result<(u8, u32), esp_idf_svc::sys::EspError> {
    let mut transport = I2cControl::new(driver);
    control_read(&mut transport, I2C_RETRY, resid, cmd, payload)
}

/// Perform an XVF3800 control WRITE over I2C, retrying transient statuses per
/// [`I2C_RETRY`]. The write counterpart to [`xvf3800_control_read`]; same return
/// contract and the same `I2C_BUS` lock requirement.
fn xvf3800_control_write(
    driver: &mut I2cDriver<'_>,
    resid: u8,
    cmd: u8,
    payload: &[u8],
) -> Result<(u8, u32), esp_idf_svc::sys::EspError> {
    let mut transport = I2cControl::new(driver);
    control_write(&mut transport, I2C_RETRY, resid, cmd, payload)
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
pub(crate) fn run_xvf3800_reg_read() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("I2C_BUS not initialized — firmware init bug"),
    };

    let mut version_payload = [0u8; VERSION_READ_LEN];
    let (status_byte, attempts) =
        match xvf3800_control_read(driver, DFU_RESID, DFU_GETVERSION_CMD, &mut version_payload) {
            Ok(result) => result,
            Err(e) => {
                // No status byte was received — the I2C transaction itself failed (NACK,
                // bus fault, or timeout). Do not emit status=0x00 here; that is the
                // CTRL_DONE success sentinel and would mislead any log reader.
                return test_report_fail_fmt(format_args!("FAIL I2C error v=[?] {:?}", e));
            }
        };

    let [v0, v1, v2] = version_payload;

    if status_byte != STATUS_DONE {
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

    let all_zero = v0 == 0x00 && v1 == 0x00 && v2 == 0x00;
    let all_ff = v0 == 0xFF && v1 == 0xFF && v2 == 0xFF;
    if all_zero || all_ff {
        return test_report_fail_fmt(format_args!(
            "FAIL status={:#04x} implausible v=[{:#04x},{:#04x},{:#04x}]",
            status_byte, v0, v1, v2
        ));
    }

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
/// 1. Read the GPO vector v0 ([`GPO_VECTOR_LEN`] = 6 bytes); record `x0d31_before = v0[2]`.
/// 2. Write v0 back via the read-only cmd 0 with index 2 flipped (`x0d31_before ^ 0x01`).
/// 3. Settle ([`GPO_SETTLE_MS`]), then re-read the vector v1.
/// 4. Assert `write_status == STATUS_DONE` **and** `v1[2] == x0d31_before` — the flip
///    did NOT take, proving the write is inert.
///
/// PASS data: `TestData::AmpGpoInert { x0d31, write_status }`.
///
/// This test encodes *expected* (proven) inert behavior, so it passes immediately and
/// stays as a guard. If a future firmware/hardware change ever makes the write actually
/// move X0D31, this test **FAILs** — the desired alarm that the always-on premise (and the
/// clean-shutdown design built on it) no longer holds, so it gets human review before
/// anyone "fixes" the test.
pub(crate) fn run_amp_always_on_gpo_inert() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => {
            return test_report_fail("amp-gpo-inert: I2C_BUS not initialized — firmware init bug");
        }
    };

    // 1. Read the GPO vector v0; record X0D31 (vector index 2).
    let mut v0 = [0u8; GPO_VECTOR_LEN];
    let (read0_status, _) = match xvf3800_control_read(driver, GPO_RESID, GPO_CMD, &mut v0) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL src=amp gpo_read0 I2C error {:?}", e));
        }
    };
    if read0_status != STATUS_DONE {
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
    let (write_status, _) = match xvf3800_control_write(driver, GPO_RESID, GPO_CMD, &v_flip) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL src=amp gpo_write I2C error {:?}", e));
        }
    };

    // 3. Settle, then re-read so the device has had time to (not) apply the write.
    FreeRtos::delay_ms(GPO_SETTLE_MS);
    let mut v1 = [0u8; GPO_VECTOR_LEN];
    let (read1_status, _) = match xvf3800_control_read(driver, GPO_RESID, GPO_CMD, &mut v1) {
        Ok(result) => result,
        Err(e) => {
            return test_report_fail_fmt(format_args!("FAIL src=amp gpo_read1 I2C error {:?}", e));
        }
    };
    if read1_status != STATUS_DONE {
        return test_report_fail_fmt(format_args!(
            "FAIL src=amp gpo_read1 status={:#04x}",
            read1_status
        ));
    }

    // 4. Assert the write was accepted-DONE yet X0D31 did NOT change — the write is inert.
    if write_status != STATUS_DONE {
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
pub(crate) fn run_xvf3800_doa_plausibility() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("DoA: I2C_BUS not initialized — firmware init bug"),
    };

    let mut az_payload = [0u8; AEC_AZIMUTH_READ_LEN];
    let (status_byte, attempts) =
        match xvf3800_control_read(driver, AEC_RESID, AEC_AZIMUTH_VALUES_CMD, &mut az_payload) {
            Ok(result) => result,
            Err(e) => {
                return test_report_fail_fmt(format_args!(
                    "FAIL DoA I2C error az=[?,?,?,?] {:?}",
                    e
                ));
            }
        };

    let [az0, az1, az2, az3] = decode_f32x4(&az_payload);

    if status_byte != STATUS_DONE {
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

    // Every non-NaN azimuth must be finite and |x| ≤ π. Index 2 (free-running
    // scanner) must not be NaN; indices 0/1/3 may legitimately be NaN.
    let check_az = |v: f32| -> Option<&'static str> {
        if doa_azimuth_ok(v) {
            None
        } else if !v.is_finite() {
            Some("infinite")
        } else {
            Some("out-of-range")
        }
    };

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

    test_report_ok(TestData::Xvf3800Doa {
        status: status_byte,
        az: [az0, az1, az2, az3],
    })
}

/// XVF3800 SPENERGY plausibility self-test — assertion-as-probe.
///
/// Reads `AEC_SPENERGY_VALUES` (resid=33, cmd=80, 4×f32 LE, 17 bytes) via
/// [`xvf3800_control_read`] using the shared `I2C_BUS`.
///
/// PASS criterion:
/// - Transaction succeeds: status=0x00 (CTRL_DONE), full 17 bytes received.
/// - Every value is finite and ≥ 0.0 (NaN, Inf, or negative → FAIL).
///
/// All-zero is valid — SPENERGY is per-beam speech energy; 0.0 = no speech present.
/// An unattended HIL run cannot guarantee speech, so all-zero is expected and correct.
/// Magnitude/threshold proving is done via interactive full-system testing, not HIL.
pub(crate) fn run_xvf3800_sp_energy() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("SpEnergy: I2C_BUS not initialized — firmware init bug"),
    };

    let mut sp_payload = [0u8; AEC_SPENERGY_READ_LEN];
    let (status_byte, attempts) =
        match xvf3800_control_read(driver, AEC_RESID, AEC_SPENERGY_VALUES_CMD, &mut sp_payload) {
            Ok(result) => result,
            Err(e) => {
                return test_report_fail_fmt(format_args!(
                    "FAIL SpEnergy I2C error sp=[?,?,?,?] {:?}",
                    e
                ));
            }
        };

    let [sp0, sp1, sp2, sp3] = decode_f32x4(&sp_payload);

    if status_byte != STATUS_DONE {
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

    // Every value must be finite and ≥ 0.0 (energy is always non-negative).
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
        // The "rejected" fallback covers any future accept-predicate reason beyond NaN,
        // non-finite, and negative, so the FAIL message stays honest if the predicate
        // gains new criteria.
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

    test_report_ok(TestData::Xvf3800SpEnergy {
        status: status_byte,
        sp: [sp0, sp1, sp2, sp3],
    })
}
