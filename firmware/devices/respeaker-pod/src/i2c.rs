//! Shared I2C bus mechanics: the process-lifetime bus singleton, the
//! millisecond→tick helper, per-transaction timeouts, driver construction, and
//! the I2C bus-scan self-test. Device I2C addresses live with their device
//! modules (`AIC3104_ADDR`, `XVF3800_ADDR` in the crate root for now); this
//! module imports them for the scan's presence assertion.

use device_protocol::{Payload, Status};
use esp_idf_svc::hal::i2c::{I2cConfig, I2cDriver};
use esp_idf_svc::hal::units::Hertz;
use std::sync::Mutex;

use crate::{AIC3104_ADDR, XVF3800_ADDR};
use device_protocol::{
    I2C_SCAN_MAX_ADDRS, TestData, test_report_fail, test_report_fail_fmt, test_report_ok,
};

// ── I2C shared bus (process-lifetime, boot-initialized) ───────────────────────

/// Process-lifetime I2C bus driver. Initialized at boot from real `Peripherals` handles;
/// shared between all I2C test handlers and (later) the telemetry thread.
///
/// # Lock discipline
/// Each caller locks the mutex for the duration of one logical transaction (one or more
/// I2C read/write calls comprising a single XVF3800 control read). Hold time is bounded:
/// one XVF3800 control read = ≤9 I2C transactions × ~1.5 ms each ≈ ≤15 ms maximum.
/// Callers must not hold the lock across yield points or sleeps.
///
/// # Poisoning policy
/// Mutex poison (another thread panicked while holding the lock) → panic fail-fast,
/// consistent with the existing WRITER / LED / WIFI_STACK convention.
pub(crate) static I2C_BUS: Mutex<Option<I2cDriver<'static>>> = Mutex::new(None);

/// I2C bus frequency: 100 kHz — the XMOS XVF3800's maximum per its datasheet.
const I2C_FREQ_HZ: u32 = 100_000;

/// Convert milliseconds to FreeRTOS ticks at compile time.
///
/// Equivalent to the C macro `pdMS_TO_TICKS(ms)`. Uses `configTICK_RATE_HZ`
/// from the bindings so the result is correct regardless of `CONFIG_FREERTOS_HZ`.
/// Uses ceiling division so any positive `ms` value produces at least 1 tick:
/// FreeRTOS counts `ticks_to_wait` from the *next* tick boundary, so a 1-tick
/// budget can give an effective wait anywhere from 0 to a full tick depending on
/// where in the current tick the call lands. Rounding up avoids a 0-tick result
/// that would give no wait budget at all.
pub(crate) const fn ms_to_ticks(ms: u32) -> u32 {
    (ms * esp_idf_svc::sys::configTICK_RATE_HZ).div_ceil(1000)
}

// If configTICK_RATE_HZ ever changes such that ms_to_ticks(100) rounds to 0,
// fail to compile rather than silently producing a 0-tick timeout.
const _: () = assert!(
    ms_to_ticks(100) > 0,
    "ms_to_ticks(100) must be non-zero — check configTICK_RATE_HZ"
);

/// Per-probe I2C timeout: 100 ms expressed as FreeRTOS ticks.
///
/// Safely larger than any real I2C transaction on a healthy bus while keeping
/// worst-case scan time at ~11 s (112 probes × 100 ms) — acceptable for a
/// one-shot HIL test.
const I2C_PROBE_TIMEOUT_TICKS: u32 = ms_to_ticks(100);

/// Per-transaction I2C timeout for XVF3800 control reads/writes: 100 ms as ticks.
///
/// The XVF3800 may clock-stretch; 100 ms gives it ample budget. Kept as a
/// separate constant from `I2C_PROBE_TIMEOUT_TICKS` so the two can be tuned
/// independently.
pub(crate) const I2C_CTRL_TIMEOUT_TICKS: u32 = ms_to_ticks(100);

/// Construct an `I2cDriver` for I2C0 on SDA=GPIO5 / SCL=GPIO6 at `I2C_FREQ_HZ`.
///
/// Called exactly once at boot — the result is stored in `I2C_BUS` for the process
/// lifetime. All I2C callers (HIL test handlers, telemetry thread) lock `I2C_BUS`
/// per transaction rather than constructing a new driver each time.
///
/// # Safety
///
/// `gpio5`, `gpio6`, and `i2c0` must be the real, not-yet-used peripheral handles
/// from `Peripherals::take()`. Passing stolen handles or handles already used elsewhere
/// causes aliased mutable access (UB per esp-idf-hal contract).
pub(crate) fn make_i2c_driver(
    i2c0: esp_idf_svc::hal::i2c::I2C0<'static>,
    gpio5: esp_idf_svc::hal::gpio::Gpio5<'static>,
    gpio6: esp_idf_svc::hal::gpio::Gpio6<'static>,
) -> Result<I2cDriver<'static>, esp_idf_svc::sys::EspError> {
    let config = I2cConfig::new().baudrate(Hertz(I2C_FREQ_HZ));
    I2cDriver::new(i2c0, gpio5, gpio6, &config)
}

/// I2C scan: assert both 0x2C (XVF3800) and 0x18 (AIC3104) ACK.
///
/// Probes the 7-bit address space (0x08–0x77) by attempting a zero-length write
/// to each address via the shared `I2C_BUS`; an ACK = address present.
/// Collects all responding addresses.
///
/// PASS iff both 0x2C (XVF3800) and 0x18 (AIC3104 codec) ACK.
/// FAIL otherwise — the full ACK list is reported either way for diagnostics.
///
/// This is an assertion-as-probe test. A FAIL is a hardware discovery (wrong pins
/// for stock firmware, MCLK dependency, electrical issue, boot timing) and must not
/// be papered over.
pub(crate) fn run_i2c_bus_scan() -> (Status, Payload) {
    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let driver = match bus_guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("I2C_BUS not initialized — firmware init bug"),
    };

    // Collect ACKing addresses in [0x08, 0x77].
    // heapless::Vec<u8, 112>: 0x77 - 0x08 + 1 = 112 possible addresses exactly;
    // push can never overflow within this loop.
    let mut found: heapless::Vec<u8, I2C_SCAN_MAX_ADDRS> = heapless::Vec::new();
    // Non-NACK errors: record (7-bit address, raw esp_err_t code) for each.
    // Capped at 4 entries; any beyond the cap are still counted via bus_errors.
    // ESP_ERR_TIMEOUT=263, ESP_ERR_INVALID_STATE=259 are the expected codes here.
    let mut bus_error_detail: heapless::Vec<(u8, i32), 4> = heapless::Vec::new();
    let mut bus_errors: u32 = 0;
    for addr in 0x08u8..=0x77u8 {
        // Zero-length write: the I2C master emits START + (addr << 1 | WRITE).
        // ACK → device present; ESP_FAIL (NACK) → absent; other error → bus fault.
        match driver.write(addr, &[], I2C_PROBE_TIMEOUT_TICKS) {
            Ok(()) => {
                // ACK received — record this address.
                found
                    .push(addr)
                    .expect("I2C scan vec overflow — capacity mismatch");
            }
            Err(e) if e.code() == esp_idf_svc::sys::ESP_FAIL => {
                // NACK — no device at this address; continue scan.
            }
            Err(e) => {
                // Non-NACK error: bus fault (timeout, invalid state, arbitration
                // loss). Record (addr, code) for diagnostics; count separately.
                bus_errors += 1;
                // Saturate at capacity (4 entries); excess errors still counted.
                // .ok() discards the Err only at-capacity — intentional discard.
                bus_error_detail.push((addr, e.code())).ok();
            }
        }
    }

    // Assert PASS criterion: both 0x2C (XVF3800) and 0x18 (AIC3104) must ACK,
    // AND bus_errors must be zero. A non-NACK bus error (e.g. ESP_ERR_TIMEOUT)
    // means the I2C bus is not clean; NACK (ESP_FAIL) on empty addresses is normal.
    // A dirty bus with both devices ACKing is not a clean pass.
    let xvf_present = found.contains(&XVF3800_ADDR);
    let aic_present = found.contains(&AIC3104_ADDR);

    if xvf_present && aic_present && bus_errors == 0 {
        return test_report_ok(TestData::I2cScan { found, bus_errors });
    }

    // Fail path only: render the found list and the bus-error detail as human text.
    // Format: "0x18,0x2c" or ""; each address is 0xNN (4 chars).
    // Capacity proof: 112 addresses × 5 chars ("0xNN,") − 1 trailing comma = 559 chars max.
    // String::<560> is sufficient by construction; expect() makes the invariant load-bearing.
    let mut addr_list = heapless::String::<560>::new();
    for (i, &addr) in found.iter().enumerate() {
        if i > 0 {
            addr_list.push(',').expect("addr_list capacity");
        }
        core::fmt::write(&mut addr_list, format_args!("{addr:#04x}")).expect("addr_list capacity");
    }

    // Build bus-error detail string: "[(0xNN,EEEE),(0xNN,EEEE),...]" or "[]".
    // Each entry is (7-bit address, raw esp_err_t integer). Capped at 4 entries;
    // bus_errors count covers any overflow beyond the cap.
    // Capacity proof: each entry "(0xNN,EEEEEEEEEE)" = 1+'('+4+','+11+')' = 18 chars max
    // (eaddr: 4 chars for 0xNN; ecode: up to 11 chars for i32 "-2147483648").
    // 4 entries: '[' + 4*18 + 3*(','between entries) + ']' = 1 + 72 + 3 + 1 = 77 chars.
    // String::<80> is sufficient by construction; expect() makes the invariant
    // load-bearing so a future capacity change fails loudly rather than silently.
    let mut error_detail = heapless::String::<80>::new();
    error_detail.push('[').expect("error_detail capacity");
    for (i, &(eaddr, ecode)) in bus_error_detail.iter().enumerate() {
        if i > 0 {
            error_detail.push(',').expect("error_detail capacity");
        }
        core::fmt::write(&mut error_detail, format_args!("({eaddr:#04x},{ecode})"))
            .expect("error_detail capacity");
    }
    error_detail.push(']').expect("error_detail capacity");

    test_report_fail_fmt(format_args!(
        "FAIL xvf={} aic={} found=[{}] bus_errors={}{}",
        if xvf_present { "ACK" } else { "NACK" },
        if aic_present { "ACK" } else { "NACK" },
        addr_list.as_str(),
        bus_errors,
        error_detail.as_str(),
    ))
}
