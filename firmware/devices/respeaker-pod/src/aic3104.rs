//! TLV320AIC3104 codec driver: page-0 power-up init sequence, read-back
//! verification, and DAC mute/unmute. All I2C transactions run against the shared
//! `I2C_BUS` driver; callers hold the bus lock for the duration.

// Host view: these items exist for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

#[cfg(target_os = "espidf")]
use crate::i2c::I2C_CTRL_TIMEOUT_TICKS;
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::{delay::FreeRtos, i2c::I2cDriver};

/// TLV320AIC3104 codec I2C address (7-bit).
#[cfg(target_os = "espidf")]
pub(crate) const AIC3104_ADDR: u8 = 0x18;

// ── AIC3104 codec init ───────────────────────────────────────────────────────

/// AIC3104 PLL R divider (register 0x0B). `0x08` = R=8, assuming **32-bit I2S slots**
/// (matching the RX capture config); if the link turns out to use 16-bit slots instead,
/// the PLL won't lock and pitch will be wrong — the fallback in that case is `0x10` (R=16).
const AIC3104_PLL_R: u8 = 0x08;

/// AIC3104 DAC L/R digital-volume **muted** value (registers 0x2B / 0x2C): D7=1 (mute).
/// Matches the power-on-reset default but is written and read-back-verified explicitly.
/// The DAC sits upstream of all output routing, so it's the one codec-side lever that
/// reaches the speaker path: the codec comes up muted, the output is brought up silent,
/// and only playback unmutes — the DAC's own soft mute→0dB step (never a hard jump) is
/// the only level change the always-on speaker ever sees. Teardown re-mutes before
/// disabling output, for the same reason.
const AIC3104_DAC_VOLUME_MUTED: u8 = 0x80;

/// AIC3104 DAC L/R digital-volume **unmuted** value (registers 0x2B / 0x2C): D7=0, 0 dB.
/// Written only at playback start (not part of init) so the DAC soft-steps from mute up
/// to 0 dB rather than snapping — a gentle ramp, not a click.
const AIC3104_DAC_VOLUME_UNMUTED: u8 = 0x00;

// Guards against a copy-paste swap of MUTED/UNMUTED, which would silently write the
// wrong polarity during unmute (read-back would "pass", producing a silent tone caught
// only by ear). Compile to nothing.
const _: () = assert!(
    AIC3104_DAC_VOLUME_MUTED & 0x80 == 0x80,
    "AIC3104_DAC_VOLUME_MUTED must have D7=1 (DAC digital mute set)"
);
const _: () = assert!(
    AIC3104_DAC_VOLUME_UNMUTED & 0x80 == 0x00,
    "AIC3104_DAC_VOLUME_UNMUTED must have D7=0 (DAC digital mute clear)"
);
const _: () = assert!(
    AIC3104_DAC_VOLUME_MUTED != AIC3104_DAC_VOLUME_UNMUTED,
    "AIC3104 DAC muted and unmuted volume constants must differ"
);

/// AIC3104 soft-reset register (page 0, 0x01). Needs `AIC3104_RESET_SETTLE_MS` after
/// writing, unlike a plain config write — named so the loop guard is discoverable.
#[cfg(target_os = "espidf")]
const AIC3104_REG_SOFT_RESET: u8 = 0x01;

/// AIC3104 DAC power & output-driver register (page 0, 0x25). Needs the load-bearing
/// de-pop settle `AIC3104_DAC_POWERUP_SETTLE_MS` after writing. Named so that's
/// discoverable from the loop guard, not a bare `0x25` literal.
#[cfg(target_os = "espidf")]
const AIC3104_REG_DAC_POWER: u8 = 0x25;

/// Settle delay after the AIC3104 soft reset (register 0x01 = 0x80).
///
/// The chip needs **≥ 1.5 ms** before continuing. `FreeRtos::delay_ms` has 1 ms
/// granularity, so 2 ms is the smallest value that guarantees the floor.
#[cfg(target_os = "espidf")]
const AIC3104_RESET_SETTLE_MS: u32 = 2;

/// **Load-bearing de-pop delay** after the AIC3104 DAC power-up write (register
/// 0x25 = 0xC0). Without it, the power-up transient reaches the always-on speaker as an
/// audible pop; the TI datasheet gives no settle-time figure for this transition. 100 ms
/// is empirically tuned with margin over the shortest delay observed to suppress it on
/// the bench — don't shorten without re-verifying by ear.
#[cfg(target_os = "espidf")]
const AIC3104_DAC_POWERUP_SETTLE_MS: u32 = 100;

/// The AIC3104 page-0 power-up register sequence, in order: `(register, value, rw_mask)`.
/// The soft reset (0x01) is first and is self-clearing, so it is not read back; every
/// other entry is read back after the write phase.
///
/// `rw_mask` masks out bits the codec sets autonomously (read-only status/reserved bits)
/// before the read-back compare `(got & rw_mask) == (val & rw_mask)`, so those bits can
/// legitimately differ from the written byte without registering as a mismatch, while any
/// flipped R/W bit still fails. Default mask is `0xFF`; only the rows below need a
/// narrower one, derived bit-by-bit from the TI datasheet:
///
/// | Reg  | Table | Read-only bits          | Mask |
/// |------|-------|-------------------------|------|
/// | 0x25 | 10-43 | D3–D0 reserved          | 0xF0 | (defensive: written value already passes)
/// | 0x33 | 10-58 | D1 HPLOUT vol-ctl status| 0xFD |
/// | 0x41 | 10-72 | D1 HPROUT vol-ctl status| 0xFD |
/// | 0x56 | 10-88 | D2 reserved, D1 status  | 0xF9 |
/// | 0x5D | 10-95 | D2 reserved, D1 status  | 0xF9 |
const AIC3104_INIT_SEQUENCE: &[(u8, u8, u8)] = &[
    (0x01, 0x80, 0xFF),          // Soft reset (self-clearing; not read back)
    (0x66, 0xA2, 0xFF),          // PLL source = BCLK
    (0x04, 0x30, 0xFF),          // PLL J = 12
    (0x05, 0x00, 0xFF),          // PLL D msb
    (0x06, 0x00, 0xFF),          // PLL D lsb
    (0x0B, AIC3104_PLL_R, 0xFF), // PLL R (16-bit-slot fallback is 0x10; see AIC3104_PLL_R doc)
    (0x65, 0x00, 0xFF),          // PLL
    (0x03, 0x91, 0xFF),          // PLL enable, Q=2, P=1
    (0x02, 0x44, 0xFF),          // Sample rate → 16 kHz
    (0x07, 0x0A, 0xFF),          // DAC datapath
    (0x08, 0x00, 0xFF),          // Audio serial iface
    (0x09, 0x00, 0xFF),          // Audio serial iface
    (0x0A, 0x00, 0xFF),          // Audio serial iface
    (0x25, 0xC0, 0xF0),          // DAC power up L+R (D3–D0 reserved; defensive mask)
    (0x2B, AIC3104_DAC_VOLUME_MUTED, 0xFF), // DAC L volume: muted at init, unmuted at playback start
    (0x2C, AIC3104_DAC_VOLUME_MUTED, 0xFF), // DAC R volume: muted at init, unmuted at playback start
    (0x2F, 0x80, 0xFF),                     // DAC → output routing
    (0x40, 0x80, 0xFF),                     // DAC → output routing
    (0x52, 0x80, 0xFF),                     // DAC → output routing (speaker path)
    (0x5C, 0x80, 0xFF),                     // DAC → output routing
    (0x33, 0x0D, 0xFD),                     // Output driver (D1 HPLOUT vol-ctl status, codec→1)
    (0x41, 0x0D, 0xFD),                     // Output driver (D1 HPROUT vol-ctl status, codec→1)
    (0x56, 0x0B, 0xF9),                     // Output driver (D2 reserved, D1 status)
    (0x5D, 0x0B, 0xF9),                     // Output driver (D2 reserved, D1 status)
];

/// Compares a codec read-back against the written value, masking out bits the codec owns
/// (read-only/status/reserved) so only host-writable bits are checked. Pure and
/// hardware-free so the mask table is unit-testable host-side. The mask is applied to
/// both sides of the compare.
#[inline]
fn masked_eq(written: u8, got: u8, rw_mask: u8) -> bool {
    (written & rw_mask) == (got & rw_mask)
}

/// Structured AIC3104 codec-init failure, naming the register and the error class so the
/// HIL FAIL message can localize the fault (`FAIL src=codec reg=0x.. …`).
#[cfg(target_os = "espidf")]
#[derive(Clone, Copy, Debug)]
pub(crate) enum Aic3104InitError {
    /// A register write returned an I2C error (NAK, bus fault, timeout). Carries the
    /// register that failed and the raw `esp_err_t` code.
    Write { reg: u8, code: i32 },
    /// A read-back returned an I2C error while re-addressing/reading the register.
    Readback { reg: u8, code: i32 },
    /// The read-back succeeded but an R/W bit didn't hold the written value under the
    /// mask — a NAK-free but ineffective write (wrong address, silently-clamped
    /// register). Carries expected/observed (the full unmasked byte) plus the mask so the
    /// FAIL message distinguishes an R/W-bit failure from a mask-table bug.
    Mismatch {
        reg: u8,
        want: u8,
        got: u8,
        rw_mask: u8,
    },
}

/// Reads register `reg` back via `write_read` and asserts its R/W bits equal `val` under
/// `rw_mask`. Shared verification idiom for `aic3104_init` and `aic3104_dac_unmute`.
///
/// Uses `write_read` (repeated-START) rather than a STOP-terminated pointer write followed
/// by a bare `read()` — the latter would return the codec's stale internal register
/// pointer instead of `reg`. Logs the full unmasked `got` byte so a genuinely wrong write
/// is never hidden by the mask. Caller holds the `I2C_BUS` lock.
#[cfg(target_os = "espidf")]
fn aic3104_verify_register(
    driver: &mut I2cDriver<'_>,
    reg: u8,
    val: u8,
    rw_mask: u8,
) -> Result<(), Aic3104InitError> {
    let mut got = [0u8; 1];
    if let Err(e) = driver.write_read(AIC3104_ADDR, &[reg], &mut got, I2C_CTRL_TIMEOUT_TICKS) {
        log::warn!("aic3104 read-back write_read reg=0x{reg:02x} error: {e:?}");
        return Err(Aic3104InitError::Readback {
            reg,
            code: e.code(),
        });
    }
    if !masked_eq(val, got[0], rw_mask) {
        log::warn!(
            "aic3104 read-back mismatch reg=0x{reg:02x} want=0x{val:02x} \
             got=0x{:02x} mask=0x{rw_mask:02x}",
            got[0]
        );
        return Err(Aic3104InitError::Mismatch {
            reg,
            want: val,
            got: got[0],
            rw_mask,
        });
    }
    Ok(())
}

/// Initializes the AIC3104 codec: writes the ordered page-0 power-up sequence
/// (`AIC3104_INIT_SEQUENCE`), waiting `AIC3104_RESET_SETTLE_MS` after the soft reset and
/// `AIC3104_DAC_POWERUP_SETTLE_MS` after the DAC power-up write, then reads every
/// persistent register back and verifies it (see `aic3104_verify_register`).
///
/// Safe to re-run: it starts with a soft reset, so the codec re-initializes from defaults
/// each call. Returns the first failing register as a structured `Aic3104InitError`.
/// Caller holds the `I2C_BUS` lock for the whole sequence.
#[cfg(target_os = "espidf")]
pub(crate) fn aic3104_init(driver: &mut I2cDriver<'_>) -> Result<(), Aic3104InitError> {
    // Write phase: issue the ordered sequence. Each write is a 2-byte [reg, val] frame.
    for &(reg, val, _rw_mask) in AIC3104_INIT_SEQUENCE {
        if let Err(e) = driver.write(AIC3104_ADDR, &[reg, val], I2C_CTRL_TIMEOUT_TICKS) {
            log::warn!("aic3104_init: write reg=0x{reg:02x} val=0x{val:02x} error: {e:?}");
            return Err(Aic3104InitError::Write {
                reg,
                code: e.code(),
            });
        }
        // Other writes have no documented settle requirement.
        if reg == AIC3104_REG_SOFT_RESET {
            FreeRtos::delay_ms(AIC3104_RESET_SETTLE_MS);
        }
        // See AIC3104_DAC_POWERUP_SETTLE_MS: without this delay the power-up
        // transient is an audible pop through the always-on speaker.
        if reg == AIC3104_REG_DAC_POWER {
            FreeRtos::delay_ms(AIC3104_DAC_POWERUP_SETTLE_MS);
        }
    }

    // Read-back phase: re-read every persistent config register and assert its R/W bits
    // took. Skip the self-clearing soft-reset register (0x01).
    for &(reg, val, rw_mask) in AIC3104_INIT_SEQUENCE {
        if reg == AIC3104_REG_SOFT_RESET {
            continue;
        }
        // A zero mask would make `masked_eq` unconditionally true, silently turning the
        // regression guard inert. Exclude a register via `continue` (like soft reset
        // above), never via a zero mask — trip a debug build if one sneaks in.
        debug_assert!(
            rw_mask != 0,
            "aic3104 reg 0x{reg:02x}: zero rw_mask would pass any read-back; \
             use 0xFF for a full-byte compare or exclude the register with `continue`"
        );
        aic3104_verify_register(driver, reg, val, rw_mask)?;
    }

    Ok(())
}

/// AIC3104 DAC L/R digital-volume registers (page 0, 0x2B / 0x2C). The mute/unmute writes
/// drive **both** so the stereo DAC mutes and unmutes together. Full R/W in the bits these
/// writes set, so the read-back compare is a full-byte compare (mask `0xFF`).
#[cfg(target_os = "espidf")]
const AIC3104_DAC_VOLUME_REGS: [u8; 2] = [0x2B, 0x2C];

/// Unmutes the AIC3104 DAC (writes `AIC3104_DAC_VOLUME_UNMUTED` to both registers,
/// read-back-verified). A failed unmute means a silent tone (amp on, DAC stuck muted),
/// caught here programmatically rather than left to the operator's ear — unlike the
/// best-effort teardown mute (`aic3104_dac_mute_best_effort`).
///
/// The DAC soft-steps from mute to 0 dB rather than snapping; the caller feeds a silence
/// margin into the TX DMA before this write and a settle window after it so the soft-step
/// clocks against silence and completes before tone samples start. Caller holds the
/// `I2C_BUS` lock.
#[cfg(target_os = "espidf")]
pub(crate) fn aic3104_dac_unmute(driver: &mut I2cDriver<'_>) -> Result<(), Aic3104InitError> {
    for &reg in &AIC3104_DAC_VOLUME_REGS {
        if let Err(e) = driver.write(
            AIC3104_ADDR,
            &[reg, AIC3104_DAC_VOLUME_UNMUTED],
            I2C_CTRL_TIMEOUT_TICKS,
        ) {
            log::warn!("aic3104_dac_unmute: write reg=0x{reg:02x} error: {e:?}");
            return Err(Aic3104InitError::Write {
                reg,
                code: e.code(),
            });
        }
        aic3104_verify_register(driver, reg, AIC3104_DAC_VOLUME_UNMUTED, 0xFF)?;
    }
    Ok(())
}

/// Mutes the AIC3104 DAC, best-effort: writes `AIC3104_DAC_VOLUME_MUTED` to both
/// registers so the DAC soft-steps *down* against the silent line before TX stops,
/// rather than snapping to zero. Errors are logged, not propagated or read-back-verified
/// — TX still stops regardless, so quiescence (DAC muted + TX stopped) holds either way.
/// Caller holds the `I2C_BUS` lock.
#[cfg(target_os = "espidf")]
pub(crate) fn aic3104_dac_mute_best_effort(driver: &mut I2cDriver<'_>) {
    for &reg in &AIC3104_DAC_VOLUME_REGS {
        if let Err(e) = driver.write(
            AIC3104_ADDR,
            &[reg, AIC3104_DAC_VOLUME_MUTED],
            I2C_CTRL_TIMEOUT_TICKS,
        ) {
            // Logged but not propagated: quiescence (DAC muted + TX stopped) is the
            // overriding teardown invariant.
            log::warn!("aic3104_dac_mute_best_effort: write reg=0x{reg:02x} error: {e:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{masked_eq, AIC3104_DAC_VOLUME_MUTED, AIC3104_INIT_SEQUENCE};

    // ── AIC3104 masked read-back compare ──────────────────────────────────

    /// Codec-owned status bits (D1, D2) are masked out in the compare. Written 0x0D,
    /// codec reports 0x0F (D1 set by hardware) → match under mask 0xFD.
    #[test]
    fn masked_eq_status_bit_d1_is_ignored() {
        assert!(
            masked_eq(0x0D, 0x0F, 0xFD),
            "0x33: written 0x0D vs got 0x0F (codec set D1) must match under mask 0xFD"
        );
        // 0x56/0x5D: D1 status bit only (D2 reserved stays 0).
        assert!(
            masked_eq(0x0B, 0x0D, 0xF9),
            "0x56/0x5D: written 0x0B vs got 0x0D (codec set D1 only) must match under 0xF9"
        );
        // D2 reserved also set: 0xF9 clears both D2 and D1.
        assert!(
            masked_eq(0x0B, 0x0F, 0xF9),
            "0x56/0x5D: written 0x0B vs got 0x0F must match under mask 0xF9 (D2/D1 cleared)"
        );
    }

    /// Full-byte mask (0xFF): any mismatch is a genuine failure.
    #[test]
    fn masked_eq_full_byte_register_genuine_mismatch_fails() {
        assert!(
            !masked_eq(0xA2, 0x00, 0xFF),
            "all-zeros read-back must fail"
        );
        assert!(
            !masked_eq(0xA2, 0xA0, 0xFF),
            "single R/W bit flip must fail"
        );
    }

    /// The mask must not hide corrupted R/W bits — only codec-owned status bits.
    #[test]
    fn masked_eq_corrupted_rw_bit_on_masked_register_fails() {
        assert!(
            !masked_eq(0x0D, 0x1D, 0xFD),
            "D4 set (R/W bit) must fail under 0xFD"
        );
        assert!(
            !masked_eq(0x0D, 0x05, 0xFD),
            "D3 dropped (R/W bit) must fail under 0xFD"
        );
        assert!(
            !masked_eq(0x0B, 0x03, 0xF9),
            "D3 dropped must fail under 0xF9"
        );
    }

    /// Pin every register's R/W mask. Adding a new non-0xFF mask requires deriving it
    /// from the SLAS510G datasheet and adding an explicit arm here.
    #[test]
    fn aic3104_mask_table_matches_datasheet() {
        for &(reg, _val, rw_mask) in AIC3104_INIT_SEQUENCE {
            let expected = match reg {
                0x01 => 0xFF,        // soft reset: self-clearing, not read back; mask unused
                0x25 => 0xF0,        // D3–D0 reserved (defensive)
                0x33 | 0x41 => 0xFD, // D1 vol-ctl status
                0x56 | 0x5D => 0xF9, // D2 reserved, D1 status
                _ => 0xFF,           // every other register is fully R/W in its set bits
            };
            assert_eq!(
                rw_mask, expected,
                "reg 0x{reg:02x} mask 0x{rw_mask:02x} != datasheet-derived 0x{expected:02x}"
            );
        }
    }

    /// DAC-volume registers (0x2B/0x2C) must init muted (0x80). Unmuting during
    /// amp-enable would defeat the de-pop mute sequencing.
    #[test]
    fn aic3104_init_sequence_dac_volume_is_muted() {
        for &(reg, val, _mask) in AIC3104_INIT_SEQUENCE {
            if reg == 0x2B || reg == 0x2C {
                assert_eq!(
                    val, AIC3104_DAC_VOLUME_MUTED,
                    "reg 0x{reg:02x} must be muted (0x80) at init (de-pop lever 3)"
                );
            }
        }
    }
}
