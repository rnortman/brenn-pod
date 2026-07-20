# reSpeaker Flex — Audio-Output Hardware Reference

**Board:** Seeed reSpeaker Flex
**Scope:** how the audio-output hardware path works and what it requires. This document describes *hardware only* — chips, wiring, buses, registers, protocols, timings, and electrical behavior. It says nothing about any firmware.

All values below were either taken from datasheets/vendor sources or confirmed empirically on the bench; items not yet confirmed are called out in §9.

---

## 1. Chip inventory

| Device | Part | Role | Bus address / control |
|---|---|---|---|
| Host MCU | Seeed XIAO **ESP32-S3** | Runs firmware; I2S **slave**, I2C **master** | — |
| Voice processor | XMOS **XVF3800** | Mic-array DSP/AEC; **I2S master** (generates BCLK/WS); re-clocks audio between host and codec; exposes GPO/control over I2C | I2C **0x2C** |
| Codec / DAC | TI **TLV320AIC3104** | DAC driving the speaker output stage | I2C **0x18** |
| Amplifier | TI **TPA3139D2** | Class-D speaker amp | Nominal XVF3800 GPO enable (X0D31), but **not software-reachable — always on**; see §6/§7. No I2C |
| Output | Speaker | — | — |

The host ESP32-S3 masters the I2C bus that reaches both the XVF3800 (0x2C) and the AIC3104 (0x18). The XVF3800 masters the I2S audio clocks.

---

## 2. Audio signal chain & clocking topology

```
ESP32-S3  I2S0 DOUT ──► XVF3800 ──(second I2S link)──► AIC3104 ──► TPA3139D2 ──► speaker
          (slave)       (I2S master,                   (DAC)       (class-D amp)
                         re-clocks)
```

Key topology facts:

- **The host is never wired directly to the codec's I2S.** The XVF3800 sits in the middle, consumes the host's I2S stream, and **re-clocks** a separate I2S link to the AIC3104. Host audio reaches the speaker only by passing through the XVF3800.
- **The XVF3800 is the I2S master.** It generates BCLK and WS. The host I2S port must operate as a **slave/target** that consumes those externally-supplied clocks. If the host is configured to master the I2S clocks, the link does not work (and mic capture breaks).
- **There is effectively one usable host I2S clock domain** — the one driven by the XVF3800. Both directions (playback host→XVF3800, capture XVF3800→host) must share that single externally-clocked port. A second, independently-clocked host I2S port is not viable in this wiring, because only the XVF3800's clocks are present.
- **Frame format:** 2-slot (stereo) I2S. Sample rate **16 kHz**. Slot width is assumed **32-bit** (see §9 for the open 16- vs 32-bit question), giving **BCLK ≈ 1.024 MHz** (16 kHz × 2 slots × 32 bits). A mono source must be presented in the stereo frame; the bus clocks both slots regardless.

---

## 3. Pin / wiring map (host ESP32-S3)

**I2S link to the XVF3800:**

| Signal | Host GPIO | Notes |
|---|---|---|
| BCLK | GPIO **8** | Driven by XVF3800 (host is slave) |
| WS / LRCLK | GPIO **7** | Driven by XVF3800 |
| DOUT (host → XVF3800, playback) | GPIO **44** | |
| DIN (XVF3800 → host, capture) | GPIO **43** | |
| MCLK | **none** | No master clock line is used |

**I2C bus (host master → XVF3800 0x2C, AIC3104 0x18):**

| Signal | Host GPIO | Notes |
|---|---|---|
| SDA | GPIO **5** | |
| SCL | GPIO **6** | 100 kHz |

**Other:**

| Function | GPIO | Notes |
|---|---|---|
| User LED (amber) | GPIO **21** | Active-low; see §8 |

---

## 4. I2C bus — behavior & gotchas

- **Bus:** host is master; SDA=GPIO5, SCL=GPIO6, 100 kHz; slaves at 0x2C (XVF3800) and 0x18 (AIC3104).

- **AIC3104 reads require a STOP between the pointer write and the read.** Reading an AIC3104 register is a two-part transaction: write the target register address, then read. These **must be separated by a STOP** (two distinct transactions). A combined/repeated-START "write-then-read" (no STOP) **wedges the bus** on this codec — do not use it.

- **AIC3104 pointer auto-increments after a write.** After any register write, the codec's internal address pointer advances by one. A read that does not re-address therefore returns register **N+1**, not N. Read-back must re-send the target address (and still obey the STOP rule above). The writes themselves are correct regardless; only naïve read-back is affected.

- **No native I2C bus recovery.** A slave that is left holding the bus (SDA stuck low / clock-stretch held) is **not** cleared by the host rebooting — a host reset does not reset the slave. To physically release a stuck slave, the master must **clock SCL up to ~9 pulses and then issue a STOP** (bit-banged). A true stuck slave clears on **loss of power to the slave** or by being clocked out; it persists across a warm host reset.

- **Two distinct I2C failure signatures — they mean different things:**
  - **Timeout (bus held):** the master waited for the bus / for a slave to release a stretched clock and gave up. Signature of a **physically held bus** (a slave stuck after a partial/aborted transaction).
  - **NAK / no-ACK (transaction completed at the timing level, but the slave did not acknowledge):** the addressed slave did not ACK its address or a data byte. On the XVF3800 this indicates the device is **momentarily busy** (still servicing a previous command) and cannot accept a new START yet — a *transient* condition, not a stuck bus.

- **XVF3800 is busy immediately after a command.** After a control/GPO write completes (STOP sent), the XVF3800 continues to process internally and will **NAK new transactions** for a short window. A settle gap is required before the next transaction to that device (see §6).

- **Early-boot bus hold.** The XVF3800 takes time to boot. Host I2C transactions issued *very* early (before the XVF3800 is ready) can fail; the codec/voice-processor must be allowed to come up before the first I2C access.

---

## 5. AIC3104 codec — required power-up sequence

**The XVF3800 does not configure the AIC3104.** The codec powers up to its plain power-on default register values and stays there. The **host must fully initialize it over I2C** before any audio will reach the speaker. The ordered page-0 register sequence below is what is required; the **DAC→output routing block (especially register 0x52, the speaker path) is the piece that actually connects the DAC to the speaker output.**

Ordered sequence (all on page 0, I2C address 0x18):

| Step | Register (hex) | Value (hex) | Meaning |
|---|---|---|---|
| Soft reset | 0x01 | 0x80 | Reset; **wait ≥ 1.5 ms** before continuing |
| PLL source | 0x66 (102) | 0xA2 | Select **BCLK** as the PLL clock source |
| PLL J | 0x04 | 0x30 | J = 12 |
| PLL D (msb) | 0x05 | 0x00 | |
| PLL D (lsb) | 0x06 | 0x00 | |
| PLL R | 0x0B (11) | 0x08 | **R = 8** (32-bit-slot assumption; see §9) |
| PLL | 0x65 (101) | 0x00 | |
| PLL enable | 0x03 | 0x91 | **PLL enabled**, Q = 2, P = 1 |
| Sample rate | 0x02 | 0x44 | fs → **16 kHz** |
| Datapath | 0x07 | 0x0A | DAC datapath setup |
| Audio serial iface | 0x08 / 0x09 / 0x0A | 0x00 / 0x00 / 0x00 | I2S, slave timing |
| DAC power | 0x25 (37) | 0xC0 | Power up L+R DAC |
| DAC volume | 0x2B / 0x2C (43/44) | (volume) | Set L/R DAC digital volume (unmute / level) |
| DAC → output routing | 0x2F / 0x40 / **0x52** / 0x5C (47/64/**82**/92) | 0x80 each | Route DAC to outputs; **0x52 = speaker path** |
| Output drivers | 0x33 / 0x41 (51/65) | 0x0D each | Enable/power output drivers |
| Output drivers | 0x56 / 0x5D (86/93) | 0x0B each | Enable/power output drivers |

Notes:
- **PLL is driven from BCLK** (register 0x66 = 0xA2). The PLL multiplier constants (P=1, R=8, J=12, Q=2) assume **32-bit I2S slots** → BCLK ≈ 1.024 MHz at 16 kHz. If the slots are actually 16-bit, the PLL must be re-tuned (see §9).
- Sources for the sequence: **TI SLAS510G** (TLV320AIC3104 datasheet), the Seeed reference volume sketch, and the palmerr23 Teensy AIC3104 driver.

---

## 6. XVF3800 control protocol & GPO map

The XVF3800 is controlled over I2C (0x2C) using the XMOS device-control protocol: transactions are addressed by a **resource ID (resid)** plus a **command**, with a payload. For a **read**, the **READ bit (0x80)** is OR'd into the command byte; the device then returns a fixed-length payload.

**General purpose outputs (GPO) — amp & carrier-board control:**

- **resid = 20** (the GPO servicer).
- **Read the GPO vector:** command **0** with the read bit → read **5** bytes (5 GPO pins: X0D11, X0D30, X0D31, X0D33, X0D39 — per the Seeed `xvf_host.py` authoritative source). The returned vector's entries map to individual GPO lines:
  - **Index 1 = X0D30** — carrier-board **mute LED + mic-mute**. (This is *not* the host's red LED — see §8.)
  - **Index 2 = X0D31** — *nominal* **amplifier enable**, **active-low** (0 = amp enabled, 1 = amp disabled). **Not software-reachable on this board** — see the always-on note below and §7.
- **After writing a GPO**, allow **~5 ms settle** before reading the vector back (the device needs time to apply and reflect the change; and per §4 it will NAK transactions issued too soon).

- **The amp GPO is NOT software-writable on this board — cmd 0 is read-only.** resid 20 / cmd 0 is `GPO_READ_VALUES`, a **read-only** command. A "write" addressed to cmd 0 is **accepted and reports DONE, but is inert**: X0D31 (and every other GPO line) **never moves**. There is therefore no working software amp-enable/disable — the amp is **always on** as far as firmware is concerned (see §7). Confirmed on the bench by bypassing every amp-enable and still hearing audio. See the diagnosis ADRs at `docs/adr/2026/06/21-audio-output-clean-shutdown/` (`diagnosis-inert-write.md`, `diagnosis-amp-mystery.md`).

**Other readable values over the same protocol:**

- **Speech energy (SPENERGY):** resid **33** (AEC), command **80**; the read command on the wire is therefore 0x80 | 80 = **0xD0 (208)**.
- **Direction of arrival (DoA):** also read from the XVF3800 via the control protocol.

---

## 7. TPA3139D2 amplifier

- **Always on — there is no working software amp-enable on this board.** X0D31 is the *nominal* amp-enable line (active-low: 0 = enabled, 1 = disabled), but it is **not software-reachable**: the only path to it is the XVF3800 GPO servicer's cmd 0, which is `GPO_READ_VALUES` (**read-only**). A write to cmd 0 is accepted-and-DONE but **inert** — X0D31 never moves (see §6). The amp is therefore effectively **hardwired on**; firmware cannot enable or disable it. Confirmed on the bench by bypassing every amp-enable and still hearing audio. Cross-references: the diagnosis ADRs `diagnosis-inert-write.md` / `diagnosis-amp-mystery.md` under `docs/adr/2026/06/21-audio-output-clean-shutdown/`. Because the amp cannot be silenced, the clean-shutdown levers are upstream of it (hold the I2S line at silence; soft-mute the DAC) — but that is firmware behavior, out of scope for this hardware doc.
- **Noise behavior:** because the amp is always on, the I2S line into the codec **must already be carrying silence** at every transition (start/stop), or the speaker emits a burst of **white noise** / a pop. If the line is **not carrying silence** (undriven / garbage / last-buffer repeats), the always-on amp faithfully reproduces it. There is no amp-off lever to mask this — silence on the line (and a soft-stepped DAC) is the only way to avoid audible artifacts.

---

## 8. LEDs (hardware truth of each)

- **Red LED — CHARGE LED, not firmware-controllable.** This is the XIAO ESP32-S3's battery-charger status LED, hardwired to the charger IC. It is **not** on a GPIO and cannot be driven by firmware. It lights whenever the board is on **USB-C power without a battery installed**, and self-extinguishes after ~30 s. It is **not** a fault indicator.
- **Amber user LED.** On host **GPIO21**, **active-low**. Used as a heartbeat (~1 Hz).
- **Mute LED.** Driven by XVF3800 GPO **X0D30** (carrier board), shared with mic-mute (see §6).

---

## 9. Open / unverified hardware questions

- **I2S slot width: 32-bit vs 16-bit.** The codec PLL constants in §5 (R=8) assume **32-bit** slots (BCLK ≈ 1.024 MHz). If the link is actually **16-bit** slots (BCLK ≈ 512 kHz), the PLL will not lock correctly and audio pitch will be wrong; the documented fallback is **R = 16**. Which is physically correct has **not** been definitively confirmed — it requires a clean bench check (lock + correct pitch).
- **XVF3800 GPO-write framing — length byte. (Moot for the amp.)** The exact semantics of the length byte in the control-**write** framing differ from the XMOS worked example (the framing appears to under-count it). For the **amp this is moot**: resid 20 / cmd 0 is `GPO_READ_VALUES` (**read-only**), so **no** length value or framing makes the amp move — the write is inert regardless (see §6/§7). The framing question only matters if a future writable GPO command is used; for amp-enable it is settled (no working write exists). Cross-reference: `diagnosis-inert-write.md` under `docs/adr/2026/06/21-audio-output-clean-shutdown/`.

---

## 10. Source references

- TI **SLAS510G** — TLV320AIC3104 datasheet (codec register map, PLL, output routing).
- TI TPA3139D2 datasheet (amplifier).
- XMOS XVF3800 control-protocol / GPO documentation and worked examples.
- Seeed reSpeaker reference material (volume sketch; board wiring).
- palmerr23 Teensy TLV320AIC3104 driver (cross-check of the power-up sequence).
- Bench confirmation on the reSpeaker Flex: I2S topology (XVF3800 as master, shared clock domain), the AIC3104 full-init requirement and speaker-path routing (0x52), the GPO vector layout (5-byte read; X0D31 amp / X0D30 mute), the **amp-always-on / cmd-0-inert-write reality** (audio persists with every amp-enable bypassed), the I2C STOP-and-re-address read requirement and pointer auto-increment, the NAK-vs-timeout distinction, and the LED identities.
- Diagnosis ADRs (`docs/adr/2026/06/21-audio-output-clean-shutdown/`): `diagnosis-inert-write.md` (resid 20 / cmd 0 = `GPO_READ_VALUES` read-only; 5-byte vector per Seeed `xvf_host.py`), `diagnosis-amp-mystery.md` (always-on amp confirmed by bypass).
