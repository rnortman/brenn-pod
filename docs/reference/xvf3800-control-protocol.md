# XVF3800 control protocol

The XMOS VocalFusion (XVF3800) control command map the firmware needs, plus the
transport mechanics. The **command map** (resid + cmd + value layout) is a
property of the XVF3800 firmware and is **transport-independent** — it carries
across USB-host control and on-device I2C/SPI alike. The **transport** used to
deliver those commands differs between the spike and the product; see
[§6 Transport](#6-transport-on-device-control).

## 1. Device identifiers [confirmed]

| Field                    | Value                                    | Source |
|--------------------------|------------------------------------------|--------|
| Vendor ID (VID)          | `0x2886` (Seeed Technology)              | usb-spike-verdict.md:17, vfctrl.py:42 |
| USB app-mode PID         | `0x0022` ("reSpeaker Flex XVF3800 L16K6Ch") | usb-spike-verdict.md:17-19, vfctrl.py:43 |
| Safe Mode / DFU PID      | `0x801c` ("reSpeaker XVF3800 Safe Mode") | usb-spike-verdict.md:20-21, vfctrl.py:44 |

These identifiers are for the **USB application/DFU firmware** flashed during the
spike. In the product I2S build the XVF3800 is not a USB device at all (see
audio-and-channels.md); the VID/PID are relevant only for the USB spike rig and
for DFU flashing.

## 2. DFU mode [confirmed]

DFU is reached via the Safe Mode PID `0x801c`. Flashing during the spike used
`dfu-util` against three alt settings (usb-spike-verdict.md:38-53):

| Alt | Name                        | Purpose                          |
|-----|-----------------------------|----------------------------------|
| 0   | reSpeaker DFU Factory       | Recovery slot — **do not write** |
| 1   | reSpeaker DFU Upgrade       | User-flashable slot              |
| 2   | reSpeaker DFU DataPartition | Config / calibration, not firmware |

Confirmed working flash (usb-spike-verdict.md:40):

```
sudo dfu-util -d 0x2886:0x801c -a 1 -D <firmware>.bin
```

On success the device reboots into app mode (`0x0022`). The single known
foot-cannon is `-a 0` (Factory slot). [confirmed]

## 3. Vendor control-transfer mechanics (USB-host) [confirmed]

This is the wire format used **over USB** in the spike. Confirmed in code
(vfctrl.py:38-94) and against the canonical `xvf_host.py`
(research-step0.md:92-141).

```
Read:   bmRequestType = 0xC0   (device→host | vendor | device)
Write:  bmRequestType = 0x40   (host→device | vendor | device)
bRequest = 0                   (always, both directions)
wValue   = (0x80 | cmdid)      on read;  = cmdid  on write
wIndex   = resid               (servicer / module ID)
wLength  = count × type_width + 1  on read   (1 status byte + payload)
```

Read response layout:
- **byte 0** = status: `0x00` = CONTROL_SUCCESS; `0x40` = SERVICER_COMMAND_RETRY
  (sleep ~10 ms, re-issue, up to 100 retries); any other = error.
  (research-step0.md:134-141)
- **bytes 1..N** = payload, little-endian, parsed per the command's type.

Type widths (research-step0.md:123-132): uint8=1, uint16=2, uint32/int32=4,
float=4 (IEEE-754 LE), `radians`=4 (alias for float).

This resid+cmd+`0x80`-read-bit scheme **is** the XMOS "transport protocol" and is
the same logical framing the chip's I2C control interface uses
(research-step0.md:158-164) — which is why the command map below is reusable
on-device.

## 4. Command map

The commands established for this project. resid = servicer/module ID (`wIndex`);
cmd = command ID (`wValue = 0x80|cmd` on read).

### 4.1 AUDIO_MGR_SELECTED_AZIMUTHS — speech-gated DoA winner

| Property | Value |
|----------|-------|
| resid    | **35** (AUDIO_MGR_RESID) |
| cmd      | **11** (read `wValue` = `0x8B`) |
| count    | 2 floats (`radians`) |
| access   | read-only |
| wLength  | 9 bytes (1 status + 2×4) |

Layout (research-step0.md:54-90, vfctrl.py:131-145):
- index 0 = speech-energy-selected DoA (radians); **NaN when no fixed beam
  contains confirmed speech**.
- index 1 = auto-select beam DoA (radians); always populated.

**[confirmed]** — this is the only command the spike actually implemented and
exercised on hardware (vfctrl.py:49-154, doa_logger.py). The spike observed
radians (not degrees), `auto_doa` always populated, `speech_doa` NaN-on-silence
(usb-spike-verdict.md:96-109,136-138). Reads as `wValue=0x8B`, `wIndex=0x23`.

### 4.2 AEC_AZIMUTH_VALUES — per-beam azimuths

| Property | Value |
|----------|-------|
| resid    | **33** (AEC_RESID) |
| cmd      | **75** (read `wValue` = `0xCB`) |
| count    | 4 floats (`radians`) |
| access   | read-only |
| wLength  | 17 bytes (1 status + 4×4) |

Index layout (research-multi-beam.md:8-45):

| Index | Beam              | Notes |
|-------|-------------------|-------|
| 0     | Focused beam 1    | adaptive, slow-update ("Tracker A") |
| 1     | Focused beam 2    | adaptive, slow-update ("Tracker B") |
| 2     | Free-running beam | fast-update scanner |
| 3     | Auto-select beam  | the currently-chosen output |

Internal update rate ~62.5 Hz (every 256 samples at 16 kHz); practical host
poll ceiling ~50 Hz over USB (research-multi-beam.md:74-78).

**[documented]** — from `xvf_host.py:39` and XMOS docs; **not exercised by the
spike** (spike only ran cmd 11). The 4-float values are consumed by an
**off-board tracker — out of firmware scope** (see STATUS.md,
architecture-arc-and-deferred-paths.md).

### 4.3 AEC_SPENERGY_VALUES — per-beam speech energy (VAD-equivalent)

| Property | Value |
|----------|-------|
| resid    | **33** (AEC_RESID) |
| cmd      | **80** (read `wValue` = `0xD0`) |
| count    | 4 floats (`float`) |
| access   | read-only |
| wLength  | 17 bytes (1 status + 4×4) |

Same index order as AEC_AZIMUTH_VALUES (0/1 focused, 2 scanner, 3 auto-select).
**Any value > 0 indicates speech**; higher = louder/closer. XMOS describes this
as VAD-equivalent. There is **no boolean per-beam VAD command** — threshold
SPENERGY client-side (research-multi-beam.md:99-131).

**[documented]** — from `xvf_host.py:44`; not exercised by the spike. Per-beam
attribution is **off-board tracker scope**.

### 4.4 Related / reference commands [documented]

These appear in the spike research but are not part of the core firmware command
set; recorded so they aren't rediscovered:

- `DOA_VALUE` (resid 20, cmd 18, 2× uint16): scalar integer-degree DoA +
  speech-detected flag, on the GPO/LED servicer. (research-step0.md:84-86)
- Fixed-beam mode: `AEC_FIXEDBEAMSONOFF` (resid 33, cmd 37, 1× int32),
  `AEC_FIXEDBEAMSAZIMUTH_VALUES` (resid 33, cmd 81, 2× float, radians),
  `AEC_FIXEDBEAMSELEVATION_VALUES` (resid 33, cmd 82, 2× float, radians) and
  `AEC_FIXEDBEAMSGATING` (resid 33, cmd 83, 1× uint8), all **rw**: **pin** the
  two focused beams to a commanded azimuth and elevation instead of letting
  them track. Declared in `xvf3800-ctrl`. The reachy pod's unattended self-test
  writes them and reads them back (`ctrl_fixedbeams`), and its bench registry
  measures the chip's DoA under them (`fixed_beams_broadside`). No runtime path
  writes them, and every value resets on chip reboot.
  (research-multi-beam.md:89-93,259-274)
- `VNR_VALUE` (CONFIGURATION_SERVICER resid 241, cmd 0): a single global
  voice-to-noise ratio exposed by the **I2S satellite** firmware variant — **not
  present** in the Flex `l16k6ch` USB build's parameter table.
  (research-multi-beam.md:133-149)

## 5. Value semantics / units

- Azimuths are **radians**, IEEE-754 32-bit float, little-endian. [confirmed for
  cmd 11; documented for cmd 75]
- **NaN** = "no confirmed speech" sentinel on AUDIO_MGR_SELECTED_AZIMUTHS
  index 0. [confirmed]
- AEC_SPENERGY: float, **0 = no speech**, >0 = speech present (magnitude =
  loudness/proximity). [documented]
- For a linear array mounted horizontally at ear height, DoA collapses to the
  broadside half-plane; the spike saw `auto_doa` sweep the full half-plane in
  the absence of confirmed speech (usb-spike-verdict.md:96-107). Interpretation
  of multiple azimuths over time is **off-board tracker scope**.

## 6. Transport: on-device control

**The spike issued every command above as USB vendor control transfers from a
Linux host** (vfctrl.py:38-94). That is a host-to-USB-device path.

**The product firmware does not work this way.** The product runs on the XIAO
ESP32-S3, which controls the XVF3800 over an **on-device** transport — the
XVF3800 control interface is I2C (or SPI), not USB-host. The ESP32-S3 is the
controller; the XVF3800 is a peripheral on a board-level bus.

What is known now vs. what gets confirmed empirically during firmware bring-up:

- **[documented]** The XVF3800 control framing (resid + cmd + `0x80` read bit,
  status byte, LE payload) is the same logical "XMOS transport protocol" over
  I2C as over USB. The formatBCE ESPHome integration talks to the chip over
  **I2C at address `0x2C`** using exactly this framing
  (research-step0.md:158-164, research-step0.md:227-232). So the **command map
  in §4 is expected to carry over unchanged** to on-device I2C.
- **[unknown — confirmed empirically during firmware bring-up]** The concrete
  on-device wire details for *our* product build are **not resolved by the spike
  sources**; they are observed once the firmware brings up the on-device control
  interface against the I2S build:
  - Which transport the **product I2S firmware** exposes for control (I2C vs
    SPI) — the USB spike build used USB control; the I2S build's control
    interface was not exercised. The formatBCE I2C-at-`0x2C` figure is from a
    *different firmware blob* (the square i2c-host build, research-step0.md:236),
    not the linear I2S build we plan to ship.
  - The exact I2C register/command-framing on the wire (how resid/cmd/length map
    to I2C write-then-read transactions, addressing, clock, any wrapper bytes).
  - Whether the XVF3800 reset/DFU lines need to be driven by the ESP32-S3 GPIO
    and how (related GPIO-BOOT routing is a deferred PCB question, see
    architecture-arc-and-deferred-paths.md:293-297).

**Do not assume the §4 byte layouts include the USB SETUP-packet wrapper when
implementing the I2C path.** The resid/cmd/payload semantics carry; the framing
bytes around them are transport-specific, confirmed against the XVF3800 I2C
control-interface spec and/or the formatBCE driver source and observed on the
wire as the Rust port brings up the on-device control interface.

## 7. Open chip-behavior questions (observed empirically during bring-up)

These two chip-behavior questions are **off-board-tracker concerns** (not
firmware work) whose answers come from observing the real device under
multi-talker conditions; they get answered empirically as the firmware brings up
the per-beam telemetry path and the device runs. Recorded once here
(research-multi-beam.md:296-314):

1. Do Tracker A / Tracker B (indices 0/1) swap identity between speakers under
   overlapping speech?
2. Do the beam azimuths hold their last DoA or drift freely during silence?
   (SPENERGY-gating index N is required to trust azimuth N regardless.)

Also unverified but minor: whether the Flex `v1.0.0` blob is built from XMOS
sw_xvf3800 2.x or 3.x, which affects the NaN-on-silence semantics of cmd 11
(research-multi-beam.md:284-293). Empirically the spike *did* observe
NaN-on-silence, so the 3.x semantics appear to hold. [confirmed for our blob]

## 8. The six-channel USB audio image (`2886:0022`): the state `pod-capture` reads

`pod-capture` reads seventeen registers from the chip of the device it records,
once, before the stream starts, over the §3 framing and the kernel's usbfs
control ioctl (`firmware/crates/xvf3800-usb`); the register table is
`STATE_REGISTERS` in `firmware/crates/xvf3800-ctrl/src/state.rs`. Every one is a
read: `bmRequestType` `0xC0`, `bRequest` 0, `wValue` `0x80 | cmd`, `wIndex`
resid, `wLength` the payload length plus the status byte. Nothing is written.
The same table is read for `38fb:1001` and `2886:001a`; only `2886:0022` has
been read on hardware. [confirmed for `2886:0022`]

### 8.1 One board's readings, 2026-11-13

What one reSpeaker Flex on this image (`2886:0022`, `bcdDevice` 0100) held on
2026-11-13, read by `make -C firmware chip-hil`. These are that board's
readings on that day, not the image's defaults: none has been confirmed as the
image's own value, and the guide cited is XMOS's XVF3800 User Guide v3.2.1
(`firmware/vendor/xvf3800/docs/`), which describes XMOS's development kit and
not Seeed's image. Every read answered status `0x00` on its first attempt;
**the image refused none of the seventeen.**

| Register | resid, cmd | `wValue`, `wLength` | Payload read | Against the guide |
|---|---|---|---|---|
| `version` | 48, 0 | `0x0080`, 4 | `01 00 00` (1.0.0) | no document states it |
| `bld_msg` | 48, 1 | `0x0081`, 51 | `"ua-io16-6ch-lin"`, NUL-padded | no document states it |
| `bld_repo_hash` | 48, 3 | `0x0083`, 41 | `"4b339d00721937451ee487759c04e2acb3215793"` | no document states it |
| `bld_modified` | 48, 4 | `0x0084`, 7 | `"FALSE"` | no document states it |
| `audio_mgr_op_l` | 35, 15 | `0x008f`, 3 | `08 00` (8, 0) | agrees (p. 23; Table 10.4, p. 81) |
| `audio_mgr_op_r` | 35, 19 | `0x0093`, 3 | `07 03` (7, 3) | no single default: an amplified microphone (p. 21, prose) or (0, 0) (Table 3.2, p. 21; p. 23; Table 10.4, p. 81); the reading is neither |
| `audio_mgr_op_all` | 35, 23 | `0x0097`, 13 | `08 00 01 00 01 02 07 03 01 01 01 03` | no default stated (Table 10.4, p. 82) |
| `audio_mgr_op_packed` | 35, 13 | `0x008d`, 3 | `00 00` | agrees (Table 10.4, p. 81) |
| `audio_mgr_op_upsample` | 35, 14 | `0x008e`, 3 | `00 00` | no default stated for a 16 kHz interface (Table 10.4, p. 81) |
| `audio_mgr_selected_channels` | 35, 12 | `0x008c`, 3 | `03 03` | agrees (Table 10.4, p. 81) |
| `aec_asroutonoff` | 33, 35 | `0x00a3`, 5 | `01 00 00 00` (1) | differs: 0 (Table 10.1, p. 75) |
| `aec_asroutgain` | 33, 36 | `0x00a4`, 5 | `00 00 80 3f` (1.0) | agrees (Table 10.1, p. 75) |
| `audio_mgr_mic_gain` | 35, 0 | `0x0080`, 5 | `00 00 20 41` (10.0) | agrees (Table 10.4, p. 80) |
| `audio_mgr_ref_gain` | 35, 1 | `0x0081`, 5 | `33 33 f3 3f` (1.9) | differs: 1.5 (Table 10.4, p. 80) |
| `audio_mgr_sys_delay` | 35, 26 | `0x009a`, 5 | `0c 00 00 00` (12) | differs: -32 (Table 10.4, p. 82) |
| `audio_mgr_far_end_dsp_enable` | 35, 25 | `0x0099`, 2 | `00` | agrees (Table 10.4, p. 82) |
| `i2s_dac_dsp_enable` | 35, 27 | `0x009b`, 2 | `00` | agrees (Table 10.4, p. 82) |

Seven agree with the guide, three differ from it, and seven have no single
stated value: the four identity registers, which no document states, and the
three the guide gives no single default for. The ten that do not agree are
tracked as `flex-usb-image-chip-readings-unconfirmed`, and the chip hardware
test fails on them until each has a confirmed source.

### 8.2 Not known

- **Which register governs which of the six capture channels.** The image's
  processed channels may be the left and right output multiplexers or may not;
  the record names readings by register and claims no mapping from register to
  capture channel.
- **Whether a reading is the image's value or a saved configuration.** A value
  saved to the chip's flash comes back after a power cycle exactly as the
  image's own would.
- **Changes during a capture.** The reading is taken once, before the stream
  starts; a write to the chip by another program during a capture is not seen.
