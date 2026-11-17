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

- **What decides capture channels 2 to 5.** Channels 0 and 1 are decided by
  the `L_PK0` and `R_PK0` slots of `AUDIO_MGR_OP_ALL`
  ([§9.3](#93-the-register-to-channel-map)); what decides channels 2 to 5 is
  not known.
- **Whether a reading is the image's value or a saved configuration.** A value
  saved to the chip's flash comes back after a power cycle exactly as the
  image's own would.
- **Changes during a capture.** The reading is taken once, before the stream
  starts; a write to the chip by another program during a capture is not seen.
  `xvf-route` ([§9.2](#92-the-write)) is such a program: it changes the
  routing between captures, and a capture's reading states the routing in
  force when it started.

## 9. The six-channel USB audio image: writing the routing, and the reference on a capture channel

What was found on 2026-11-17 by the routing hardware tests
(`firmware/devices/pod-capture/tests/flex_route_hil.rs`, run by
`make -C firmware route-hil` and `make -C firmware route-measure-hil`) on one
reSpeaker Flex on this image (`2886:0022`, build `ua-io16-6ch-lin`), serial
`100026178261700017` as the tests print it, played and recorded at 16 kHz
through its own USB audio interface with the mixer's playback volume at 40.
Each statement is marked **[measured]** (read off that board by a test) or
**[inferred]** (reasoned from measurements and the vendor's documents, not
tested). The played signal throughout is band-limited Gaussian noise, 100 Hz
to 6 kHz, at −20 dBFS RMS, the same on both playback channels; the *host's
copy* is the samples the host wrote to the playback device.

### 9.1 What the device does when it plays and records together

- **[measured]** While the device plays and records at once, the delay of the
  recorded echo behind the host's copy steps within single plays. Over twenty
  plays of 20 s it stepped in 18 of 20 in one run and 11 of 20 in another, and
  in 11 to 15 of 20 in each of four further runs. Step sizes, one microphone,
  first run: −8 frames twenty times, +24 seven times, +2 three times, and +3,
  +5, +6 once each; second run: −8 eleven times, +24 five times, +2 twice, and
  +3, +4, +6, +7, −15 once each. Across the six runs, a step appears on all
  four microphones in the same window or the next, with two exceptions: a −2
  step that appears on three microphones together and on the fourth twelve
  windows (1.5 s) later, and one play in which the delay alternates by 8
  frames, back and forth, dozens of times on every microphone, the
  alternations mostly but not always in the same windows.
- **[measured]** Capture side: runs of exactly 24 frames in which every
  capture channel is zero, 45 of them across six runs of twenty plays: 43
  inside plays and 2 in the silence between plays. At 39 of the 43, the host's
  copy steps by +24 on every microphone in the same window (+23 on some
  microphones at two of them); at one, inside the alternating play above, by
  +16 and +8 in consecutive windows; and at three it steps the other way, by
  −15, −16 and −26 frames, the content that much earlier. In one run the
  plays with a +24 step were exactly the plays with a zero run. Earlier bench
  runs on the same device saw the content 16 to 17 frames earlier after a
  zero run (40 frames lost behind the zeros) and, rarely, one USB packet of 8
  frames missing from the capture's frame count against host time.
- **[measured]** Playback side: steps of −8 frames, and of +1 to +7, with no
  zero run and nothing in the host's playback position, the driver's delay
  report or the captured frame count. Only the echo shows them.
- **[measured]** Neither kind passes through the host's streams as an error:
  the capture reports no overrun and no gap, the playback no underrun.

### 9.2 The write

- **[measured]** The chip on this image accepts a write of `AUDIO_MGR_OP_ALL`
  (resid 35, cmd 23) over USB: `bmRequestType` `0x40`, `bRequest` 0, `wValue`
  `0x0017` (the command id without the read bit), `wIndex` 35, `wLength` 12,
  and the twelve payload bytes, six (category, source) pairs. The transfer
  completes with all twelve bytes moved and no stall, and a read of the
  register (§3) then returns what was written. Every write the tests made
  read back as written.
- **[documented]** The chip returns no status for a write: a completed
  transfer says only that the bytes were moved. `xvf-route` therefore judges
  every write by reading the register back, and treats a stalled transfer
  (`EPIPE`) as a refusal.
- **[measured]** The write is volatile in the sense that it is undone by
  writing the earlier value back: every test read the routing before writing,
  wrote it back afterwards and read it back, and every restore succeeded.
  **[documented]** It is lost on a power cycle unless saved to flash.
- Nothing here sends `SAVE_CONFIGURATION`, `CLEAR_CONFIGURATION` or `REBOOT`.
  `xvf-route` (`firmware/crates/xvf-route`) refuses, before anything is sent,
  every write that is not twelve bytes to `AUDIO_MGR_OP_ALL`; it is the only
  program in the tree that writes to a chip on this image, and `pod-capture`
  does not link it.

```sh
xvf-route show --card "Flex XVF3800"            # AUDIO_MGR_OP_ALL and the six PK registers
xvf-route set --card "Flex XVF3800" --op-all 8,0,1,0,1,2,7,3,1,1,1,3
```

`set` prints the reading before, writes, and prints the reading after; it
fails unless the read-back matches. The bytes above are this board's routing
as found (§8.1), which every writing test requires before it starts and
leaves behind.

### 9.3 The register-to-channel map

- **[measured]** `AUDIO_MGR_OP_ALL`'s six pairs are the six `PK` registers in
  the order `L_PK0`, `L_PK1`, `L_PK2`, `R_PK0`, `R_PK1`, `R_PK2`: each `PK`
  register reads the pair at its slot.
- **[measured]** `L_PK0` (equivalently `AUDIO_MGR_OP_L`) decides capture
  channel 0, and `R_PK0` (equivalently `AUDIO_MGR_OP_R`) decides channel 1.
  Each was found by writing silence, (0, 0), into the slot, capturing two
  seconds, and finding exactly that channel all zero and every other channel
  live.
- **[measured]** The same write into each of `L_PK1`, `R_PK1`, `L_PK2` and
  `R_PK2` is stored and reads back, and zeroes no channel: every channel of
  the capture has samples other than zero. Figures, two-second captures in a
  quiet room: in the captures made before any write, channel 0 peaks at 218
  and 240 and channels 1 to 5 at 31 to 43, with no channel all zero; with any
  one of these four slots silenced, channel 0 peaks at 233 to 403 and no
  channel is all zero.
- **[inferred]** The four slots are unused while packing is off: the vendor's
  control table describes `PK1` and `PK2` as the second and third sources of a
  side "in packed mode", and this board reads `AUDIO_MGR_OP_PACKED` as
  `00 00`. Channels 2 to 5 are placed on USB by the image independently of
  `AUDIO_MGR_OP_ALL`. The baseline's channels 2 to 5 are the four raw
  microphones (`docs/reference/audio-and-channels.md`, section 2).
- **Not established:** what decides channels 2 to 5, and whether any command
  reroutes them. Turning packing on is a write `xvf-route` refuses and changes
  what every channel carries; it has not been tried.

The hardware test that expected the four packed slots to decide channels 2 to
5 fails on this reading and is tracked as
`usb-image-capture-channel-register-map-unknown`.

### 9.4 Which far-end categories carry the played signal

The guide describes each far-end category as data "received over I2S" (XMOS
XVF3800 User Guide v3.2.1, Table 3.2, p. 21). On this image each one tried
carries the USB playback stream. **[measured]** Each was routed to channel 1
through `R_PK0` while two seconds of noise played. A channel's *figure* is
the median, over windows of 0.25 s with a hop of 0.125 s, of the peak
normalised cross-correlation with the host's copy within ±64 frames of the
channel's best overall lag. A channel *carries* the played signal at a figure
of 0.8 or more that is above every raw microphone's.

| (category, source) | Guide's description | Figure | Best microphone | Carries |
|---|---|---|---|---|
| (12, 0) | amplified far end with system delay; the reference passed to the canceller | 1.000 | 0.322 | yes |
| (5, 0) | far end with system delay | 0.999 | 0.318 | yes |
| (4, 0) | far end | 0.999 | 0.330 | yes |
| (10, 0) | far end at native rate | 1.000 | 0.316 | yes |
| (10, 1) | far end at native rate | 1.000 | 0.327 | yes |

**[measured]** The routed channel is zero outside the play, and the
microphones' copies of the played signal arrive 26 to 38 frames after the
routed channel's. The conference beam's figure in the same captures is 0.03 to
0.04.

### 9.5 The measurement

The routing: this board's routing with `R_PK0` set to (12, 0),
`8,0,1,0,1,2,12,0,1,1,1,3`. Twenty plays of 20 s, one second of silence
between them, in one capture. In windows of 0.25 s with a hop of 0.125 s, the
delay from the reference channel to each raw microphone, and from the host's
copy to each raw microphone, each read by matching the window's
cross-correlation over ±64 lags against that pair's cross-correlation over
the first 2 s (of the run for the reference, of the play for the host's
copy).
The reference *holds still* if its delay to each microphone stays within one
frame of the run's median in every window of every play; a run in which the
host's copy does not step has no control. A window whose cross-correlation
peaks below 0.1 within the ±64 lags cannot be read.

How a window's delay is read was changed once, after a first run, and the
change is stated here so it can be judged.

- **[measured]** A run of 2026-11-17 on the routing
  `5,0,1,0,1,2,12,0,1,1,1,3`, (5, 0) on channel 0 beside (12, 0) on channel 1,
  read each window's delay as the lag of its strongest correlation peak. Read
  so, 2672 of 12 720 windows of (12, 0) against the microphones were more
  than one frame from the median, and 2500 of 12 720 of (5, 0). Every one was
  later than the median, by 2 frames (1738 of (12, 0)'s, 1613 of (5, 0)'s) or
  by 3 (934 and 887); none was earlier. (12, 0)'s were 860, 803, 462 and 547
  on the microphones of channels 2, 3, 4 and 5. Of the 1010 windows in which
  (12, 0) was off the median against some microphone, it was off against all
  four in 378, three in 142, two in 244 and exactly one in 246. The delay
  between the two taps had a median of 0 in every play and no step in any
  play; a step being a change of more than one frame, this does not exclude
  a window one frame off.
- **[measured]** Read the same way, an excursion of (12, 0) (consecutive
  windows off the median against one microphone) lasted one window in 722 of
  1499 and two windows in 544; the longest lasted 12. Of the 2972 changes of
  more than one frame in (12, 0)'s delay to a microphone, each excursion's
  start or end, 2715 fell in the same window as a change of the same size in
  the host's copy's delay to the same microphone, also read by the peak, and
  2851 within one window of one.
- What those matching changes show, and no more: the reference's delay to a
  microphone and the host's copy's delay to the same microphone share the
  path from the tap through the loudspeaker and the room to that microphone's
  capture; the reference's own route from the tap to its capture channel is
  in the first alone. A change in the same window in both lies on the shared
  path, not in the reference's own route. It does not tell an acoustic
  arrival from a device slip on that path, on the loudspeaker side or the
  microphone side, which would move the host's copy's delay as well.
- **[inferred]** The excursions are the peak moving between two acoustic
  arrivals of nearly equal strength 2 to 3 frames apart at each microphone,
  not the device slipping. The excursions take two values only, in one
  direction; they undo themselves; in most windows they fall on the
  microphones separately; and read by the whole shape of the window's
  correlation, which a slip shifts and a change of the stronger arrival does
  not, the four runs of the same kind reported below show none. The analysis
  tests in `firmware/devices/pod-capture/tests/route_analysis/mod.rs` hold
  the method to this on generated signals: two arrivals 26 and 29 frames late
  under noise keep their delay until a slip, and the slip is found as one
  step of its size, for a slip of −8 frames
  (`two_nearly_equal_arrivals_in_noise_hold_their_delay_until_a_slip`) and
  for one of +3, the arrivals' own spacing
  (`a_slip_of_the_two_arrivals_own_spacing_is_one_step_of_that_size`).
- Every per-window delay and every step figure in §9.1 from this cycle's
  runs, in §9.5 and in §9.6 is read by the matching described above. §9.4's
  figures are a peak-correlation figure by definition, not per-window delays,
  and the earlier bench runs of §9.1 were read by an earlier method. The
  peak-read run is not among the runs §9 reports, and no run of the
  reference's own routing read by the peak was kept. The one-frame criterion
  is the same under both readings.
- **A difference of one frame is within the criterion and fails no test.**
  Such differences were measured, between the plays of a run and inside
  plays, and are given below. A change of one frame inside a play shows only
  in the window-by-window delays; a run's figures and a play's median do not
  show it.

Eleven runs are reported here, all on 2026-11-17: six in an earlier session
(two of (12, 0) alone and one for each of four other taps) and five in a
later one (one for each tap and then one of (12, 0) alone). The earlier
session's runs were made under a test that failed only on a window more than
one frame from the median, and counted unreadable windows without holding
them against the run. The later session's were made under the test as it
ships, which also fails a run on a window in which the reference's delay to a
microphone cannot be read while the host's copy's delay to that microphone is
read in that window and in the windows either side of it (an *unexplained
window*).

Every figure in this section was read again from its run's kept log by the
analysis as it now ships (`make -C firmware route-reread`), and each
reading gave every figure its run had reported; for the earlier session's
runs, whose reports were written before the report gained its later fields,
every figure the two reports share. Nothing was played or captured again:
the logs were read again. The re-reading also gives each window's delay, and
the host's copy's delay to the reference channel itself, read as its delay
to a microphone is; the window-by-window figures below come from it.

The earlier session:

- **[measured]** The recorder's log of each of the six runs below holds no
  loss and no break (its summary lists no discontinuity), so no step counted
  here is a gap in the recording joined over. The test refuses a log that
  holds one.
- **[measured]** Two runs of (12, 0): the host's copy stepped in
  18 and in 11 of 20 plays; the reference's delay to the microphones on
  channels 2, 3, 4 and 5 was 26, 25, 24 and 23 frames in both runs, with no
  window more than one frame from the median and no step in any play.
- **[measured]** Each of the 13 zero runs inside a play in these two runs
  zeroed the reference channel over the same frames as the microphones and
  the beam, and left every reference-to-microphone delay, read in the windows
  either side of it, as it was before.
- **[measured]** In the second run 10 of the 12 720 reference-to-microphone
  windows could not be read (the window's cross-correlation peaks below 0.1):
  all against channel 2: windows 89, 90, 125 and 126 of play 8, and 80, 81,
  85, 86, 92 and 93 of play 14. In each of the ten the host's copy's delay to
  channel 2 could not be read either, so none is an unexplained window and the
  run holds the shipped test's rule; in each, the reference's delay to
  channels 3, 4 and 5 was read. In the first run every window was read.
- **[measured]** The reference's level against the host's copy: −40.45 dB in
  every run. `AUDIO_MGR_REF_GAIN` reads 1.9, which is 5.58 dB.
- **[measured]** Each other far-end category, routed to channel 0 through
  `L_PK0` beside (12, 0) on channel 1, in a run of twenty plays with its
  control (the host's copy stepping in 15, 14, 12 and 11 of 20 plays), held
  still against every microphone in every window in which its delay could be
  read, as did (12, 0) in the same run. Windows that could not be read, of
  25 440 per run: none in the (5, 0) run; in the others 5, 11 and 2 for the
  tap and as many for (12, 0), all against channel 2, in plays where the
  host's copy's delay to channel 2 could not be read in at least as many:

  | Tap | Delay to channels 2/3/4/5 | Tap to (12, 0) | Level against the host's copy |
  |---|---|---|---|
  | (12, 0) | 26/25/24/23 | — | −40.45 dB |
  | (5, 0) | 26/25/24/23 | 0 | −46.02 dB |
  | (4, 0) | 38/37/36/35 | 12 | −46.02 dB |
  | (10, 0) | 38/37/36/35 | 12 | −40.00 dB |
  | (10, 1) | 39/38/37/36 | 13 | −40.00 dB |

  `AUDIO_MGR_SYS_DELAY` reads 12: (4, 0) and (10, 0) lead (12, 0) and (5, 0)
  by exactly that. (12, 0) is (5, 0) to the frame and 5.57 dB louder, the
  gain `AUDIO_MGR_REF_GAIN` states; (4, 0) and (5, 0) are 6.02 dB below
  (10, 0), half its amplitude.
- **[measured]** The conference beam (channel 0), in the two (12, 0) runs:
  its power during a play, median over plays, is 2332 and 2383 against 2976
  and 3102 in the silence before it, while the microphones' mean rises from
  100 to 144 and from 71 to 113. The beam carries less power while the device
  plays than in the silence before.

The later session:

- **[measured]** Its five logs hold no discontinuity either.
- **[measured]** The run of (12, 0) alone: the host's copy stepped in 11 of
  20 plays. The reference's delay to the microphones on channels 2, 3, 4 and
  5 was 27, 26, 24 and 23 frames, with no window more than one frame from the
  median and no step in any play. 1237 of the 12 720 reference-to-microphone
  windows could not be read (365, 330, 337 and 205 against channels 2 to 5),
  none in plays 0 to 9; in 9 of them the host's copy's delay to the same
  microphone was read in the same window, and one of those 9 is an
  unexplained window (below). The host's copy's delay to the reference
  channel was read in every window of every play. Of the run's 7 zero runs,
  the 6 inside plays, each of 24 frames, zeroed the reference with the beam
  and the microphones and left every reference-to-microphone delay as it was
  before; the seventh, 43 frames at the start of the capture, is outside any
  play and nothing was measured across it. The reference's level against the
  host's copy: −40.45 dB.
- **[measured]** The room was not quiet in that run. The microphones' mean
  power during plays 0 to 9 was 125 to 159, and during plays 10 to 19 577 to
  2276; in the silence before each play it was 67 to 121 for plays 0 to 9 and
  97 to 6717 for plays 10 to 19, and the beam's power in those silences 2712
  to 3711 and 2514 to 161 133. In each of plays 10 to 19 the host's copy's
  delay to each microphone could not be read in nearly as many windows as the
  reference's (1251 in all). What made the sound is not known.
- **[measured]** The beam in that run: in plays 0 to 8 its power during the
  play, 2259 to 2751, was below that in the silence before, 2712 to 3711, as
  in the earlier session; in play 9 (4227 against 3509) and in five of the
  loud plays it was above. Median over plays: 5991 against 3651, the
  microphones' mean 368 against 109.
- **[measured]** Each other tap, routed as in the earlier session, in a run
  of twenty plays with its control, beside its own earlier run. No window of
  the tap or of (12, 0) was more than one frame from its run's median and no
  delay stepped in any play. Every zero run inside a play (33 in the later
  session's five runs) zeroed every routed reference with the microphones
  and shifted none. Each level is the earlier session's to 0.01 dB.

  | Tap | Plays with a host-copy step | Delay to channels 2/3/4/5, earlier / later | Tap to (12, 0), earlier / later | (12, 0) to channels 2/3/4/5 in the later run | Unreadable windows of 25 440, tap / (12, 0) |
  |---|---|---|---|---|---|
  | (5, 0) | 15 | 26/25/24/23 / 26/25/24/23 | 0 / 0 | 26/25/24/23 | 226 / 228 |
  | (4, 0) | 14 | 38/37/36/35 / 39/38/36/35 | 12 / 12 | 27/26/24/23 | 617 / 620 |
  | (10, 0) | 15 | 38/37/36/35 / 39/38/36/35 | 12 / 12 | 27/26/24/23 | 0 / 0 |
  | (10, 1) | 15 | 39/38/37/36 / 41/38/36/35 | 13 / 13 | 29/25/24/23 | 495 / 493 |

  In the tap runs the two references lose the same windows: of the 1341
  windows of (12, 0) that could not be read in the later session's tap runs,
  1332 could not be read for the tap either, and in the earlier session all
  18.
- **[measured]** The unexplained windows. In the eleven runs (12, 0) has
  five, all in the later session, and the tap beside it has the same windows.
  Each is *shown in place* in two ways: in the same window the reference's
  delay to another microphone was read, every such delay being within one
  frame of its run's median; and in the window and the windows either side
  of it the host's copy's delay to the reference channel was read, and the
  host's copy's delay to that microphone less its delay to the reference was
  within one frame of the play's median of that difference. The second is a
  reading the room does not enter, since the reference channel is a digital
  copy of what was played. By both, the reference was there and in place
  against that microphone, and was neither silent nor displaced beyond the
  ±64 lags tracked.

  | Run | Play | Window | Microphone channel | Reference read against channels | Host's copy: delay to the microphone less delay to the reference, windows either side and the window (play's median) |
  |---|---|---|---|---|---|
  | (12, 0) alone | 19 | 9 | 2 | 3, 4, 5 | 27, 27, 27 (27) |
  | (4, 0) tap | 14 | 16 | 2 | 3, 4, 5 | 26, 26, 26 (26) |
  | (10, 1) tap | 3 | 134 | 3 | 4, 5 | 25, 25, 25 (25) |
  | (10, 1) tap | 4 | 109 | 2 | 3, 4, 5 | 26, 26, 26 (26) |
  | (10, 1) tap | 6 | 39 | 3 | 4, 5 | 25, 25, 25 (25) |

  The first is in a play of the loud room above; the reports of the tap runs
  hold no microphone power, and in the plays holding their four the host's
  copy's delay to the same microphone could not be read in 5, 18, 33 and 15
  windows. The shipped test fails on each of the five. The earlier session's
  runs, read again, have none.

Delays that differ between runs, and between the plays of a run:

- **[measured]** Each run's delay from (12, 0) to channels 2, 3, 4 and 5: 26,
  25, 24, 23 in all six runs of the earlier session; in the later session
  26, 25, 24, 23 beside (5, 0), then 27, 26, 24, 23 beside (4, 0), 27, 26, 24,
  23 beside (10, 0), 29, 25, 24, 23 beside (10, 1), and 27, 26, 24, 23 alone.
  Between runs it differed by up to three frames on channel 2 (26 to 29) and
  by up to one on channel 3. No between-run difference of (12, 0)'s delay is
  the same on all four microphones.
- **[measured]** Each tap against its own earlier run (table above): (5, 0)
  the same; (4, 0) and (10, 0) one frame later on channels 2 and 3 and the
  same on 4 and 5, with their delay to (12, 0) unchanged at 12, as (12, 0)'s
  own delay to channels 2 and 3 was one frame later; (10, 1) +2, 0, −1 and −1,
  with its delay to (12, 0) 13 in both.
- **[measured]** Two runs have plays whose median delay differs from another
  play's of the same run; none of the other nine does.
  - The later (4, 0) run: in plays 8 to 14 and 16, (12, 0)'s delay to
    channel 3 reads 25 and (4, 0)'s 37, against 26 and 38 in the other twelve
    plays; the delay between the two is 12 in every play.
  - The later (10, 1) run: (12, 0)'s delay to channels 2 to 5 reads 29, 26,
    24, 23 in plays 0 to 2 and 16 to 19, and 28, 25, 23, 22 in plays 3 to 14
    (28, 25, 24, 22 in play 15); the delay from (10, 1) to (12, 0) reads 12 in
    the first set of plays and 13 in the second. (10, 1)'s own delay reads 41,
    38, 36, 35 in every play, but for channel 4 at 37 in plays 8, 11 and 15
    and 36.5 in play 7. Window by window, the delay between the two taps
    changed once each way, both times inside a play: from 12 to 13 at window
    99 of play 2, and back at window 14 of play 16. (12, 0)'s delay to all four
    microphones fell by one frame together at window 100 of play 2.
  - In that run the two comparisons disagree on which plays are the
    departure. Against the later session's other runs (27, 26, 24, 23), (12,
    0)'s delay to channels 3, 4 and 5 matches in plays 0 to 2 and 16 to 19.
    Against the earlier (10, 1) run, whose delay from (10, 1) to (12, 0) was
    13 in all twenty plays, the pair matches in plays 3 to 15.
- **[measured]** Read the other way, as the host's copy's delay to channel 2
  less its delay to the reference channel, window by window, (12, 0)'s delay
  to channel 2 in the (10, 1) run has a median of 26, against the 29 read
  directly. That reading uses each play's own start as its template and
  moves with every step of the host's copy; it is given as a figure only.
- **[inferred]** A move of the reference channel alone would change its
  delay to all four microphones alike, so a difference that is not alike on
  the four is not that alone. Where both taps of a run differ alike against
  a microphone, each from its own earlier run, and their delay to each other
  is the earlier run's, the difference lies on the path the two share to that
  microphone: so on channels 2 and 3 in the later (4, 0) and (10, 0) runs,
  and in the per-play change against channel 3 in the later (4, 0) run.
- **[inferred]** In the later (10, 1) run, between its two sets of plays the
  (12, 0) channel sits one frame differently against the other tap and
  against all four microphones, since (10, 1) kept its delay to the
  microphones.
- **[inferred]**, not established: what fits the 29 against channel 2 is the
  run's template, taken from the first play, peaking on the later of the two
  arrivals described above, or that arrival having become the stronger at
  that microphone.
- **Not established:** in the (10, 1) run, which set of plays is the
  departure, which of the two capture channels moved between the sessions,
  and how much of the three frames on channel 2 lies on the microphone's
  path; the cause of any of these differences.

**[measured]** How many windows read each value of (12, 0)'s delay to each
microphone, window by window, in each run (159 windows a play, 3180 a run;
windows that could not be read are not counted):

| Run | Channel 2 | Channel 3 | Channel 4 | Channel 5 |
|---|---|---|---|---|
| earlier, (12, 0) alone, first | 26: 3180 | 25: 3180 | 24: 3180 | 23: 3180 |
| earlier, (12, 0) alone, second | 26: 3170 | 25: 3180 | 24: 3180 | 23: 3180 |
| earlier, beside (5, 0) | 26: 3043, 27: 137 | 25: 3149, 26: 31 | 24: 3007, 25: 173 | 23: 3113, 24: 67 |
| earlier, beside (4, 0) | 26: 3078, 27: 97 | 25: 3083, 26: 97 | 24: 3071, 25: 109 | 23: 3132, 24: 48 |
| earlier, beside (10, 0) | 26: 3169 | 25: 3180 | 24: 3180 | 23: 3180 |
| earlier, beside (10, 1) | 26: 3178 | 25: 3180 | 24: 3180 | 23: 3180 |
| later, beside (5, 0) | 26: 2593, 27: 486 | 25: 2966, 26: 159 | 24: 2674, 25: 458 | 23: 2727, 24: 429 |
| later, beside (4, 0) | 26: 495, 27: 2467 | 25: 803, 26: 2222 | 23: 486, 24: 2552 | 22: 552, 23: 2523 |
| later, beside (10, 0) | 27: 3180 | 26: 3180 | 24: 3180 | 23: 3180 |
| later, beside (10, 1) | 28: 1301, 29: 1657 | 25: 1709, 26: 1347 | 23: 1238, 24: 1837 | 22: 1507, 23: 1631 |
| later, (12, 0) alone | 27: 2815 | 26: 2850 | 24: 2843 | 23: 2975 |

**[measured]** Windows inside a play in which (12, 0)'s delay to all four
microphones differs from its delay in the window before by one frame in the
same direction, as play/window and the direction: 60 in five runs, none in
the three runs of (12, 0) alone, in either (10, 0) run or in the earlier
(10, 1) run. In none of the 60 did the delay between (12, 0) and the tap
beside it change in that window; in 55 the tap's delay to all four
microphones changed alike in the same window.

- Earlier, beside (5, 0): 7/87 −, 8/54 −, 9/6 +.
- Earlier, beside (4, 0): 15/33 −, 16/17 +.
- Later, beside (5, 0): 9/142 +, 10/4 −, 10/55 −, 10/72 −, 11/40 −,
  11/109 +, 12/1 −, 12/30 +, 12/69 −, 15/30 +, 15/86 −, 15/155 +, 16/147 −,
  17/25 +, 17/142 −, 18/36 +, 18/59 +, 18/60 −.
- Later, beside (4, 0): 7/118 +, 8/62 +, 8/115 −, 9/36 +, 10/48 −, 10/70 +,
  10/72 −, 11/9 −, 11/62 +, 11/71 −, 11/86 +, 11/103 +, 12/146 +, 13/10 −,
  13/78 +, 13/95 +, 13/112 −, 14/67 −, 15/26 −, 15/111 +, 16/8 +, 16/137 −,
  17/6 −.
- Later, beside (10, 1): 2/100 −, 3/114 +, 6/158 +, 7/96 +, 7/115 +,
  7/116 −, 9/44 −, 9/158 −, 10/72 −, 11/110 +, 12/141 +, 15/3 +, 15/29 +,
  15/58 +.

**Outcome: reference present and still.** For a stated routing, (12, 0) on
`R_PK0`, the chip's own echo reference is on capture channel 1, and in every
window that could be read, in every one of the eleven runs, its delay to each
of the four raw microphones is within one frame of that run's median while
the host's copy steps; that is the property within a run of twenty plays, and
between runs the delay to a microphone differed, by up to three frames on
channel 2 (26 to 29) and differently on each microphone, as stated above.
Within the one frame the delay is not one constant: per-play medians differ
by one frame in two runs, and in one of them, beside (10, 1), (12, 0) read one
frame differently against every microphone and against that tap for
thirteen plays of twenty and parts of two more, which plays departed and why
not known (§9.9). The
shipped test failed on its run of the later session for one window in which
the reference's delay to one microphone could not be read while the host's
copy's could, in a play made in a loud room; that window, like the four of the
tap runs, is shown in place both against the other microphones and against
the host's copy, and the reading is tracked as
`flex-chip-reference-unread-windows`. The earlier session's runs were made
under a rule that did not count unreadable windows; read again, they hold
the shipped rule. The register-to-channel map of §9.3 covers channels 0 and 1
of the six.

### 9.6 Where the slips are

- **[measured]** In every window in which its delay could be read, in each
  of the eleven runs of §9.5, every far-end tap's delay to each microphone is
  within one frame of its run's median and none steps, while the host's copy
  steps against all of them by up to tens of frames. **[inferred]** Every
  playback-side step of more than one frame therefore lies before all five
  taps: the far end the chip receives
  already holds it, and the loudspeaker plays it. It sits between the USB OUT
  packets and the chip's far-end input, in the device's own playback path.
- **[measured]** Each of the 43 capture-side zero runs inside a play zeroed
  every routed reference and the microphones (and the beam, where it was
  kept) over the same frames and shifted none against another.
  **[inferred]** It arises after the chip's channels are assembled into one
  frame and before the USB IN packets, where all six channels move together.
- **[inferred]** Between the chip's far-end input and its microphones'
  arrival at its processing, nothing slips by more than one frame within a
  run. That the two are locked to the sample is not shown: §9.5 gives
  differences of one frame between the plays of a run and inside plays, among
  them one of (12, 0) against every microphone and the tap beside it, and
  differences of up to three frames between runs.

### 9.7 Why the chip's own canceller works through them

**[inferred]** The canceller compares the microphones with (12, 0), and §9.5
and §9.6 show that pair holding within one frame of a run's median: a
playback-side slip changes what the loudspeaker plays and what the reference
holds alike, and a capture-side zero run comes after the canceller. Nothing
the host sees step moves the canceller's reference against its microphones by
more than one frame; §9.5 gives the differences of one frame and the
differences between runs.

### 9.8 What a host must use as the echo reference

- **[measured]** The host's copy of what it played is not a reference: it
  steps against the echo in most plays of 20 s; nothing on the host removes
  the steps, and the host's streams show only some of them.
- **[inferred]** A host should use the chip's own reference on a capture
  channel: (12, 0) on `R_PK0`, written with
  `xvf-route set --card <NAME> --op-all 8,0,1,0,1,2,12,0,1,1,1,3`. It is then
  in the same stream as the microphones, on one clock, and every slip the host
  sees moves both together or neither. Its delay to a microphone held within
  one frame of a run's median, not at one frame: a host should not assume the
  delay is the same frame from one capture to the next, or from one play to
  the next (§9.5 gives differences of one frame between plays and inside
  plays, and of up to three frames between runs).
- What it costs:
  - **[measured]** Channels 0 and 1 were each given to another source by a
    write to `L_PK0` or `R_PK0`; silence written into any of the other four
    slots changed no channel. **[inferred]** On that and the vendor table, no
    other channel can be given to another source while packing is off. The
    reference therefore takes channel 1, the ASR beam; a second routed tap
    would take channel 0, the conference beam, too.
  - The routing is the board's until a power cycle or another write. A
    capture states the routing it was made under (`audio_mgr_op_all` in its
    header and on `capture_started`), and that, not the board's usual
    routing, says what channel 1 carries.
  - `pod-capture`'s `--layout` names are the user's: a capture under this
    routing must be given a layout naming channel 1 as the reference, not as
    the ASR beam.
  - **[measured]** The reference's level: −40.45 dB below the host's copy at
    the mixer's volume of 40: played noise peaking near 13 000 peaks at 125
    on channel 1.

### 9.9 What remains unexplained

- What decides capture channels 2 to 5 (§9.3).
- Where in the device the playback-side slips and the capture-side zero runs
  arise, beyond the bounds §9.6 places on them, and what the USB packets carry
  at each.
- Why a zero run is followed by content 24 frames later in most cases and 15
  to 26 frames earlier in some.
- Why, in one play, the delay of the echo behind the host's copy alternated
  by 8 frames dozens of times (§9.1).
- Why the far end, even at native rate (category 10), is 40 dB below the
  host's copy: whether the mixer's playback volume is applied before the
  chip's far-end input. The volume was not varied.
- Why (10, 1) is one frame later than (10, 0), and why (4, 0) and (5, 0) are
  half the amplitude of (10, 0). In the later session their delays to the
  microphones differ by other amounts: 41, 38, 36, 35 against 39, 38, 36, 35
  (§9.5).
- In the later run beside (10, 1), (12, 0) read one frame differently against
  the other tap and every microphone for thirteen plays and parts of two
  more: whether the chip moved its reference by a frame against its
  microphones or the reading sits between two frames; which plays departed;
  and whether it happens on the routing a capture would use, (12, 0) alone,
  where three runs show no per-play change.
- Why the delay to a microphone differs by up to three frames between runs.
- Why the reference's correlation with a microphone falls below the 0.1
  floor in a window where the host's copy's does not (the unexplained windows
  of §9.5).
- Which of the two arrivals a run's template peaks on.
- Why the conference beam carries less power while the device plays than in
  the silence before a play, in a quiet room.
- The second arrival 2 to 3 frames after the first at every microphone
  (§9.5) is taken for the room; it has not been measured apart from the
  device.
