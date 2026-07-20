# XVF3800 vendor firmware

Vendor XVF3800 (XMOS) firmware blobs for the reSpeaker Flex, plus the reflash
procedure. Moved here from the (soon-to-be-deleted) `usb-spike/` so the binaries
and the procedure persist. NOTE: `usb-spike/firmware/` was gitignored; this dir is
tracked.

Source: official Seeed/reSpeaker repo
`https://github.com/respeaker/reSpeaker_Flex` → `xmos_firmwares/{usb,i2s}/`.

## Files present

| Filename | Mode | Description | SHA256 |
|---|---|---|---|
| `respeaker_flex_usb_l16k6ch_v1.0.0.bin` | USB | UAC2 6-ch USB-audio standalone device (Stage-1 bring-up image; XVF3800 talks straight to a USB host, ESP32-S3 not involved). App-mode VID:PID `0x2886:0x0022`. | `136727693ce56cb77953a7db76ec51602971793ff43e42939d89217c305e2ac8` |
| `respeaker_flex_i2s_l16k2ch_v1.0.0.bin` | I2S | Stock product image: linear 2-ch I2S audio to the XIAO ESP32-S3 + I2C control. XVF3800 is a peripheral and does NOT enumerate as a USB device in this mode. | `c7495bf8a1944d667c33a9eda751cbdddae31283fb43c3e9f84fa45c0827c79f` |

Naming convention: `respeaker_flex_{usb,i2s}_{c|l}{16k|48k}{2,6}ch_v*.bin` — `l`=linear
array (ours), `c`=circular; USB builds add 6ch@16k variants; I2S builds are always 2ch.

Mode selection: firmware mode is chosen by WHICH .bin you flash — USB and I2S are
mutually-exclusive builds, no runtime flag or fuse.

Verify after download: `sha256sum <bin>` against the table above.

## Flashing procedure

`dfu-util` over the XVF3800 USB-C port:

1. Enter Safe/DFU mode: unplug USB; hold the **Boot button on the XVF3800 core board**
   (NOT the XIAO ESP32-S3 Boot button); replug USB-C while holding; release.
2. Confirm: `dfu-util -l` should list three alt-settings — alt 0
   `reSpeaker DFU Factory` (recovery, DO NOT write), alt 1 `reSpeaker DFU Upgrade`
   (the write target), alt 2 `reSpeaker DFU DataPartition`.
3. Flash the Upgrade slot (alt 1). DFU-mode device VID:PID is `0x2886:0x801c`:

   ```
   sudo dfu-util -d 0x2886:0x801c -a 1 -R -e -D firmware/vendor/xvf3800/<bin>
   ```

   (`-R` reset, `-e` detach after.)

After flashing the I2S image: the XVF3800 STOPS enumerating as a USB audio device
(expected — it's now an I2C/I2S peripheral). Control is over I2C; address `0x2C` (from
the formatBCE component, NOT yet confirmed for this exact build) and the on-device wire
framing are to be pinned down empirically by the first HIL assertion-tests.

## Current hardware state (2026-06-05)

The board is flashed with the USB image and must be reflashed to the I2S image before
Milestone 1 I2C work.
