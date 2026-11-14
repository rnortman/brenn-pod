# Runbook: recording a capture device with `pod-capture`

Record every channel of one ALSA capture device on a Linux workstation to a
frame log, stop it, and read it back. The format is
[`docs/reference/frame-log.md`](../reference/frame-log.md); the program's
command line and event lines are
[`docs/reference/pod-capture.md`](../reference/pod-capture.md).

## Prerequisites

- libasound's headers (`libasound2-dev`): `pod-capture` links libasound and no
  other system library.
- Access to the device node. ALSA's `/dev/snd/pcmC<N>D0c` is granted to the
  `audio` group, and to the user of an active local seat; check with
  `ls -l /dev/snd/` and `id -Gn`. For the reSpeaker Flex, install the udev rules
  as `firmware/docs/setup.md` describes.
- The capture device free. A `hw:` device has one opener, and a desktop's sound
  server (PipeWire, PulseAudio) may be holding the card. That reads as
  `capture_failed` with reason `open` and "Device or resource busy" in `detail`.
  `pod-capture` does not take the device from its holder: release the card for
  the run by setting its profile to off in the sound server (for PipeWire,
  `pactl set-card-profile <card> off`, or the card's profile in the desktop's
  sound settings), and set it back afterwards.
  `cat /proc/asound/card<N>/pcm0c/sub0/status` reads `closed` when nothing
  holds the capture stream.
- For the chip's state in the record, access to the board's USB node,
  `/dev/bus/usb/<bus>/<address>` (`ls -l` on it; `lsusb` gives the bus and
  address). The XVF3800 udev rule (`firmware/udev/99-respeaker-xvf3800.rules`,
  installed as `firmware/docs/setup.md` describes) grants it for every `2886`
  board. Without it the capture still records, and its header says
  `device_state: {"outcome": "unavailable", "why": "open", …}` with the node's
  path and "Permission denied".

## Build

```sh
cd firmware
cargo build --release -p pod-capture -p frame-log
```

The binaries are `target/release/pod-capture` and
`target/release/frame-log`.

## Record

```sh
target/release/pod-capture --card "Flex XVF3800 L16K6Ch" --rate 16000 --format s16_le \
    --layout conference_beam,asr_beam,raw_mic_0,raw_mic_1,raw_mic_2,raw_mic_3 \
    --pod-id flex --out flex.framelog
```

- `--card` is matched case-insensitively against each card's name and long name
  (`cat /proc/asound/cards`); exactly one card must match. A long name ends in
  the USB port, so two identical boards are told apart by naming the port.
- `--rate`, `--format` and the channel count (one per `--layout` name) are
  demanded exactly. A device that will not grant them is refused with `params`
  and what it advertises; nothing is resampled or converted.
- `--pod-id` names the stream's source in the log's hello: 1 to 32 bytes.
- `--out` must not exist.

One JSON line per event goes to stdout; `capture_started` says the stream runs
and names the capture. The capture runs until it is stopped, the destination
runs low (it stops with five seconds of capture plus 1 MiB still free), or it
fails.

## Stop

`Ctrl-C` (`SIGINT`) or `SIGTERM`. The capture stops the stream at once, writes
the log's end, syncs and closes the file, prints `capture_stopped` and exits 0.
Any other ending is one `capture_failed` line and exit 1; its `reason` and
`detail` say what happened and whether the log has its end.

## Read it back

```sh
target/release/frame-log summary flex.framelog
target/release/frame-log export-wav flex.framelog flex.wav
```

The summary's `discontinuities` lists every place audio is missing, `[]` when
there is none; `tail.verdict` is `ended` for a capture that stopped cleanly.
The export's map says which WAV frames are audio and which are silence standing
for a hole. The same tool reads the voice host's frame logs.

The summary's `header.capture.device_state` is what the recorder read of the
device's chip before the stream started, and `header.capture.device.usb` names
the USB device it read it from (port, bus, address, serial number); the same
two are on the `capture_started` line. `outcome` is `read` with every
register's `bytes` and `value`, or says why nothing was read
(`docs/reference/frame-log.md`, `device_state`).

## Hardware test

With the Flex attached and its capture device free:

```sh
make -C firmware capture-hil
```

Two tests, silent (nothing is played), about 21 seconds: ten seconds of every
channel in one segment with no discontinuity and every channel live by the
repository's waveform guards; and a capture paused for one second (`SIGSTOP`,
`SIGCONT`) that must come back as an `overrun` `capture_gap` line and an
`overrun` loss in the log whose clock estimates sum to between 0.9 s and 1.2 s. Each prints
its event lines, the summary, and every channel's statistics. A capture that
fails to start fails the test with its `capture_failed` line printed whole. The
waveform guards were calibrated on the ESP pod's I2S image; a channel of the USB
image that fails one is a reading for the owner to review, not a threshold to
move.

The chip's state, with the Flex attached, its USB node openable (the udev rule
above) and its capture device free:

```sh
make -C firmware chip-hil
```

Four tests, silent and read-only (nothing is written to the chip), a few
seconds: the chip answers a version read; its firmware identity and its
routing and reference-path registers are the expected ones, each reading
printed beside its expectation and that expectation's source; and a two-second
capture's `capture_started` and header carry the same reading as one taken
directly, from the board sysfs names. **The command exits non-zero today**, on
`the_chip_is_the_expected_firmware` and `the_chip_routing_is_the_expected_routing`
and on those two only: ten registers have no expectation the maintainer has
confirmed. The tracker entry `flex-usb-image-chip-readings-unconfirmed` says
which, why, and what ends it. A failure of any other test, a failure naming any
other register, or a reading that differs from the entry's is a new reading
for the owner to review.
