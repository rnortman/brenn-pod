# Runbook: recording a capture device with `pod-capture`

Record every channel of one ALSA capture device on a Linux workstation to a
capture record, stop it, and read it back. The format is
[`docs/reference/capture-record.md`](../reference/capture-record.md).

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

## Build

```sh
cd firmware
cargo build --release -p pod-capture -p capture-record
```

The binaries are `target/release/pod-capture` and
`target/release/capture-record`.

## Record

```sh
target/release/pod-capture --card "Flex XVF3800 L16K6Ch" --rate 16000 --format s16_le \
    --layout conference_beam,asr_beam,raw_mic_0,raw_mic_1,raw_mic_2,raw_mic_3 \
    --out flex.bpcr
```

- `--card` is matched case-insensitively against each card's name and long name
  (`cat /proc/asound/cards`); exactly one card must match. A long name ends in
  the USB port, so two identical boards are told apart by naming the port.
- `--rate`, `--format` and the channel count (one per `--layout` name) are
  demanded exactly. A device that will not grant them is refused with `params`
  and what it advertises; nothing is resampled or converted.
- `--out` must not exist.

One JSON line per event goes to stdout; `capture_started` says the stream runs
and names the record. The capture runs until it is stopped, the destination
runs low (it stops with five seconds of capture plus 1 MiB still free), or it
fails.

## Stop

`Ctrl-C` (`SIGINT`) or `SIGTERM`. The capture stops the stream at once, writes
the end record, syncs and closes the file, prints `capture_stopped` and exits 0.
Any other ending is one `capture_failed` line and exit 1; its `reason` and
`detail` say what happened and whether the record has its end record.

## Read it back

```sh
target/release/capture-record summary flex.bpcr
target/release/capture-record export-wav flex.bpcr flex.wav
```

The summary's `gaps` lists every place audio is missing, `[]` when there is
none; `tail.verdict` is `ended` for a capture that stopped cleanly. The export's
map says which WAV frames are audio and which are silence standing for a gap.

## Hardware test

With the Flex attached and its capture device free:

```sh
make -C firmware capture-hil
```

Two tests, silent (nothing is played), about 21 seconds: ten seconds of every
channel with no gap and every channel live by the repository's waveform guards;
and a capture paused for one second (`SIGSTOP`, `SIGCONT`) that must come back
as an `overrun` gap whose clock estimate is between 0.9 s and 1.2 s. Each prints
its event lines, the summary, and every channel's statistics. A capture that
fails to start fails the test with its `capture_failed` line printed whole. The
waveform guards were calibrated on the ESP pod's I2S image; a channel of the USB
image that fails one is a reading for the owner to review, not a threshold to
move.
