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

## Routing the chip's echo reference to a capture channel

`xvf-route` is the one program in the tree that writes to the chip of a Flex
on the six-channel USB image (`2886:0022`). It writes one register,
`AUDIO_MGR_OP_ALL`, the chip's output routing, and refuses every other write;
it never saves to flash or reboots the chip. A write lasts until the board is
power-cycled or written again, and changes what every later capture's
channels carry. What each slot decides, and which routing puts the chip's own
echo reference on a capture channel:
[`docs/reference/xvf3800-control-protocol.md`](../reference/xvf3800-control-protocol.md),
section 9.

```sh
cargo build --release -p xvf-route
target/release/xvf-route show --card "Flex XVF3800"
target/release/xvf-route set --card "Flex XVF3800" --op-all 8,0,1,0,1,2,12,0,1,1,1,3
```

`show` reads only. `set` prints the routing before, writes all twelve bytes,
reads them back, and exits 1 unless the read-back matches. Both print one JSON
object on stdout. It needs the board's USB node openable by this account, as
the chip's state in the record does; nothing else.

The routing above puts the chip's reference, (12, 0), on capture channel 1 in
place of the ASR beam. Name it so in the capture's `--layout` (for example
`conference_beam,far_end_reference,raw_mic_0,raw_mic_1,raw_mic_2,raw_mic_3`):
the layout is the user's statement and is not read from the chip. The
capture's header and `capture_started` line state the routing it was made
under (`audio_mgr_op_all`).

**Put the board back** to the routing it was found with, which the hardware
tests and other tooling assume:

```sh
target/release/xvf-route set --card "Flex XVF3800" --op-all 8,0,1,0,1,2,7,3,1,1,1,3
```

A hardware test whose restore fails prints a line beginning
`ROUTING NOT RESTORED` with this command. A test killed between a write and
its restore prints nothing; the next writing test refuses to start, and
`xvf-route show` says what the board holds. If `set` cannot restore it, unplug
and replug the board.

## Routing hardware tests

These write to the chip and play sound through the Flex at the mixer's
current volume, which they do not change. Each writing test first requires the
board to hold the routing above, and refuses to start otherwise, naming the
`xvf-route set` command that restores it. Every write is read back, and every
test restores the routing and reads it back before it ends. They need the
Flex attached, its capture and playback devices free, and its USB node
openable by this account.

```sh
make -C firmware route-hil
```

About a minute; plays about ten seconds of band-limited noise at −20 dBFS
RMS in two-second plays. Tests that `AUDIO_MGR_OP_ALL` is the six `PK`
registers in order, that silence written into `L_PK0` and `R_PK0` zeroes
channels 0 and 1, that silence written into each of the four other slots
zeroes channels 2 to 5, and that the chip's reference, (12, 0), routed to
channel 1 carries the played signal; four other far-end categories are routed
the same way and measured. **The command exits non-zero today**, on
`the_packed_slots_feed_capture_channels_2_to_5` and on that only: none of the
four slots zeroes any channel, and the tracker entry
`usb-image-capture-channel-register-map-unknown` holds that reading for the
maintainer's review. A failure of any other test is a new reading for the
owner to review.

```sh
make -C firmware route-measure-hil
```

About eight minutes per routing, one for the chip's reference and one for
each other far-end category that carries the played signal: about forty
minutes on a board where all four do. Plays throughout, in plays of twenty
seconds.
Measures the routed reference's delay to each raw microphone against the
host's copy of what it played; asserts that the host's copy steps in at least
one play (otherwise the run has no control and is repeated) and that the
reference holds still. Each run's log and JSON report go under the cargo
target directory's `tmp/route-measure/`. **The command exited non-zero on
its last bench run**, on
`the_routed_reference_holds_still_against_the_microphones` and on that only:
in one window the reference's delay to one microphone could not be read while
the host's copy's could, and the tracker entry
`flex-chip-reference-unread-windows` holds that reading for the maintainer's
review. A failure of any other test, or on a window more than one frame from
the run's median, is a new reading for the owner to review.

The two measuring tests have not been run on the board since their measuring
function was split into a half that captures and a half that analyses. The
analysing half was run over every kept run and reproduced each run's report.
The capturing half, and the step that places each play from the host's clock,
have not run since the split.

```sh
make -C firmware route-reread RUN=target/tmp/route-measure/<run>
```

Reads one kept run's log through the measuring analysis, writes `reread.json`
beside its `report.json` with each window's delays added, and fails unless
the reading reproduces the kept report. Touches no device: writes nothing to
the chip, opens no card and plays nothing. `RUN` is a run's directory,
relative to `firmware/` or absolute.

After a session, `xvf-route show` reads `8,0,1,0,1,2,7,3,1,1,1,3` when the
board is back as found.

After a session, `make -C firmware chip-hil` shows the routing the board
holds; it reads the routing as found when the board is back.
