# `pod-capture`

`pod-capture` records every channel of one ALSA capture device, continuously,
to a frame log ([`frame-log.md`](frame-log.md)), until it is stopped. It links
libasound and no other system library, and needs no configuration, network or
key.

## Command line

```text
pod-capture --card <NAME> --rate <HZ> --format <s16_le|s32_le> --layout <a,b,c,…> --pod-id <NAME> --out <PATH>
```

Six flags, every one required, each at most once, in any order.

- `--card` is matched case-insensitively against each card's name and long
  name; exactly one card must match.
- `--rate`, `--format` and the channel count (one per `--layout` name) are
  demanded exactly. A device that will not grant them is refused with `params`;
  nothing is resampled or converted.
- `--layout` names each channel, in interleave order; the names go into the
  log's `Describe` frame verbatim. More than 255 names, or names too long in
  total for one frame, are refused with `params`.
- `--pod-id` is what the log's hello carries as `pod_id`: the stream's source,
  1 to 32 bytes.
- `--out` must not exist.

A command line it does not understand exits 2 with the usage on stderr, before
anything is opened.

The period asked for is a 20 ms slice of the rate, or the most whole frames
that fit one frame's PCM, whichever is smaller. The period the device grants is
the one recorded and the one a block is judged late against.

## The device's state

With the capture device open and before the stream starts, `pod-capture`
reads the state of the chip behind it, once, and records what it read in the
log's header (`capture.device_state`, [`frame-log.md`](frame-log.md)) and on
`capture_started`. It never writes to the device.

- The card's USB device is found through sysfs (`/sys/class/sound/card<N>/device`),
  not by searching for an id, so the reading is of the card being recorded,
  whichever of several identical boards that is. Its port, bus, address,
  serial number and `bcdDevice` are the header's `device.usb`.
- The devices it reads are an XVF3800 under `38fb:1001`, `2886:001a` or
  `2886:0022` (`kind: xvf3800`): seventeen registers of the chip's firmware
  identity, output routing and reference path, listed in
  [`xvf3800-control-protocol.md`](xvf3800-control-protocol.md), section 8.
  Only `2886:0022` has been read on hardware. They are read over USB control
  transfers on the device's `/dev/bus/usb/<bus>/<address>` node, through the
  kernel's usbfs ioctl; no USB library is linked.
- A card not on USB, or a USB device of another id, is `not_attempted`, with
  why; nothing is sent to it.
- A device it reads but cannot reach is `unavailable`, with why and the
  system's words: the node would not open (on a machine without the udev rule,
  "Permission denied"), a transfer failed, or the device behind the card
  changed during the read. One line of prose goes to stderr, and **the capture
  goes on**: whoever runs the recorder decides what a capture without its
  device's state is worth.
- A register the chip refuses is recorded as refused, and the read goes on. A
  chip that answers every read with a retry status holds the start for 101
  transfers per register, 10 ms apart: about two seconds per register where a
  transfer takes about 9 ms, as on the bench board, so about half a minute for
  the seventeen. Nothing bounds the read as a whole, and a stop signal is not
  acted on until it ends.

The reading precedes the stream's start; `ended_mono_us` against the
`capture_started` line's `mono_us` says by how much. A write to the chip by
another program during the capture is not seen.

## Event lines

`pod-capture` writes one JSON line per event on stdout in the pod's line
envelope (`ts_ms`, `event`, then the fields). Prose goes to stderr. A **frame**
is one sample per channel, so `at_frame` is the log's sample index.

| Event | Fields beyond `record_id` | A reader may conclude |
|---|---|---|
| `capture_started` | `path`, `device`, `granted`, `layout`, `build`, `device_state`, `frame: 0`, `mono_us`, `wall_us` | The destination exists with its header, hello and `Describe`, and the stream is running. The pair is the start anchor for this device. `device_state` is the header's statement of the device's state, identical to it. |
| `capture_gap` | `cause`, `at_frame`, `frames` (number or `null`), `mono_us`, `wall_us`, `detail` | Audio is missing at `at_frame`, and the same fact is in the log. A `writer_backlog` gap is announced once its length is final. |
| `capture_late` | `at_frame`, `backlog_frames`, `mono_us`, `wall_us` | The reader fell a period or more behind here. Nothing was lost. One line per run of late blocks the log holds; a late block dropped into a `writer_backlog` gap gets none, and a gap ends a run. |
| `capture_stopped` | `frames`, `gaps`, `mono_us`, `wall_us` | The log ends with `End{stopped}`, is synced and closed, and holds every delivered frame outside its gaps. Exit status 0 follows. |
| `capture_failed` | `reason`, `detail`, `at_frame`, `mono_us`, `wall_us`; `cards` for `no_card` and `ambiguous_card` | The capture is over and did not stop cleanly. Exit status 1 follows. `record_id` and `at_frame` are `null` before a log exists. |

`granted` has `rate_hz`, `channels`, `sample_format`, `period_frames` and
`buffer_frames`; `device`, `build` and `device_state` are the log header's
`capture.device`, `capture.writer.build` and `capture.device_state`; `cause` is
the `Loss` frame's.

`capture_failed` reasons: `destination_exists`, `no_card`, `ambiguous_card`,
`open`, `params`, `destination`, `stream_died`, `destination_full`,
`write_failed`, `write_stalled`.

- `write_failed`: the writer cut the file back to the end of the last item
  written whole and made one attempt to append the end with the OS error.
  `detail` says whether it landed.
- `destination_full`: free space fell under five seconds of capture plus 1 MiB;
  the log ends with `End{destination_full}`.
- `write_stalled` says only that the writer had not reported within 5 s of the
  stream stopping. It may have been held on a write before the end or on the
  sync after it, so whether the log ends is the reader tool's verdict to give,
  not the line's.

A stdout or stderr whose reader has gone does not stop the capture: the first
failed line is noted on stderr, later lines are not attempted, and prose that
cannot be written is dropped.
