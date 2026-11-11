# Capture record format

A capture record is what `pod-capture` writes: every channel of one ALSA
capture device, continuously, to one local file that explains itself without
the process that wrote it. Every block says where it sits in the capture and on
two clocks, and every loss is a record at the place it happened.

The format is owned by `firmware/crates/capture-record`, which writes and reads
it; read records through that crate or its `capture-record` tool rather than
re-implementing the layout.

## Container

The container is shared with the voice host's frame log
(`firmware/crates/stamped-log`):

```text
header:  magic (4 bytes) | format_version u16 LE | meta_len u16 LE | meta (meta_len bytes)
records: stamp u64 LE | len u16 LE | payload (len bytes, 1..=65535)
```

A capture record has magic `BPCR` and `format_version` 1. Every record's stamp
is `mono_us`: `CLOCK_MONOTONIC` in microseconds on the capturing machine. The
first payload byte is the record kind. Each record reaches the file whole in one
write followed by a flush, so a file copied while it is being written is whole
records followed by at most one torn one.

## Header metadata

JSON. Unknown keys are ignored on read, so a key added later needs no version
bump; a known key with a value version 1 does not define is refused.

```json
{
  "record_id": "9f0c…32 hex digits",
  "writer": {"program": "pod-capture",
             "build": {"commit": "<40 hex>", "dirty": false, "source": "git"}},
  "device": {"selector": "Flex XVF3800 L16K6Ch",
             "card_index": 2,
             "card_name": "reSpeaker Flex XVF3800 L16K6Ch",
             "card_long_name": "Seeed Studio reSpeaker Flex XVF3800 L16K6Ch at usb-…",
             "pcm": "hw:2,0",
             "usb_id": "2886:0022"},
  "granted": {"rate_hz": 16000, "channels": 6, "sample_format": "s16_le",
              "period_frames": 320, "buffer_frames": 5120},
  "layout": ["conference_beam", "asr_beam", "raw_mic_0", "raw_mic_1", "raw_mic_2", "raw_mic_3"],
  "clocks": {"mono": "CLOCK_MONOTONIC", "wall": "CLOCK_REALTIME", "unit": "us"},
  "started": {"mono_us": 123456789, "wall_us": 1794000000000000},
  "device_control": {"performed": false}
}
```

- `record_id`: 128 random bits as 32 lowercase hex digits. It is the record's
  identity; another stream cites audio as `{record_id, start_frame,
  end_frame}`. The file name is the caller's and is not part of it.
- `writer.build.source`: `git` when a checkout of this repository vouched for
  the commit, `declared` when the builder declared it through
  `BRENN_POD_BUILD_COMMIT` (a build from a fetched revision). `dirty` is `null`
  when the source is `declared`.
- `device.usb_id`: the kernel's `/proc/asound/card<N>/usbid`, or `null` for a
  card with no USB id.
- `granted`: read back from the open stream, not echoed from the request.
- `layout`: one name per channel, in interleave order, verbatim from the
  caller.
- `started`: one clock pair read directly after the stream was triggered.
  Frame 0 was captured within the device's start latency after it.
- `device_control.performed: false`: the writer sent the device no control
  command — no reboot, no register write.
- `sample_format`: `s16_le` or `s32_le` in version 1. A reader refuses any
  other.

## Records

**Kind 1, block:**

```text
0x01 | wall_us u64 LE | first_frame u64 LE | backlog_frames u32 LE | pcm
```

`pcm` is interleaved frames exactly as the device delivered them: a whole
number of frames, at least one. A frame is one sample per channel. A block is
the frames of one read, at most 65514 bytes of PCM.

- `first_frame`: the block's position in the **delivered-frame count**, how many
  frames the device had handed to the capture before this block's first one,
  whether or not they reached the file.
- `mono_us` (the stamp): read when the read that delivered the block returned;
  an upper bound on when its last frame was captured.
- `backlog_frames`: frames already waiting in the device's buffer at that
  moment. The block's last frame was captured at about
  `mono_us − backlog_frames / rate`, to within the device's packet interval.
- `wall_us`: `CLOCK_REALTIME`, read as a pair with `mono_us` (monotonic, wall,
  monotonic again, retried until the two monotonic readings are within 50 µs).

**Kind 2, event:** `0x02 | UTF-8 JSON object`. Every event has `event`,
`wall_us` and `at_frame`, the delivered-frame position at which it happened.
Unknown keys are ignored; an unknown `event`, or an unknown kind byte, is
refused, because a reader that skipped a record it does not understand could
not vouch for the capture's continuity.

- `{"event":"gap","cause":…,"frames":N|null,"detail":"…"}`: audio is missing
  here.
  - `writer_backlog`: blocks were dropped because the destination was not
    taking them fast enough. The device delivered the frames, so `frames` is
    exact and the next block's `first_frame` is `at_frame + frames`.
  - `overrun`, `suspend`, `transfer_error`: the stream stopped and was
    restarted (ALSA `EPIPE`, `ESTRPIPE`, anything else recovery cleared). ALSA
    reports no count, so `frames` is `null` and the position does not advance
    across the gap. `detail` carries ALSA's error text.
- `{"event":"end","reason":…,"detail":"…"}`: the last record of a capture that
  ended itself. `reason` is `stopped`, `destination_full`, `stream_died` or
  `write_failed`.

## What a reader may conclude

- Blocks between two events are contiguous in `first_frame` and in the device's
  sample clock.
- A record with no `gap` lost nothing between its first and last block.
- A record whose last item is not an `end` did not stop cleanly, whatever else
  it holds.
- A block is **late** when `backlog_frames >= period_frames`: the read that
  delivered it returned a whole period or more behind the device. A late block
  lost nothing.
- `CLOCK_MONOTONIC` and `CLOCK_REALTIME` are slewed together, so `wall_us −
  mono_us` changes only when the wall clock is stepped. A change of more than
  1 ms between consecutive records is a **wall-clock step**. A gap is a `gap`
  event and a step is a change in the pair; neither is mistaken for the other.
- A stream stamped on `CLOCK_REALTIME` on the same machine is placed against the
  record through the per-block `wall_us`, interpolating between the two blocks
  around a wall time; the `mono_us` beside each shows whether the wall clock
  stepped between them. A stream that carries `CLOCK_MONOTONIC` joins on that
  clock directly.

## Tail verdicts

A reader stops with one verdict, and everything before it is usable in all four:

| Verdict | Means |
|---|---|
| `ended` | An `end` record, then end of file. |
| `unterminated` | End of file on a record boundary with no `end`: the writer was killed, or the file was copied while it ran, or its last write failed and so did the attempt to end it. |
| `torn` | End of file inside a record. |
| `corrupt` | A bad length, an unknown kind or event, PCM that is not whole frames, a `first_frame` or `at_frame` that disagrees with the position counted so far, or anything after an `end`. |

Each verdict carries the byte offset it applies at and a reason.

## Event lines

`pod-capture` writes one JSON line per event on stdout in the pod's line
envelope (`ts_ms`, `event`, then the fields). Prose goes to stderr.

| Event | Fields beyond `record_id` | A reader may conclude |
|---|---|---|
| `capture_started` | `path`, `device`, `granted`, `layout`, `build`, `frame: 0`, `mono_us`, `wall_us` | The destination exists with its header and the stream is running. The pair is the start anchor for this device. |
| `capture_gap` | `cause`, `at_frame`, `frames` (number or `null`), `mono_us`, `wall_us`, `detail` | Audio is missing at `at_frame`, and the same fact is in the record. A `writer_backlog` gap is announced once its length is final. |
| `capture_late` | `at_frame`, `backlog_frames`, `mono_us`, `wall_us` | The reader fell a period or more behind here. Nothing was lost. One line per run of late blocks the record holds; a late block dropped into a `writer_backlog` gap gets none, and a gap ends a run. |
| `capture_stopped` | `frames`, `gaps`, `mono_us`, `wall_us` | The record ends with `end`/`stopped`, is synced and closed, and holds every delivered frame outside its gaps. Exit status 0 follows. |
| `capture_failed` | `reason`, `detail`, `at_frame`, `mono_us`, `wall_us`; `cards` for `no_card` and `ambiguous_card` | The capture is over and did not stop cleanly. Exit status 1 follows. `record_id` and `at_frame` are `null` before a record exists. |

`capture_failed` reasons: `destination_exists`, `no_card`, `ambiguous_card`,
`open`, `params`, `destination`, `stream_died`, `destination_full`,
`write_failed`, `write_stalled`.

`write_stalled` says only that the writer had not reported within 5 s of the
stream stopping. It may have been held on a write before the `end` record or
on the sync after it, so whether the record ends is the reader's verdict to
give, not the line's.

A stdout or stderr whose reader has gone does not stop the capture: the first
failed line is noted on stderr, later lines are not attempted, and prose that
cannot be written is dropped.

## The reader tool

`capture-record summary <record>` prints one JSON object: `record_id`, the
header metadata verbatim (`header`), `frames_delivered`, `frames_recorded`,
`duration_frames_s`, `duration_clock_s`, `blocks`, `short_blocks`, `late`,
`gaps`, `wall_steps`, `end` and `tail`. `gaps: []` is the explicit statement
that there were none. Each gap carries `cause`, `at_frame`, `frames`,
`clock_estimate_frames`, `mono_us`, `wall_us` and `detail`; for a gap with
`frames: null`, `clock_estimate_frames` is the hole's length estimated from the
capture times of the blocks on either side at the nominal rate, reported beside
`frames: null` and never in its place.

`capture-record export-wav <record> <out.wav>` writes a
`WAVE_FORMAT_EXTENSIBLE` PCM file with the record's channel count, rate and
sample width and a zero channel mask, and prints a map as JSON: `wav_frames`,
`runs` (`{wav_frame, first_frame, frames}`), `holes` (`{wav_frame, at_frame,
filled_frames, basis}`, with `basis` one of `measured`, `clock_estimate` or
`unknown` where no block follows the gap) and `tail`. Each gap becomes that
much silence. The output must not exist; a record past the 4 GiB RIFF limit is
refused.

Exit status for both: 0 when the output was produced, whatever the tail; 1 when
the header is unreadable or I/O failed; 2 for a command line it does not
understand.
