# Frame log format

A frame log is a stream of wire frames, each with a stamp, in one file that
explains itself. The voice host records each pod connection into one: the
pod's frames as they arrived, stamped with the host's receive time.
`pod-capture` records an ALSA capture device into one: every channel,
continuously, as wire frames of its own making, with every loss at the place
it happened.

The format is owned by `firmware/crates/frame-log`, which writes and reads it,
and the frames by `firmware/crates/audio-pipeline` (`wire` for the connection's
frames, `wide` for a capture's). Read logs through those crates or the
`frame-log` tool rather than re-implementing the layout.

## Container

```text
header:  magic (4 bytes) | format_version u16 LE | meta_len u16 LE | meta (meta_len bytes)
records: stamp u64 LE | len u16 LE | payload (len bytes, 1..=65535)
```

The magic is `RSFL` and `format_version` is 1. A record's payload is one
framed wire frame, its own `u16` length prefix included, so a frame's postcard
bytes are at most 65533. A record's stamp is epoch microseconds on the writing
machine (`CLOCK_REALTIME`): for the voice host, when the frame was received;
for a capture, when the frame's content was observed, which for audio is when
the ALSA read that delivered it returned.

A file copied while it is being written is whole records followed by at most
one torn one. A record of length 0 is corrupt; every other length is a record.

## Header metadata

JSON. Unknown keys are ignored on read.

```json
{
  "build_id": "<the writer's build>",
  "created_epoch_us": 1794000000000000,
  "conn_seq": 0,
  "capture": {
    "record_id": "9f0c…32 hex digits",
    "writer": {"program": "pod-capture",
               "build": {"commit": "<40 hex>", "dirty": false, "source": "git"}},
    "device": {"selector": "Flex XVF3800 L16K6Ch",
               "card_index": 2,
               "card_name": "reSpeaker Flex XVF3800 L16K6Ch",
               "card_long_name": "Seeed Studio reSpeaker Flex XVF3800 L16K6Ch at usb-…",
               "pcm": "hw:2,0",
               "usb_id": "2886:0022"},
    "granted": {"period_frames": 320, "buffer_frames": 5120},
    "clocks": {"mono": "CLOCK_MONOTONIC", "wall": "CLOCK_REALTIME", "unit": "us"},
    "device_control": {"performed": false}
  }
}
```

- `build_id`, `created_epoch_us`, `conn_seq` and the optional `rolled_from` are
  every log's. A capture's `build_id` is the commit, its `created_epoch_us` the
  wall reading of its start pair, and its `conn_seq` 0.
- `capture` is present only in a capture's log; a voice host's header has no
  such key. It describes the writer and the recording:
  - `record_id`: 128 random bits as 32 lowercase hex digits, the capture's
    identity. Another stream cites audio as `{record_id, start_frame,
    end_frame}`. The file name is the caller's and is not part of it.
  - `writer.build.source`: `git` when a checkout of this repository vouched for
    the commit, `declared` when the builder declared it through
    `BRENN_POD_BUILD_COMMIT`. `dirty` is `null` when the source is `declared`.
  - `device.usb_id`: the kernel's `/proc/asound/card<N>/usbid`, or `null` for a
    card with no USB id.
  - `granted`: read back from the open stream, not echoed from the request.
    The rate, channel count and sample format are in the hello.
  - `device_control.performed: false`: the writer sent the device no control
    command.

  A `capture` block that breaks these rules (a malformed id or commit, a zero
  period, a buffer smaller than the period, other clocks) is refused on write
  and on read.

## The version rule

`AUDIO_PROTOCOL_VERSION`, carried in every hello, versions the **connection
schema**: the frames and field values two peers of a device–host connection
may send each other. It is bumped when either peer starts sending something
the other end of an existing deployment cannot decode.

Frame tags 0 to 63 belong to the connection schema. Tag 64 is the **capture
extension**: one frame, `Capture`, whose body is the capture schema's own enum.
No connection peer sends or accepts it.

The capture schema has its own version, `CAPTURE_SCHEMA_VERSION` (1), carried
in the `Describe` frame. It covers the capture bodies and the enum values only
a capture uses: `Codec::S32Le`, `ChannelSource::Described`,
`EndReason::SourceLoss` and `EndReason::StreamEnd`. It is bumped when a capture
body's layout changes or a body is appended, and forces nothing on any device.
The day a connection peer sends one of those values, or a tag-64 frame, that is
a connection-schema change and bumps `AUDIO_PROTOCOL_VERSION`.

The hello's version says which connection frames may follow. A
`Capture(Describe)` directly after the hello says the stream also uses capture
schema `schema`. A reader that does not know the extension fails at the
`Describe`, the second record of every capture, before any audio; a reader that
knows it and meets a newer `schema` refuses the log there.

## The sample index and loss

The sample index counts samples per channel **delivered by the capture source**
since the stream's start. A sample has one index for the life of the stream and
no two samples share one. Inside a segment the index never goes backwards.

- Audio the source delivered and the stream does not carry shows as a forward
  jump of exactly its length. **Inside a segment** the jump is a **measured**
  loss. **Between two segments** it is audio **not sent**: the gate was closed,
  or the producer lost it and says so in a frame (the earlier segment's
  `Overrun` end, or a `Loss` frame).
- Audio the source never delivered is an **unmeasured** loss: the index does
  not advance, because nothing counted the samples.
- A segment may begin **before** the previous one ended: a gated producer opens
  a segment with pre-roll, and when its gate reopens within the pre-roll length
  the new segment's base lies inside the previous segment. The samples sent
  again carry the indices and values they had.

A loss is what a frame marks as one: a `Loss` frame, a segment closed with
`Overrun`, `InternalError` or `SourceLoss`, or a jump inside a segment. The
distance between two segments is never by itself a loss.

| Event | ESP pod and Linux voice pod | `pod-capture` |
|---|---|---|
| Delivered, not carried (ring lapped; writer queue full) | `SegmentEnd{Overrun}`; the next segment's base has advanced | `Capture(Loss{writer_backlog, samples: n})`; the next `Audio` frame's index has advanced by `n`, inside the same segment |
| Source lost it (driver overrun, suspend, transfer error) | No frame. Index contiguous. The `Status` frame's capture counters count it, with no position | `SegmentEnd{SourceLoss}` if a segment is open, then `Capture(Loss{cause, samples: null})`. The next segment's base is the index the loss happened at |

## A capture's stream

```text
Hello                                  version 4, pod_id, rate, bits, channels, codec, Described
Capture(Describe)
then, repeated:
  SegmentStart                         at the first block after the start or after an unmeasured loss
  Audio, Capture(Read)                 one pair per ALSA read
  Capture(Loss{writer_backlog, n})     where blocks were dropped; the next Audio's index is n further on
  SegmentEnd{SourceLoss}, Capture(Loss{cause, None})      at an unmeasured loss
finally:
  SegmentEnd{StreamEnd}                if a segment is open
  Capture(End)
```

- `Hello`: `sample_rate_hz` is the granted rate; `bits_per_sample` and `codec`
  are 16 and `S16Le`, or 32 and `S32Le`; `channels` the granted count;
  `channel_source` is `Described`.
- `SegmentStart`: ids count from 0; `base_sample_index` and `base_device_ts_us`
  are the first block's; `preroll_samples` is 0. It is written with the
  segment's first block, so no segment is empty.
- `Audio`: one ALSA read, at most `Describe.period_frames` frames of
  interleaved PCM exactly as the device delivered it. `device_ts_us` is the
  capture time of its first sample, extrapolated at the nominal rate from the
  read: `mono_us − (backlog_frames + frames) × 1 000 000 / rate`.
- `SegmentEnd`: `frames_sent` and `samples_sent` count the `Audio` frames
  written in the segment and their samples per channel; `device_ts_us` is the
  monotonic reading of the event that closed it.
- A loss with no segment open is its `Loss` frame alone.

`device_ts_us` is the producer's monotonic clock in microseconds on every
producer: `CLOCK_MONOTONIC` on a Linux pod and on `pod-capture`,
`esp_timer_get_time` on the ESP pod.

### Capture frames

- `Describe { schema, channel_names, period_frames, mono_us }`: sent once,
  directly after the hello. `channel_names` has exactly `Hello.channels`
  entries, in interleave order, verbatim from the caller. `period_frames` is the
  block size the source granted. `mono_us` and the record's stamp are the clock
  pair read directly after the source was started; sample 0 was captured within
  the source's start latency after it.
- `Read { first_sample_index, mono_us, backlog_frames }`: follows each `Audio`
  frame and names it. `mono_us` is the monotonic reading when the read that
  delivered the block returned, paired with the record's stamp: an upper bound
  on when the block's last sample was captured. `backlog_frames` is how many
  frames were already waiting in the source's buffer then.
- `Loss { cause, at_index, samples, mono_us, detail }`: audio is missing at
  `at_index`. `cause` is `writer_backlog` (the destination was not taking
  blocks fast enough; `samples` is exact), or `overrun`, `suspend` or
  `transfer_error` (ALSA `EPIPE`, `ESTRPIPE`, anything else recovery cleared;
  `samples` is `null` and the index does not advance). `detail` is the
  source's own error text, cut to 4096 bytes.
- `End { reason, at_index, mono_us, detail }`: the last frame of a capture that
  ended itself. `reason` is `stopped`, `destination_full`, `stream_died` or
  `write_failed`.

Each item `pod-capture` writes (a block, a loss, an end) reaches the file whole
in one write followed by a flush, and the file is synced at the end.

## What a reader may conclude

- Audio frames inside one segment with no jump between them are contiguous in
  the index and in the device's sample clock.
- A log with no loss (no `Loss` frame, no lossy segment end, no jump inside a
  segment) lost nothing the producer knew of between its first and last frame.
  A voice pod's unmeasured losses are counted only in its `Status` frames.
- A capture whose log does not end in `Capture(End)` did not stop cleanly.
- A block is **late** when `backlog_frames >= period_frames`: the read that
  delivered it returned a whole period or more behind the device. A late block
  lost nothing.
- `CLOCK_MONOTONIC` and `CLOCK_REALTIME` are slewed together, so stamp minus
  `mono_us` changes only when the wall clock is stepped. A change of more than
  1 ms between consecutive capture frames is a **wall-clock step**; a loss is
  a frame and a step is a change in the pair, and neither is mistaken for the
  other.
- A stream stamped on `CLOCK_REALTIME` on the same machine is placed against a
  capture through the records' stamps, interpolating between the two `Read`
  frames around a wall time; the `mono_us` beside each shows whether the wall
  clock stepped between them. A stream that carries `CLOCK_MONOTONIC` joins on
  `mono_us` directly.

## Tail verdicts

A reader stops with one verdict, and everything before it is usable in all four:

| Verdict | Means |
|---|---|
| `ended` | A `Capture(End)` frame, then end of file. |
| `unterminated` | End of file on a record boundary with no `End`. Every voice-host log ends this way; for a capture it means the writer was killed, the file was copied while it ran, or its last write failed and so did the attempt to end it. |
| `torn` | End of file inside a record. |
| `corrupt` | A zero length; a frame that does not decode; a first frame that is not a hello; a hello version outside the readable range, or with no channels or a zero rate; a capture schema above the reader's; a sample format other than 16-bit `S16Le` and 32-bit `S32Le`; PCM that is not whole frames; audio or a segment end outside a segment or naming another segment; a second hello; a frame only the host sends; a capture frame with no `Describe` after the hello, a `Described` hello with none, or a misplaced `Describe`; a `Describe` whose names are not the hello's channel count; anything after an `End`; a `Read` that does not name the `Audio` frame before it; a `Loss` or `End` whose `at_index` is not the position; an index past its limit. |

Each verdict carries the byte offset it applies at, a reason, and the id of the
segment left open, if any. A jump of the index is never corrupt.

## The reader tool

```sh
frame-log summary <log>
frame-log export-wav <log> <out.wav>
```

Both read a capture's log and a voice host's, at any rate, channel count and
either sample format.

`summary` prints one JSON object:

- `header`: the header metadata as written; `record_id`: the capture's, or
  `null`.
- `hello`: `version`, `pod_id`, `sample_rate_hz`, `bits_per_sample`,
  `channels`, `codec`, `channel_source`.
- `capture`: `schema`, `channel_names`, `period_frames` and the start pair
  `started` (`mono_us`, `wall_us`), or `null` for a log with no `Describe`.
- `frames_delivered` (from the first index to the furthest, losses included),
  `frames_recorded`, `duration_frames_s`, `duration_clock_s` (from the frames'
  `device_ts_us`).
- `segments`: each with `segment_id`, `base_frame`, `not_sent_frames` (how far
  the base lies past everything before it: audio the stream did not send),
  `resent_frames` (how far it lies before: audio sent again as pre-roll),
  `frames`, `audio_frames`, `end` (the reason, or `null` if never closed) and
  `count_check` (`match`, `mismatch` or `null`).
- `discontinuities`: every loss and every break inside a segment, each with
  `kind`, `at_frame`, `frames` (a number, or `null` when unmeasured),
  `clock_estimate_frames`, `segment_id`, `cause`, `mono_us`, `wall_us`,
  `detail`. Kinds: `loss` (a `Loss` frame), `gap` and `overlap` (a forward or
  backward jump inside a segment that no `Loss` frame accounts for),
  `segment_end` (a segment closed with `overrun`, `internal_error` or
  `source_loss` and no `Loss` frame beside it), `segment_replaced` (a
  `SegmentStart` while one was open), `count_mismatch`. `[]` is the explicit
  statement that there were none. The distance between two segments is never a
  discontinuity.
- `blocks`, `short_blocks`, `late` (runs of late blocks: `first_frame`,
  `blocks`, `max_backlog_frames`), `wall_steps`: for a capture; `null` for a log
  with no `Describe`, which does not measure them.
- `other_frames`: counts of `telemetry` and `status` frames.
- `end`: the `End` frame's fields, or `null`.
- `tail`: `verdict`, `offset`, `reason`, `open_segment`.

For an unmeasured loss, `clock_estimate_frames` is the hole's length estimated
from `device_ts_us` on either side at the nominal rate, from the end of the last
audio before it (or the start pair, when none precedes it) to the first audio
after it. It is reported beside `frames: null`, never in its place, and is
`null` where no audio follows.

`export-wav` writes a `WAVE_FORMAT_EXTENSIBLE` PCM file with the log's channel
count, rate and sample width and a zero channel mask, and prints a map as JSON:
`wav_frames`, `runs` (`{wav_frame, first_frame, frames}`), `holes`
(`{wav_frame, at_frame, filled_frames, basis}`), `resent` (`{segment_id,
at_frame, frames}`) and `tail`. Each hole becomes that much silence. `basis` is
`measured` (a `Loss` with a length, or a jump inside a segment), `not_sent` (a
segment's `not_sent_frames`), `clock_estimate`, or `unknown` (an unmeasured loss
with no audio after it, filled with nothing). Every index is in the WAV once: a
segment that resends pre-roll is written from where the WAV already reaches,
and the frames not written again are listed under `resent`.

The output must not exist. A log with a backward jump inside a segment (an
`overlap`) is refused with its position, and a log whose export would pass the
4 GiB RIFF limit is refused; a voice-host log with hours of closed gate meets
the second refusal, because each closed stretch becomes silence. A failed
export leaves no output.

Exit status for both: 0 when the output was produced, whatever the tail; 1 when
the header is unreadable, the log has no hello, the export is refused or I/O
failed; 2 for a command line it does not understand.
