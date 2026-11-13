# Audio pipeline and channel map

Firmware-facing audio facts for the XVF3800 on the reSpeaker Flex Linear board.

## 1. Firmware audio modes: I2S vs USB

The XVF3800 ships in two mutually-exclusive firmware families
(research-multi-beam.md:159-170):

- **USB firmware** — XVF3800 enumerates as a USB audio device to a host. Used in
  the **Stage-1 spike** for capture/playback/control over a single USB cable.
  6-channel builds exist here.
- **I2S firmware** — XVF3800 streams audio over I2S to/from the XIAO ESP32-S3.
  This is the **product path**. All Seeed I2S builds for the Flex Linear are
  **2-channel** (choice of 16 kHz or 48 kHz) [confirmed via Seeed firmware
  catalogue, architecture-arc-and-deferred-paths.md:76-79,
  research-multi-beam.md:159-170]. What each of the two I2S slots carries is
  runtime-configurable per slot through `AUDIO_MGR_OP_L` / `AUDIO_MGR_OP_R`, so a
  raw mic is reachable on the stock I2S build without a custom XMOS build — see
  "Routing the I2S slots" in §2. [measured]

Seeed firmware catalogue (research-multi-beam.md:159-170), naming
`<conn>_<geom><rate><ch>`:

```
i2s/  respeaker_flex_i2s_{c,l}{16k,48k}2ch_v1.0.0.bin
usb/  respeaker_flex_usb_{c,l}16k2ch_v1.0.0.bin
usb/  respeaker_flex_usb_{c,l}16k6ch_v1.0.0.bin   (6-ch only at 16 kHz)
usb/  respeaker_flex_usb_{c,l}48k2ch_v1.0.0.bin
```

`l` = linear array (ours), `c` = circular. **No `-spatial` build is published
for the Flex** (research-multi-beam.md:155-244). Product target firmware:
`respeaker_flex_i2s_l16k2ch_v1.0.0.bin` (2-ch, 16 kHz, linear).
[decision, architecture-arc-and-deferred-paths.md:192]

## 2. Confirmed 6-channel map (USB l16k6ch spike build) [confirmed]

The spike flashed `respeaker_flex_usb_l16k6ch_v1.0.0.bin` (929,792 bytes, sha256
`136727693ce56cb77953a7db76ec51602971793ff43e42939d89217c305e2ac8`) and
confirmed the map empirically by tap/speech RMS testing
(usb-spike-verdict.md:23-24,57-92, audio-routing-investigation.md:14-31):

| Channel | Signal |
|---------|--------|
| 0 | **Conference beam** — processed, post-AEC/beamform, auto-select, AGC |
| 1 | **ASR beam** — processed, ASR-tuned |
| 2 | Raw mic 0 |
| 3 | Raw mic 1 |
| 4 | Raw mic 2 |
| 5 | Raw mic 3 |

Empirical confirmation (audio-routing-investigation.md):
- ch0↔ch1 correlation 0.91 (both beams from same look direction); ch0/ch1 ↔
  raw mics ~0.00–0.03 (DSP introduces ~54 ms group delay + nonlinear
  processing). (audio-routing-investigation.md:23-31)
- All 4 raw mics within 0.16 dB RMS of each other — matched array.
  (usb-spike-verdict.md:86-89)
- ch0 (Conference) peaks at 0.00 dBFS — AGC limiter pinned. ch1 (ASR) has ~15 dB
  more headroom. (usb-spike-verdict.md:79-90, audio-routing-investigation.md:66-70)

Earlier intuition that ch2-3 were AEC reference channels was **wrong**; they are
raw mics (usb-spike-verdict.md:136, audio-routing-investigation.md corrected the
map). The two processed beams track the same auto-selected DoA, not separate
talkers.

> Note: this map is for the **6-ch USB** build. The **product 2-ch I2S** build
> carries two slots whose sources are set per slot at runtime; it is not limited to
> the processed beams.

### Routing the I2S slots (product 2-ch I2S build) [measured]

The output mux behind each I2S slot is runtime-configurable through the
`AUDIO_MGR_OP_L` and `AUDIO_MGR_OP_R` control commands (a `(category, index)`
pair per slot). The respeaker pod leaves the left slot at its default, `(8, 0)` —
the auto-select communication beam — and at boot writes `(3, 0)` to the right
slot: amplified raw microphone 0 (uncompressed, no AGC, no noise suppression, no
beamforming). It reads both registers back and logs the outcome
(`firmware/devices/respeaker-pod/src/routing.rs`).

Measured on that image:

- The I2S slots are 32-bit, and the sample in them is wider than 16 bits and
  left-justified: the low half of the slot carries precision beyond the 16 bits the
  pod keeps, not padding.
- Raw mic 0 at the chip's default gain, in top-16-bit terms: ≈ −78 dBFS rms in a
  quiet room, and ≈ −52 dBFS for a normal voice at about 0.8 m.

Standing evidence, run by `firmware/crates/hil-host` on every full suite:
`Xvf3800OutputRouting` (the boot write accepted and both registers reading
`(8, 0)` / `(3, 0)` now) and `I2sRightSlotSanity` (the right slot is live,
correlated, distinct from the left, judged by the raw-mic liveness guards).

## 3. Sample rates / formats [confirmed]

From `/proc/asound/.../stream0` during the spike (usb-spike-verdict.md:59):

- Capture: **16 kHz, S16_LE, 6 channels** (USB l16k6ch build).
- Playback: **16 kHz, S16_LE, 2 channels** (FL FR).
- ALSA mixer: PCM Playback Volume 0–60 (−60..0 dB, 1 dB steps, per channel, with
  pswitch mute); Headset Capture Volume 0–60 per channel.
  (usb-spike-verdict.md:69)

Product I2S builds offer 16 kHz or 48 kHz, 2 channels (S16_LE assumed; not
independently confirmed for the I2S path — [documented]).

## 4. DSP fingerprint of the processed beams [confirmed]

Measured from spike captures (audio-routing-investigation.md). Firmware-relevant
because it bounds what the two I2S channels carry:

- **High-pass ~80–100 Hz, ~24 dB/oct** on both beams. Sub-100 Hz content is
  gutted. (audio-routing-investigation.md:46,143)
- **Conference beam (ch0):** ~+15 to +24 dB flat-ish gain above ~200 Hz vs a raw
  mic; runs at ~−20 dBFS RMS, so high-crest transients clip at the 0 dBFS
  ceiling. (audio-routing-investigation.md:40-47,66-70)
- **ASR beam (ch1):** ~+1 to +6 dB gain; ~−35 dBFS RMS mean → ~15 dB more
  transient headroom → better for non-speech / transient fidelity.
  (audio-routing-investigation.md:41,70,146)
- Noise suppression adds ~7–10 dB extra attenuation to stationary, non-speechy
  content; moderate gate, not a hole. (audio-routing-investigation.md:82-85)
- DSP group delay ~54 ms (≈861 samples at 16 kHz) constant across DoA.
  (audio-routing-investigation.md:28)

Implication for the product: voice pods use these processed beams as-is; ambient
acoustic events are handled off-board (cameras + Frigate/YAMNet), so the
beams' non-speech attenuation is acceptable. [decision,
architecture-arc-and-deferred-paths.md:88-92]

## 5. Codec / headphone-out routing [confirmed + documented]

<!-- TODO(usb-image-output-silenced-by-esp32-firmware): the "plug in, audio
plays" bullet below has been seen to hold only while the XIAO ESP32-S3 module
runs no firmware that configures the XVF3800. -->

- Codec: **TI TLV320AIC3104**, I2C address `0x18`. The XVF3800 sends analog
  audio to it over I2S; the codec's analog outs drive both the 3.5 mm headphone
  jack and the TPA3139D2 speaker amp **in hardware-parallel** — not a software
  mux in the XVF3800 firmware. (research-step0.md:264-301)
- **Stock USB firmware routes to HPOUT (3.5 mm) with no register flip** —
  confirmed in the spike: plug in, audio plays. (usb-spike-verdict.md:117,
  research-step0.md:264-269) [confirmed]
- Idle hiss is audible in IEMs (codec DAC + headphone amp always running while
  the host holds the stream); expected for the AIC3104, not significant on a
  passive speaker. (usb-spike-verdict.md:120)
- **No XVF3800 control command selects HPOUT vs SPK.** The AIC3104's own
  registers are reachable only over I2C `0x18`; the USB-mode firmware does not
  expose that path. In the product I2S design the ESP32-S3 can drive the AIC3104
  directly over I2C (the formatBCE `aic3104` component demonstrates this).
  (research-step0.md:316-322) [documented]
- The TPA3139D2 amp has a hardware enable on XVF3800 GPO **X0D31** (active-low).
  Forcing the amp off in software would use the GPO write interface; the exact
  `(port, pin)` decomposition of X0D31 is **[unknown]** in the sources, and is
  confirmed empirically during firmware bring-up. (research-step0.md:281-314)

## 6. Open / deferred audio items

- **Mic 0–3 → physical position** on the linear board (FPC-connector end vs far
  end) is **[unknown]**; FPC pinout shows MIC_D1..D4 are positional but the
  AUDIO_MGR mux index→data-line mapping is not documented. Confirmed empirically
  during firmware bring-up: tap each mic, watch per-channel RMS (use
  `wavstats.py`). (research-step0.md:204-213,329-339)
- **`-spatial` firmware** would pan the *single* auto-select beam across L/R for
  a stereo cue — it is **not** tracker-A-on-L / tracker-B-on-R source
  separation, and Seeed publishes no Flex spatial build. Not relevant to the
  product. (research-multi-beam.md:152-244) [documented]
- TPA3139D2 wattage (10 W vs 5 W per conflicting sources) and thermal headroom
  on USB-5V power: **[unknown]**. (research-step0.md:352-355)

## 7. The status frame (audio wire v4)

A pod sends `StreamFrame::Status` (tag 7) about once a second (`STATUS_PERIOD_US`)
for as long as it holds a connection to the host, idle or inside a segment, after its
`Hello`. Defined in `firmware/crates/audio-pipeline/src/wire.rs`; built in
`firmware/crates/pod-streamer/src/status.rs`.

**Version rule.** `AUDIO_PROTOCOL_VERSION` is 4. The host reads uplinks from
`AUDIO_PROTOCOL_MIN_READABLE` (3) to 4: a v3 uplink is a v4 uplink that never sends
`Status`, so frame logs recorded under v3 still replay. A live pod still requires the
host's `Hello` to match its own version exactly, so pod and host deploy together.

**Position.**

- `device_ts_us` — the pod's monotonic µs when the frame went to the encoder.
- `capture_head` — the absolute index of the next sample to be captured, in the
  `SegmentStart.base_sample_index` domain; `capture_head_ts_us` is when sample
  `capture_head − 1` was captured.
- `segment_reach` — how far behind `capture_head` a segment opened now would begin:
  the pre-roll it would carry (16 000 samples once the ring holds a second, the held
  length before that). `capture_head − segment_reach` is the position before which no
  later segment begins, except for an onset already read by the gate and not yet taken
  by the streamer when the frame was built (ordinarily one idle wake, 10 ms).

**Level blocks.** The pod meters its **level channel** in 100 ms blocks
(`LEVEL_BLOCK_SAMPLES` = 1 600 samples at 16 kHz) as it captures. The level channel is
the raw microphone where `Hello` declares `StereoBeamRawMic` (the I2S right slot, §2),
and the streamed channel otherwise. Block `n` covers samples
`[n · 1600, (n + 1) · 1600)`, so block indices are in the same domain as
`capture_head`. Each block is two bytes:

- `half_db` — the block's mean-square level as −2·dBFS, rounded and clamped to
  0..=200 (0 dBFS … −100 dBFS in 0.5 dB steps).
- `flags` — bit 0 (`LEVEL_FLAG_PLAYING`): audio was handed to the pod's speaker output
  during the block, or within the pod's hold before it. The hold is each pod's own, a
  fixed time of at most 200 ms after the last hand-over: 200 ms on the ESP pod (counted
  from the last audio its TX DMA accepted), none on Reachy. With nothing handed to the
  output the flag clears within the hold, however the stream ended and whether or not
  it ended at all. It says nothing about the DAC or the amplifier.

  What a receiver may conclude: a flagged block may contain the pod's own output. An
  unflagged block had no audio handed to the speaker during it, and may still contain
  the end of what was handed over before it, for the output's buffering depth — the
  flag marks the hand-over, not the sound. On a pod with no hold the flag may also
  clear up to one capture period before the last hand-over ended, and further, up to
  that pod's capture buffering, when its capture is running late. A receiver that needs
  blocks free of the pod's output applies its own tail after the last flagged block;
  that tail covers both.

The pod keeps the last 300 finished blocks (`LEVEL_HISTORY_BLOCKS`, 30 s). Each frame
carries the finished blocks the host has not yet been sent whole on this connection;
the first frame after `Hello` carries the whole history, so a host that restarted has
the last 30 s of room at once. A frame the link would not take is skipped and counted,
and its blocks ride the next one.

**Counters** (`PodCounters`) are cumulative since the pod booted and wrap at
`u32::MAX`; a total lower than an earlier frame's means the pod rebooted. A counter
the pod has no source for is `None`, never 0. The ESP pod does not measure
`capture_xruns`; the Reachy pod measures `capture_xruns` and none of the other optional
counters.

**Gauges** (`PodGauges`), read when the frame is built; `None` is "not available on
this pod or not readable now":

- `heap_free`, `heap_min`, `heap_largest` — internal heap bytes (ESP only).
- `rssi_dbm` — Wi-Fi RSSI (ESP only; `None` when the Wi-Fi stack lock is busy).
- `gate_open` — whether the pod's speech gate was open; `gate_energy` — the largest
  gate input since the previous frame.
- `stack_free_min` — the least stack, in bytes, the sending thread (the streamer
  thread) has had free since it started (ESP only). A frame reports the minimum as it
  stood before that frame was encoded.

A full frame is about 700 bytes; a steady-state frame, carrying ten blocks, under 100.

**On the host.** `speech-surface` writes a `pod_status` JSONL line per connection on
the first frame, whenever a counter moved or `gate_open` changed, and otherwise every
10 s; its fields and what a reader may conclude from them are documented at
`PodStatusLog::observe` (`host/crates/speech-surface/src/pod_status.rs`). A v4
connection that sends no whole frame for 10 s is closed with a `pod_silent` line and
`conn_closed.cause = "silent"`.
