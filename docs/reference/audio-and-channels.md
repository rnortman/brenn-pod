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
  **2-channel** (choice of 16 kHz or 48 kHz). No raw-mic access on I2S without a
  custom XMOS build. [confirmed via Seeed firmware catalogue,
  architecture-arc-and-deferred-paths.md:76-79, research-multi-beam.md:159-170]

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
> exposes only the two processed beams (Conference + ASR), both tracking the
> auto-selected beam — no raw mics.

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
