//! ALSA capture from the XVF3800's USB audio interface.
//!
//! The board is a standard USB audio class device: 16 kHz stereo, where the two
//! channels are processed renderings of the chip's one auto-selected look
//! direction. One of them is picked and written into the shared capture ring the
//! segment engine drains; the other is dropped here rather than carried through the
//! pipeline, because the wire format is mono and the host judges one stream.
//!
//! The card is found by name and the parameters are demanded exactly, through the
//! Linux pod's shared ALSA layer in `alsa-pcm`. If the board will not give 16 kHz
//! S16_LE stereo, the failure prints what it does advertise and the pipeline stops.

use std::time::Duration;

use alsa::Direction;
use alsa::pcm::{Format, PCM, State};
use alsa_pcm::{
    CardInfo, PcmError, PcmParams, RecoveryBudget, enumerate_cards, list_cards, open_pcm,
};
use audio_pipeline::level::LevelHistory;
use audio_pipeline::ring::{CaptureRing, RING_CAPACITY_SAMPLES, RingIndex, SAMPLE_RATE_HZ};
use audio_pipeline::wire::AUDIO_SAMPLES_PER_FRAME;

use crate::config::CHANNELS;

/// Card names the board is known to present, in the order the factory software
/// searches them: the updated firmware's name first, the module's own name second.
/// Matched case-insensitively as a substring of both the card's name and its long
/// name, because which of the two carries the marketing name has changed with
/// firmware.
pub const CARD_NAMES: [&str; 2] = ["reachy mini audio", "respeaker"];

/// Frames in one period — the capture quantum, and one wire frame's worth of audio
/// (20 ms at 16 kHz) so a period read maps to a frame without restaging.
pub const PERIOD_FRAMES: i64 = AUDIO_SAMPLES_PER_FRAME as i64;

/// Periods in the ring the driver fills. Four periods is 80 ms of slack against a
/// scheduling hiccup, at 5 KiB of DMA buffer.
pub const PERIODS: u32 = 4;

/// The parameters both directions of the board's audio are opened with.
///
/// S16_LE is an expectation, not an established fact: the factory pipeline converts
/// to float immediately and never states what the hardware gave it. If the board
/// refuses this format, the advertised-parameter dump is the finding, and the lever
/// is the chip's own bit-depth setting — a reviewed, one-time provisioning action,
/// not something this pipeline should paper over at startup.
pub const CAPTURE_PARAMS: PcmParams = PcmParams {
    rate_hz: SAMPLE_RATE_HZ,
    channels: CHANNELS as u32,
    format: Format::S16LE,
    period_frames: PERIOD_FRAMES,
    periods: PERIODS,
};

// ── Finding the card ──────────────────────────────────────────────────────────

/// The board's card, by name and in the factory's preference order.
///
/// Deliberately no fallback to the default card: the factory tooling falls back to
/// card 0, and a pipeline that streams the wrong card's silence is harder to
/// diagnose than one that says which cards it saw and stops.
pub fn select_card(cards: &[CardInfo]) -> Result<&CardInfo, String> {
    for needle in CARD_NAMES {
        if let Some(found) = cards.iter().find(|c| c.matches(needle)) {
            return Ok(found);
        }
    }
    Err(format!(
        "no sound card named any of {:?}; the kernel is presenting: {}",
        CARD_NAMES,
        list_cards(cards)
    ))
}

/// Open the board's capture stream: find the card by name, open its first PCM at the
/// pipeline's parameters.
pub fn open_capture() -> Result<(CardInfo, PCM), PcmError> {
    let cards = enumerate_cards().map_err(PcmError::Enumerate)?;
    let card = select_card(&cards).map_err(PcmError::NoCard)?.clone();
    let pcm = open_pcm(&card.hw_device(), Direction::Capture, &CAPTURE_PARAMS)?;
    log::info!(
        "capture: {card} at {} on {}",
        CAPTURE_PARAMS,
        card.hw_device()
    );
    Ok((card, pcm))
}

/// A capture stream and the staging buffer its periods land in.
pub struct CaptureStream<'a> {
    io: alsa::pcm::IO<'a, i16>,
    pcm: &'a PCM,
    /// One period of interleaved stereo frames.
    staging: Vec<i16>,
    budget: RecoveryBudget,
}

impl<'a> CaptureStream<'a> {
    /// Wrap an open capture PCM.
    pub fn new(pcm: &'a PCM) -> Result<Self, PcmError> {
        let io = pcm.io_i16().map_err(|e| PcmError::Stream {
            reason: format!("the device will not give a 16-bit interleaved reader: {e}"),
        })?;
        Ok(Self {
            io,
            pcm,
            staging: vec![0i16; PERIOD_FRAMES as usize * CHANNELS],
            budget: RecoveryBudget::new(),
        })
    }

    /// Overruns recovered from since the stream opened.
    pub fn recoveries(&self) -> u64 {
        self.budget.recoveries()
    }

    /// Read one period, recovering from an overrun and retrying.
    ///
    /// Returns the interleaved frames actually read, which can be short of a period:
    /// a partial read is audio, and dropping it to keep the arithmetic tidy would put
    /// a hole in the ring the wire's sample indices then have to explain.
    pub fn read_period(&mut self) -> Result<&[i16], PcmError> {
        loop {
            match self.io.readi(&mut self.staging) {
                Ok(frames) => {
                    self.budget.note_success();
                    // The borrow checker cannot see that the `Ok` arm ends the loop,
                    // so the slice is taken after it rather than inside it.
                    return Ok(&self.staging[..frames * CHANNELS]);
                }
                Err(e) => {
                    self.budget
                        .note_failure("capture", self.pcm, e)
                        .map_err(|reason| PcmError::Stream { reason })?;
                }
            }
        }
    }

    /// Trigger the stream if it has only been prepared.
    ///
    /// ALSA starts a capture stream from inside the first read, so a caller that
    /// waits before it reads would poll a stream nothing ever told to run: a
    /// prepared capture PCM has no frames available and never will, so every wait
    /// times out and a healthy card reads as one that delivers nothing. Recovery
    /// re-prepares the stream, which is why this is checked on every wait rather
    /// than once at open.
    fn ensure_started(&mut self) -> Result<(), PcmError> {
        if self.pcm.state() != State::Prepared {
            return Ok(());
        }
        self.pcm.start().map_err(|e| PcmError::Stream {
            reason: format!("the prepared stream would not start: {e}"),
        })
    }

    /// Wait up to `timeout` for the stream to have a period ready.
    ///
    /// `Ok(false)` is the timeout expiring, which is what lets a caller bound a
    /// stalled card: [`read_period`](Self::read_period) blocks indefinitely on a card
    /// that opened, accepted the parameters and then delivered nothing, so a deadline
    /// consulted only between reads bounds a slow card but not a stopped one.
    pub fn wait_ready(&mut self, timeout: Duration) -> Result<bool, PcmError> {
        self.ensure_started()?;
        let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        match self.pcm.wait(Some(ms)) {
            Ok(ready) => Ok(ready),
            // An xrun noticed while waiting is the same event as one noticed during a
            // read: charged, recovered, and reported as not-ready so the caller
            // re-checks its own deadline before asking again.
            Err(e) => self
                .budget
                .note_failure("capture", self.pcm, e)
                .map(|_| false)
                .map_err(|reason| PcmError::Stream { reason }),
        }
    }
}

// ── Into the ring ─────────────────────────────────────────────────────────────

/// Copy one channel of an interleaved chunk into the capture ring and date it.
///
/// `now_us` is the platform monotonic reading taken when the chunk arrived. The
/// anchor names the last sample written, which is what the streamer extrapolates
/// every frame's timestamp from — so it is refreshed per chunk, not per sample.
///
/// A chunk with a partial trailing frame (fewer samples than the channel count)
/// drops that remainder: half a frame carries no sample for the channel we stream.
///
/// The streamed channel is also the level channel; `playing` flags the chunk's level
/// blocks as captured while the speaker output was active.
pub fn append_channel(
    ring: &mut CaptureRing<Box<[i16]>>,
    interleaved: &[i16],
    channel: usize,
    playing: bool,
    now_us: u64,
) {
    debug_assert!(channel < CHANNELS, "channel is validated at configuration");
    let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
    for frame in interleaved.chunks_exact(CHANNELS) {
        ring.commit(&ridx, frame[channel], None, playing);
    }
    if !interleaved.is_empty() {
        ring.anchor_sample = ring.write_head.saturating_sub(1);
        ring.anchor_ts_us = now_us;
    }
}

/// Split an interleaved chunk into one buffer per channel, appending, and report
/// the frames it carried.
///
/// A partial trailing frame is neither kept nor counted.
pub fn append_channels(interleaved: &[i16], out: &mut [Vec<i16>; CHANNELS]) -> usize {
    let mut frames = 0;
    for frame in interleaved.chunks_exact(CHANNELS) {
        for (channel, buffer) in out.iter_mut().enumerate() {
            buffer.push(frame[channel]);
        }
        frames += 1;
    }
    frames
}

/// A capture ring sized for this platform: a plain heap allocation, where the ESP
/// pod's is in PSRAM.
pub fn new_ring() -> CaptureRing<Box<[i16]>> {
    CaptureRing {
        samples: vec![0i16; RING_CAPACITY_SAMPLES].into_boxed_slice(),
        aux: None,
        write_head: 0,
        anchor_sample: 0,
        anchor_ts_us: 0,
        levels: LevelHistory::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(index: i32, name: &str, long_name: &str) -> CardInfo {
        CardInfo {
            index,
            name: name.to_string(),
            long_name: format!("{long_name} at usb-xhci-hcd.1-1.4, high speed"),
        }
    }

    #[test]
    fn the_board_is_found_by_name_at_whatever_index_it_landed_on() {
        let cards = vec![
            card(0, "bcm2835 Headphones", "bcm2835 Headphones"),
            card(1, "Reachy Mini Audio", "Seeed Reachy Mini Audio"),
        ];
        let found = select_card(&cards).expect("select");
        assert_eq!(found.index, 1);
        assert_eq!(found.hw_device(), "hw:1,0");
    }

    #[test]
    fn the_updated_firmwares_name_wins_over_the_modules_own() {
        // Both names present at once is not a shape hardware produces, but the
        // preference order is the factory's and a tie must not be resolved by
        // enumeration order.
        let cards = vec![
            card(0, "ReSpeaker 4 Mic Array", "Seeed ReSpeaker"),
            card(1, "Reachy Mini Audio", "Seeed Reachy Mini Audio"),
        ];
        assert_eq!(select_card(&cards).expect("select").index, 1);
    }

    #[test]
    fn the_pre_update_name_is_still_a_match() {
        let cards = vec![card(2, "ReSpeaker 4 Mic Array", "Seeed ReSpeaker")];
        assert_eq!(select_card(&cards).expect("select").index, 2);
    }

    #[test]
    fn a_name_matches_whichever_field_carries_it_and_in_any_case() {
        // The marketing name has moved between the short name and the long name
        // across firmware revisions, so both are searched, and the case a board
        // reports is not something to depend on.
        let by_long = CardInfo {
            index: 3,
            name: "USB Audio".into(),
            long_name: "Seeed REACHY MINI AUDIO at usb-xhci-hcd.1-1.4".into(),
        };
        assert_eq!(select_card(&[by_long]).expect("select").index, 3);
        let by_name = vec![card(0, "reachy mini audio", "USB Audio Device")];
        assert!(select_card(&by_name).is_ok());
    }

    #[test]
    fn no_matching_card_names_what_was_there_instead_of_falling_back_to_zero() {
        let cards = vec![card(0, "bcm2835 Headphones", "bcm2835 Headphones")];
        let err = select_card(&cards).unwrap_err();
        assert!(err.contains("bcm2835 Headphones"), "{err}");
        assert!(err.contains("card 0"), "{err}");
        let empty = select_card(&[]).unwrap_err();
        assert!(empty.contains("no cards at all"), "{empty}");
    }

    #[test]
    fn the_wanted_parameters_render_as_the_thing_that_was_asked_for() {
        let rendered = CAPTURE_PARAMS.to_string();
        assert_eq!(rendered, "16000 Hz S16_LE 2 ch, period 320 frames × 4");
    }

    #[test]
    fn the_configured_channel_is_the_one_that_reaches_the_ring() {
        let mut ring = new_ring();
        // Two frames, left and right distinguishable.
        let chunk = [10i16, -10, 20, -20];
        append_channel(&mut ring, &chunk, 0, false, 1_000);
        assert_eq!(ring.write_head, 2);
        assert_eq!(&ring.samples[..2], &[10, 20]);

        let mut ring = new_ring();
        append_channel(&mut ring, &chunk, 1, false, 1_000);
        assert_eq!(&ring.samples[..2], &[-10, -20]);
    }

    #[test]
    fn the_anchor_dates_the_last_sample_written() {
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2, 3, 4], 0, false, 5_000);
        assert_eq!(ring.anchor_sample, 1, "the last sample's absolute index");
        assert_eq!(ring.anchor_ts_us, 5_000);
        append_channel(&mut ring, &[5, 6], 0, false, 6_000);
        assert_eq!(ring.anchor_sample, 2);
        assert_eq!(ring.anchor_ts_us, 6_000);
    }

    #[test]
    fn an_empty_chunk_leaves_the_anchor_where_it_was() {
        // A read that returned nothing is not a moment any sample was captured at,
        // and re-dating the anchor to it would slide every later frame's timestamp.
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2], 0, false, 5_000);
        append_channel(&mut ring, &[], 0, false, 9_000);
        assert_eq!(ring.write_head, 1);
        assert_eq!(ring.anchor_ts_us, 5_000);
    }

    #[test]
    fn a_partial_trailing_frame_is_dropped_rather_than_half_written() {
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2, 3], 0, false, 5_000);
        assert_eq!(ring.write_head, 1, "one whole frame, not one and a half");
        assert_eq!(ring.samples[0], 1);
    }

    #[test]
    fn writes_wrap_the_ring_and_keep_the_absolute_index() {
        let mut ring = new_ring();
        ring.write_head = RING_CAPACITY_SAMPLES as u64 - 1;
        append_channel(&mut ring, &[7, 0, 8, 0], 0, false, 5_000);
        assert_eq!(ring.write_head, RING_CAPACITY_SAMPLES as u64 + 1);
        assert_eq!(ring.samples[RING_CAPACITY_SAMPLES - 1], 7);
        assert_eq!(ring.samples[0], 8, "the second sample lapped the ring");
        assert_eq!(ring.anchor_sample, RING_CAPACITY_SAMPLES as u64);
    }

    #[test]
    fn splitting_keeps_each_channel_in_its_own_buffer_across_chunks() {
        // The count is what every bounded collection measures its progress by, so a
        // frame counted but not kept would let a short window pass as a whole one.
        let mut out = [Vec::new(), Vec::new()];
        assert_eq!(append_channels(&[1, -1, 2, -2], &mut out), 2);
        assert_eq!(append_channels(&[3, -3], &mut out), 1);
        assert_eq!(out[0], vec![1, 2, 3]);
        assert_eq!(out[1], vec![-1, -2, -3]);
        // A partial trailing frame is dropped here for the same reason it is on the
        // ring path: it carries no sample for one of the channels. Neither kept nor
        // counted.
        assert_eq!(append_channels(&[4], &mut out), 0);
        assert_eq!(out[0].len(), 3);
        assert_eq!(append_channels(&[], &mut out), 0);
        assert_eq!(out[0].len(), 3);
    }

    #[test]
    fn a_prepared_capture_stream_is_started_before_it_is_waited_on() {
        // ALSA starts a capture stream from inside the first read. A caller that
        // waits before it reads would poll a stream nothing ever told to run: no
        // frames become available, every wait times out, and a healthy board reads
        // as one that delivers nothing at all. Against the null device: no
        // hardware, but the same state machine.
        let pcm = open_pcm("null", Direction::Capture, &CAPTURE_PARAMS)
            .expect("ALSA's built-in null device takes any parameters");
        assert_eq!(
            pcm.state(),
            State::Prepared,
            "setting hardware parameters leaves a stream prepared, not running"
        );

        let mut stream = CaptureStream::new(&pcm).expect("a 16-bit reader");
        assert!(
            stream.wait_ready(Duration::ZERO).expect("a wait"),
            "the null device always has room"
        );
        assert_eq!(
            pcm.state(),
            State::Running,
            "the wait must trigger the stream, or it is waiting on one that never fills"
        );
    }

    #[test]
    fn the_ring_is_the_shared_capacity_and_starts_empty() {
        let ring = new_ring();
        assert_eq!(ring.samples.len(), RING_CAPACITY_SAMPLES);
        assert_eq!(ring.write_head, 0);
        assert!(ring.samples.iter().all(|s| *s == 0));
    }
}
