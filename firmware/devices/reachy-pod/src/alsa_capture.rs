//! ALSA capture from the XVF3800's USB audio interface.
//!
//! The board is a standard USB audio class device: 16 kHz stereo, where the two
//! channels are processed renderings of the chip's one auto-selected look
//! direction. One of them is picked and written into the shared capture ring the
//! segment engine drains; the other is dropped here rather than carried through the
//! pipeline, because the wire format is mono and the host judges one stream.
//!
//! Two rules this module holds to, both of them bring-up discipline rather than
//! convenience:
//!
//! - The card is found by name, never by index. Index order depends on probe order,
//!   and a pipeline that opens card 0 works until the day something else enumerates
//!   first and then streams silence from the wrong device.
//! - The hardware parameters are demanded, not negotiated. If the board will not
//!   give 16 kHz S16_LE stereo, the failure prints what it does advertise and the
//!   pipeline stops. Resampling or converting in software would hide a firmware
//!   configuration finding behind audio that sounds nearly right.

use std::fmt;
use std::time::Duration;

use alsa::pcm::{Access, Format, HwParams, PCM, State};
use alsa::{Direction, ValueOr};
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

/// How many consecutive recoveries are tolerated before the stream is declared dead.
///
/// An xrun under load is normal and recoverable; a device that xruns on every
/// transfer has gone away or wedged, and the pipeline's answer to that is to exit and
/// be restarted rather than to log a line every 20 ms forever. Both directions of the
/// board hold to the same ceiling.
pub const MAX_CONSECUTIVE_RECOVERIES: u32 = 8;

/// One hardware configuration, in the terms ALSA takes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcmParams {
    pub rate_hz: u32,
    pub channels: u32,
    pub format: Format,
    pub period_frames: i64,
    pub periods: u32,
}

impl fmt::Display for PcmParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} Hz {} {} ch, period {} frames × {}",
            self.rate_hz, self.format, self.channels, self.period_frames, self.periods
        )
    }
}

// ── Finding the card ──────────────────────────────────────────────────────────

/// A sound card as ALSA enumerated it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardInfo {
    /// The kernel's index for the card — what `hw:` addresses, and what probe order
    /// decides.
    pub index: i32,
    /// The card's own name — where the board's marketing name usually lands.
    pub name: String,
    /// The long name, which also names the bus the card is attached to.
    pub long_name: String,
}

impl CardInfo {
    /// Whether either of this card's names contains `needle`, case-insensitively.
    fn matches(&self, needle: &str) -> bool {
        [&self.name, &self.long_name]
            .iter()
            .any(|field| field.to_lowercase().contains(needle))
    }

    /// The `hw:` device this card's first PCM is opened as. Direct hardware access:
    /// the application is the card's only user, so there is nothing to mix with and
    /// no reason to route through a plugin that might resample behind our back.
    pub fn hw_device(&self) -> String {
        format!("hw:{},0", self.index)
    }
}

impl fmt::Display for CardInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "card {} [{}] {}", self.index, self.name, self.long_name)
    }
}

/// Every sound card the kernel is presenting.
///
/// A card whose name cannot be read is still listed, under an empty name: the
/// failure to match is then reported with the index that was there, which is the
/// finding, where skipping it would report a card that is not there at all.
pub fn enumerate_cards() -> Result<Vec<CardInfo>, alsa::Error> {
    let mut cards = Vec::new();
    for card in alsa::card::Iter::new() {
        let card = card?;
        cards.push(CardInfo {
            index: card.get_index(),
            name: card.get_name().unwrap_or_default(),
            long_name: card.get_longname().unwrap_or_default(),
        });
    }
    Ok(cards)
}

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
    let seen: Vec<String> = cards.iter().map(|c| c.to_string()).collect();
    Err(format!(
        "no sound card named any of {:?}; the kernel is presenting: {}",
        CARD_NAMES,
        if seen.is_empty() {
            "no cards at all".to_string()
        } else {
            seen.join("; ")
        }
    ))
}

/// Why a stream on the board could not be brought up, or could not continue.
///
/// Shared by both directions: finding the card, opening a node and demanding
/// parameters fail the same way whichever way the audio is going.
#[derive(Debug)]
pub enum PcmError {
    /// The card list could not be read.
    Enumerate(alsa::Error),
    /// No card answers to the board's names.
    NoCard(String),
    /// The device node would not open — usually the `audio` group.
    Open { device: String, source: alsa::Error },
    /// The device opened and refused the parameters. Carries what it does offer.
    Params {
        device: String,
        wanted: PcmParams,
        advertised: String,
        source: alsa::Error,
    },
    /// A transfer failed and recovery did not restore the stream.
    Stream { reason: String },
}

impl fmt::Display for PcmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Enumerate(e) => write!(f, "cannot enumerate sound cards: {e}"),
            Self::NoCard(why) => write!(f, "{why}"),
            Self::Open { device, source } => write!(f, "cannot open {device}: {source}"),
            Self::Params {
                device,
                wanted,
                advertised,
                source,
            } => write!(
                f,
                "{device} refused {wanted} ({source})\n  it advertises: {advertised}"
            ),
            Self::Stream { reason } => write!(f, "stream stopped: {reason}"),
        }
    }
}

impl std::error::Error for PcmError {}

// ── Recovery accounting ───────────────────────────────────────────────────────

/// How many xruns a stream has recovered from, and when it has had enough.
///
/// Both directions of the board hold to one recovery policy — the same ceiling, the
/// same counters, the same warn line — so it is decided here once. What the two
/// callers keep is their own control flow after a recovery: capture retries the read
/// in place, playback hands the underrun back so the drain can rebuild its cushion.
#[derive(Default)]
pub struct RecoveryBudget {
    recoveries: u64,
    consecutive: u32,
}

/// What the caller of a failed transfer should do with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// A signal interrupted the transfer before it moved anything. Nothing about
    /// the stream changed and nothing was recovered: reissue the transfer.
    Interrupted,
    /// An xrun was recovered from. The transfer did not happen and the stream has
    /// been restarted, so the caller decides what to do about the audio that did
    /// not move.
    Recovered,
}

impl RecoveryBudget {
    /// A stream that has recovered from nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Xruns recovered from since the stream opened — a health counter, reported
    /// rather than acted on until they stop being occasional.
    pub fn recoveries(&self) -> u64 {
        self.recoveries
    }

    /// A transfer went through: the streak is over.
    pub fn note_success(&mut self) {
        self.consecutive = 0;
    }

    /// Count a failed transfer and warn, or refuse because the ceiling is spent.
    ///
    /// The whole policy, with no driver in it, so the ceiling and the counters are
    /// asserted without a sound card. `direction` names the failing stream in the log
    /// line, which is what an operator greps when a pod goes quiet.
    ///
    /// A transfer a signal interrupted is not charged. `EINTR` says the thread was
    /// woken, not that the card faltered — alsa-lib's own recovery treats it as
    /// nothing to do — and spending a slot of the budget that exists to detect a
    /// wedged card would let a signal cadence above one per period kill a healthy
    /// stream, under a log line naming the sound card.
    pub fn charge(&mut self, direction: &str, e: &alsa::Error) -> Result<Retry, String> {
        if e.errno() == libc::EINTR {
            return Ok(Retry::Interrupted);
        }
        self.recoveries += 1;
        self.consecutive += 1;
        if self.consecutive > MAX_CONSECUTIVE_RECOVERIES {
            return Err(format!(
                "{MAX_CONSECUTIVE_RECOVERIES} consecutive recoveries did not restore the \
                 stream; last error: {e}"
            ));
        }
        log::warn!(
            "{direction}: xrun or transfer error ({e}); recovering (attempt {}, {} total)",
            self.consecutive,
            self.recoveries
        );
        Ok(Retry::Recovered)
    }

    /// A transfer failed. Charges it, then asks the driver to recover.
    ///
    /// `Ok` means the stream is running again and carries what the caller should do
    /// with the transfer that did not happen. `Err` means the stream is done — either
    /// the ceiling is spent or recovery itself failed — and carries the reason for the
    /// caller's own error type.
    pub fn note_failure(
        &mut self,
        direction: &str,
        pcm: &PCM,
        e: alsa::Error,
    ) -> Result<Retry, String> {
        if self.charge(direction, &e)? == Retry::Interrupted {
            return Ok(Retry::Interrupted);
        }
        pcm.try_recover(e, true)
            .map(|()| Retry::Recovered)
            .map_err(|e| format!("recovery failed: {e}"))
    }
}

/// Open one direction of a device at exactly [`CAPTURE_PARAMS`].
///
/// Every setter is an exact one — no `_near` variant anywhere. A near-setter is how
/// a device that cannot do 16 kHz ends up opened at 48 kHz with nothing said, and
/// the whole point of this path is that a board which cannot do what we need says so
/// here, loudly, before any audio is judged.
pub fn open_pcm(device: &str, direction: Direction, params: &PcmParams) -> Result<PCM, PcmError> {
    let pcm = PCM::new(device, direction, false).map_err(|source| PcmError::Open {
        device: device.to_string(),
        source,
    })?;
    if let Err(source) = apply_params(&pcm, params) {
        return Err(PcmError::Params {
            device: device.to_string(),
            wanted: *params,
            advertised: describe_supported(&pcm),
            source,
        });
    }
    Ok(pcm)
}

/// Demand `params` of an open PCM.
fn apply_params(pcm: &PCM, params: &PcmParams) -> Result<(), alsa::Error> {
    let hwp = HwParams::any(pcm)?;
    hwp.set_access(Access::RWInterleaved)?;
    hwp.set_format(params.format)?;
    hwp.set_channels(params.channels)?;
    // Resampling off before the rate is set: with it on, alsa-lib can satisfy an
    // exact rate request through a converter and report success for a device that
    // does not run at that rate at all.
    hwp.set_rate_resample(false)?;
    hwp.set_rate(params.rate_hz, ValueOr::Nearest)?;
    hwp.set_period_size(params.period_frames, ValueOr::Nearest)?;
    hwp.set_periods(params.periods, ValueOr::Nearest)?;
    pcm.hw_params(&hwp)
}

/// What a device will accept, as one line, for the failure that prints it.
///
/// Best-effort by construction: a device that refuses a query is reported as having
/// refused it, because a parameter dump with a hole in it is still the most useful
/// thing available when the open has already failed.
pub fn describe_supported(pcm: &PCM) -> String {
    let Ok(hwp) = HwParams::any(pcm) else {
        return "nothing — the device would not state its capabilities".to_string();
    };
    let formats: Vec<String> = [
        Format::S16LE,
        Format::S24LE,
        Format::S243LE,
        Format::S32LE,
        Format::FloatLE,
    ]
    .iter()
    .filter(|f| hwp.test_format(**f).is_ok())
    .map(|f| f.to_string())
    .collect();
    format!(
        "rate {}, channels {}, period {} frames, buffer {} frames, formats [{}]",
        range(hwp.get_rate_min(), hwp.get_rate_max()),
        range(hwp.get_channels_min(), hwp.get_channels_max()),
        range(hwp.get_period_size_min(), hwp.get_period_size_max()),
        range(hwp.get_buffer_size_min(), hwp.get_buffer_size_max()),
        if formats.is_empty() {
            "none of the ones we know how to ask for".to_string()
        } else {
            formats.join(", ")
        }
    )
}

/// One `min..max` pair, or what went wrong reading it.
fn range<T: fmt::Display>(min: alsa::Result<T>, max: alsa::Result<T>) -> String {
    match (min, max) {
        (Ok(min), Ok(max)) => format!("{min}..{max}"),
        _ => "unreadable".to_string(),
    }
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
pub fn append_channel(
    ring: &mut CaptureRing<Box<[i16]>>,
    interleaved: &[i16],
    channel: usize,
    now_us: u64,
) {
    debug_assert!(channel < CHANNELS, "channel is validated at configuration");
    let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
    for frame in interleaved.chunks_exact(CHANNELS) {
        let slot = ridx.slot(ring.write_head);
        ring.samples[slot] = frame[channel];
        ring.write_head += 1;
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
        write_head: 0,
        anchor_sample: 0,
        anchor_ts_us: 0,
    }
}

/// The platform's monotonic clock, in microseconds — the epoch every timestamp in a
/// segment is an offset from.
///
/// `CLOCK_MONOTONIC` rather than the wall clock: this board has no battery-backed
/// clock and believes it is 1970 until NTP lands, and a segment whose timestamps
/// jump when that happens is a segment the host cannot reassemble.
pub fn monotonic_us() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a live, correctly typed timespec for the duration of the call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        // CLOCK_MONOTONIC is always available; a failure here is a kernel that has
        // stopped keeping time, and a segment dated from a frozen clock is worse
        // than none.
        panic!("clock_gettime(CLOCK_MONOTONIC) failed");
    }
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
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
    fn a_parameter_refusal_prints_both_what_was_wanted_and_what_is_offered() {
        let err = PcmError::Params {
            device: "hw:1,0".into(),
            wanted: CAPTURE_PARAMS,
            advertised: "rate 48000..48000, formats [S32_LE]".into(),
            source: alsa::Error::unsupported("test"),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("hw:1,0"), "{rendered}");
        assert!(rendered.contains("16000 Hz S16_LE"), "{rendered}");
        assert!(rendered.contains("48000..48000"), "{rendered}");
    }

    #[test]
    fn a_range_reads_as_its_bounds_or_says_it_could_not_be_read() {
        assert_eq!(range::<u32>(Ok(16_000), Ok(48_000)), "16000..48000");
        // Either end missing makes the pair meaningless, and a dump that invented
        // half of it would be read as the device's answer.
        let unreadable = alsa::Error::unsupported("snd_pcm_hw_params_get_rate_max");
        assert_eq!(range(Ok(16_000u32), Err(unreadable)), "unreadable");
        let unreadable = alsa::Error::unsupported("snd_pcm_hw_params_get_rate_min");
        assert_eq!(range(Err(unreadable), Ok(48_000u32)), "unreadable");
    }

    #[test]
    fn the_advertised_parameter_dump_is_a_reading_and_not_a_placeholder() {
        // The one open hardware question this pipeline carries is whether the board
        // gives S16_LE, and this dump is the mechanism that answers it — "the failure
        // output is the discovery". So the query path itself is exercised, against
        // ALSA's own `null` device: no hardware, but a real `HwParams` to interrogate.
        // Only the board's actual answer is left for the bench.
        let pcm = PCM::new("null", Direction::Capture, false)
            .expect("ALSA's built-in null device is part of libasound2-data");
        let dump = describe_supported(&pcm);
        assert!(
            !dump.starts_with("nothing"),
            "the query must produce a reading, not the could-not-ask fallback: {dump}"
        );
        for field in ["rate ", "channels ", "period ", "buffer ", "formats ["] {
            assert!(dump.contains(field), "{field} missing from {dump}");
        }
        assert!(
            dump.contains("S16_LE"),
            "a device that accepts S16_LE must be reported as accepting it: {dump}"
        );
        assert!(
            !dump.contains("unreadable"),
            "every range on a healthy device must read: {dump}"
        );
    }

    #[test]
    fn the_configured_channel_is_the_one_that_reaches_the_ring() {
        let mut ring = new_ring();
        // Two frames, left and right distinguishable.
        let chunk = [10i16, -10, 20, -20];
        append_channel(&mut ring, &chunk, 0, 1_000);
        assert_eq!(ring.write_head, 2);
        assert_eq!(&ring.samples[..2], &[10, 20]);

        let mut ring = new_ring();
        append_channel(&mut ring, &chunk, 1, 1_000);
        assert_eq!(&ring.samples[..2], &[-10, -20]);
    }

    #[test]
    fn the_anchor_dates_the_last_sample_written() {
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2, 3, 4], 0, 5_000);
        assert_eq!(ring.anchor_sample, 1, "the last sample's absolute index");
        assert_eq!(ring.anchor_ts_us, 5_000);
        append_channel(&mut ring, &[5, 6], 0, 6_000);
        assert_eq!(ring.anchor_sample, 2);
        assert_eq!(ring.anchor_ts_us, 6_000);
    }

    #[test]
    fn an_empty_chunk_leaves_the_anchor_where_it_was() {
        // A read that returned nothing is not a moment any sample was captured at,
        // and re-dating the anchor to it would slide every later frame's timestamp.
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2], 0, 5_000);
        append_channel(&mut ring, &[], 0, 9_000);
        assert_eq!(ring.write_head, 1);
        assert_eq!(ring.anchor_ts_us, 5_000);
    }

    #[test]
    fn a_partial_trailing_frame_is_dropped_rather_than_half_written() {
        let mut ring = new_ring();
        append_channel(&mut ring, &[1, 2, 3], 0, 5_000);
        assert_eq!(ring.write_head, 1, "one whole frame, not one and a half");
        assert_eq!(ring.samples[0], 1);
    }

    #[test]
    fn writes_wrap_the_ring_and_keep_the_absolute_index() {
        let mut ring = new_ring();
        ring.write_head = RING_CAPACITY_SAMPLES as u64 - 1;
        append_channel(&mut ring, &[7, 0, 8, 0], 0, 5_000);
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
        let pcm = PCM::new("null", Direction::Capture, false)
            .expect("ALSA's built-in null device is part of libasound2-data");
        apply_params(&pcm, &CAPTURE_PARAMS).expect("the null device takes any parameters");
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

    #[test]
    fn the_monotonic_clock_advances_and_never_goes_backwards() {
        let first = monotonic_us();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = monotonic_us();
        assert!(second > first, "{first} then {second}");
        // Sane magnitude: microseconds, not nanoseconds or seconds. Two milliseconds
        // of sleep must not read as two thousand seconds.
        assert!((second - first) < 1_000_000, "{}", second - first);
    }

    // ── RecoveryBudget ────────────────────────────────────────────────────────
    //
    // One policy for both directions, so these are the assertions that hold for
    // capture and playback alike.

    /// An xrun as alsa-lib reports one: the crate negates the syscall's return, so
    /// the errno a caller reads back is positive.
    fn xrun() -> alsa::Error {
        alsa::Error::new("snd_pcm_readi", libc::EPIPE)
    }

    /// A transfer a signal interrupted, in the same shape.
    fn interrupted() -> alsa::Error {
        alsa::Error::new("snd_pcm_readi", libc::EINTR)
    }

    #[test]
    fn an_occasional_xrun_is_recovered_and_only_counted() {
        let mut budget = RecoveryBudget::new();
        for _ in 0..MAX_CONSECUTIVE_RECOVERIES {
            assert_eq!(budget.charge("capture", &xrun()), Ok(Retry::Recovered));
            budget.note_success();
        }
        assert_eq!(
            budget.recoveries(),
            u64::from(MAX_CONSECUTIVE_RECOVERIES),
            "the lifetime counter keeps every recovery"
        );
        // A transfer in between resets the streak, so an intermittently xrunning card
        // runs indefinitely rather than being declared dead on its eighth hiccup.
        assert!(budget.charge("capture", &xrun()).is_ok());
    }

    #[test]
    fn a_signal_is_not_a_sound_card_fault() {
        let mut budget = RecoveryBudget::new();
        for _ in 0..(MAX_CONSECUTIVE_RECOVERIES * 4) {
            assert_eq!(
                budget.charge("capture", &interrupted()),
                Ok(Retry::Interrupted),
                "an interrupted transfer is reissued, not recovered"
            );
        }
        assert_eq!(
            budget.recoveries(),
            0,
            "nothing was recovered from, so nothing is counted"
        );
        for attempt in 1..=MAX_CONSECUTIVE_RECOVERIES {
            assert_eq!(
                budget.charge("capture", &xrun()),
                Ok(Retry::Recovered),
                "attempt {attempt} is within the ceiling"
            );
        }
    }

    #[test]
    fn an_interruption_does_not_break_an_xrun_streak_either() {
        // The streak is about consecutive *recoveries*; a transfer that was merely
        // interrupted neither advances it nor resets it, because it says nothing
        // about the card in either direction.
        let mut budget = RecoveryBudget::new();
        for _ in 0..MAX_CONSECUTIVE_RECOVERIES {
            assert_eq!(budget.charge("playback", &xrun()), Ok(Retry::Recovered));
            assert_eq!(
                budget.charge("playback", &interrupted()),
                Ok(Retry::Interrupted)
            );
        }
        assert!(
            budget.charge("playback", &xrun()).is_err(),
            "the ceiling is reached on the xruns alone"
        );
    }

    #[test]
    fn an_unbroken_streak_past_the_ceiling_ends_the_stream() {
        let mut budget = RecoveryBudget::new();
        for attempt in 1..=MAX_CONSECUTIVE_RECOVERIES {
            assert!(
                budget.charge("playback", &xrun()).is_ok(),
                "attempt {attempt} is within the ceiling"
            );
        }
        let spent = budget
            .charge("playback", &xrun())
            .expect_err("one past the ceiling is fatal");
        assert!(
            spent.contains(&MAX_CONSECUTIVE_RECOVERIES.to_string()) && spent.contains("last error"),
            "the reason must name the ceiling and what finally failed: {spent}"
        );
        assert_eq!(
            budget.recoveries(),
            u64::from(MAX_CONSECUTIVE_RECOVERIES) + 1,
            "the failure that spent the budget is counted too"
        );
    }
}
