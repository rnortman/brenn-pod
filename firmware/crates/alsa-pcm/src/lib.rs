//! The Linux pod's ALSA layer.
//!
//! Two rules this crate holds to, both of them bring-up discipline rather than
//! convenience:
//!
//! - A card is found by name, never by index. Index order depends on probe order,
//!   and a pipeline that opens card 0 works until the day something else enumerates
//!   first and then streams silence from the wrong device.
//! - Hardware parameters are demanded, not negotiated. If a device will not give
//!   what was asked for, the failure prints what it does advertise and the caller
//!   stops. Resampling or converting in software would hide a configuration finding
//!   behind audio that sounds nearly right.

use std::fmt;
use std::io;
use std::path::Path;

use alsa::PCM;
use alsa::pcm::{Access, Format, HwParams};
use alsa::{Direction, ValueOr};

mod capture;
mod clock;

pub use capture::{ByteCapture, Period, Read, Recovery, Wait};
pub use clock::{ClockPair, clock_pair, monotonic_us, realtime_us};

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
    pub fn matches(&self, needle: &str) -> bool {
        let needle = needle.to_lowercase();
        [&self.name, &self.long_name]
            .iter()
            .any(|field| field.to_lowercase().contains(&needle))
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

/// Why [`select_one`] found no single card. Both carry every card the kernel
/// presented, so the refusal says what was there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectError {
    /// No card's name or long name contains the needle.
    NoMatch {
        needle: String,
        cards: Vec<CardInfo>,
    },
    /// More than one card matched; `matched` holds their indices.
    Ambiguous {
        needle: String,
        cards: Vec<CardInfo>,
        matched: Vec<i32>,
    },
}

impl SelectError {
    /// Every card the kernel presented when the selection was made.
    pub fn cards(&self) -> &[CardInfo] {
        match self {
            Self::NoMatch { cards, .. } | Self::Ambiguous { cards, .. } => cards,
        }
    }
}

/// The cards the kernel presented, as a refusal lists them: each card's
/// `Display` joined with `"; "`, or `no cards at all`.
pub fn list_cards(cards: &[CardInfo]) -> String {
    if cards.is_empty() {
        return "no cards at all".to_string();
    }
    cards
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

impl fmt::Display for SelectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMatch { needle, cards } => write!(
                f,
                "no sound card's name contains {needle:?}; the kernel is presenting: {}",
                list_cards(cards)
            ),
            Self::Ambiguous {
                needle,
                cards,
                matched,
            } => write!(
                f,
                "{} sound cards' names contain {needle:?} (cards {matched:?}); name one of \
                 them by more of its long name, such as its USB port; the kernel is \
                 presenting: {}",
                matched.len(),
                list_cards(cards)
            ),
        }
    }
}

impl std::error::Error for SelectError {}

/// The one card whose name or long name contains `needle`, case-insensitively.
///
/// No match and more than one match are both refusals: a capture of the wrong one of
/// two identical boards is indistinguishable from a capture of the right one. A long
/// name ends in the USB port path, so two identical boards are told apart by naming
/// the port.
pub fn select_one<'a>(cards: &'a [CardInfo], needle: &str) -> Result<&'a CardInfo, SelectError> {
    let matched: Vec<&CardInfo> = cards.iter().filter(|c| c.matches(needle)).collect();
    match matched.as_slice() {
        [one] => Ok(one),
        [] => Err(SelectError::NoMatch {
            needle: needle.to_string(),
            cards: cards.to_vec(),
        }),
        several => Err(SelectError::Ambiguous {
            needle: needle.to_string(),
            cards: cards.to_vec(),
            matched: several.iter().map(|c| c.index).collect(),
        }),
    }
}

/// The USB vendor and product id the kernel reports for a card, as it prints them
/// (`2886:0022`): the contents of `<proc_asound>/card<N>/usbid`. `None` when the
/// kernel has no such file for the card, which is how a card that is not on USB
/// reads. `proc_asound` is `/proc/asound` outside tests.
pub fn usb_id(card_index: i32, proc_asound: &Path) -> io::Result<Option<String>> {
    let path = proc_asound.join(format!("card{card_index}")).join("usbid");
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok(Some(text.trim_end_matches('\n').to_string())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Why a stream on a device could not be brought up, or could not continue.
///
/// Shared by both directions: finding the card, opening a node and demanding
/// parameters fail the same way whichever way the audio is going.
#[derive(Debug)]
pub enum PcmError {
    /// The card list could not be read.
    Enumerate(alsa::Error),
    /// No card answers to the names asked for.
    NoCard(String),
    /// The device node would not open — usually the `audio` group, or another
    /// opener holding a `hw:` device.
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
/// Every stream holds to one recovery policy — the same ceiling, the same counters,
/// the same warn line — so it is decided here once. What to do after a recovery is
/// the caller's.
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

/// Open one direction of a device at exactly `params`.
///
/// Every setter is an exact one — no `_near` variant anywhere. A near-setter is how
/// a device that cannot do 16 kHz ends up opened at 48 kHz with nothing said, and
/// the whole point of this path is that a device which cannot do what we need says
/// so here, loudly, before any audio is judged.
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

/// What an open stream was granted, read back from the device rather than echoed
/// from the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Granted {
    pub rate_hz: u32,
    pub channels: u32,
    pub format: Format,
    pub period_frames: u32,
    pub buffer_frames: u32,
}

/// Read back the hardware configuration an open PCM is running with.
pub fn granted(pcm: &PCM) -> Result<Granted, alsa::Error> {
    let hwp = pcm.hw_params_current()?;
    let frames = |n: alsa::pcm::Frames, what: &'static str| {
        u32::try_from(n).map_err(|_| alsa::Error::new(what, libc::ERANGE))
    };
    Ok(Granted {
        rate_hz: hwp.get_rate()?,
        channels: hwp.get_channels()?,
        format: hwp.get_format()?,
        period_frames: frames(hwp.get_period_size()?, "snd_pcm_hw_params_get_period_size")?,
        buffer_frames: frames(hwp.get_buffer_size()?, "snd_pcm_hw_params_get_buffer_size")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARAMS: PcmParams = PcmParams {
        rate_hz: 16_000,
        channels: 2,
        format: Format::S16LE,
        period_frames: 320,
        periods: 4,
    };

    fn card(index: i32, name: &str, long_name: &str) -> CardInfo {
        CardInfo {
            index,
            name: name.to_string(),
            long_name: long_name.to_string(),
        }
    }

    /// The Flex as this workstation's kernel names it, at the port it is on.
    fn flex(index: i32, port: &str) -> CardInfo {
        card(
            index,
            "reSpeaker Flex XVF3800 L16K6Ch",
            &format!("Seeed Studio reSpeaker Flex XVF3800 L16K6Ch at usb-{port}, high speed"),
        )
    }

    #[test]
    fn a_mixed_case_needle_matches_a_mixed_case_name() {
        let c = card(1, "Reachy Mini Audio", "Seeed Reachy Mini Audio at usb-1");
        assert!(c.matches("Reachy MINI audio"));
        assert!(c.matches("seeed reachy"), "the long name is searched too");
        assert!(!c.matches("Flex"));
    }

    #[test]
    fn select_one_picks_the_single_match_at_whatever_index_it_landed_on() {
        let cards = vec![
            card(0, "HDA Intel PCH", "HDA Intel PCH at 0xfe, irq 1"),
            flex(2, "0000:c4:00.0-2.2"),
        ];
        let found = select_one(&cards, "Flex XVF3800 L16K6Ch").expect("one match");
        assert_eq!(found.index, 2);
        assert_eq!(found.hw_device(), "hw:2,0");
    }

    #[test]
    fn select_one_with_no_match_lists_every_card() {
        let cards = vec![
            card(0, "HDA Intel PCH", "HDA Intel PCH at 0xfe, irq 1"),
            card(1, "USB Audio", "Generic USB Audio at usb-3"),
        ];
        let err = select_one(&cards, "Flex").unwrap_err();
        assert!(matches!(err, SelectError::NoMatch { .. }), "{err:?}");
        assert_eq!(err.cards(), cards.as_slice());
        let text = err.to_string();
        for c in &cards {
            assert!(text.contains(&c.to_string()), "{text}");
        }
        let none = select_one(&[], "Flex").unwrap_err();
        assert!(none.to_string().contains("no cards at all"), "{none}");
    }

    #[test]
    fn select_one_with_several_matches_names_them_and_lists_every_card() {
        let cards = vec![
            flex(1, "0000:c4:00.0-2.2"),
            card(2, "HDA Intel PCH", "HDA Intel PCH at 0xfe, irq 1"),
            flex(3, "0000:c4:00.0-2.3"),
        ];
        let err = select_one(&cards, "flex xvf3800").unwrap_err();
        match &err {
            SelectError::Ambiguous { matched, .. } => assert_eq!(matched, &vec![1, 3]),
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        let text = err.to_string();
        assert!(text.contains("[1, 3]"), "{text}");
        for c in &cards {
            assert!(text.contains(&c.to_string()), "{text}");
        }
    }

    #[test]
    fn a_port_path_tells_two_identical_boards_apart() {
        let cards = vec![flex(1, "0000:c4:00.0-2.2"), flex(3, "0000:c4:00.0-2.3")];
        let found = select_one(&cards, "usb-0000:c4:00.0-2.3").expect("one match");
        assert_eq!(found.index, 3);
    }

    #[test]
    fn usb_id_reads_the_kernels_file_or_says_there_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("card2")).expect("card dir");
        std::fs::write(dir.path().join("card2/usbid"), "2886:0022\n").expect("usbid");
        std::fs::create_dir(dir.path().join("card0")).expect("card dir");
        assert_eq!(
            usb_id(2, dir.path()).expect("read"),
            Some("2886:0022".to_string())
        );
        assert_eq!(usb_id(0, dir.path()).expect("read"), None, "no usbid file");
        assert_eq!(usb_id(7, dir.path()).expect("read"), None, "no card dir");
    }

    #[test]
    fn the_wanted_parameters_render_as_the_thing_that_was_asked_for() {
        assert_eq!(
            PARAMS.to_string(),
            "16000 Hz S16_LE 2 ch, period 320 frames × 4"
        );
    }

    #[test]
    fn a_parameter_refusal_prints_both_what_was_wanted_and_what_is_offered() {
        let err = PcmError::Params {
            device: "hw:1,0".into(),
            wanted: PARAMS,
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
        // Whether a board gives the format asked for is the open hardware question,
        // and this dump is the mechanism that answers it. So the query path itself is exercised, against ALSA's own
        // `null` device: no hardware, but a real `HwParams` to interrogate. Only the
        // board's actual answer is left for the bench.
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
    fn what_was_granted_is_read_back_from_the_open_stream() {
        let pcm = open_pcm("null", Direction::Capture, &PARAMS).expect("null device");
        assert_eq!(
            granted(&pcm).expect("granted"),
            Granted {
                rate_hz: 16_000,
                channels: 2,
                format: Format::S16LE,
                period_frames: 320,
                buffer_frames: 1280,
            }
        );
    }

    // ── RecoveryBudget ────────────────────────────────────────────────────────
    //
    // One policy for every stream, so these are the assertions that hold for
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
