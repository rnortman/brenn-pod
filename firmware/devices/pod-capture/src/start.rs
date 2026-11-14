//! Start-up: from the command line to a running capture, in the one order that
//! never leaves a half-made log behind a refusal it could have made first.
//!
//! 1. Parse the arguments (exit 2 with usage on stderr if not understood).
//! 2. Refuse if `--out` exists.
//! 3. Refuse a layout the stream cannot state: more than 255 channels, a frame
//!    no audio frame holds, or names too long for a `Describe` frame.
//! 4. Enumerate the cards and select exactly one.
//! 5. Open its first PCM for capture at exactly the demanded parameters.
//! 6. Read back what was granted and the card's USB id.
//! 7. Read the state of the card's device, with the PCM held open
//!    ([`chip_state`](crate::chip_state)); every outcome is recorded and none
//!    refuses the capture.
//! 8. Create the destination with `create_new`.
//! 9. Hand over to [`run`](crate::run), which triggers the stream, takes the
//!    start pair, writes the header, hello and `Describe` and emits
//!    `capture_started`.
//!
//! Each refusal before the capture runs is one `capture_failed` line and exit 1.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use alsa::Direction;
use alsa::pcm::Format;
use alsa_pcm::{ByteCapture, CardInfo, PcmError, PcmParams, SelectError};
use audio_pipeline::wide::{CaptureFrame, MAX_WIDE_FRAME_BYTES, WideFrame, encode_wide};
use frame_log::{BuildSource, Device, DeviceControl, DeviceState, WriterBuild, WriterInfo};

use crate::capture::{Recording, STOP_BOUND, failed, period_frames, run};
use crate::chip_state::{self, DeviceReading};
use crate::cli::{self, Args};
use crate::granted::{Granted, SampleFormat};
use crate::lines::{CardLine, FailReason, Lines, note};
use crate::seams::SystemClock;

/// Periods the device's buffer is demanded to hold.
pub const BUFFER_PERIODS: u32 = 16;

/// Where the kernel publishes each card's USB id.
const PROC_ASOUND: &str = "/proc/asound";

/// Where the kernel publishes each card's device.
const SYS: &str = "/sys";

/// Set by `SIGINT` and `SIGTERM`; the capture loop reads it every turn.
static STOP: AtomicBool = AtomicBool::new(false);

/// The whole program: `args` without the program name, event lines on `out`.
/// Returns the exit status.
pub fn main_with<W: Write>(args: &[OsString], out: W) -> u8 {
    let mut lines = Lines::new(out);
    let args = match cli::parse(args) {
        Ok(args) => args,
        Err(why) => {
            note(format_args!("{why}\n{}", cli::USAGE));
            return 2;
        }
    };
    install_stop_handler();
    match start(&args, &mut lines) {
        Ok(code) => code,
        Err(refusal) => {
            failed(
                &mut lines,
                None,
                refusal.reason,
                &refusal.detail,
                None,
                alsa_pcm::clock_pair(),
                refusal.cards,
            );
            1
        }
    }
}

/// Why start-up refused, before any log exists.
struct Refusal {
    reason: FailReason,
    detail: String,
    cards: Option<Vec<CardLine>>,
}

impl Refusal {
    fn new(reason: FailReason, detail: impl Into<String>) -> Self {
        Refusal {
            reason,
            detail: detail.into(),
            cards: None,
        }
    }
}

fn start<W: Write>(args: &Args, lines: &mut Lines<W>) -> Result<u8, Refusal> {
    let out = args.out.display().to_string();
    // `symlink_metadata`, so a dangling link counts as there: `create_new` would
    // refuse it too, after the device had been opened for nothing.
    if args.out.symlink_metadata().is_ok() {
        return Err(Refusal::new(
            FailReason::DestinationExists,
            format!("{out} already exists; a capture never writes over a file"),
        ));
    }

    let channels = layout_channels(&args.layout)?;
    let frame_bytes = args.layout.len() * args.format.bytes_per_sample();
    let period = period_frames(args.rate_hz, frame_bytes).ok_or_else(|| {
        Refusal::new(
            FailReason::Params,
            format!(
                "one frame of {channels} channels of {} is {frame_bytes} bytes, more than one \
                 audio frame holds",
                args.format.name()
            ),
        )
    })?;

    let cards = alsa_pcm::enumerate_cards()
        .map_err(|e| Refusal::new(FailReason::Open, PcmError::Enumerate(e).to_string()))?;
    let card = alsa_pcm::select_one(&cards, &args.card).map_err(|e| {
        let reason = match e {
            SelectError::NoMatch { .. } => FailReason::NoCard,
            SelectError::Ambiguous { .. } => FailReason::AmbiguousCard,
        };
        Refusal {
            reason,
            detail: e.to_string(),
            cards: Some(e.cards().iter().map(card_line).collect()),
        }
    })?;

    let wanted = PcmParams {
        rate_hz: args.rate_hz,
        channels,
        format: alsa_format(args.format),
        period_frames: i64::from(period),
        periods: BUFFER_PERIODS,
    };
    let hw = card.hw_device();
    let pcm = alsa_pcm::open_pcm(&hw, Direction::Capture, &wanted).map_err(|e| {
        let reason = match e {
            PcmError::Params { .. } => FailReason::Params,
            _ => FailReason::Open,
        };
        Refusal::new(reason, e.to_string())
    })?;

    let granted = granted(&pcm, &hw, &wanted)?;
    let usb_id = alsa_pcm::usb_id(card.index, Path::new(PROC_ASOUND)).map_err(|e| {
        Refusal::new(
            FailReason::Open,
            format!("reading card {}'s USB id failed: {e}", card.index),
        )
    })?;
    // TODO(pod-capture-reachy-pod-one-capture-path): the capture's device and its
    // chip reading are recorded here, by this program only; reachy-pod captures and
    // reads the same chip by its own code and records neither.
    // TODO(usb-image-capture-channel-register-map-unknown): the routing registers
    // are recorded by name; which of them decides each of the six capture
    // channels on the USB image is not established.
    let DeviceReading { usb, state } = chip_state::read_device(
        card.index,
        Path::new(SYS),
        |device| xvf3800_usb::open_reader(device, Path::new(xvf3800_usb::DEV_BUS_USB)),
        &SystemClock,
    );
    if let DeviceState::Unavailable { detail, .. } = &state {
        note(format_args!(
            "the device's state could not be read: {detail}; the capture goes on and its \
             record says so"
        ));
    }
    let mut source =
        ByteCapture::new(&pcm).map_err(|e| Refusal::new(FailReason::Params, e.to_string()))?;

    let rec = Recording {
        record_id: record_id(),
        pod_id: args.pod_id.clone(),
        writer: WriterInfo {
            program: "pod-capture".to_owned(),
            build: writer_build(),
        },
        device: Device {
            selector: args.card.clone(),
            card_index: u32::try_from(card.index).map_err(|_| {
                Refusal::new(
                    FailReason::Open,
                    format!("card index {} is negative", card.index),
                )
            })?,
            card_name: card.name.clone(),
            card_long_name: card.long_name.clone(),
            pcm: hw,
            usb_id,
            usb,
        },
        granted,
        layout: args.layout.clone(),
        device_control: DeviceControl { performed: false },
        device_state: state,
    };

    let dest = File::create_new(&args.out).map_err(|e| {
        if e.kind() == io::ErrorKind::AlreadyExists {
            Refusal::new(
                FailReason::DestinationExists,
                format!("{out} appeared after it was checked: {e}"),
            )
        } else {
            Refusal::new(
                FailReason::Destination,
                format!("creating {out} failed: {e}"),
            )
        }
    })?;

    let outcome = run(
        &mut source,
        SystemClock,
        dest,
        rec,
        &out,
        &STOP,
        lines,
        STOP_BOUND,
    );
    Ok(outcome.exit_code())
}

/// The channel count of `layout`, refused when a hello cannot state it or the
/// names do not fit one `Describe` frame.
fn layout_channels(layout: &[String]) -> Result<u32, Refusal> {
    if layout.len() > usize::from(u8::MAX) {
        return Err(Refusal::new(
            FailReason::Params,
            format!(
                "--layout names {} channels; a hello states at most {}",
                layout.len(),
                u8::MAX
            ),
        ));
    }
    // The largest values the other fields can take, so a layout that fits
    // here fits whatever the device grants and whenever the stream starts.
    let describe = WideFrame::Capture(CaptureFrame::Describe {
        schema: u8::MAX,
        channel_names: layout.to_vec(),
        period_frames: u32::MAX,
        mono_us: u64::MAX,
    });
    let mut buf = vec![0; MAX_WIDE_FRAME_BYTES + 2];
    encode_wide(&describe, &mut buf).map_err(|e| {
        Refusal::new(
            FailReason::Params,
            format!(
                "--layout's names do not fit one Describe frame of {MAX_WIDE_FRAME_BYTES} \
                 bytes ({e:?})"
            ),
        )
    })?;
    Ok(layout.len() as u32)
}

/// What the device granted, refused unless it is what was demanded and a period
/// fits in one audio frame.
fn granted(pcm: &alsa::PCM, hw: &str, wanted: &PcmParams) -> Result<Granted, Refusal> {
    let g = alsa_pcm::granted(pcm).map_err(|e| {
        Refusal::new(
            FailReason::Params,
            format!("reading back what {hw} granted failed: {e}"),
        )
    })?;
    let refused = |what: String| {
        Refusal::new(
            FailReason::Params,
            format!(
                "{hw} granted {what} where {wanted} was demanded\n  it advertises: {}",
                alsa_pcm::describe_supported(pcm)
            ),
        )
    };
    let sample_format = match g.format {
        Format::S16LE => SampleFormat::S16Le,
        Format::S32LE => SampleFormat::S32Le,
        other => return Err(refused(format!("format {other}"))),
    };
    if g.rate_hz != wanted.rate_hz || g.channels != wanted.channels || g.format != wanted.format {
        return Err(refused(format!(
            "{} Hz {} {} ch",
            g.rate_hz, g.format, g.channels
        )));
    }
    let granted = Granted {
        rate_hz: g.rate_hz,
        channels: g.channels,
        sample_format,
        period_frames: g.period_frames,
        buffer_frames: g.buffer_frames,
    };
    if g.period_frames == 0 || g.period_frames as usize > granted.max_block_frames() {
        return Err(refused(format!(
            "a period of {} frames, which one audio frame cannot hold",
            g.period_frames
        )));
    }
    Ok(granted)
}

fn alsa_format(format: SampleFormat) -> Format {
    match format {
        SampleFormat::S16Le => Format::S16LE,
        SampleFormat::S32Le => Format::S32LE,
    }
}

fn card_line(c: &CardInfo) -> CardLine {
    CardLine {
        index: c.index,
        name: c.name.clone(),
        long_name: c.long_name.clone(),
    }
}

/// This binary's build id as the log's header states it.
fn writer_build() -> WriterBuild {
    let id = build_id::build_id();
    match build_id::source() {
        build_id::Source::Git => WriterBuild {
            commit: id.commit.to_string(),
            dirty: Some(id.dirty),
            source: BuildSource::Git,
        },
        build_id::Source::Declared => WriterBuild {
            commit: id.commit.to_string(),
            dirty: None,
            source: BuildSource::Declared,
        },
    }
}

/// 128 bits from the kernel's random source, as 32 lowercase hex digits.
fn record_id() -> String {
    let mut bits = [0u8; 16];
    getrandom::fill(&mut bits).expect("the kernel's random source failed");
    bits.iter().map(|b| format!("{b:02x}")).collect()
}

extern "C" fn on_stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

/// `SIGINT` and `SIGTERM` set [`STOP`]. Installed without `SA_RESTART`, so a
/// wait the signal interrupts returns and the loop sees the flag within one
/// turn. `sigaction` refuses only an invalid signal or pointer, neither of which
/// this passes, so a refusal is a defect here and panics.
fn install_stop_handler() {
    for signal in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: a zeroed sigaction is a valid starting value; the handler only
        // stores to an atomic, which is async-signal-safe; both pointers are
        // valid for the call.
        let rc = unsafe {
            let mut act: libc::sigaction = std::mem::zeroed();
            act.sa_sigaction = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
            act.sa_flags = 0;
            libc::sigemptyset(&mut act.sa_mask);
            libc::sigaction(signal, &act, std::ptr::null_mut())
        };
        assert!(
            rc == 0,
            "sigaction({signal}) failed: {}",
            io::Error::last_os_error()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize, len: usize) -> Vec<String> {
        (0..n).map(|i| format!("{i:0len$}")).collect()
    }

    #[test]
    fn a_layout_is_at_most_255_channels() {
        assert_eq!(layout_channels(&names(255, 3)).ok(), Some(255));
        let refused = layout_channels(&names(256, 3)).expect_err("refused");
        assert_eq!(refused.reason, FailReason::Params);
        assert!(refused.detail.contains("256"), "{}", refused.detail);
    }

    #[test]
    fn a_layout_whose_names_overflow_a_describe_frame_is_refused() {
        // 255 names of 256 bytes is 65280 bytes of names, plus their length
        // prefixes and the other fields: past the limit.
        let refused = layout_channels(&names(255, 256)).expect_err("refused");
        assert_eq!(refused.reason, FailReason::Params);
        assert!(refused.detail.contains("Describe"), "{}", refused.detail);
        assert_eq!(layout_channels(&names(255, 250)).ok(), Some(255));
    }

    #[test]
    fn a_record_id_is_32_lowercase_hex_digits_and_not_repeated() {
        let a = record_id();
        let b = record_id();
        assert_eq!(a.len(), 32);
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_ne!(a, b);
    }

    #[test]
    fn the_build_is_stated_with_its_source() {
        let b = writer_build();
        assert_eq!(b.commit.len(), 40);
        match b.source {
            BuildSource::Git => assert!(b.dirty.is_some()),
            BuildSource::Declared => assert!(b.dirty.is_none()),
        }
    }

    #[test]
    fn a_card_line_carries_the_kernel_names() {
        let line = card_line(&CardInfo {
            index: 2,
            name: "reSpeaker Flex XVF3800 L16K6Ch".to_owned(),
            long_name: "Seeed Studio reSpeaker Flex XVF3800 L16K6Ch at usb-x".to_owned(),
        });
        assert_eq!(line.index, 2);
        assert_eq!(line.name, "reSpeaker Flex XVF3800 L16K6Ch");
    }

    #[test]
    fn the_null_device_grants_what_was_demanded() {
        let wanted = PcmParams {
            rate_hz: 16_000,
            channels: 6,
            format: Format::S16LE,
            period_frames: 320,
            periods: BUFFER_PERIODS,
        };
        let pcm = alsa_pcm::open_pcm("null", Direction::Capture, &wanted)
            .expect("ALSA's built-in null device is part of libasound2-data");
        let g = granted(&pcm, "null", &wanted).ok().expect("granted");
        assert_eq!(g.rate_hz, 16_000);
        assert_eq!(g.channels, 6);
        assert_eq!(g.sample_format, SampleFormat::S16Le);
        assert_eq!(g.period_frames, 320);
        assert_eq!(g.buffer_frames, 320 * BUFFER_PERIODS);
    }
}
