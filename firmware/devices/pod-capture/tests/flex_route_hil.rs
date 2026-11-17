//! Hardware: the output routing of the reSpeaker Flex's XVF3800 (USB id
//! `2886:0022`, Seeed's six-channel USB audio image), read, written through
//! `xvf-route`, and observed in the recorder's capture.
//!
//! These tests WRITE TO THE CHIP. Every write is `AUDIO_MGR_OP_ALL` alone, is
//! volatile, is read back, and is restored at the end of the test; a test that
//! writes refuses to start unless the board reads the bench's routing, so a
//! board left routed by a killed run is never taken as the state to restore.
//! `the_reference_category_carries_what_the_device_plays` PLAYS band-limited
//! noise at −20 dBFS RMS through the device, five plays of two seconds; the
//! mixer is not touched. Not in `make check` or CI; run by
//! `make -C firmware route-hil` with the Flex attached and its capture and
//! playback devices free.
//!
//! `the_routed_reference_holds_still_against_the_microphones` and
//! `the_other_far_end_taps_are_measured_against_the_reference` play the same
//! noise at the same level in twenty plays of twenty seconds per routing
//! measured, each play from its own seed, about seven
//! minutes each, and are run by `make -C firmware route-measure-hil`. Each
//! writes its log and a JSON report under the cargo target's scratch
//! directory, `route-measure/`.
//!
//! `a_kept_measuring_run_reads_again_as_it_was_reported` reads one such run's
//! log through the same analysis and checks it against the run's report. It
//! touches no device, and is run by `make -C firmware route-reread`.

mod route_analysis;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alsa::Direction;
use alsa::pcm::{Format, PCM};
use alsa_pcm::{PcmParams, enumerate_cards, open_pcm, select_one};
use frame_log::{Step, summarize, walk};
use route_analysis::{
    COARSE, Class, HOP, PLACE_SPAN, RATE_HZ, Step as Change, WINDOW, classify, coarse_offset,
    correlogram, db, figure, median, noise, place_whole, power, steps, track, widen, zero_runs,
};
use xvf_route::{
    ChipWriter, OpAll, Routed, bytes, flex_device, open_writer, read_op_all, read_slot, set_command,
};
use xvf3800_ctrl::OP_ALL_SLOTS;
use xvf3800_usb::{DEV_BUS_USB, open_reader};

const BIN: &str = env!("CARGO_BIN_EXE_pod-capture");
const CARD: &str = "Flex XVF3800 L16K6Ch";
const CHANNELS: usize = 6;

/// The bench board's `AUDIO_MGR_OP_ALL` as found: (8, 0) the conference beam,
/// (1, 0) raw microphone 0, (1, 2) microphone 2, (7, 3) the ASR beam, (1, 1)
/// microphone 1, (1, 3) microphone 3.
const BASELINE: OpAll = [8, 0, 1, 0, 1, 2, 7, 3, 1, 1, 1, 3];

/// The capture channel each slot of [`OP_ALL_SLOTS`] feeds, by the hypothesis
/// that the six channels take the slots alternately left and right:
/// `L_PK0`→0, `L_PK1`→2, `L_PK2`→4, `R_PK0`→1, `R_PK1`→3, `R_PK2`→5.
const CHANNEL_OF_SLOT: [usize; 6] = [0, 2, 4, 1, 3, 5];

/// The capture's layout names. Neutral, since a routing changes what a channel
/// carries.
const LAYOUT: &str = "ch0,ch1,ch2,ch3,ch4,ch5";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The routing with `slot`'s pair replaced by `pair`.
fn with_slot(base: &OpAll, slot: usize, pair: [u8; 2]) -> OpAll {
    let mut op_all = *base;
    op_all[2 * slot..2 * slot + 2].copy_from_slice(&pair);
    op_all
}

/// Refuse to go on unless the board reads [`BASELINE`], naming the command that
/// puts it back.
fn require_baseline<T: xvf3800_ctrl::ControlRead>(transport: &mut T)
where
    T::Error: std::fmt::Display,
{
    let found = read_op_all(transport).unwrap_or_else(|e| panic!("{e}"));
    println!("AUDIO_MGR_OP_ALL before any write: {}", bytes(&found));
    assert_eq!(
        found,
        BASELINE,
        "the board does not hold the bench routing {}; an earlier run may have left it routed. \
         Put it back with: {}",
        bytes(&BASELINE),
        set_command(CARD, &BASELINE)
    );
}

/// Record `seconds` of every channel with the recorder, silent.
fn capture(routing: &OpAll, seconds: u64) -> [Vec<i16>; CHANNELS] {
    capture_while(routing, || thread::sleep(Duration::from_secs(seconds)))
}

/// Record every channel with the recorder while the board holds `routing`, from
/// its `capture_started` line until `during` returns, and return each channel's
/// samples. Asserts that the capture's `capture_started` line and its log's
/// header both state `routing` as `audio_mgr_op_all`, and that the log holds
/// no loss and no break, so that its audio frames join into one unbroken run.
fn capture_while(routing: &OpAll, during: impl FnOnce()) -> [Vec<i16>; CHANNELS] {
    let dir = tempfile::tempdir().unwrap();
    capture_to(&dir.path().join("route.framelog"), routing, during).0
}

/// A running recorder. Dropped before it has been reaped, as when the test
/// panics while it records, it is sent SIGINT, then SIGKILL if it has not
/// exited within five seconds, and reaped, so that it neither holds the capture
/// device nor goes on writing its log after the test.
struct Recorder(Child);

impl Drop for Recorder {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(None)) {
            return;
        }
        if let Ok(pid) = libc::pid_t::try_from(self.0.id()) {
            // SAFETY: kill(2) on a child this test spawned and has not reaped.
            unsafe { libc::kill(pid, libc::SIGINT) };
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(self.0.try_wait(), Ok(None)) {
            if Instant::now() >= deadline {
                let _ = self.0.kill();
                let _ = self.0.wait();
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

/// [`capture_while`], writing the log to `path`, and returning with the samples
/// the chip state the `capture_started` line reports.
fn capture_to(
    path: &Path,
    routing: &OpAll,
    during: impl FnOnce(),
) -> ([Vec<i16>; CHANNELS], serde_json::Value) {
    // TODO(reuse-hil-recorder-harness-copied-again): flex_hil.rs and
    // flex_chip_hil.rs start, stop and read the recorder by copies of their own.
    let mut recorder = Recorder(
        Command::new(BIN)
            .args(["--card", CARD, "--rate", "16000", "--format", "s16_le"])
            .args(["--layout", LAYOUT, "--pod-id", "route-hil", "--out"])
            .arg(path)
            .stdout(Stdio::piped())
            .spawn()
            .expect("the pod-capture binary runs"),
    );
    let child = &mut recorder.0;
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut first = String::new();
    stdout.read_line(&mut first).unwrap();
    let started: serde_json::Value = serde_json::from_str(first.trim_end())
        .unwrap_or_else(|e| panic!("first line is not JSON ({e}): {first:?}"));
    if started["event"] != "capture_started" {
        let _ = child.wait();
        panic!("the capture did not start:\n{first}");
    }
    let began = Instant::now();
    during();
    let elapsed = began.elapsed();
    let pid = libc::pid_t::try_from(child.id()).unwrap();
    // SAFETY: kill(2) on a child this test spawned and has not yet reaped.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0, "kill({pid})");
    for line in stdout.lines() {
        let line = line.unwrap();
        if !line.contains("\"capture_stopped\"") {
            println!("{line}");
        }
    }
    assert_eq!(child.wait().unwrap().code(), Some(0), "pod-capture's exit");

    let recorded = &started["device_state"]["registers"]["audio_mgr_op_all"]["bytes"];
    assert_eq!(
        recorded.as_str(),
        Some(hex(routing).as_str()),
        "capture_started's audio_mgr_op_all against the routing written: {}",
        started["device_state"]
    );
    let (channels, header) = read_log(path);
    assert_eq!(
        header["capture"]["device_state"]["registers"]["audio_mgr_op_all"]["bytes"].as_str(),
        Some(hex(routing).as_str()),
        "the header's audio_mgr_op_all against the routing written"
    );
    assert!(
        channels[0].len() as f64 >= elapsed.as_secs_f64() * f64::from(RATE_HZ) / 2.0,
        "{} frames in a capture of {elapsed:?}",
        channels[0].len()
    );
    (channels, started["device_state"].clone())
}

/// The recorder's log at `path`, each channel's samples and the log's header.
/// Refuses a log with a loss or a break, so that its audio frames join into
/// one unbroken run.
fn read_log(path: &Path) -> ([Vec<i16>; CHANNELS], serde_json::Value) {
    // TODO(reuse-hil-recorder-harness-copied-again): both callers check the
    // header's audio_mgr_op_all against their routing, each spelling its path.
    let summary = summarize(path).expect("the log reads back");
    assert!(
        summary.discontinuities.is_empty(),
        "the log is not contiguous, so its audio cannot be read as one run of frames: {:#?}",
        summary.discontinuities
    );
    let mut channels: [Vec<i16>; CHANNELS] = Default::default();
    for step in walk(path).expect("the log opens") {
        if let Step::Audio(a) = step.expect("the log reads") {
            for frame in a.pcm.chunks_exact(2 * CHANNELS) {
                for (ch, sample) in frame.chunks_exact(2).enumerate() {
                    channels[ch].push(i16::from_le_bytes([sample[0], sample[1]]));
                }
            }
        }
    }
    (channels, summary.header)
}

/// The index in [`OP_ALL_SLOTS`] of the slot [`CHANNEL_OF_SLOT`] gives `channel`.
fn slot_of_channel(channel: usize) -> usize {
    CHANNEL_OF_SLOT
        .iter()
        .position(|&ch| ch == channel)
        .unwrap()
}

/// Play `samples` on both playback channels of the Flex, at 16 kHz, until the
/// device has played them all. Any error is a failure: a write that has to be
/// recovered from is a gap in what the analysis takes as played.
fn play(samples: &[i16]) {
    let cards = enumerate_cards().expect("the card list reads");
    let card = select_one(&cards, CARD).unwrap_or_else(|e| panic!("{e}"));
    let params = PcmParams {
        rate_hz: RATE_HZ,
        channels: 2,
        format: Format::S16LE,
        period_frames: 320,
        periods: 4,
    };
    let pcm: PCM =
        open_pcm(&card.hw_device(), Direction::Playback, &params).unwrap_or_else(|e| panic!("{e}"));
    let stereo: Vec<i16> = samples.iter().flat_map(|&s| [s, s]).collect();
    let io = pcm.io_i16().expect("an i16 writer on the playback device");
    let mut at = 0;
    while at < stereo.len() {
        let frames = io
            .writei(&stereo[at..])
            .unwrap_or_else(|e| panic!("playback write at frame {}: {e}", at / 2));
        at += 2 * frames;
    }
    pcm.drain().expect("the playback drains");
}

/// The channels whose every sample is zero.
fn all_zero(channels: &[Vec<i16>; CHANNELS]) -> Vec<usize> {
    (0..CHANNELS)
        .filter(|&ch| channels[ch].iter().all(|&s| s == 0))
        .collect()
}

fn describe(channels: &[Vec<i16>; CHANNELS]) {
    for (ch, samples) in channels.iter().enumerate() {
        let zeros = samples.iter().filter(|&&s| s == 0).count();
        let peak = samples.iter().map(|s| s.unsigned_abs()).max().unwrap_or(0);
        println!(
            "  channel {ch}: {} frames, {zeros} zero, peak {peak}",
            samples.len()
        );
    }
}

/// Each `PK` register reads the pair at its slot of `AUDIO_MGR_OP_ALL`. Reads
/// only.
#[test]
#[ignore = "needs the reSpeaker Flex attached; run by `make -C firmware route-hil`"]
fn op_all_is_the_six_pk_registers_in_order() {
    let device = flex_device(CARD).unwrap_or_else(|e| panic!("{e}"));
    println!(
        "{device}, bcdDevice {:04x}, serial {:?}",
        device.bcd_device, device.serial
    );
    let mut reader = open_reader(&device, Path::new(DEV_BUS_USB)).unwrap_or_else(|e| panic!("{e}"));
    let op_all = read_op_all(&mut reader).unwrap_or_else(|e| panic!("{e}"));
    println!("AUDIO_MGR_OP_ALL: {}", bytes(&op_all));
    let mut wrong = Vec::new();
    for (i, slot) in OP_ALL_SLOTS.iter().enumerate() {
        let pair = read_slot(&mut reader, slot).unwrap_or_else(|e| panic!("{e}"));
        let at_slot = &op_all[2 * i..2 * i + 2];
        println!(
            "{}: {}; AUDIO_MGR_OP_ALL bytes {}..{}: {}",
            slot.label,
            bytes(&pair),
            2 * i,
            2 * i + 1,
            bytes(at_slot)
        );
        if pair != at_slot {
            wrong.push(slot.name);
        }
    }
    assert!(
        wrong.is_empty(),
        "PK registers that do not read their slot of AUDIO_MGR_OP_ALL (printed above): {}",
        wrong.join(", ")
    );
}

/// For each slot of `slots` (indices into [`OP_ALL_SLOTS`]), write the bench
/// routing with that slot alone set to silence, (0, 0), capture two seconds,
/// and compare the all-zero channels with the one [`CHANNEL_OF_SLOT`] names.
/// A baseline capture first asserts that no channel is all zero. Prints every
/// capture's per-channel zero count and peak, and returns the slots whose
/// capture was not as named.
fn silence_each(slots: &[usize]) -> Vec<String> {
    let (mut writer, _) = open_flex();
    require_baseline(&mut writer);

    println!("baseline capture, routing {}", bytes(&BASELINE));
    let baseline = capture(&BASELINE, 2);
    describe(&baseline);
    assert_eq!(
        all_zero(&baseline),
        Vec::<usize>::new(),
        "channels all zero under the bench routing, before any write"
    );

    let mut wrong = Vec::new();
    for &i in slots {
        let slot = &OP_ALL_SLOTS[i];
        let routing = with_slot(&BASELINE, i, [0, 0]);
        let (routed, written) =
            Routed::apply(&mut writer, CARD, &routing).unwrap_or_else(|e| panic!("{e}"));
        println!(
            "{} silenced: routing {} (write error reported: {:?})",
            slot.name,
            bytes(&routing),
            written.write_error
        );
        let channels = capture(&routing, 2);
        let restored = routed.restore();
        assert!(
            restored.is_ok(),
            "restore after {}: {restored:?}",
            slot.name
        );
        describe(&channels);
        let zero = all_zero(&channels);
        println!(
            "  all-zero channels: {zero:?}; expected [{}]",
            CHANNEL_OF_SLOT[i]
        );
        if zero != [CHANNEL_OF_SLOT[i]] {
            wrong.push(format!(
                "{}: all-zero channels {zero:?}, expected [{}]",
                slot.name, CHANNEL_OF_SLOT[i]
            ));
        }
    }
    require_baseline(&mut writer);
    wrong
}

/// `L_PK0` decides capture channel 0 and `R_PK0` channel 1: with either slot
/// alone set to silence, exactly its channel is all zero. Every later test
/// routes through these two slots. Writes; silent.
#[test]
#[ignore = "writes the Flex's routing; run by `make -C firmware route-hil`"]
fn the_first_slot_of_each_side_feeds_its_capture_channel() {
    let wrong = silence_each(&[slot_of_channel(0), slot_of_channel(1)]);
    assert!(
        wrong.is_empty(),
        "slots that do not feed the channel the hypothesis names: {wrong:#?}"
    );
}

/// `L_PK1`, `R_PK1`, `L_PK2` and `R_PK2` decide capture channels 2, 3, 4 and 5,
/// as [`CHANNEL_OF_SLOT`] states the hypothesis: with each slot alone set to
/// silence, exactly its channel is all zero. Writes; silent.
///
/// On the bench (the six-channel image, `AUDIO_MGR_OP_PACKED` reading `00 00`)
/// the reading for each of the four is no all-zero channel, though each write
/// reads back as written. The assertion states the hypothesis.
#[test]
#[ignore = "writes the Flex's routing; run by `make -C firmware route-hil`"]
fn the_packed_slots_feed_capture_channels_2_to_5() {
    let wrong = silence_each(&[
        slot_of_channel(2),
        slot_of_channel(3),
        slot_of_channel(4),
        slot_of_channel(5),
    ]);
    // TODO(usb-image-capture-channel-register-map-unknown): fails on the bench,
    // where silencing any of these four slots zeroes no channel.
    assert!(
        wrong.is_empty(),
        "slots that do not feed the channel the hypothesis names: {wrong:#?}"
    );
}

/// The far-end categories tried as the chip's echo reference, as (category,
/// source): (12, 0) is the reference the chip's own canceller uses; the rest are
/// measured only.
const FAR_END: [[u8; 2]; 5] = [[12, 0], [5, 0], [4, 0], [10, 0], [10, 1]];

/// The capture channels that carry raw microphones while the bench routing
/// holds on `L_PK0`.
const MICROPHONES: [usize; 4] = [2, 3, 4, 5];

/// The capture channel a category is routed to: channel 1, through `R_PK0`.
const ROUTED: usize = 1;

/// The noise's seed.
const SEED: u64 = 0x0005_eed0_f1e5;

/// The silence before a play, from the capture's start, and after it, in seconds.
const LEAD_S: f64 = 1.0;
const TAIL_S: f64 = 1.0;

/// How a channel stands against the host's copy of a play.
struct Placed {
    offset: isize,
    figure: f64,
}

/// Place `played` in `channel`, whose capture began [`LEAD_S`] before the play,
/// and take its figure there.
fn place(played: &[f64], channel: &[i16]) -> Placed {
    let channel = widen(channel);
    let nominal = (LEAD_S * f64::from(RATE_HZ)) as isize;
    let offset = coarse_offset(played, &channel, nominal, COARSE);
    Placed {
        offset,
        figure: figure(played, &channel, offset),
    }
}

/// Record every channel while two seconds of noise play, between a second of
/// silence either side.
fn capture_playing(routing: &OpAll, samples: &[i16]) -> [Vec<i16>; CHANNELS] {
    capture_while(routing, || {
        thread::sleep(Duration::from_secs_f64(LEAD_S));
        play(samples);
        thread::sleep(Duration::from_secs_f64(TAIL_S));
    })
}

/// Route `pair` to capture channel 1 through `R_PK0`, record while two seconds
/// of noise play, restore, and class channel 1 against the played noise and
/// the raw microphones. Prints every channel's placement and figure.
fn carries(writer: &mut ChipWriter, pair: [u8; 2]) -> Class {
    let samples = noise(SEED, 2 * RATE_HZ as usize);
    let played = widen(&samples);
    let slot = slot_of_channel(ROUTED);
    let routing = with_slot(&BASELINE, slot, pair);
    let (routed, written) = Routed::apply(writer, CARD, &routing).unwrap_or_else(|e| panic!("{e}"));
    println!(
        "({}, {}) on {}: routing {} (write error reported: {:?})",
        pair[0],
        pair[1],
        OP_ALL_SLOTS[slot].name,
        bytes(&routing),
        written.write_error
    );
    let channels = capture_playing(&routing, &samples);
    let restored = routed.restore();
    assert!(restored.is_ok(), "restore after {pair:?}: {restored:?}");
    describe(&channels);

    let placed: Vec<Placed> = channels.iter().map(|c| place(&played, c)).collect();
    for (ch, p) in placed.iter().enumerate() {
        println!(
            "  channel {ch}: offset {} frames, figure {:.3}",
            p.offset, p.figure
        );
    }
    let reference = &placed[ROUTED];
    let microphones: Vec<f64> = MICROPHONES.iter().map(|&ch| placed[ch].figure).collect();
    let best = microphones.iter().copied().fold(0.0, f64::max);
    let class = classify(reference.figure, &microphones);
    println!(
        "  ({}, {}): figure {:.3}, best microphone {best:.3}, margin {:.3}: {class:?}",
        pair[0],
        pair[1],
        reference.figure,
        reference.figure - best
    );
    class
}

/// Routed to capture channel 1, the far-end category (12, 0) carries what the
/// device plays: its figure is at least 0.8 and above every raw microphone's.
/// (5, 0), (4, 0), (10, 0) and (10, 1) are routed the same way and measured.
/// Writes; plays two seconds of noise per category.
#[test]
#[ignore = "writes the Flex's routing and plays through it; run by `make -C firmware route-hil`"]
fn the_reference_category_carries_what_the_device_plays() {
    let (mut writer, _) = open_flex();
    require_baseline(&mut writer);

    let classes: Vec<([u8; 2], Class)> = FAR_END
        .iter()
        .map(|&pair| (pair, carries(&mut writer, pair)))
        .collect();
    require_baseline(&mut writer);

    println!("classes: {classes:?}");
    assert_eq!(
        classes[0],
        ([12, 0], Class::Carries),
        "(12, 0) against the played signal"
    );
}

/// The measurement's plays: twenty of twenty seconds in one capture, a second
/// of silence before each and after the last.
const PLAYS: usize = 20;
const PLAY_S: usize = 20;
const GAP_S: usize = 1;

/// A run of at least this many frames, zero on every channel but the
/// references, is reported with what it does to the references.
const ZERO_RUN: usize = 24;

/// The reference the chip's own canceller uses, as (category, source).
const CHIP_REFERENCE: [u8; 2] = [12, 0];

/// What one measuring run routes and treats as what.
struct Measurement {
    /// The run's name, which names its directory.
    name: String,
    routing: OpAll,
    /// Each reference's capture channel and (category, source).
    references: Vec<(usize, [u8; 2])>,
    /// The channels other than the references; a zero run is a run in which
    /// every one of them is zero.
    others: Vec<usize>,
    /// The conference beam's channel, where the routing keeps it.
    beam: Option<usize>,
    /// Pairs of indices into `references` whose delay to each other is tracked,
    /// the second behind the first.
    pairs: Vec<(usize, usize)>,
}

/// What a measuring run found: whether the host's copy stepped, what each
/// reference's windows show against the microphones, each pair's constant delay
/// (the median over the run's placed windows), and the chip's
/// `AUDIO_MGR_SYS_DELAY` as the capture read it.
struct Measured {
    control: bool,
    unsteady: Vec<Unsteady>,
    pair_delays: Vec<Option<f64>>,
    sys_delay: serde_json::Value,
}

/// The windows in which one reference's delay to a microphone was not shown to
/// hold within one frame of the run's median for that microphone.
struct Unsteady {
    reference: [u8; 2],
    /// Placed windows more than one frame from the median.
    outside: Vec<String>,
    /// Unplaced windows where the host copy's delay to the same microphone was
    /// placed in that window and in each window either side, so that nothing
    /// in the room explains the loss. A silent reference, or one that has moved
    /// out of the tracked lags, leaves its windows here.
    unexplained: Vec<String>,
}

/// One play's placements and tracked delays.
struct Play {
    /// The play's own position in each microphone, and in each reference.
    mic_at: Vec<isize>,
    ref_at: Vec<isize>,
    /// The host copy's delay to each microphone, window by window.
    host: Vec<Vec<Option<isize>>>,
    /// Each reference's delay to each microphone, window by window.
    refs: Vec<Vec<Vec<Option<isize>>>>,
    /// Each pair's delay, window by window.
    pairs: Vec<Vec<Option<isize>>>,
    /// The host copy's delay to each reference, window by window.
    host_refs: Vec<Vec<Option<isize>>>,
}

/// The played signal's level on the beam against the microphones' mean in one
/// play: power during the play less power in the silence before it, as a dB
/// figure where both differences are positive, with the powers it is taken
/// from, since the differences need not be positive.
struct BeamLevel {
    db: Option<f64>,
    beam_playing: f64,
    beam_quiet: f64,
    microphones_playing: f64,
    microphones_quiet: f64,
}

impl BeamLevel {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "db": self.db,
            "beam_power_playing": self.beam_playing,
            "beam_power_quiet": self.beam_quiet,
            "microphones_mean_power_playing": self.microphones_playing,
            "microphones_mean_power_quiet": self.microphones_quiet,
        })
    }
}

fn label(pair: [u8; 2]) -> String {
    format!("({}, {})", pair[0], pair[1])
}

fn steps_json(delays: &[Option<isize>]) -> serde_json::Value {
    steps(delays)
        .iter()
        .map(|s| serde_json::json!({ "window": s.window, "size": s.size }))
        .collect()
}

/// The median of the placed windows of `tracks`, or `None` if none is placed.
fn median_delay<'a>(tracks: impl Iterator<Item = &'a Vec<Option<isize>>>) -> Option<f64> {
    let placed: Vec<f64> = tracks.flatten().flatten().map(|&d| d as f64).collect();
    (!placed.is_empty()).then(|| median(&placed))
}

/// A register's decoded value as the `capture_started` line reports it.
fn register(device_state: &serde_json::Value, name: &str) -> serde_json::Value {
    device_state["registers"][name]["value"].clone()
}

/// The noise of play `p` of a measuring run.
fn play_noise(p: usize) -> Vec<i16> {
    noise(SEED + 1 + p as u64, PLAY_S * RATE_HZ as usize)
}

/// Write `report` to `path`, and print it without its per-play part.
fn write_report(path: &Path, report: &serde_json::Value) {
    std::fs::write(path, serde_json::to_string_pretty(report).unwrap()).unwrap();
    let mut shown = report.clone();
    shown["per_play"] = serde_json::json!(format!("in {}", path.display()));
    println!("{}", serde_json::to_string_pretty(&shown).unwrap());
}

/// Write `m.routing`, record [`PLAYS`] plays of noise in one capture, restore,
/// and measure. The log and a JSON report go to one directory under the cargo
/// target's scratch directory, whose path is printed; the report is written
/// before the caller asserts anything.
fn measure(writer: &mut ChipWriter, serial: Option<&str>, m: &Measurement) -> Measured {
    let noises: Vec<Vec<i16>> = (0..PLAYS).map(play_noise).collect();
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("route-measure")
        .join(format!("{}-{secs}", m.name));
    std::fs::create_dir_all(&dir).unwrap();
    println!("{}: log and report in {}", m.name, dir.display());

    let (routed, written) =
        Routed::apply(writer, CARD, &m.routing).unwrap_or_else(|e| panic!("{e}"));
    println!(
        "routing {} (write error reported: {:?})",
        bytes(&m.routing),
        written.write_error
    );
    let mut starts = Vec::with_capacity(PLAYS);
    let (channels, device_state) = capture_to(&dir.join("route.framelog"), &m.routing, || {
        let t0 = Instant::now();
        for samples in &noises {
            thread::sleep(Duration::from_secs(GAP_S as u64));
            starts.push(t0.elapsed().as_secs_f64());
            play(samples);
        }
        thread::sleep(Duration::from_secs(GAP_S as u64));
    });
    let restored = routed.restore();
    assert!(restored.is_ok(), "restore after {}: {restored:?}", m.name);
    describe(&channels);

    let nominals: Vec<isize> = starts
        .iter()
        .map(|&start| (start * f64::from(RATE_HZ)) as isize)
        .collect();
    let (measured, report) = analyse(m, &channels, &device_state, serial, &nominals);
    write_report(&dir.join("report.json"), &report);
    measured
}

/// Measure a run of [`PLAYS`] plays captured under `m.routing`: `channels` as
/// the capture holds them, `device_state` the chip state it read, and
/// `nominals` each play's position in the capture to within [`COARSE`]
/// frames. Returns what the run found and its report.
fn analyse(
    m: &Measurement,
    channels: &[Vec<i16>; CHANNELS],
    device_state: &serde_json::Value,
    serial: Option<&str>,
    nominals: &[isize],
) -> (Measured, serde_json::Value) {
    let rate = RATE_HZ as usize;
    let noises: Vec<Vec<i16>> = (0..PLAYS).map(play_noise).collect();
    let wide: Vec<Vec<f64>> = channels.iter().map(|c| widen(c)).collect();
    let len = PLAY_S * rate;
    // Each play's position in every microphone and reference.
    let placed: Vec<(Vec<isize>, Vec<isize>)> = noises
        .iter()
        .zip(nominals)
        .map(|(samples, &nominal)| {
            let h = widen(samples);
            let at = |ch: usize| place_whole(&h, &wide[ch], nominal, COARSE);
            (
                MICROPHONES.iter().map(|&ch| at(ch)).collect(),
                m.references.iter().map(|&(ch, _)| at(ch)).collect(),
            )
        })
        .collect();
    let from = |at: isize| usize::try_from(at).expect("a play placed before the capture");
    // The delay of `b` behind `a` is read in every play against one template,
    // taken over the first two seconds of the first play, so that a delay means
    // the same thing from play to play.
    let run_track =
        |a: usize, a_at: &dyn Fn(usize) -> isize, b: usize, b_at: &dyn Fn(usize) -> isize| {
            let lag = b_at(0) - a_at(0);
            let template = correlogram(&wide[a], from(a_at(0)), PLACE_SPAN, &wide[b], lag)
                .expect("the first play's start is inside the capture");
            (0..PLAYS)
                .map(|p| track(&wide[a], from(a_at(p)), len, &wide[b], lag, &template))
                .collect::<Vec<_>>()
        };
    // [reference][microphone][play], then [pair][play].
    let ref_tracks: Vec<Vec<Vec<Vec<Option<isize>>>>> = m
        .references
        .iter()
        .enumerate()
        .map(|(r, &(ch, _))| {
            (0..MICROPHONES.len())
                .map(|k| run_track(ch, &|p| placed[p].1[r], MICROPHONES[k], &|p| placed[p].0[k]))
                .collect()
        })
        .collect();
    let pair_tracks: Vec<Vec<Vec<Option<isize>>>> = m
        .pairs
        .iter()
        .map(|&(a, b)| {
            run_track(
                m.references[a].0,
                &|p| placed[p].1[a],
                m.references[b].0,
                &|p| placed[p].1[b],
            )
        })
        .collect();
    let plays: Vec<Play> = noises
        .iter()
        .zip(placed)
        .enumerate()
        .map(|(p, (samples, (mic_at, ref_at)))| {
            // The host copy's own position differs from play to play, so its
            // template is the play's own first two seconds.
            let h = widen(samples);
            let host_track = |ch: usize, lag: isize| {
                let template = correlogram(&h, 0, PLACE_SPAN, &wide[ch], lag).unwrap();
                track(&h, 0, len, &wide[ch], lag, &template)
            };
            let host = MICROPHONES
                .iter()
                .zip(&mic_at)
                .map(|(&mic, &lag)| host_track(mic, lag))
                .collect();
            let host_refs = m
                .references
                .iter()
                .zip(&ref_at)
                .map(|(&(ch, _), &lag)| host_track(ch, lag))
                .collect();
            Play {
                mic_at,
                ref_at,
                host,
                refs: ref_tracks
                    .iter()
                    .map(|per_mic| per_mic.iter().map(|t| t[p].clone()).collect())
                    .collect(),
                pairs: pair_tracks.iter().map(|t| t[p].clone()).collect(),
                host_refs,
            }
        })
        .collect();

    let host_steps: Vec<Vec<Vec<Change>>> = plays
        .iter()
        .map(|p| p.host.iter().map(|t| steps(t)).collect())
        .collect();
    let host_plays = host_steps
        .iter()
        .filter(|p| p.iter().any(|s| !s.is_empty()))
        .count();
    let control = host_plays > 0;

    let mut per_play = Vec::new();
    // Indexed [play][reference] and [play] respectively.
    let mut ref_levels: Vec<Vec<Option<f64>>> = Vec::with_capacity(PLAYS);
    let mut beam_levels: Vec<Option<BeamLevel>> = Vec::with_capacity(PLAYS);
    for (i, (p, hs)) in plays.iter().zip(&host_steps).enumerate() {
        let h = widen(&noises[i]);
        let levels: Vec<Option<f64>> = m
            .references
            .iter()
            .enumerate()
            .map(|(r, &(ch, _))| {
                let from = p.ref_at[r] as usize;
                db(
                    power(&wide[ch][from..(from + len).min(wide[ch].len())]),
                    power(&h),
                )
            })
            .collect();
        let mics: Vec<serde_json::Value> = (0..MICROPHONES.len())
            .map(|k| {
                serde_json::json!({
                    "channel": MICROPHONES[k],
                    "offset": p.mic_at[k],
                    "steps": steps_json(&p.host[k]),
                    "unplaced_windows": p.host[k].iter().filter(|d| d.is_none()).count(),
                    "track": p.host[k],
                })
            })
            .collect();
        let refs: Vec<serde_json::Value> = m
            .references
            .iter()
            .enumerate()
            .map(|(r, &(ch, pair))| {
                let to_mics: Vec<serde_json::Value> = (0..MICROPHONES.len())
                    .map(|k| {
                        let found = steps(&p.refs[r][k]);
                        let with_host: Vec<bool> = found
                            .iter()
                            .map(|s| hs[k].iter().any(|h| h.window.abs_diff(s.window) <= 1))
                            .collect();
                        serde_json::json!({
                            "channel": MICROPHONES[k],
                            "median_delay": median_delay(std::iter::once(&p.refs[r][k])),
                            "steps": steps_json(&p.refs[r][k]),
                            "with_a_host_copy_step": with_host,
                            "unplaced_windows": p.refs[r][k].iter().filter(|d| d.is_none()).count(),
                            "track": p.refs[r][k],
                        })
                    })
                    .collect();
                serde_json::json!({
                    "reference": label(pair),
                    "channel": ch,
                    "offset": p.ref_at[r],
                    "level_db_against_host_copy": levels[r],
                    "to_microphones": to_mics,
                    "host_copy_track": p.host_refs[r],
                })
            })
            .collect();
        let pairs: Vec<serde_json::Value> = m
            .pairs
            .iter()
            .zip(&p.pairs)
            .map(|(&(a, b), t)| {
                serde_json::json!({
                    "from": label(m.references[a].1),
                    "to": label(m.references[b].1),
                    "median_delay": median_delay(std::iter::once(t)),
                    "steps": steps_json(t),
                    "track": t,
                })
            })
            .collect();
        let beam_level = m.beam.map(|beam| {
            let start = p.mic_at[0] as usize;
            let quiet = start - rate * 3 / 4..start - rate / 8;
            let during = start..(start + len).min(wide[beam].len());
            let powers = |ch: usize| {
                (
                    power(&wide[ch][during.clone()]),
                    power(&wide[ch][quiet.clone()]),
                )
            };
            let (beam_playing, beam_quiet) = powers(beam);
            let (mut microphones_playing, mut microphones_quiet) = (0.0, 0.0);
            for &ch in &MICROPHONES {
                let (play, quiet) = powers(ch);
                microphones_playing += play / MICROPHONES.len() as f64;
                microphones_quiet += quiet / MICROPHONES.len() as f64;
            }
            BeamLevel {
                db: db(
                    beam_playing - beam_quiet,
                    microphones_playing - microphones_quiet,
                ),
                beam_playing,
                beam_quiet,
                microphones_playing,
                microphones_quiet,
            }
        });
        per_play.push(serde_json::json!({
            "play": i,
            "host_copy": mics,
            "references": refs,
            "pairs": pairs,
            "beam_level_against_microphones": beam_level.as_ref().map(BeamLevel::json),
        }));
        ref_levels.push(levels);
        beam_levels.push(beam_level);
    }

    let mut unsteady = Vec::new();
    let mut summary_refs = Vec::new();
    for (r, &(ch, pair)) in m.references.iter().enumerate() {
        let mut outside = Vec::new();
        let mut unplaced = 0;
        let mut unexplained = Vec::new();
        let mut to_mics = Vec::new();
        for (k, &mic) in MICROPHONES.iter().enumerate() {
            let median = median_delay(plays.iter().map(|p| &p.refs[r][k]));
            for (i, p) in plays.iter().enumerate() {
                for (w, d) in p.refs[r][k].iter().enumerate() {
                    match (*d, median) {
                        (Some(d), Some(mid)) if (d as f64 - mid).abs() > 1.0 => {
                            outside.push(format!(
                                "play {i}, window {w}, channel {mic}: {d} against {mid}"
                            ));
                        }
                        (Some(_), _) => {}
                        (None, _) => {
                            unplaced += 1;
                            let near =
                                &p.host[k][w.saturating_sub(1)..(w + 2).min(p.host[k].len())];
                            if near.iter().all(Option::is_some) {
                                unexplained.push(format!(
                                    "play {i}, window {w}, channel {mic}: not placed, \
                                     the host copy placed"
                                ));
                            }
                        }
                    }
                }
            }
            let stepping = plays
                .iter()
                .filter(|p| !steps(&p.refs[r][k]).is_empty())
                .count();
            to_mics.push(serde_json::json!({
                "channel": mic,
                "constant_delay": median,
                "plays_with_a_step": stepping,
            }));
        }
        let levels: Vec<f64> = ref_levels.iter().filter_map(|l| l[r]).collect();
        println!(
            "{} on channel {ch}: {} windows more than one frame from the run's median; \
             {unplaced} of {} windows not placed, {} of them where the host copy was placed",
            label(pair),
            outside.len(),
            plays
                .iter()
                .map(|p| p.refs[r].iter().map(Vec::len).sum::<usize>())
                .sum::<usize>(),
            unexplained.len()
        );
        summary_refs.push(serde_json::json!({
            "reference": label(pair),
            "channel": ch,
            "to_microphones": to_mics,
            "windows_more_than_one_frame_from_the_median": outside,
            "unplaced_windows": unplaced,
            "unplaced_windows_where_the_host_copy_was_placed": unexplained,
            "level_db_against_host_copy_median": (!levels.is_empty()).then(|| median(&levels)),
        }));
        unsteady.push(Unsteady {
            reference: pair,
            outside,
            unexplained,
        });
    }
    let pair_delays: Vec<Option<f64>> = (0..m.pairs.len())
        .map(|j| median_delay(plays.iter().map(|p| &p.pairs[j])))
        .collect();
    let pair_summary: Vec<serde_json::Value> = m
        .pairs
        .iter()
        .enumerate()
        .map(|(j, &(a, b))| {
            serde_json::json!({
                "from": label(m.references[a].1),
                "to": label(m.references[b].1),
                "constant_delay": pair_delays[j],
                "plays_with_a_step": plays.iter().filter(|p| !steps(&p.pairs[j]).is_empty()).count(),
            })
        })
        .collect();

    let others: Vec<&[i16]> = m.others.iter().map(|&ch| channels[ch].as_slice()).collect();
    let zero = zero_runs(&others, ZERO_RUN);
    let zero_json: Vec<serde_json::Value> = zero
        .iter()
        .map(|&(start, n)| {
            let end = start + n;
            let in_play = plays.iter().position(|p| {
                let at = p.mic_at[0];
                at >= 0 && (at as usize..at as usize + len).contains(&start)
            });
            let refs_zero: Vec<bool> = m
                .references
                .iter()
                .map(|&(ch, _)| channels[ch][start..end].iter().all(|&s| s == 0))
                .collect();
            // (reference, microphone channel, delay before, delay after).
            type Around = ([u8; 2], usize, Option<isize>, Option<isize>);
            let around: Option<Vec<Around>> = in_play.map(|i| {
                let p = &plays[i];
                m.references
                    .iter()
                    .enumerate()
                    .flat_map(|(r, &(_, pair))| (0..MICROPHONES.len()).map(move |k| (r, pair, k)))
                    .map(|(r, pair, k)| {
                        let window_at = |w: usize| p.ref_at[r] as usize + w * HOP;
                        let t = &p.refs[r][k];
                        let before = (0..t.len())
                            .rev()
                            .find(|&w| window_at(w) + WINDOW <= start)
                            .and_then(|w| t[w]);
                        let after = (0..t.len())
                            .find(|&w| window_at(w) >= end)
                            .and_then(|w| t[w]);
                        (pair, MICROPHONES[k], before, after)
                    })
                    .collect()
            });
            // A run is as expected when every reference is zero over it and no
            // reference-to-microphone delay moves by more than a frame across
            // it; a run outside a play, or beside an unplaced window, has no
            // shift measured.
            let shifted = around.as_ref().is_some_and(|a| {
                a.iter()
                    .any(|&(_, _, before, after)| match (before, after) {
                        (Some(b), Some(a)) => (a - b).abs() > 1,
                        _ => false,
                    })
            });
            let shift_measured = around.as_ref().is_some_and(|a| {
                a.iter()
                    .all(|&(_, _, before, after)| before.is_some() && after.is_some())
            });
            let verdict = if !refs_zero.iter().all(|&z| z) || shifted {
                "not as expected"
            } else if shift_measured {
                "as expected"
            } else {
                "not measured"
            };
            let delays: Option<Vec<serde_json::Value>> = around.map(|a| {
                a.iter()
                    .map(|&(pair, channel, before, after)| {
                        serde_json::json!({
                            "reference": label(pair),
                            "channel": channel,
                            "delay_before": before,
                            "delay_after": after,
                        })
                    })
                    .collect()
            });
            serde_json::json!({
                "frame": start,
                "frames": n,
                "play": in_play,
                "references_zero": refs_zero,
                "delays": delays,
                "verdict": verdict,
            })
        })
        .collect();

    let beam_median = |value: fn(&BeamLevel) -> Option<f64>| {
        let v: Vec<f64> = beam_levels.iter().flatten().filter_map(value).collect();
        (!v.is_empty()).then(|| median(&v))
    };
    let beam = match m.beam {
        Some(ch) => serde_json::json!({
            "channel": ch,
            "plays_with_a_db_figure": beam_levels.iter().flatten().filter(|b| b.db.is_some()).count(),
            "median_db": beam_median(|b| b.db),
            "median_beam_power_playing": beam_median(|b| Some(b.beam_playing)),
            "median_beam_power_quiet": beam_median(|b| Some(b.beam_quiet)),
            "median_microphones_mean_power_playing": beam_median(|b| Some(b.microphones_playing)),
            "median_microphones_mean_power_quiet": beam_median(|b| Some(b.microphones_quiet)),
        }),
        None => {
            serde_json::json!("not measured: this routing gives the beam's channel to a reference")
        }
    };
    let verdicts: serde_json::Map<String, serde_json::Value> =
        ["as expected", "not as expected", "not measured"]
            .into_iter()
            .map(|v| {
                let n = zero_json.iter().filter(|z| z["verdict"] == v).count();
                (v.to_owned(), serde_json::json!(n))
            })
            .collect();
    let report = serde_json::json!({
        "name": m.name,
        "serial": serial,
        "routing": bytes(&m.routing),
        "plays": PLAYS,
        "play_seconds": PLAY_S,
        "audio_mgr_sys_delay": register(device_state, "audio_mgr_sys_delay"),
        "audio_mgr_ref_gain": register(device_state, "audio_mgr_ref_gain"),
        "control": { "plays_with_a_host_copy_step": host_plays, "held": control },
        "references": summary_refs,
        "pairs": pair_summary,
        "beam_level_against_microphones": beam,
        "zero_run_verdicts": verdicts,
        "zero_runs": zero_json,
        "per_play": per_play,
    });
    let measured = Measured {
        control,
        unsteady,
        pair_delays,
        sys_delay: register(device_state, "audio_mgr_sys_delay"),
    };
    (measured, report)
}

/// The run of [`the_routed_reference_holds_still_against_the_microphones`]:
/// (12, 0) on capture channel 1, the conference beam and the four microphones
/// kept.
fn reference_run() -> Measurement {
    Measurement {
        name: "reference".into(),
        routing: with_slot(&BASELINE, slot_of_channel(ROUTED), CHIP_REFERENCE),
        references: vec![(ROUTED, CHIP_REFERENCE)],
        others: vec![0, 2, 3, 4, 5],
        beam: Some(0),
        pairs: vec![],
    }
}

/// The run of [`the_other_far_end_taps_are_measured_against_the_reference`]
/// for `pair`: `pair` on capture channel 0 beside (12, 0) on channel 1, the
/// four microphones kept.
fn tap_run(pair: [u8; 2]) -> Measurement {
    let chip = with_slot(&BASELINE, slot_of_channel(ROUTED), CHIP_REFERENCE);
    Measurement {
        name: format!("tap-{}-{}", pair[0], pair[1]),
        routing: with_slot(&chip, slot_of_channel(0), pair),
        references: vec![(0, pair), (ROUTED, CHIP_REFERENCE)],
        others: MICROPHONES.to_vec(),
        beam: None,
        pairs: vec![(0, 1)],
    }
}

/// The measuring run a report names: `reference`, or `tap-<category>-<source>`
/// for one of [`FAR_END`] other than (12, 0).
fn run_named(name: &str) -> Measurement {
    if name == "reference" {
        return reference_run();
    }
    FAR_END[1..]
        .iter()
        .map(|&pair| tap_run(pair))
        .find(|m| m.name == name)
        .unwrap_or_else(|| panic!("no measuring run is named {name:?}"))
}

/// The Flex's device, printed, with a writer on it and its serial.
fn open_flex() -> (ChipWriter, Option<String>) {
    let device = flex_device(CARD).unwrap_or_else(|e| panic!("{e}"));
    println!(
        "{device}, bcdDevice {:04x}, serial {:?}",
        device.bcd_device, device.serial
    );
    let writer = open_writer(&device, Path::new(DEV_BUS_USB)).unwrap_or_else(|e| panic!("{e}"));
    (writer, device.serial)
}

/// With (12, 0) routed to capture channel 1, the reference's delay to each raw
/// microphone stays within one frame of the run's median, in every window of
/// every one of twenty plays, while the delay of the host's own copy steps. A
/// window in which the reference's delay cannot be read counts against it
/// unless the host copy's delay to the same microphone cannot be read there
/// either, in that window or one beside it. Writes; plays twenty plays of
/// twenty seconds.
#[test]
#[ignore = "writes the Flex's routing and plays for seven minutes; run by `make -C firmware route-measure-hil`"]
fn the_routed_reference_holds_still_against_the_microphones() {
    let (mut writer, serial) = open_flex();
    require_baseline(&mut writer);
    let measured = measure(&mut writer, serial.as_deref(), &reference_run());
    require_baseline(&mut writer);

    assert!(
        measured.control,
        "the host's copy did not step in any play: the run has no control; repeat it"
    );
    // TODO(flex-chip-reference-unread-windows): on the bench this fails on a window in
    // which the reference's delay to one microphone is not read while the host copy's is.
    for u in &measured.unsteady {
        assert!(
            u.outside.is_empty() && u.unexplained.is_empty(),
            "{}: windows more than one frame from the run's median delay: {:#?}; windows not \
             placed where the host copy was: {:#?}",
            label(u.reference),
            u.outside,
            u.unexplained
        );
    }
}

/// Each other far-end category that carries the played signal, routed to
/// capture channel 0 beside (12, 0) on channel 1, measured as
/// [`the_routed_reference_holds_still_against_the_microphones`] measures (12,
/// 0), with the delay between the two. Asserts only that each run has its
/// control. Writes; plays a two-second check per category and twenty plays of
/// twenty seconds per category run.
#[test]
#[ignore = "writes the Flex's routing and plays for up to half an hour; run by `make -C firmware route-measure-hil`"]
fn the_other_far_end_taps_are_measured_against_the_reference() {
    let (mut writer, serial) = open_flex();
    require_baseline(&mut writer);
    let mut runs = Vec::new();
    for &pair in &FAR_END[1..] {
        let class = carries(&mut writer, pair);
        if class != Class::Carries {
            println!("{}: {class:?}; not measured", label(pair));
            continue;
        }
        runs.push((
            pair,
            measure(&mut writer, serial.as_deref(), &tap_run(pair)),
        ));
    }
    require_baseline(&mut writer);

    if runs.is_empty() {
        println!("no other far-end category carries the played signal; none measured");
    }
    let to_chip: Vec<([u8; 2], f64, &serde_json::Value)> = runs
        .iter()
        .filter_map(|(pair, m)| m.pair_delays[0].map(|d| (*pair, d, &m.sys_delay)))
        .collect();
    for (i, (a, da, sys)) in to_chip.iter().enumerate() {
        for (b, db_, _) in &to_chip[i + 1..] {
            println!(
                "{a} to (12, 0): {da} frames; {b} to (12, 0): {db_} frames; difference {} \
                 frames; AUDIO_MGR_SYS_DELAY {sys}",
                da - db_,
                a = label(*a),
                b = label(*b),
            );
        }
    }
    let without: Vec<String> = runs
        .iter()
        .filter(|(_, m)| !m.control)
        .map(|(pair, _)| label(*pair))
        .collect();
    assert!(
        without.is_empty(),
        "runs in which the host's copy did not step (no control; repeat them): {without:?}"
    );
}

/// The environment variable naming the directory of the kept run
/// [`a_kept_measuring_run_reads_again_as_it_was_reported`] reads.
const REREAD_RUN: &str = "ROUTE_REREAD_RUN";

/// The keys a report written before it held `zero_run_verdicts` shares, with
/// one meaning, with the report written now: every placement, the control's
/// count, every constant delay, every per-play median delay and every step.
const SHARED_KEYS: [&str; 5] = [
    "offset",
    "plays_with_a_host_copy_step",
    "constant_delay",
    "median_delay",
    "steps",
];

/// Every difference between `kept` and `read` at `path` and below it. Scalars
/// are compared in full where `whole`, and otherwise only under the keys of
/// [`SHARED_KEYS`]. An array of `kept` is compared for its length, and for
/// `read` holding an array there, under every key whether or not `whole`.
fn differences(
    path: &str,
    kept: &serde_json::Value,
    read: &serde_json::Value,
    whole: bool,
    out: &mut Vec<String>,
) {
    use serde_json::Value;
    match kept {
        Value::Object(map) => {
            for (key, value) in map {
                differences(
                    &format!("{path}.{key}"),
                    value,
                    read.get(key).unwrap_or(&Value::Null),
                    whole || SHARED_KEYS.contains(&key.as_str()),
                    out,
                );
            }
        }
        Value::Array(items) => match read.as_array() {
            Some(again) if again.len() == items.len() => {
                for (i, (value, again)) in items.iter().zip(again).enumerate() {
                    differences(&format!("{path}[{i}]"), value, again, whole, out);
                }
            }
            _ => out.push(format!("{path}: kept {kept}, read {read}")),
        },
        _ if whole && kept != read => out.push(format!("{path}: kept {kept}, read {read}")),
        _ => {}
    }
}

/// A kept run of [`the_routed_reference_holds_still_against_the_microphones`]
/// or [`the_other_far_end_taps_are_measured_against_the_reference`], read from
/// its log by the measuring tests' analysis, reproduces its report. The run's
/// directory is named by [`REREAD_RUN`]. Each play is placed starting from the
/// kept report's position of that play in the first microphone. The new report,
/// with each window's delays, is written beside the kept one as `reread.json`.
/// A kept report that holds `zero_run_verdicts` must be reproduced in every
/// value it holds; an older one in those of [`SHARED_KEYS`]. Writes nothing to
/// the chip, opens no device and plays nothing.
#[test]
#[ignore = "reads a kept measuring run; run by `make -C firmware route-reread RUN=<dir>`"]
fn a_kept_measuring_run_reads_again_as_it_was_reported() {
    let dir = std::env::var_os(REREAD_RUN)
        .unwrap_or_else(|| panic!("{REREAD_RUN} is not set: set it to a kept run's directory"));
    let dir = Path::new(&dir);
    let text = std::fs::read_to_string(dir.join("report.json"))
        .unwrap_or_else(|e| panic!("{}: {e}", dir.join("report.json").display()));
    let kept: serde_json::Value = serde_json::from_str(&text).expect("the kept report is JSON");
    let name = kept["name"]
        .as_str()
        .expect("the kept report names its run");
    let m = run_named(name);
    assert_eq!(
        kept["routing"].as_str(),
        Some(bytes(&m.routing).as_str()),
        "the kept report's routing against the routing of the run it names"
    );

    let (channels, header) = read_log(&dir.join("route.framelog"));
    let device_state = &header["capture"]["device_state"];
    assert_eq!(
        device_state["registers"]["audio_mgr_op_all"]["bytes"].as_str(),
        Some(hex(&m.routing).as_str()),
        "the log header's audio_mgr_op_all against the run's routing"
    );
    let plays = kept["per_play"]
        .as_array()
        .expect("the kept report's plays");
    assert_eq!(plays.len(), PLAYS, "plays in the kept report");
    let nominals: Vec<isize> = plays
        .iter()
        .map(|p| {
            let at = p["host_copy"][0]["offset"].as_i64();
            isize::try_from(at.expect("each kept play has its position")).unwrap()
        })
        .collect();

    let (_, report) = analyse(
        &m,
        &channels,
        device_state,
        kept["serial"].as_str(),
        &nominals,
    );
    let reread = dir.join("reread.json");
    write_report(&reread, &report);
    // Both reports are compared as parsed from their files, since parsing a
    // number need not give back the exact value that was written.
    let read: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&reread).unwrap()).unwrap();
    let whole = kept.get("zero_run_verdicts").is_some();
    let mut differ = Vec::new();
    differences("", &kept, &read, whole, &mut differ);
    assert!(
        differ.is_empty(),
        "{} values of the kept report are not reproduced{}: {differ:#?}",
        differ.len(),
        if whole { "" } else { " (shared keys only)" }
    );
    println!(
        "{}: reproduced {}",
        dir.display(),
        if whole {
            "every value of the kept report"
        } else {
            "every placement, control count, delay and step of the kept report"
        }
    );
}

/// [`differences`] of `kept` against `read`, from the root.
fn differences_of(kept: serde_json::Value, read: serde_json::Value, whole: bool) -> Vec<String> {
    let mut out = Vec::new();
    differences("", &kept, &read, whole, &mut out);
    out
}

#[test]
fn equal_reports_have_no_differences() {
    let report = serde_json::json!({
        "name": "reference",
        "per_play": [{"offset": 7, "references": [{"median_delay": 26.0}]}],
    });
    assert!(differences_of(report.clone(), report.clone(), true).is_empty());
    assert!(differences_of(report.clone(), report, false).is_empty());
}

#[test]
fn a_whole_comparison_reports_every_differing_or_missing_value() {
    let kept = serde_json::json!({
        "serial": "a",
        "per_play": [{"play": 0, "level": 1.5}],
        "gone": 3,
    });
    let read = serde_json::json!({
        "serial": "b",
        "per_play": [{"play": 0, "level": 2.5}],
        "added": 4,
    });
    assert_eq!(
        differences_of(kept, read, true),
        [
            ".gone: kept 3, read null",
            ".per_play[0].level: kept 1.5, read 2.5",
            ".serial: kept \"a\", read \"b\"",
        ]
    );
}

#[test]
fn a_shared_keys_comparison_reports_a_shared_value_nested_under_another_key() {
    let kept = serde_json::json!({
        "serial": "a",
        "per_play": [{"level": 1.5, "references": [{"median_delay": 26.0, "steps": []}]}],
    });
    let read = serde_json::json!({
        "serial": "b",
        "per_play": [{"level": 2.5, "references": [{"median_delay": 27.0, "steps": []}]}],
    });
    assert_eq!(
        differences_of(kept, read, false),
        [".per_play[0].references[0].median_delay: kept 26.0, read 27.0"]
    );
}

#[test]
fn a_shared_keys_comparison_reports_a_missing_shared_key() {
    let kept = serde_json::json!({"pairs": [{"constant_delay": 12}]});
    let read = serde_json::json!({"pairs": [{}]});
    assert_eq!(
        differences_of(kept, read, false),
        [".pairs[0].constant_delay: kept 12, read null"]
    );
}

#[test]
fn a_shared_keys_comparison_compares_array_lengths_under_every_key() {
    let kept = serde_json::json!({"zero_runs": [1, 2]});
    let read = serde_json::json!({"zero_runs": [1, 2, 3]});
    assert_eq!(
        differences_of(kept, read, false),
        [".zero_runs: kept [1,2], read [1,2,3]"]
    );
}
