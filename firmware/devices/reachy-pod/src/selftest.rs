//! The on-device self-test registry.
//!
//! Bring-up here follows the same discipline as the ESP pod's HIL suite: each case
//! asserts the behavior we expect and is expected to fail until hardware confirms
//! it, an unexpected reading is reviewed before the assertion is changed to accept
//! it, and every case stays afterwards as a regression guard. Presence cases (is the
//! board there, does its node open) are kept apart from identity cases (does it
//! report the firmware and telemetry we think it does), because a board that is
//! absent and a board that answers wrongly call for different work.
//!
//! The registry runs on the device, invoked over SSH, and prints one line per case
//! as it completes — so a case that hangs on a transfer names itself.
//!
//! Invocation identity matters as much as the assertions: SSH lands as root, and
//! root opens any device node whatever udev said, so a root-run permission case
//! passes vacuously. Runs drop to the account the payload runs as, and the report
//! states which uid it observed the device as.

use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use alsa::pcm::PCM;
use audio_pipeline::playback::WIRE_BYTES_PER_SAMPLE;
use audio_pipeline::ring::{SAMPLE_RATE_HZ, WaveformStats};
use device_protocol::{doa_azimuth_ok, sp_energy_ok};
use xvf3800_ctrl::{
    AEC_AZIMUTH_READ_LEN, AEC_AZIMUTH_VALUES_CMD, AEC_RESID, AEC_SPENERGY_READ_LEN,
    AEC_SPENERGY_VALUES_CMD, APPLICATION_SERVICER_RESID, ControlTransport, STATUS_DONE, USB_RETRY,
    VERSION_CMD, VERSION_READ_LEN, control_read, decode_f32x4,
};

use crate::alsa_capture::{
    CAPTURE_PARAMS, CaptureStream, CardInfo, PERIOD_FRAMES, PcmError, append_channels, open_capture,
};
use crate::beam::beam_energy_speech;
use crate::config::CHANNELS;
use crate::playback::{AlsaOut, PlaybackFault, StereoOut, expand_mono_to_stereo, open_playback_on};
use crate::run::PeriodSource;
use crate::usb_ctrl::{Generation, UsbControl, find_boards, log_generation, select_board};

/// The application-servicer `VERSION` a board running the reachy firmware is
/// expected to report at least. The factory software's own floor, and the reading to
/// expect from the `38fb:1001` id; a board still on the pre-update id reports older
/// and that is not a failure. The exact triple observed on hardware is baked in here
/// once a human has reviewed it.
pub const REACHY_FIRMWARE_MIN_VERSION: (u8, u8, u8) = (2, 1, 0);

/// The control-plane cases, in the order they run. Named so a run that never opened
/// the board can still account for each of them.
pub const CONTROL_PLANE_CASES: [&str; 3] = ["ctrl_version", "ctrl_spenergy", "ctrl_doa"];

/// The audio-plane cases, in the order they run. Same rule as the control-plane
/// list: a run that never opened the card still accounts for each of them.
pub const AUDIO_PLANE_CASES: [&str; 3] = ["alsa_params", "alsa_waveform_sanity", "alsa_playback"];

// ── Results ───────────────────────────────────────────────────────────────────

/// What one case concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The assertion held. The detail carries the observed reading, which is what
    /// makes a passing run reviewable rather than merely green.
    Pass(String),
    /// The assertion did not hold. One line per fact, first line first.
    Fail(Vec<String>),
    /// The case could not be attempted, and why. Counted as a failure by the run:
    /// a case that did not execute has asserted nothing.
    NotRun(String),
}

impl Outcome {
    /// A single-line failure.
    pub fn fail(line: impl Into<String>) -> Self {
        Self::Fail(vec![line.into()])
    }

    /// Whether this outcome lets the run report success.
    pub fn passed(&self) -> bool {
        matches!(self, Self::Pass(_))
    }
}

/// One case's name and outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseResult {
    pub name: &'static str,
    pub outcome: Outcome,
}

impl fmt::Display for CaseResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outcome {
            Outcome::Pass(detail) => write!(f, "PASS {} — {detail}", self.name),
            Outcome::NotRun(reason) => write!(f, "SKIP {} — {reason}", self.name),
            Outcome::Fail(lines) => {
                write!(f, "FAIL {}", self.name)?;
                for line in lines {
                    write!(f, "\n     {line}")?;
                }
                Ok(())
            }
        }
    }
}

/// Every case a run attempted.
#[derive(Debug, Default)]
pub struct Report {
    pub cases: Vec<CaseResult>,
}

impl Report {
    /// Record `outcome` for `name` and print it, so the transcript reaches the
    /// operator before the next case starts.
    pub fn record(
        &mut self,
        out: &mut dyn io::Write,
        name: &'static str,
        outcome: Outcome,
    ) -> io::Result<()> {
        let case = CaseResult { name, outcome };
        writeln!(out, "{case}")?;
        self.cases.push(case);
        Ok(())
    }

    /// Whether every case ran and passed.
    pub fn all_passed(&self) -> bool {
        !self.cases.is_empty() && self.cases.iter().all(|c| c.outcome.passed())
    }

    /// The closing line: counts, so a long transcript has a verdict at the end.
    pub fn summary(&self) -> String {
        let passed = self.cases.iter().filter(|c| c.outcome.passed()).count();
        let not_run = self
            .cases
            .iter()
            .filter(|c| matches!(c.outcome, Outcome::NotRun(_)))
            .count();
        let failed = self.cases.len() - passed - not_run;
        format!(
            "{} cases: {passed} passed, {failed} failed, {not_run} not run",
            self.cases.len()
        )
    }
}

// ── The cases ─────────────────────────────────────────────────────────────────

/// The account this process is running as. A permission assertion is only about the
/// identity that took it.
///
/// SAFETY: `geteuid` takes no arguments, touches no memory and cannot fail.
pub fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

/// Case 1 (presence) — exactly one board enumerates, and its node opens read/write
/// as the invoking account.
///
/// Returns the open transport so the control-plane cases run against the same handle
/// the pipeline would hold.
pub fn usb_presence() -> (Outcome, Option<UsbControl>) {
    let uid = effective_uid();
    let found = match find_boards() {
        Ok(found) => found,
        Err(e) => {
            return (
                Outcome::fail(format!("cannot enumerate the USB bus: {e}")),
                None,
            );
        }
    };
    let board = match select_board(&found) {
        Ok(board) => board,
        Err(why) => return (Outcome::fail(why), None),
    };
    log_generation(board);
    match UsbControl::open(board) {
        Ok(transport) => (
            Outcome::Pass(format!("{board} opens read/write as uid {uid}")),
            Some(transport),
        ),
        Err(e) => (
            Outcome::Fail(vec![
                format!("{board} enumerates but does not open as uid {uid}: {e}"),
                format!(
                    "the node is granted to the audio group; an account outside it fails here \
                     while enumeration succeeds"
                ),
            ]),
            None,
        ),
    }
}

/// Case 2 (identity) — the application servicer reports a plausible firmware
/// version.
///
/// Read, not guessed: the version is what says the command table this pipeline
/// assumes is the table the board carries.
pub fn ctrl_version<T: ControlTransport>(transport: &mut T, generation: Generation) -> Outcome
where
    T::Error: fmt::Display,
{
    let mut payload = [0u8; VERSION_READ_LEN];
    let (status, attempts) = match control_read(
        transport,
        USB_RETRY,
        APPLICATION_SERVICER_RESID,
        VERSION_CMD,
        &mut payload,
    ) {
        Ok(answer) => answer,
        Err(e) => return Outcome::fail(format!("VERSION read (resid 48 cmd 0) failed: {e}")),
    };
    if status != STATUS_DONE {
        return Outcome::fail(format!(
            "VERSION read (resid 48 cmd 0) returned status 0x{status:02x} after {attempts} \
             transaction(s)"
        ));
    }
    let observed = (payload[0], payload[1], payload[2]);
    let rendered = format!("{}.{}.{}", observed.0, observed.1, observed.2);
    if observed == (0, 0, 0) {
        return Outcome::fail(format!(
            "VERSION read reported {rendered}, which is no version at all"
        ));
    }
    if generation == Generation::ReachyFirmware && observed < REACHY_FIRMWARE_MIN_VERSION {
        let (major, minor, patch) = REACHY_FIRMWARE_MIN_VERSION;
        return Outcome::Fail(vec![
            format!(
                "firmware {rendered} is below the {major}.{minor}.{patch} the reachy id implies"
            ),
            "a board on this id running older firmware is a reading to review, not one to accept"
                .to_string(),
        ]);
    }
    Outcome::Pass(format!("firmware {rendered} in {attempts} transaction(s)"))
}

/// Case 3 — per-beam speech energy reads back plausibly: four finite, non-negative
/// values.
pub fn ctrl_spenergy<T: ControlTransport>(transport: &mut T) -> Outcome
where
    T::Error: fmt::Display,
{
    let values = match read_f32x4(
        transport,
        AEC_RESID,
        AEC_SPENERGY_VALUES_CMD,
        AEC_SPENERGY_READ_LEN,
        "AEC_SPENERGY_VALUES (resid 33 cmd 80)",
    ) {
        Ok(values) => values,
        Err(outcome) => return outcome,
    };
    match values.iter().position(|v| !sp_energy_ok(*v)) {
        None => Outcome::Pass(format!("speech energy {}", render(&values))),
        Some(i) => Outcome::fail(format!(
            "speech energy beam {i} is {}, which is not finite and non-negative: {}",
            values[i],
            render(&values)
        )),
    }
}

/// Case 4 — per-beam direction of arrival reads back plausibly. NaN is accepted on
/// any beam: it is what the chip reports for a beam with nothing focused on it, and
/// a room in which nobody is speaking is the normal state for this case.
pub fn ctrl_doa<T: ControlTransport>(transport: &mut T) -> Outcome
where
    T::Error: fmt::Display,
{
    let values = match read_f32x4(
        transport,
        AEC_RESID,
        AEC_AZIMUTH_VALUES_CMD,
        AEC_AZIMUTH_READ_LEN,
        "AEC_AZIMUTH_VALUES (resid 33 cmd 75)",
    ) {
        Ok(values) => values,
        Err(outcome) => return outcome,
    };
    match values.iter().position(|v| !doa_azimuth_ok(*v)) {
        None => Outcome::Pass(format!("azimuths {} rad", render(&values))),
        Some(i) => Outcome::fail(format!(
            "azimuth beam {i} is {} rad, outside the plausible range: {}",
            values[i],
            render(&values)
        )),
    }
}

// ── The audio plane ───────────────────────────────────────────────────────────

/// Case 5 (identity) — the card resolves by name and accepts the pipeline's exact
/// hardware parameters.
///
/// Returns the open stream so the waveform case reads from the same configuration
/// the pipeline will run on. A refusal carries the device's advertised parameters:
/// that dump is the finding, and the recovery lever for a board that offers no
/// 16-bit format is the chip's own bit-depth setting, applied once and by hand after
/// review — not negotiated here.
pub fn alsa_params() -> (Outcome, Option<(CardInfo, PCM)>) {
    match open_capture() {
        Ok((card, pcm)) => (
            Outcome::Pass(format!("{card} accepts {CAPTURE_PARAMS}")),
            Some((card, pcm)),
        ),
        Err(e) => (
            Outcome::Fail(e.to_string().lines().map(str::to_string).collect()),
            None,
        ),
    }
}

/// Case 6 — the captured waveform is live audio on both channels.
///
/// Both are judged, not just the configured one: both are processed beams, so a dead
/// channel is a finding whichever one the pipeline is pointed at, and which channel
/// carries which beam is established by the bench case that speaks at the array.
///
/// The window is bounded by waiting rather than by checking the clock between reads:
/// a card that opens, accepts the parameters and then delivers nothing would leave a
/// read blocked forever, and a deadline the case never returns to is a deadline that
/// bounds a slow card but not a stopped one — the very failure this case exists to
/// report, and the one that would hang `make reachy-selftest`.
pub fn alsa_waveform_sanity<S: PeriodSource>(source: &mut S, now: &dyn Fn() -> Instant) -> Outcome {
    let mut channels: [Vec<i16>; CHANNELS] = Default::default();
    let deadline = now() + WAVEFORM_CAPTURE_TIMEOUT;
    let collected = {
        let mut fold = |period: &[i16]| append_channels(period, &mut channels);
        match collect_window(source, WAVEFORM_SAMPLES, deadline, now, &mut fold) {
            Ok(collected) => collected,
            Err(e) => return Outcome::fail(e.to_string()),
        }
    };
    if collected < WAVEFORM_SAMPLES {
        return Outcome::fail(format!(
            "the card delivered {collected} of {WAVEFORM_SAMPLES} samples in \
             {WAVEFORM_CAPTURE_TIMEOUT:?}"
        ));
    }
    let stats: Vec<WaveformStats> = channels
        .iter()
        .map(|samples| WaveformStats::of(&samples[..WAVEFORM_SAMPLES]))
        .collect();
    assess_waveforms(&stats, source.recoveries())
}

/// How much audio the waveform case judges, per channel: 2 s at 16 kHz.
pub const WAVEFORM_SAMPLES: usize = 2 * SAMPLE_RATE_HZ as usize;

/// How long that capture may take before the card is declared unresponsive. Twice
/// the audio's own duration: a stream running at rate needs half of this, and a
/// stream that needs more than this is not running at rate.
pub const WAVEFORM_CAPTURE_TIMEOUT: Duration = Duration::from_secs(4);

/// Judge one window per channel, reporting every channel's reading either way.
///
/// `recoveries` rides along because an overrun during the window is not a failure by
/// itself, and a passing run that needed several is worth seeing.
pub fn assess_waveforms(stats: &[WaveformStats], recoveries: u64) -> Outcome {
    let rendered: Vec<String> = stats
        .iter()
        .enumerate()
        .map(|(channel, s)| format!("ch{channel} {s}"))
        .collect();
    let recovered = if recoveries == 0 {
        String::new()
    } else {
        format!(" ({recoveries} overrun(s) recovered)")
    };
    let defects: Vec<String> = stats
        .iter()
        .enumerate()
        .filter_map(|(channel, s)| s.defect().map(|why| format!("ch{channel} is {why}: {s}")))
        .collect();
    if defects.is_empty() {
        Outcome::Pass(format!("{}{recovered}", rendered.join(" | ")))
    } else {
        Outcome::Fail(defects)
    }
}

/// Collect `samples` frames per channel from `source`, or report how few arrived.
///
/// `fold` receives each period of interleaved frames and reports the frames it
/// kept; the total comes back for the caller to compare against `samples`.
///
/// The bound is on waiting, not on reading. `deadline` caps how long the run will
/// sit on a card with nothing ready; a `deadline` already past is a zero-wait poll
/// that takes whatever is there and stops at the first quiet answer. A recovered
/// xrun appears as a not-ready answer ([`PeriodSource::wait_ready`]), so a window
/// with time remaining retries rather than ending on it.
///
/// A read that yields no frames counts as a quiet answer rather than as a turn of
/// the loop: the deadline is the only thing bounding this collection, and a source
/// that answered ready forever while delivering nothing would otherwise never
/// consult it.
pub fn collect_window<S: PeriodSource>(
    source: &mut S,
    samples: usize,
    deadline: Instant,
    now: &dyn Fn() -> Instant,
    fold: &mut dyn FnMut(&[i16]) -> usize,
) -> Result<usize, PcmError> {
    let mut collected = 0;
    while collected < samples {
        let remaining = deadline.saturating_duration_since(now());
        let arrived = if source.wait_ready(remaining)? {
            fold(source.read_period()?)
        } else {
            0
        };
        collected += arrived;
        if arrived == 0 && remaining.is_zero() {
            break;
        }
    }
    Ok(collected)
}

/// Case 7 — the board takes a second of audio, and capture survives being run
/// full duplex.
///
/// Two assertions in one pass, because they can only be taken together. The
/// playback half is the simpler one: the sink accepts every sample of the tone,
/// within a bound, without the device underrunning. The duplex half is the reason
/// the capture stream is threaded through: the board is one USB device carrying
/// both directions, and a capture stream that dies, goes silent or saturates the
/// moment the speakers are driven is a finding no single-direction case can see.
///
/// The tone is written through [`expand_mono_to_stereo`] — the pipeline's own
/// expansion — so what reaches the card is shaped exactly like host speech.
///
/// The capture window is judged by the same classifier case 6 uses. A quiet room
/// with the chip's echo canceller working is expected to leave the microphones
/// live rather than dead: the canceller removes what the board played, not the
/// room.
pub fn alsa_playback<O: StereoOut, S: PeriodSource>(
    sink: &mut O,
    capture: &mut S,
    now: &dyn Fn() -> Instant,
) -> Outcome {
    let tone = tone_pcm(TONE_SAMPLES);
    let chunk_frames = PERIOD_FRAMES as usize;
    let mut interleaved = vec![0i16; chunk_frames * CHANNELS];
    let mut heard: [Vec<i16>; CHANNELS] = Default::default();
    let deadline = now() + TONE_TIMEOUT;
    let mut played = 0usize;
    let mut underruns = 0u32;
    // The stream's recovery counter is cumulative across all cases that ran on it;
    // this delta isolates what this window cost.
    let recoveries_before = capture.recoveries();

    while played < TONE_SAMPLES {
        if now() >= deadline {
            return Outcome::fail(format!(
                "the card took {played} of {TONE_SAMPLES} samples in {TONE_TIMEOUT:?}"
            ));
        }
        let chunk = chunk_frames.min(TONE_SAMPLES - played);
        let raw = &tone[played * WIRE_BYTES_PER_SAMPLE..(played + chunk) * WIRE_BYTES_PER_SAMPLE];
        let frames = expand_mono_to_stereo(raw, &mut interleaved);
        match sink.write_frames(&interleaved[..frames * CHANNELS]) {
            Ok(accepted) => played += accepted,
            Err(PlaybackFault::Underrun) => underruns += 1,
            Err(PlaybackFault::Fatal(why)) => {
                return Outcome::fail(format!(
                    "the playback stream stopped after {played} of {TONE_SAMPLES} samples: {why}"
                ));
            }
        }
        // Whatever capture has ready, without waiting for it: the write is what
        // paces this loop, and a wait here would be a wait the speaker's clock
        // has to make up for. A deadline already past is what makes the collection
        // a poll.
        let mut fold = |period: &[i16]| append_channels(period, &mut heard);
        if let Err(e) = collect_window(capture, DUPLEX_SAMPLES_PER_PASS, now(), now, &mut fold) {
            return Outcome::fail(format!("capture stopped under duplex: {e}"));
        }
    }

    let tone_facts = format!("played {TONE_SAMPLES} samples of {TONE_HZ} Hz");
    if underruns > 0 {
        return Outcome::Fail(vec![
            format!("{tone_facts}, but the device underran {underruns} time(s)"),
            "a recovered underrun means the stream ran dry while it was being written to at \
             rate, which is the pipeline's steady state"
                .to_string(),
        ]);
    }
    let window = heard[0].len();
    if window < DUPLEX_MIN_SAMPLES {
        return Outcome::fail(format!(
            "{tone_facts}, but capture delivered only {window} of {DUPLEX_MIN_SAMPLES} samples \
             while it did"
        ));
    }
    let stats: Vec<WaveformStats> = heard
        .iter()
        .map(|samples| WaveformStats::of(samples))
        .collect();
    match assess_waveforms(&stats, capture.recoveries() - recoveries_before) {
        Outcome::Pass(detail) => Outcome::Pass(format!("{tone_facts}; capture live: {detail}")),
        Outcome::Fail(mut lines) => {
            lines.insert(
                0,
                format!("{tone_facts}, but capture did not stay sane while it did:"),
            );
            Outcome::Fail(lines)
        }
        other => other,
    }
}

/// The tone the playback case writes, as raw S16_LE mono wire bytes — the format
/// the host sends speech in, so the case exercises the pipeline's own expansion
/// rather than a second one written for the test.
pub fn tone_pcm(samples: usize) -> Vec<u8> {
    let mut pcm = Vec::with_capacity(samples * WIRE_BYTES_PER_SAMPLE);
    for i in 0..samples {
        let phase = i as f32 * std::f32::consts::TAU * TONE_HZ / SAMPLE_RATE_HZ as f32;
        let sample = (phase.sin() * TONE_AMPLITUDE as f32) as i16;
        pcm.extend_from_slice(&sample.to_le_bytes());
    }
    pcm
}

/// The tone's frequency: well inside the band the board's speakers reproduce and
/// the microphones hear, and far enough from mains hum that a reading is not
/// arguing with it.
pub const TONE_HZ: f32 = 440.0;

/// Its amplitude, about a fifth of full scale. Audible across a room, and short
/// of the level at which the speakers or the echo canceller are being asked
/// something unusual — this case is about whether the path works, not about how
/// loud it goes.
pub const TONE_AMPLITUDE: i16 = 6_000;

/// How much of it is written: one second at the pipeline's rate.
pub const TONE_SAMPLES: usize = SAMPLE_RATE_HZ as usize;

/// How long that write may take before the card is declared unresponsive. Four
/// times the audio's own duration: a device taking it at rate needs a quarter of
/// this, and one that needs more than this is not taking it at rate.
pub const TONE_TIMEOUT: Duration = Duration::from_secs(4);

/// Periods the duplex half collects per write pass. A bound rather than a drain:
/// a card that always claims readiness must not be able to hold the tone up.
pub const DUPLEX_PERIODS_PER_PASS: usize = 2;

/// The same bound in frames.
pub const DUPLEX_SAMPLES_PER_PASS: usize = DUPLEX_PERIODS_PER_PASS * PERIOD_FRAMES as usize;

/// The least capture the duplex half will judge — half the tone's duration. Below
/// that the reading is too short to say anything about liveness, and the shortfall
/// itself is the finding.
pub const DUPLEX_MIN_SAMPLES: usize = SAMPLE_RATE_HZ as usize / 2;

/// One register's payload, or the one-line reading that says why there is none.
///
/// Every case reads through this: the retry budget, the status check and the two
/// failure readings are the registry's, not each case's. The failure is a string
/// rather than an [`Outcome`] because not every caller's verdict turns on it.
pub(crate) fn read_register<T: ControlTransport>(
    transport: &mut T,
    resid: u8,
    cmd: u8,
    payload: &mut [u8],
    label: &str,
) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    let (status, attempts) = control_read(transport, USB_RETRY, resid, cmd, payload)
        .map_err(|e| format!("{label} read failed: {e}"))?;
    if status != STATUS_DONE {
        return Err(format!(
            "{label} returned status 0x{status:02x} after {attempts} transaction(s)"
        ));
    }
    Ok(())
}

/// One four-f32 AEC reading, or the outcome that says why there is none.
pub(crate) fn read_f32x4<T: ControlTransport>(
    transport: &mut T,
    resid: u8,
    cmd: u8,
    read_len: usize,
    label: &str,
) -> Result<[f32; 4], Outcome>
where
    T::Error: fmt::Display,
{
    let mut payload = [0u8; 16];
    debug_assert_eq!(read_len, payload.len());
    read_register(transport, resid, cmd, &mut payload, label).map_err(Outcome::fail)?;
    Ok(decode_f32x4(&payload))
}

/// Four values, rendered for a human reviewing a reading.
pub(crate) fn render(values: &[f32; 4]) -> String {
    let rendered: Vec<String> = values.iter().map(|v| format!("{v:.4}")).collect();
    format!("[{}]", rendered.join(", "))
}

// ── The run ───────────────────────────────────────────────────────────────────

/// Run the registry in order, printing each case as it completes.
///
/// The hardware-touching shell: everything below the presence case is sequenced by
/// [`run_control_plane`], which a scripted transport can drive off the device.
pub fn run(out: &mut dyn io::Write) -> io::Result<Report> {
    let mut report = Report::default();
    writeln!(
        out,
        "reachy-pod selftest — observing the device as uid {}",
        effective_uid()
    )?;

    let (outcome, transport) = usb_presence();
    report.record(out, "usb_presence", outcome)?;

    match transport {
        Some(mut transport) => {
            let generation = transport.generation();
            run_control_plane(out, &mut report, Some((&mut transport, generation)))?;
        }
        None => run_control_plane::<UsbControl>(out, &mut report, None)?,
    }

    audio_plane_on_hardware(out, &mut report)?;

    writeln!(out, "{}", report.summary())?;
    Ok(report)
}

/// Open both directions of the card, then hand the sequencing to
/// [`run_audio_plane`].
///
/// The only part of the audio plane that touches hardware: the PCMs are owned here
/// because the period reader and the writer borrow them, and everything downstream
/// of those borrows is off-device testable.
///
/// Playback is opened on the card `alsa_params` just resolved rather than by a
/// second lookup: the echo canceller only removes what the board itself played, so
/// the two directions have to be provably the same device.
fn audio_plane_on_hardware(out: &mut dyn io::Write, report: &mut Report) -> io::Result<()> {
    let (params, opened) = alsa_params();
    let Some((card, capture_pcm)) = opened else {
        return run_audio_plane::<CaptureStream, AlsaOut>(
            out,
            report,
            params,
            Err(NO_CAPTURE_STREAM.to_string()),
            Err("no open card to play through".to_string()),
        );
    };
    let mut source = capture_source(Some(&capture_pcm));
    let playback_pcm = open_playback_on(&card);
    let mut sink = match &playback_pcm {
        Ok(pcm) => AlsaOut::new(pcm).map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    run_audio_plane(
        out,
        report,
        params,
        source.as_mut().map_err(|e| e.clone()),
        sink.as_mut().map_err(|e| e.clone()),
    )
}

/// The reader both run entry points take their audio from, or the one reason there
/// is none — so the registry says the same thing whichever entry point could not
/// open the stream.
fn capture_source(pcm: Option<&PCM>) -> Result<CaptureStream<'_>, String> {
    match pcm {
        Some(pcm) => CaptureStream::new(pcm).map_err(|e| e.to_string()),
        None => Err(NO_CAPTURE_STREAM.to_string()),
    }
}

/// What every case that needed the capture stream reports when there is none.
const NO_CAPTURE_STREAM: &str = "no open capture stream to read";

/// Record [`AUDIO_PLANE_CASES`], in order.
///
/// The control plane and the audio plane are reached through different kernel
/// drivers — EP0 through libusb, the audio interfaces through `snd_usb_audio` — so
/// the audio cases run whether or not the control cases got a handle: a board that
/// answers no control transfer and still streams audio is a different finding from
/// one that does neither.
///
/// `source` and `sink` are what the opens produced, or the reason there is nothing
/// to read from and nothing to play through. Binding the later cases to those
/// streams is the point: they must judge the configuration `alsa_params` just
/// accepted, not a second open.
pub fn run_audio_plane<S: PeriodSource, O: StereoOut>(
    out: &mut dyn io::Write,
    report: &mut Report,
    params: Outcome,
    source: Result<&mut S, String>,
    sink: Result<&mut O, String>,
) -> io::Result<()> {
    report.record(out, AUDIO_PLANE_CASES[0], params)?;
    let mut source = source;
    let waveform = match &mut source {
        Ok(source) => alsa_waveform_sanity(*source, &Instant::now),
        Err(why) => Outcome::NotRun(why.clone()),
    };
    report.record(out, AUDIO_PLANE_CASES[1], waveform)?;
    let playback = match (&mut source, sink) {
        (Ok(source), Ok(sink)) => alsa_playback(sink, *source, &Instant::now),
        (Err(why), _) => Outcome::NotRun(why.clone()),
        (_, Err(why)) => Outcome::NotRun(why),
    };
    report.record(out, AUDIO_PLANE_CASES[2], playback)
}

/// Record [`CONTROL_PLANE_CASES`], in order, against an open board — or as not-run
/// when there is none.
///
/// This is what binds each case name to the register it reads, so a swapped label
/// would put the wrong register in front of the human reviewing a reading.
pub fn run_control_plane<T: ControlTransport>(
    out: &mut dyn io::Write,
    report: &mut Report,
    board: Option<(&mut T, Generation)>,
) -> io::Result<()>
where
    T::Error: fmt::Display,
{
    match board {
        Some((transport, generation)) => {
            let version = ctrl_version(transport, generation);
            report.record(out, CONTROL_PLANE_CASES[0], version)?;
            let spenergy = ctrl_spenergy(transport);
            report.record(out, CONTROL_PLANE_CASES[1], spenergy)?;
            let doa = ctrl_doa(transport);
            report.record(out, CONTROL_PLANE_CASES[2], doa)?;
        }
        None => {
            for name in CONTROL_PLANE_CASES {
                let outcome = Outcome::NotRun("no open board to talk to".to_string());
                report.record(out, name, outcome)?;
            }
        }
    }
    Ok(())
}

// ── The bench run ─────────────────────────────────────────────────────────────

/// The cases a human has to be at the bench for, in the order they run.
///
/// Kept out of [`run`] rather than skipped inside it: a case that needs someone to
/// speak would otherwise fail on an unattended device for a reason that says
/// nothing about the hardware, and a registry whose green depends on who is in the
/// room is a registry nobody trusts.
pub const MANUAL_CASES: [&str; 1] = ["beam_energy_speech"];

/// Run the bench registry: the two opens the bench case needs, then the case.
///
/// The presence cases are recorded rather than merely performed, because a bench
/// run that could not open one of the two interfaces has to say which one before it
/// says anything about beams.
pub fn run_manual(out: &mut dyn io::Write) -> io::Result<Report> {
    let mut report = Report::default();
    writeln!(
        out,
        "reachy-pod selftest --manual — observing the device as uid {}",
        effective_uid()
    )?;
    writeln!(
        out,
        "     this run needs someone at the bench: it will ask you to speak at the array"
    )?;

    let (presence, transport) = usb_presence();
    report.record(out, "usb_presence", presence)?;
    let (params, opened) = alsa_params();
    report.record(out, AUDIO_PLANE_CASES[0], params)?;

    let mut transport = transport.ok_or_else(|| "no open board to talk to".to_string());
    let capture_pcm = opened.map(|(_, pcm)| pcm);
    let mut source = capture_source(capture_pcm.as_ref());
    run_manual_plane(
        out,
        &mut report,
        transport.as_mut().map_err(|e| e.clone()),
        source.as_mut().map_err(|e| e.clone()),
        &Instant::now,
    )?;

    writeln!(out, "{}", report.summary())?;
    Ok(report)
}

/// Record [`MANUAL_CASES`], in order.
///
/// The bench case is the one that needs both interfaces at once — the chip's own
/// reading of the room and the audio the stream delivered — so unlike the audio
/// plane there is nothing to run when either is missing, and which one it was is
/// the reason recorded.
pub fn run_manual_plane<T: ControlTransport, S: PeriodSource>(
    out: &mut dyn io::Write,
    report: &mut Report,
    board: Result<&mut T, String>,
    source: Result<&mut S, String>,
    now: &dyn Fn() -> Instant,
) -> io::Result<()>
where
    T::Error: fmt::Display,
{
    let outcome = match (board, source) {
        (Ok(transport), Ok(source)) => beam_energy_speech(transport, source, out, now)?,
        (Err(why), _) => Outcome::NotRun(why),
        (_, Err(why)) => Outcome::NotRun(why),
    };
    report.record(out, MANUAL_CASES[0], outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Clock, Scripted, ScriptedCard, detail, f32x4_bytes};
    use xvf3800_ctrl::STATUS_RETRY;

    // ── ctrl_version ──────────────────────────────────────────────────────────

    #[test]
    fn a_current_firmware_version_passes_and_is_reported() {
        let mut t = Scripted::answering(STATUS_DONE, vec![2, 1, 3]);
        let outcome = ctrl_version(&mut t, Generation::ReachyFirmware);
        assert!(outcome.passed(), "{}", detail(&outcome));
        assert!(detail(&outcome).contains("2.1.3"), "{}", detail(&outcome));
    }

    #[test]
    fn old_firmware_fails_on_the_reachy_id_and_passes_on_the_pre_update_one() {
        let mut t = Scripted::answering(STATUS_DONE, vec![1, 0, 4]);
        let reachy = ctrl_version(&mut t, Generation::ReachyFirmware);
        assert!(!reachy.passed());
        assert!(
            detail(&reachy).contains("1.0.4") && detail(&reachy).contains("2.1.0"),
            "{}",
            detail(&reachy)
        );
        // The same reading under the pre-update id is expected, not a finding.
        let mut t = Scripted::answering(STATUS_DONE, vec![1, 0, 4]);
        let legacy = ctrl_version(&mut t, Generation::LegacyModule);
        assert!(legacy.passed(), "{}", detail(&legacy));
    }

    #[test]
    fn an_all_zero_version_is_not_a_version() {
        let mut t = Scripted::answering(STATUS_DONE, vec![0, 0, 0]);
        let outcome = ctrl_version(&mut t, Generation::LegacyModule);
        assert!(!outcome.passed());
        assert!(detail(&outcome).contains("0.0.0"), "{}", detail(&outcome));
    }

    #[test]
    fn a_fatal_status_and_a_dead_transport_are_distinguishable_failures() {
        // A status the retry driver does not retry: reported with the status byte.
        let mut t = Scripted::answering(0x02, vec![9, 9, 9]);
        let status = ctrl_version(&mut t, Generation::ReachyFirmware);
        assert!(!status.passed());
        assert!(detail(&status).contains("0x02"), "{}", detail(&status));
        assert_eq!(t.reads, 1, "a fatal status is not retried");

        let mut t = Scripted::failing("no such device");
        let broken = ctrl_version(&mut t, Generation::ReachyFirmware);
        assert!(!broken.passed());
        assert!(
            detail(&broken).contains("no such device"),
            "{}",
            detail(&broken)
        );
    }

    #[test]
    fn a_transient_status_is_retried_under_the_usb_budget() {
        let mut t = Scripted::answering(STATUS_RETRY, vec![2, 1, 0]);
        let outcome = ctrl_version(&mut t, Generation::ReachyFirmware);
        assert!(!outcome.passed());
        // 1 + USB_RETRY.max_retries transactions, each re-issue preceded by a sleep.
        assert_eq!(t.reads, USB_RETRY.max_retries + 1);
        assert_eq!(t.delays, USB_RETRY.max_retries);
    }

    // ── ctrl_spenergy / ctrl_doa ──────────────────────────────────────────────

    #[test]
    fn plausible_telemetry_passes_with_the_reading_in_the_detail() {
        let mut t = Scripted::f32x4([0.0, 0.5, 1.25, 12.0]);
        let energy = ctrl_spenergy(&mut t);
        assert!(energy.passed(), "{}", detail(&energy));
        assert!(detail(&energy).contains("1.2500"), "{}", detail(&energy));

        // NaN azimuths are the normal reading for a beam with nothing focused.
        let mut t = Scripted::f32x4([f32::NAN, 0.0, -1.5, 1.5]);
        let doa = ctrl_doa(&mut t);
        assert!(doa.passed(), "{}", detail(&doa));
        assert!(detail(&doa).contains("NaN"), "{}", detail(&doa));
    }

    #[test]
    fn an_implausible_beam_is_named_by_index_with_the_whole_reading() {
        let mut t = Scripted::f32x4([0.1, -0.2, 0.3, 0.4]);
        let energy = ctrl_spenergy(&mut t);
        assert!(!energy.passed());
        assert!(
            detail(&energy).contains("beam 1") && detail(&energy).contains("0.4000"),
            "{}",
            detail(&energy)
        );

        let mut t = Scripted::f32x4([0.0, 0.0, 0.0, f32::INFINITY]);
        let doa = ctrl_doa(&mut t);
        assert!(!doa.passed());
        assert!(detail(&doa).contains("beam 3"), "{}", detail(&doa));
    }

    #[test]
    fn a_telemetry_read_that_never_completes_names_the_register() {
        let mut t = Scripted::failing("pipe error");
        let outcome = ctrl_spenergy(&mut t);
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("resid 33 cmd 80") && detail(&outcome).contains("pipe error"),
            "{}",
            detail(&outcome)
        );
    }

    // ── The run's sequencing ──────────────────────────────────────────────────

    #[test]
    fn the_control_plane_runs_every_case_in_order_against_its_own_register() {
        let mut t = Scripted::sequenced(vec![
            (STATUS_DONE, vec![2, 1, 0]),
            (STATUS_DONE, f32x4_bytes([0.1, 0.2, 0.3, 0.4])),
            (STATUS_DONE, f32x4_bytes([0.0, 1.0, -1.0, f32::NAN])),
        ]);
        let mut out = Vec::new();
        let mut report = Report::default();
        run_control_plane(
            &mut out,
            &mut report,
            Some((&mut t, Generation::ReachyFirmware)),
        )
        .expect("record");

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(
            names, CONTROL_PLANE_CASES,
            "the transcript must carry every control-plane case, in the registry's order"
        );
        assert_eq!(
            t.registers,
            vec![
                (APPLICATION_SERVICER_RESID, VERSION_CMD),
                (AEC_RESID, AEC_SPENERGY_VALUES_CMD),
                (AEC_RESID, AEC_AZIMUTH_VALUES_CMD),
            ],
            "each case must read the register its name promises"
        );
        assert!(report.all_passed(), "{}", report.summary());
        // Each case's detail is its own reading, so a swapped label shows up here.
        assert!(
            detail(&report.cases[0].outcome).contains("2.1.0"),
            "{}",
            detail(&report.cases[0].outcome)
        );
        assert!(
            detail(&report.cases[1].outcome).contains("0.4000"),
            "{}",
            detail(&report.cases[1].outcome)
        );
        assert!(
            detail(&report.cases[2].outcome).contains("NaN"),
            "{}",
            detail(&report.cases[2].outcome)
        );
    }

    #[test]
    fn no_open_board_records_every_control_case_as_not_run() {
        let mut out = Vec::new();
        let mut report = Report::default();
        run_control_plane::<Scripted>(&mut out, &mut report, None).expect("record");

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(
            names, CONTROL_PLANE_CASES,
            "a run that never opened the board still accounts for every case"
        );
        for case in &report.cases {
            match &case.outcome {
                Outcome::NotRun(reason) => {
                    assert!(
                        !reason.is_empty(),
                        "{} must say why it did not run",
                        case.name
                    );
                }
                other => panic!("{} must be recorded NotRun, got {other:?}", case.name),
            }
        }
        assert!(
            !report.all_passed(),
            "cases that did not run asserted nothing"
        );
    }

    // ── The audio plane ───────────────────────────────────────────────────────

    /// A correlated, quiet-room-shaped window: a slow sine at a level a real quiet
    /// room reaches, so it must pass every gate without a loudness floor helping it.
    fn quiet_speech(samples: usize) -> Vec<i16> {
        (0..samples)
            .map(|i| {
                let phase = i as f32 * std::f32::consts::TAU / 40.0;
                (phase.sin() * 120.0) as i16
            })
            .collect()
    }

    // The classifier itself — the five thresholds, the defect precedence, and each
    // broken shape — is pinned in `audio_pipeline::ring`, which owns it for every
    // liveness check in the tree. What is asserted here is this registry's use of it.

    #[test]
    fn the_verdict_covers_every_channel_and_reports_each_ones_reading() {
        let live = WaveformStats::of(&quiet_speech(2_000));
        let dead = WaveformStats::of(&[0i16; 2_000]);

        let both = assess_waveforms(&[live, live], 0);
        assert!(both.passed(), "{}", detail(&both));
        assert!(
            detail(&both).contains("ch0") && detail(&both).contains("ch1"),
            "both readings must be in the detail: {}",
            detail(&both)
        );

        // One dead channel fails the case even though the other is fine, and the
        // failure names which one.
        let one_dead = assess_waveforms(&[live, dead], 0);
        assert!(!one_dead.passed());
        let rendered = detail(&one_dead);
        assert!(rendered.contains("ch1 is all-zero"), "{rendered}");
        assert!(!rendered.contains("ch0 is"), "{rendered}");
    }

    #[test]
    fn recovered_overruns_are_reported_alongside_a_pass() {
        let live = WaveformStats::of(&quiet_speech(2_000));
        let clean = assess_waveforms(&[live], 0);
        assert!(!detail(&clean).contains("overrun"), "{}", detail(&clean));
        let bumpy = assess_waveforms(&[live], 3);
        assert!(bumpy.passed());
        assert!(
            detail(&bumpy).contains("3 overrun(s)"),
            "{}",
            detail(&bumpy)
        );
    }

    #[test]
    fn a_windows_reading_renders_every_statistic_a_reviewer_needs() {
        let stats = WaveformStats::of(&quiet_speech(1_000));
        let rendered = stats.to_string();
        for field in ["min=", "max=", "rms=", "sat=", "ac1=", "samples=1000"] {
            assert!(rendered.contains(field), "{field} missing from {rendered}");
        }
    }

    /// `frames` interleaved stereo frames of the quiet-room-shaped waveform above,
    /// continuing from absolute sample `from` so consecutive periods form one
    /// unbroken signal. The two channels are told apart by sign.
    fn stereo(frames: usize, from: usize) -> Vec<i16> {
        quiet_speech(from + frames)[from..]
            .iter()
            .flat_map(|v| [*v, -*v])
            .collect()
    }

    #[test]
    fn the_window_is_a_whole_one_per_channel_however_the_periods_divide_it() {
        // 300 frames is not a divisor of the window, so the last period overshoots
        // and must be truncated rather than judged long, and the two channels must
        // stay in their own buffers across every one of them.
        let period = 300;
        let count = WAVEFORM_SAMPLES.div_ceil(period) + 2;
        let periods: Vec<Vec<i16>> = (0..count).map(|i| stereo(period, i * period)).collect();
        let mut card = ScriptedCard::delivering(periods);
        let clock = Clock::new();
        let outcome = alsa_waveform_sanity(&mut card, &|| clock.now());
        // Live on both channels: an ascending ramp is neither zero, stuck nor
        // saturated, and it correlates.
        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("samples={WAVEFORM_SAMPLES}")),
            "exactly one window per channel is judged: {rendered}"
        );
        assert!(
            rendered.contains("ch0") && rendered.contains("ch1"),
            "{rendered}"
        );
        assert!(
            card.remaining() >= 2,
            "collection stops at the window rather than draining the card"
        );
    }

    #[test]
    fn a_card_that_stops_delivering_ends_the_case_on_its_own_error() {
        let mut card = ScriptedCard::delivering(vec![stereo(320, 0)]);
        let clock = Clock::new();
        let outcome = alsa_waveform_sanity(&mut card, &|| clock.now());
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("the card stopped delivering"),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_stalled_card_is_reported_within_the_bound_rather_than_hung_on() {
        // The failure the bound exists for: the card opened, took the parameters and
        // then delivered nothing. Every wait must be bounded by what is left of the
        // deadline, and the case must end saying how little arrived.
        let mut card = ScriptedCard::stalled();
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(500));
            now
        };
        let outcome = alsa_waveform_sanity(&mut card, &advancing);
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("0 of {WAVEFORM_SAMPLES}")),
            "the reading must name the shortfall: {rendered}"
        );
        assert!(
            rendered.contains(&format!("{WAVEFORM_CAPTURE_TIMEOUT:?}")),
            "and the bound it was measured against: {rendered}"
        );
        assert!(!card.waits.is_empty(), "the case must actually wait");
        assert!(
            card.waits.iter().all(|w| *w <= WAVEFORM_CAPTURE_TIMEOUT),
            "no wait may outlast the deadline: {:?}",
            card.waits
        );
        assert!(
            card.waits.windows(2).all(|w| w[1] < w[0]),
            "each wait is what is left of the deadline: {:?}",
            card.waits
        );
    }

    #[test]
    fn the_audio_plane_runs_every_case_in_order_against_the_stream_that_opened() {
        let period = 320;
        let periods: Vec<Vec<i16>> = (0..WAVEFORM_SAMPLES.div_ceil(period) + tone_periods(period))
            .map(|i| stereo(period, i * period))
            .collect();
        let mut card = ScriptedCard::delivering(periods).quiet_when_drained();
        card.recoveries = 2;
        let mut sink = ScriptedOut::taking_everything();
        let mut out = Vec::new();
        let mut report = Report::default();
        run_audio_plane(
            &mut out,
            &mut report,
            Outcome::Pass("card 1 accepts 16000 Hz S16_LE 2 ch".to_string()),
            Ok(&mut card),
            Ok(&mut sink),
        )
        .expect("record");

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(
            names, AUDIO_PLANE_CASES,
            "the transcript must carry every audio-plane case, in the registry's order"
        );
        assert!(report.all_passed(), "{}", report.summary());
        // Each case's detail is its own reading, so a swapped label shows up here.
        assert!(
            detail(&report.cases[0].outcome).contains("S16_LE"),
            "{}",
            detail(&report.cases[0].outcome)
        );
        assert!(
            detail(&report.cases[1].outcome).contains("2 overrun(s)"),
            "the waveform verdict must come from the stream the open returned: {}",
            detail(&report.cases[1].outcome)
        );
        assert!(
            detail(&report.cases[2].outcome).contains(&format!("{TONE_HZ} Hz")),
            "{}",
            detail(&report.cases[2].outcome)
        );
        assert_eq!(
            sink.written.len(),
            TONE_SAMPLES * CHANNELS,
            "the playback verdict must come from the sink the open returned"
        );
    }

    #[test]
    fn no_open_card_records_the_cases_that_needed_it_as_not_run() {
        let mut out = Vec::new();
        let mut report = Report::default();
        run_audio_plane::<ScriptedCard, ScriptedOut>(
            &mut out,
            &mut report,
            Outcome::fail("hw:1,0 refused 16000 Hz S16_LE 2 ch"),
            Err("no open capture stream to read".to_string()),
            Err("no open card to play through".to_string()),
        )
        .expect("record");

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(
            names, AUDIO_PLANE_CASES,
            "a run that never opened the card still accounts for every case"
        );
        for case in &report.cases[1..] {
            match &case.outcome {
                Outcome::NotRun(reason) => {
                    assert!(!reason.is_empty(), "{} must say why", case.name)
                }
                other => panic!("{} must be recorded NotRun, got {other:?}", case.name),
            }
        }
        assert!(
            !report.all_passed(),
            "a case that did not run asserted nothing"
        );
    }

    #[test]
    fn a_card_that_reads_but_will_not_play_still_runs_the_capture_cases() {
        // The two directions are separate streams on one device, and a board that
        // captures and refuses to play is a different finding from one that does
        // neither. The capture cases must reach their verdicts either way.
        let period = 320;
        let periods: Vec<Vec<i16>> = (0..WAVEFORM_SAMPLES.div_ceil(period))
            .map(|i| stereo(period, i * period))
            .collect();
        let mut card = ScriptedCard::delivering(periods).quiet_when_drained();
        let mut out = Vec::new();
        let mut report = Report::default();
        run_audio_plane::<ScriptedCard, ScriptedOut>(
            &mut out,
            &mut report,
            Outcome::Pass("card 1 accepts 16000 Hz S16_LE 2 ch".to_string()),
            Ok(&mut card),
            Err("hw:1,0 refused the playback direction".to_string()),
        )
        .expect("record");

        assert!(
            report.cases[1].outcome.passed(),
            "{}",
            detail(&report.cases[1].outcome)
        );
        match &report.cases[2].outcome {
            Outcome::NotRun(reason) => assert!(reason.contains("playback direction"), "{reason}"),
            other => panic!("the playback case must be recorded NotRun, got {other:?}"),
        }
    }

    #[test]
    fn a_card_that_plays_but_will_not_read_reports_the_capture_reason() {
        // The mirror of the case above, and the one whose arm *ordering* matters:
        // the playback case needs both streams, and with only the sink it must say
        // which one it lacked. A board that plays and will not read is a capture
        // finding, and nothing may reach a sink whose case never ran.
        let mut sink = ScriptedOut::taking_everything();
        let mut out = Vec::new();
        let mut report = Report::default();
        run_audio_plane::<ScriptedCard, ScriptedOut>(
            &mut out,
            &mut report,
            Outcome::Pass("card 1 accepts 16000 Hz S16_LE 2 ch".to_string()),
            Err(NO_CAPTURE_STREAM.to_string()),
            Ok(&mut sink),
        )
        .expect("record");

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(names, AUDIO_PLANE_CASES);
        for case in &report.cases[1..] {
            match &case.outcome {
                Outcome::NotRun(reason) => assert_eq!(
                    reason, NO_CAPTURE_STREAM,
                    "{} must name the stream it lacked, not the other interface",
                    case.name
                ),
                other => panic!("{} must be recorded NotRun, got {other:?}", case.name),
            }
        }
        assert!(
            sink.written.is_empty(),
            "a case that did not run played nothing"
        );
    }

    // ── The bench run's sequencing ────────────────────────────────────────────

    /// A board the bench case can read: the two output-routing registers it takes
    /// once per run, then AEC readings for as long as it asks.
    fn bench_board() -> Scripted {
        Scripted::sequenced(vec![
            (STATUS_DONE, vec![1, 4]),
            (STATUS_DONE, vec![1, 5]),
            (STATUS_DONE, f32x4_bytes([1.0; 4])),
        ])
    }

    /// A bench plane against a card that never delivers: the case runs, fails on
    /// its first tick and costs the test no wall time.
    fn stalled_bench_plane(
        report: &mut Report,
        board: Result<&mut Scripted, String>,
        source: Result<&mut ScriptedCard, String>,
    ) {
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(250));
            now
        };
        let mut out = Vec::new();
        run_manual_plane(&mut out, report, board, source, &advancing).expect("record");
    }

    #[test]
    fn the_bench_plane_records_its_case_when_both_interfaces_are_there() {
        let mut board = bench_board();
        let mut card = ScriptedCard::stalled();
        let mut report = Report::default();
        stalled_bench_plane(&mut report, Ok(&mut board), Ok(&mut card));

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(
            names, MANUAL_CASES,
            "a bench run accounts for every manual case, in the registry's order"
        );
        match &report.cases[0].outcome {
            // The case ran: a card that delivers nothing is a reading about the
            // card, not a case that could not be attempted.
            Outcome::Fail(lines) => assert!(
                lines[0].contains("the card delivered"),
                "{:?}",
                report.cases[0].outcome
            ),
            other => panic!("the bench case must have run, got {other:?}"),
        }
    }

    #[test]
    fn a_bench_run_without_a_board_names_the_board() {
        let mut card = ScriptedCard::delivering(vec![vec![0i16; 4]; 100]);
        let mut report = Report::default();
        stalled_bench_plane(
            &mut report,
            Err("no open board to talk to".to_string()),
            Ok(&mut card),
        );

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(names, MANUAL_CASES);
        match &report.cases[0].outcome {
            Outcome::NotRun(reason) => assert!(reason.contains("board"), "{reason}"),
            other => panic!("the bench case must be recorded NotRun, got {other:?}"),
        }
        assert_eq!(
            card.waits.len(),
            0,
            "the case cannot be attempted, so the card is never asked for audio"
        );
    }

    #[test]
    fn a_bench_run_without_a_capture_stream_names_the_stream_and_not_the_board() {
        // The bench case needs both interfaces, so which one is missing is the whole
        // of what it can report. Transposing the two arms would send the operator to
        // the wrong interface.
        let mut board = Scripted::f32x4([1.0; 4]);
        let mut report = Report::default();
        stalled_bench_plane(
            &mut report,
            Ok(&mut board),
            Err(NO_CAPTURE_STREAM.to_string()),
        );

        let names: Vec<&str> = report.cases.iter().map(|c| c.name).collect();
        assert_eq!(names, MANUAL_CASES);
        match &report.cases[0].outcome {
            Outcome::NotRun(reason) => assert_eq!(reason, NO_CAPTURE_STREAM),
            other => panic!("the bench case must be recorded NotRun, got {other:?}"),
        }
        assert_eq!(board.reads, 0, "nor is the board asked for a reading");
    }

    // ── collect_window ────────────────────────────────────────────────────────

    /// Frames per period, for a fold that only counts.
    fn frames(period: &[i16]) -> usize {
        period.len() / CHANNELS
    }

    #[test]
    fn a_window_stops_on_the_period_that_completes_it() {
        let mut card = ScriptedCard::delivering((0..4).map(|i| stereo(100, i * 100)).collect());
        let clock = Clock::new();
        let collected = collect_window(
            &mut card,
            250,
            clock.now() + Duration::from_secs(1),
            &|| clock.now(),
            &mut frames,
        )
        .expect("a healthy card");

        assert_eq!(
            collected, 300,
            "a period is read whole, so the count is the first one at or past the window"
        );
        assert_eq!(card.remaining(), 1, "and nothing beyond it is taken");
    }

    #[test]
    fn a_not_ready_answer_inside_the_window_waits_again_rather_than_ending_it() {
        // An xrun recovered from while waiting reaches the caller as a not-ready
        // answer, and a window that ended on one would report a short reading for a
        // stream that is fine.
        let mut card = ScriptedCard::delivering((0..2).map(|i| stereo(100, i * 100)).collect())
            .answering_waits(vec![false, true]);
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(100));
            now
        };
        let deadline = clock.now() + Duration::from_secs(1);
        let collected = collect_window(&mut card, 200, deadline, &advancing, &mut frames)
            .expect("a healthy card");

        assert_eq!(
            collected, 200,
            "the not-ready answer cost a wait, not the window"
        );
        assert!(
            card.waits.windows(2).all(|w| w[1] < w[0]),
            "each wait is what is left of the deadline: {:?}",
            card.waits
        );
    }

    #[test]
    fn a_deadline_already_past_is_a_poll_that_takes_only_what_is_there() {
        // The duplex case's regime: never wait, take whatever the card has ready,
        // and stop at its first quiet answer however far short of the count it is.
        let mut card = ScriptedCard::delivering((0..2).map(|i| stereo(100, i * 100)).collect())
            .quiet_when_drained();
        let clock = Clock::new();
        let collected =
            collect_window(&mut card, 10_000, clock.now(), &|| clock.now(), &mut frames)
                .expect("a card with nothing more to give has not failed");

        assert_eq!(collected, 200);
        assert!(
            card.waits.iter().all(|w| w.is_zero()),
            "a past deadline leaves nothing to wait with: {:?}",
            card.waits
        );
    }

    #[test]
    fn a_source_that_answers_ready_and_delivers_nothing_is_still_bounded() {
        // A read that yields no frames makes no progress. A loop that consulted its
        // deadline only on a not-ready answer would never reach it, and the registry
        // would hang on the bench with no output and no exit status — the worst
        // ending an on-hardware run can have.
        let mut card = ScriptedCard::delivering(vec![Vec::new(); 20_000]);
        let clock = Clock::new();
        let collected = collect_window(&mut card, 1_000, clock.now(), &|| clock.now(), &mut frames)
            .expect("a period with nothing in it is not a stream error");
        assert_eq!(collected, 0);
        assert_eq!(card.waits.len(), 1, "a spent deadline ends it at once");

        // With time left it is the deadline that ends it, not the count.
        let mut card = ScriptedCard::delivering(vec![Vec::new(); 20_000]);
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(100));
            now
        };
        let deadline = clock.now() + Duration::from_secs(1);
        let collected = collect_window(&mut card, 1_000, deadline, &advancing, &mut frames)
            .expect("a period with nothing in it is not a stream error");
        assert_eq!(collected, 0);
        assert!(
            card.waits.len() <= 12,
            "one wait per 100 ms of the deadline, not a spin: {}",
            card.waits.len()
        );
    }

    #[test]
    fn a_stream_that_fails_ends_the_window_on_its_own_error() {
        let mut card = ScriptedCard::delivering(vec![stereo(100, 0)]);
        let clock = Clock::new();
        let e = collect_window(&mut card, 1_000, clock.now(), &|| clock.now(), &mut frames)
            .expect_err("the card stopped mid-window");

        assert!(e.to_string().contains("the card stopped delivering"), "{e}");
    }

    #[test]
    fn the_waveform_window_is_two_seconds_of_the_pipelines_own_rate() {
        assert_eq!(WAVEFORM_SAMPLES as u32, 2 * SAMPLE_RATE_HZ);
        // The capture bound must outlast the audio it is waiting for, or a healthy
        // card fails the case on a stopwatch.
        assert!(
            WAVEFORM_CAPTURE_TIMEOUT.as_secs_f32()
                > WAVEFORM_SAMPLES as f32 / SAMPLE_RATE_HZ as f32
        );
    }

    // ── alsa_playback ─────────────────────────────────────────────────────────

    /// A playback sink the test writes the device's behavior into.
    ///
    /// `accepts` is the frames taken per call, in order, with the last entry
    /// repeating; `underrun_at` and `fatal_at` are the call numbers (1-based) that
    /// fail instead. Everything taken is kept, so a test can assert what actually
    /// reached the card and in what order.
    struct ScriptedOut {
        accepts: std::collections::VecDeque<usize>,
        underrun_at: Option<usize>,
        fatal_at: Option<usize>,
        calls: usize,
        written: Vec<i16>,
    }

    impl ScriptedOut {
        fn accepting(accepts: Vec<usize>) -> Self {
            assert!(!accepts.is_empty(), "a script needs at least one answer");
            Self {
                accepts: accepts.into(),
                underrun_at: None,
                fatal_at: None,
                calls: 0,
                written: Vec::new(),
            }
        }

        /// A device that takes whatever it is offered, the healthy case.
        fn taking_everything() -> Self {
            Self::accepting(vec![usize::MAX])
        }
    }

    impl StereoOut for ScriptedOut {
        fn write_frames(&mut self, samples: &[i16]) -> Result<usize, PlaybackFault> {
            self.calls += 1;
            if self.fatal_at == Some(self.calls) {
                return Err(PlaybackFault::Fatal("the device disappeared".to_string()));
            }
            if self.underrun_at == Some(self.calls) {
                return Err(PlaybackFault::Underrun);
            }
            let offered = samples.len() / CHANNELS;
            let scripted = if self.accepts.len() > 1 {
                self.accepts.pop_front().expect("checked non-empty")
            } else {
                *self.accepts.front().expect("checked non-empty")
            };
            let taken = scripted.min(offered);
            self.written.extend_from_slice(&samples[..taken * CHANNELS]);
            Ok(taken)
        }
    }

    /// Periods of `period` frames the duplex half can collect while the tone plays:
    /// two per write pass, and the tone is written a period at a time.
    fn tone_periods(period: usize) -> usize {
        TONE_SAMPLES.div_ceil(period) * DUPLEX_PERIODS_PER_PASS
    }

    /// A card delivering `count` periods of live stereo, idle once they run out.
    fn duplex_card(count: usize) -> ScriptedCard {
        let period = PERIOD_FRAMES as usize;
        let periods: Vec<Vec<i16>> = (0..count).map(|i| stereo(period, i * period)).collect();
        ScriptedCard::delivering(periods).quiet_when_drained()
    }

    /// The tone as it should reach the card: every sample in both channels.
    fn expected_stereo_tone() -> Vec<i16> {
        tone_pcm(TONE_SAMPLES)
            .chunks_exact(WIRE_BYTES_PER_SAMPLE)
            .flat_map(|s| {
                let v = i16::from_le_bytes([s[0], s[1]]);
                [v; CHANNELS]
            })
            .collect()
    }

    #[test]
    fn a_second_of_tone_plays_and_the_capture_it_hears_stays_live() {
        let mut sink = ScriptedOut::taking_everything();
        let mut card = duplex_card(60);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("{TONE_SAMPLES} samples of {TONE_HZ} Hz")),
            "{rendered}"
        );
        assert!(
            rendered.contains("ch0") && rendered.contains("ch1"),
            "both channels' readings belong in the detail: {rendered}"
        );
        assert_eq!(
            sink.written,
            expected_stereo_tone(),
            "every sample of the tone reaches the card, in both channels, in order"
        );
    }

    #[test]
    fn a_short_write_is_resumed_from_where_it_stopped() {
        // A device that takes a hundred frames at a time: the tone still lands
        // whole and in order, which is what says the case tracks its own offset
        // rather than assuming the write took everything.
        let mut sink = ScriptedOut::accepting(vec![100]);
        let mut card = duplex_card(60);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(outcome.passed(), "{}", detail(&outcome));
        assert_eq!(sink.written, expected_stereo_tone());
    }

    #[test]
    fn a_recovered_underrun_fails_the_case_and_says_how_many() {
        let mut sink = ScriptedOut::taking_everything();
        sink.underrun_at = Some(3);
        let mut card = duplex_card(60);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("underran 1 time(s)"),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_dead_playback_stream_ends_the_case_on_the_devices_own_reason() {
        let mut sink = ScriptedOut::taking_everything();
        sink.fatal_at = Some(2);
        let mut card = duplex_card(60);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("the device disappeared"), "{rendered}");
        assert!(
            rendered.contains(&format!("of {TONE_SAMPLES} samples")),
            "the reading must say how far it got: {rendered}"
        );
    }

    #[test]
    fn a_capture_that_dies_under_duplex_is_the_finding() {
        // The reason the capture stream is threaded through this case at all: the
        // board is one device carrying both directions, and a capture that stops
        // when the speakers start is what full duplex is being asked about.
        let mut sink = ScriptedOut::taking_everything();
        let mut card = ScriptedCard::delivering(vec![stereo(PERIOD_FRAMES as usize, 0)]);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("capture stopped under duplex"),
            "{rendered}"
        );
        assert!(
            rendered.contains("the card stopped delivering"),
            "{rendered}"
        );
    }

    #[test]
    fn a_silent_capture_under_duplex_fails_with_the_channel_named() {
        let mut sink = ScriptedOut::taking_everything();
        let period = PERIOD_FRAMES as usize;
        let dead: Vec<Vec<i16>> = (0..60).map(|_| vec![0i16; period * CHANNELS]).collect();
        let mut card = ScriptedCard::delivering(dead).quiet_when_drained();
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("did not stay sane"), "{rendered}");
        assert!(rendered.contains("ch0 is all-zero"), "{rendered}");
    }

    #[test]
    fn overruns_from_before_the_duplex_window_are_not_charged_to_it() {
        // The stream's recovery counter is cumulative: an earlier case already ran on
        // this stream, and reporting its total would misattribute those overruns.
        let mut sink = ScriptedOut::taking_everything();
        let mut card = duplex_card(60);
        card.recoveries = 2;
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(outcome.passed(), "{}", detail(&outcome));
        assert!(
            !detail(&outcome).contains("recovered"),
            "an overrun this case did not see is not this case's reading: {}",
            detail(&outcome)
        );
    }

    #[test]
    fn an_overrun_during_the_duplex_window_is_reported_by_it() {
        let periods = 30;
        let mut sink = ScriptedOut::taking_everything();
        let mut card = duplex_card(periods).recovering_each_read();
        card.recoveries = 2;
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(outcome.passed(), "{}", detail(&outcome));
        assert!(
            detail(&outcome).contains(&format!("({periods} overrun(s) recovered)")),
            "every overrun the window itself cost belongs in its reading: {}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_capture_too_short_to_judge_is_reported_rather_than_judged() {
        // Five periods is 1 600 samples: live, but not enough of a window to say
        // anything about liveness. The shortfall is the reading.
        let mut sink = ScriptedOut::taking_everything();
        let mut card = duplex_card(5);
        let clock = Clock::new();
        let outcome = alsa_playback(&mut sink, &mut card, &|| clock.now());

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("of {DUPLEX_MIN_SAMPLES} samples")),
            "{rendered}"
        );
    }

    #[test]
    fn a_sink_that_never_takes_the_tone_is_reported_within_the_bound() {
        // The failure the bound exists for: the device accepted the parameters and
        // then took nothing. Without it the case would spin for as long as the
        // card felt like refusing.
        let mut sink = ScriptedOut::accepting(vec![0]);
        let mut card = duplex_card(60);
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(500));
            now
        };
        let outcome = alsa_playback(&mut sink, &mut card, &advancing);

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("0 of {TONE_SAMPLES} samples")),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!("{TONE_TIMEOUT:?}")),
            "{rendered}"
        );
    }

    #[test]
    fn the_tone_is_a_whole_second_of_the_pipelines_own_rate() {
        assert_eq!(TONE_SAMPLES as u32, SAMPLE_RATE_HZ);
        assert!(
            TONE_TIMEOUT.as_secs_f32() > TONE_SAMPLES as f32 / SAMPLE_RATE_HZ as f32,
            "the write bound must outlast the audio, or a healthy card fails on a stopwatch"
        );
        const {
            assert!(
                DUPLEX_MIN_SAMPLES < TONE_SAMPLES,
                "the duplex window has to fit inside the tone it is collected during"
            )
        };

        let pcm = tone_pcm(TONE_SAMPLES);
        assert_eq!(pcm.len(), TONE_SAMPLES * WIRE_BYTES_PER_SAMPLE);
        let peak = pcm
            .chunks_exact(WIRE_BYTES_PER_SAMPLE)
            .map(|s| i16::from_le_bytes([s[0], s[1]]).unsigned_abs())
            .max()
            .expect("a second of audio is not empty");
        assert!(
            peak <= TONE_AMPLITUDE as u16 && peak as f32 > 0.99 * TONE_AMPLITUDE as f32,
            "the tone reaches its stated amplitude and does not exceed it: {peak}"
        );
    }

    // ── Reporting ─────────────────────────────────────────────────────────────

    #[test]
    fn a_report_counts_each_kind_and_only_passes_when_every_case_ran() {
        let mut out = Vec::new();
        let mut report = Report::default();
        report
            .record(&mut out, "usb_presence", Outcome::Pass("38fb:1001".into()))
            .expect("record");
        report
            .record(&mut out, "ctrl_version", Outcome::fail("status 0x02"))
            .expect("record");
        report
            .record(
                &mut out,
                "ctrl_doa",
                Outcome::NotRun("no open board to talk to".into()),
            )
            .expect("record");
        assert_eq!(report.summary(), "3 cases: 1 passed, 1 failed, 1 not run");
        assert!(!report.all_passed());
        // A run that attempted nothing has asserted nothing.
        assert!(!Report::default().all_passed());

        let printed = String::from_utf8(out).expect("utf8");
        assert!(
            printed.contains("PASS usb_presence — 38fb:1001"),
            "{printed}"
        );
        assert!(
            printed.contains("FAIL ctrl_version\n     status 0x02"),
            "{printed}"
        );
        assert!(
            printed.contains("SKIP ctrl_doa — no open board"),
            "{printed}"
        );
    }

    #[test]
    fn a_multi_line_failure_indents_every_fact_under_its_case() {
        let case = CaseResult {
            name: "usb_presence",
            outcome: Outcome::Fail(vec!["first".into(), "second".into()]),
        };
        assert_eq!(
            case.to_string(),
            "FAIL usb_presence\n     first\n     second"
        );
    }
}
