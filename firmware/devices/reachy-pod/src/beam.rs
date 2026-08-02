//! The bench case: does the audio this pod streams carry the speech the chip gated
//! on, and what are the two capture channels?
//!
//! The board hands the host two processed outputs on one stereo capture stream, and
//! reports per-beam speech energy and direction over the control interface. The two
//! are not two beams: both processed outputs render the same auto-selected look
//! direction, and the four telemetry indices are the beamformer's internal beams —
//! two fixed, one free-running, one auto-select that mirrors whichever fixed beam it
//! has settled on. So there is no channel↔beam pairing to establish, and the chip's
//! own output-routing registers say what each channel carries anyway.
//!
//! What this case asserts of a working board: speech raises at least one beam's
//! energy to the level the pipeline's gate opens at, that beam resolves a direction
//! while it does, the capture stream hears the same speech the chip did, and every
//! capture channel's level follows its best-correlated beam closely enough that the
//! audio streamed under an open gate is the audio the gate opened on. No margin, no
//! distinctness: both channels tracking one beam is the expected reading, not a
//! failure.
//!
//! What it reads rather than concludes: the ch0↔ch1 relationship (their per-tick
//! power correlation, and whether the two sample streams are identical) and the raw
//! `AUDIO_MGR_OP_L`/`OP_R` bytes. Those go in the detail on every run. The one
//! contradiction it will fail on is sample-identical channels while the registers
//! say the two outputs differ — the registers and the stream cannot both be right,
//! and which is wrong is not something the bench can adjudicate.
//!
//! That makes this the one case a human has to be present for, which is why it is
//! not in the unattended registry. Everything below the collection loop is pure, so
//! the rules are asserted off-hardware and only the reading itself comes from the
//! bench.

use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use audio_pipeline::ring::{SAMPLE_RATE_HZ, WaveformStats, ZERO_ABS_THRESHOLD};
use device_protocol::{doa_azimuth_ok, sp_energy_ok};
use pod_streamer::telemetry::{VAD_POLL_HZ, VAD_THRESHOLD_DEFAULT};
use xvf3800_ctrl::{
    AEC_AZIMUTH_READ_LEN, AEC_AZIMUTH_VALUES_CMD, AEC_RESID, AEC_SPENERGY_READ_LEN,
    AEC_SPENERGY_VALUES_CMD, AUDIO_MGR_OP_L_CMD, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_READ_LEN,
    AUDIO_MGR_RESID, ControlTransport,
};

use crate::alsa_capture::{PERIOD_FRAMES, append_channels};
use crate::config::CHANNELS;
use crate::run::PeriodSource;
use crate::selftest::{Outcome, collect_window, read_f32x4, read_register, render};

/// Beams the chip reports per AEC reading.
pub const BEAMS: usize = 4;

/// Whole capture periods one tick summarizes.
///
/// The stream is read a period at a time, so a tick that asked for part of one
/// would take the whole thing anyway: a tick is the fewest periods that cover the
/// pipeline's own SPENERGY poll interval, and the chip is read once per tick.
pub const TICK_PERIODS: usize =
    (SAMPLE_RATE_HZ as usize / VAD_POLL_HZ as usize).div_ceil(PERIOD_FRAMES as usize);

/// Capture samples per channel each tick summarizes, so that what the chip reports
/// and what the stream delivered describe the same span of room.
pub const TICK_SAMPLES: usize = TICK_PERIODS * PERIOD_FRAMES as usize;

/// How long that span is, at rate.
pub const TICK_MS: usize = TICK_SAMPLES * 1_000 / SAMPLE_RATE_HZ as usize;

/// How long one tick's capture may take before the card is called stopped. Several
/// times the audio it is waiting for: a stream running at rate needs a fraction of
/// this, and the bound exists for a card that delivers nothing at all rather than
/// for one that is merely late.
pub const TICK_TIMEOUT: Duration = Duration::from_millis(500);

/// Ticks covering at least `ms` of room.
const fn ticks_for_ms(ms: usize) -> usize {
    ms.div_ceil(TICK_MS)
}

/// The quiet window: two seconds of the room with nobody speaking, which is what
/// the speech window is measured against.
pub const QUIET_TICKS: usize = ticks_for_ms(2_000);

/// The speech window: five seconds, long enough that a speaker's pauses and
/// syllables give the two levels something to correlate over, and short enough that
/// a bench operator will hold still for it.
pub const SPEECH_TICKS: usize = ticks_for_ms(5_000);

/// The fewest ticks either window may carry and still be judged — one second. Below
/// that a correlation is arithmetic over noise.
pub const MIN_TICKS: usize = ticks_for_ms(1_000);

/// How long a window of `ticks` takes at rate — what the operator is asked to hold
/// the room for, rather than the round number the tick count was derived from.
pub fn window_duration(ticks: usize) -> Duration {
    Duration::from_millis((ticks * TICK_MS) as u64)
}

/// How much a beam's mean energy must grow from the quiet room to count as having
/// heard the speech.
pub const BEAM_RISE_FACTOR: f32 = 4.0;

/// And the level it must reach: the threshold the pipeline's VAD gate opens at. A
/// beam that rises but stays under it is a beam that would never open a segment,
/// which is a finding rather than a pass.
pub const BEAM_ENERGY_FLOOR: f32 = VAD_THRESHOLD_DEFAULT;

/// The share of a risen beam's speech ticks that must carry a finite azimuth. The
/// chip reports NaN for a beam with nothing focused on it, so this is what "the
/// direction tracks" means as an assertion.
pub const DOA_TRACK_FRACTION: f32 = 0.5;

/// How much a capture channel's power must grow across the same two windows. Lower
/// than the beam factor because the capture channel carries the whole room and the
/// beam carries what the chip steered at.
pub const CHANNEL_RISE_FACTOR: f32 = 2.0;

/// And the level it must reach. A growth factor alone accepts a channel that is
/// digital silence in both windows — nothing is twice nothing — so a dead capture
/// path would be recorded as having heard the speech and the routing finding below
/// would never be reached. The floor is the shared waveform classifier's own
/// all-zero amplitude, as power.
pub const CHANNEL_POWER_FLOOR: f32 = (ZERO_ABS_THRESHOLD * ZERO_ABS_THRESHOLD) as f32;

/// The weakest correlation that says a capture channel carries what the chip's
/// telemetry is reporting.
///
/// Only the height is asserted. Every beam is driven by the same speech and the
/// auto-select index mirrors whichever fixed beam it has settled on, so a gap
/// between a channel's best beam and its runner-up is not something this hardware
/// produces — and both channels naming the same beam is the expected reading.
pub const CORRELATION_FLOOR: f32 = 0.5;

/// One tick: the chip's view of the room, and the audio that arrived alongside it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BeamTick {
    /// Per-beam speech energy, as `AEC_SPENERGY_VALUES` reported it.
    pub energy: [f32; BEAMS],
    /// Per-beam direction of arrival in radians, NaN where the chip has nothing
    /// focused on that beam.
    pub azimuth: [f32; BEAMS],
    /// Mean square of the samples each capture channel delivered during the tick.
    /// Power rather than amplitude, so it moves with the energy the chip reports.
    pub channel_power: [f32; CHANNELS],
    /// Whether every capture channel delivered the same samples this tick. Two
    /// renderings of one look direction still differ sample by sample; identical
    /// streams mean one source reaches both channels, which is a reading about the
    /// board rather than about the room.
    pub channels_identical: bool,
}

/// Every tick of one window.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct BeamWindow {
    pub ticks: Vec<BeamTick>,
}

impl BeamWindow {
    /// Mean energy per beam over the window.
    pub fn mean_energy(&self) -> [f32; BEAMS] {
        let mut means = [0.0; BEAMS];
        if self.ticks.is_empty() {
            return means;
        }
        for (beam, mean) in means.iter_mut().enumerate() {
            *mean =
                self.ticks.iter().map(|t| t.energy[beam]).sum::<f32>() / self.ticks.len() as f32;
        }
        means
    }

    /// Mean power per capture channel over the window.
    pub fn mean_channel_power(&self) -> [f32; CHANNELS] {
        let mut means = [0.0; CHANNELS];
        if self.ticks.is_empty() {
            return means;
        }
        for (channel, mean) in means.iter_mut().enumerate() {
            *mean = self
                .ticks
                .iter()
                .map(|t| t.channel_power[channel])
                .sum::<f32>()
                / self.ticks.len() as f32;
        }
        means
    }

    /// Mean of the finite azimuth readings per beam — NaN where a beam never
    /// resolved one, which is what the chip itself reports for that beam.
    pub fn mean_finite_azimuths(&self) -> [f32; BEAMS] {
        let mut means = [f32::NAN; BEAMS];
        for (beam, mean) in means.iter_mut().enumerate() {
            let finite: Vec<f32> = self
                .ticks
                .iter()
                .map(|t| t.azimuth[beam])
                .filter(|v| v.is_finite())
                .collect();
            if !finite.is_empty() {
                *mean = finite.iter().sum::<f32>() / finite.len() as f32;
            }
        }
        means
    }

    /// The share of ticks in which `beam` reported a finite direction.
    pub fn azimuth_tracking(&self, beam: usize) -> f32 {
        if self.ticks.is_empty() {
            return 0.0;
        }
        let finite = self
            .ticks
            .iter()
            .filter(|t| t.azimuth[beam].is_finite())
            .count();
        finite as f32 / self.ticks.len() as f32
    }

    /// The first reading in the window that is not a value the chip should ever
    /// report: `(tick, beam, what, value)`.
    ///
    /// Checked with the same predicates the unattended control-plane cases use, so a
    /// board that answers implausibly is the same finding here as there rather than
    /// arithmetic nobody looks at.
    pub fn implausible(&self) -> Option<(usize, usize, &'static str, f32)> {
        for (index, tick) in self.ticks.iter().enumerate() {
            for beam in 0..BEAMS {
                if !sp_energy_ok(tick.energy[beam]) {
                    return Some((index, beam, "energy", tick.energy[beam]));
                }
                if !doa_azimuth_ok(tick.azimuth[beam]) {
                    return Some((index, beam, "azimuth", tick.azimuth[beam]));
                }
            }
        }
        None
    }

    /// One beam's energy over the window, in tick order.
    pub fn beam_series(&self, beam: usize) -> Vec<f32> {
        self.ticks.iter().map(|t| t.energy[beam]).collect()
    }

    /// One capture channel's power over the window, in tick order.
    pub fn channel_series(&self, channel: usize) -> Vec<f32> {
        self.ticks
            .iter()
            .map(|t| t.channel_power[channel])
            .collect()
    }

    /// Whether every tick of the window delivered the same samples on every capture
    /// channel. An empty window is not a reading, so it answers false.
    pub fn channels_sample_identical(&self) -> bool {
        !self.ticks.is_empty() && self.ticks.iter().all(|t| t.channels_identical)
    }

    /// How the two capture channels' levels move together over the window — the
    /// reading that tells one source routed to both channels from two processed
    /// renderings of one look direction.
    pub fn cross_channel_correlation(&self) -> Option<f32> {
        pearson(&self.channel_series(0), &self.channel_series(1))
    }
}

// ── Output routing ────────────────────────────────────────────────────────────

/// What the chip says each of its two output channels carries.
///
/// Each register answers a `(category, source)` byte pair. The pair is carried and
/// printed raw: nothing in-tree has ever written or exercised these registers on
/// this firmware fork, so a reading of them is evidence to be reviewed rather than a
/// value to be interpreted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputRouting {
    /// `AUDIO_MGR_OP_L`.
    pub left: [u8; AUDIO_MGR_OP_READ_LEN],
    /// `AUDIO_MGR_OP_R`.
    pub right: [u8; AUDIO_MGR_OP_READ_LEN],
}

impl OutputRouting {
    /// Whether the two outputs are routed from different sources, as the registers
    /// have it.
    pub fn channels_differ(&self) -> bool {
        self.left != self.right
    }
}

impl fmt::Display for OutputRouting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OP_L (category {}, source {}) | OP_R (category {}, source {})",
            self.left[0], self.left[1], self.right[0], self.right[1]
        )
    }
}

/// Read both output-routing registers, or say why not.
///
/// The failure is a string rather than an [`Outcome`] because this reading never
/// decides the case's verdict: it is reported either way, and a board that will not
/// answer it is judged on the arms that do not need it.
pub fn read_output_routing<T: ControlTransport>(transport: &mut T) -> Result<OutputRouting, String>
where
    T::Error: fmt::Display,
{
    let mut routing = OutputRouting {
        left: [0; AUDIO_MGR_OP_READ_LEN],
        right: [0; AUDIO_MGR_OP_READ_LEN],
    };
    for (cmd, payload, label) in [
        (
            AUDIO_MGR_OP_L_CMD,
            &mut routing.left,
            "AUDIO_MGR_OP_L (resid 35 cmd 15)",
        ),
        (
            AUDIO_MGR_OP_R_CMD,
            &mut routing.right,
            "AUDIO_MGR_OP_R (resid 35 cmd 19)",
        ),
    ] {
        read_register(transport, AUDIO_MGR_RESID, cmd, payload, label)?;
    }
    Ok(routing)
}

// ── Correlation ───────────────────────────────────────────────────────────────

/// Pearson correlation of two equal-length series, or `None` when there is no
/// correlation to speak of: different lengths, fewer than two points, a series that
/// never varies, or a non-finite value anywhere in either.
///
/// A flat series is `None` rather than zero because it carries no information about
/// the other one, and reporting that as "uncorrelated" would read as evidence.
pub fn pearson(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || a.len() < 2 {
        return None;
    }
    let n = a.len() as f32;
    let mean_a = a.iter().sum::<f32>() / n;
    let mean_b = b.iter().sum::<f32>() / n;
    let mut covariance = 0.0;
    let mut variance_a = 0.0;
    let mut variance_b = 0.0;
    for (x, y) in a.iter().zip(b) {
        let dx = x - mean_a;
        let dy = y - mean_b;
        covariance += dx * dy;
        variance_a += dx * dx;
        variance_b += dy * dy;
    }
    // The finiteness of the ratio is the whole guard: a series that never varies
    // makes the denominator zero and the ratio NaN, and a non-finite reading
    // anywhere poisons the means and lands in the same place. Clamped because the
    // accumulation is in f32 — a series correlated with itself can come out a hair
    // past 1.0, and a reading of 1.0001 reads as a bug.
    let r = covariance / (variance_a * variance_b).sqrt();
    r.is_finite().then(|| r.clamp(-1.0, 1.0))
}

/// Every capture channel against every beam, over one window.
pub fn correlation_matrix(window: &BeamWindow) -> [[Option<f32>; BEAMS]; CHANNELS] {
    let mut matrix = [[None; BEAMS]; CHANNELS];
    for (channel, row) in matrix.iter_mut().enumerate() {
        let series = window.channel_series(channel);
        for (beam, cell) in row.iter_mut().enumerate() {
            *cell = pearson(&series, &window.beam_series(beam));
        }
    }
    matrix
}

/// Which beam one channel's level follows most closely.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChannelMapping {
    /// The beam whose energy this channel's level follows most closely.
    pub beam: usize,
    /// That correlation. The height is the whole reading — the distance to the
    /// runner-up says nothing here, because the auto-select index tracks whichever
    /// fixed beam the chip has settled on and duplicates its series.
    pub correlation: f32,
}

/// The best-correlated beam in one row of the matrix, with the number behind it.
///
/// The floor is not applied here: a reading below it is still the reading, and the
/// case reports it either way. The whole matrix goes into the detail regardless, so
/// nothing is lost by reducing a row to its top entry.
pub fn best_beam(row: &[Option<f32>; BEAMS]) -> Option<ChannelMapping> {
    // The lowest beam index wins a tie, which two channels carrying one source
    // produce exactly: their rows are the same numbers, and a mapping that named a
    // different beam for each would read as a difference between them.
    let (beam, correlation) = row
        .iter()
        .enumerate()
        .filter_map(|(beam, r)| r.map(|r| (beam, r)))
        .reduce(|best, next| if next.1 > best.1 { next } else { best })?;
    Some(ChannelMapping { beam, correlation })
}

/// Judge one bench run: the quiet room, then the same room with someone speaking
/// into the array, against what the chip says its outputs carry.
///
/// A pass carries every reading a human bakes from — what each channel follows, the
/// whole correlation matrix, how the two channels relate, the routing registers, the
/// energies, the directions and the capture levels. A failure carries every fact
/// behind it, first line first, so the transcript is reviewable without re-running
/// the bench.
pub fn assess_beam_speech(
    quiet: &BeamWindow,
    speech: &BeamWindow,
    routing: &Result<OutputRouting, String>,
) -> Outcome {
    // The registers were read off the board before either window and the channel
    // relationship is in the samples: both describe the board rather than the room,
    // so they hold whatever the room did and every verdict carries them. An early
    // arm firing is the likely bring-up outcome, and it is the one that must not
    // cost a second bench session to recover a reading already taken.
    let routing_line = render_routing(routing);

    if quiet.ticks.len() < MIN_TICKS || speech.ticks.len() < MIN_TICKS {
        return Outcome::Fail(vec![
            format!(
                "too little to judge: {} quiet and {} speech tick(s), {MIN_TICKS} needed of each",
                quiet.ticks.len(),
                speech.ticks.len()
            ),
            routing_line,
        ]);
    }

    let relationship_line = render_relationship(speech);

    for (window, name) in [(quiet, "quiet"), (speech, "speech")] {
        if let Some((tick, beam, what, value)) = window.implausible() {
            return Outcome::Fail(vec![
                format!(
                    "the {name} window's beam {beam} reported {what} {value} at tick {tick}, \
                     which is not a value the chip should ever report"
                ),
                relationship_line,
                routing_line,
            ]);
        }
    }

    let quiet_energy = quiet.mean_energy();
    let speech_energy = speech.mean_energy();
    let energy_line = format!(
        "beam energy quiet {} → speech {}",
        render(&quiet_energy),
        render(&speech_energy)
    );
    let risen: Vec<usize> = (0..BEAMS)
        .filter(|&beam| {
            speech_energy[beam] >= quiet_energy[beam] * BEAM_RISE_FACTOR
                && speech_energy[beam] >= BEAM_ENERGY_FLOOR
        })
        .collect();
    if risen.is_empty() {
        return Outcome::Fail(vec![
            "no beam's energy rose with the speech".to_string(),
            energy_line,
            format!(
                "a rise is {BEAM_RISE_FACTOR}× the quiet reading and at least \
                 {BEAM_ENERGY_FLOOR}, the level the pipeline's own gate opens at"
            ),
            relationship_line,
            routing_line,
        ]);
    }

    let tracking_line = format!(
        "azimuths in speech {} rad, finite on {}",
        render(&speech.mean_finite_azimuths()),
        beams_as_percent(speech),
    );
    let tracking: Vec<usize> = risen
        .iter()
        .copied()
        .filter(|&beam| speech.azimuth_tracking(beam) >= DOA_TRACK_FRACTION)
        .collect();
    if tracking.is_empty() {
        return Outcome::Fail(vec![
            format!(
                "beam(s) {} rose with the speech but resolved no direction while they did",
                index_list(&risen)
            ),
            energy_line,
            tracking_line,
            format!(
                "the chip reports NaN for a beam with nothing focused on it; a beam carrying \
                 speech is expected to hold an angle for at least {:.0}% of the window",
                DOA_TRACK_FRACTION * 100.0
            ),
            relationship_line,
            routing_line,
        ]);
    }

    let quiet_power = quiet.mean_channel_power();
    let speech_power = speech.mean_channel_power();
    let capture_line = format!(
        "capture rms quiet {} → speech {}",
        render_rms(&quiet_power),
        render_rms(&speech_power)
    );
    let heard: Vec<usize> = (0..CHANNELS)
        .filter(|&channel| {
            speech_power[channel] >= quiet_power[channel] * CHANNEL_RISE_FACTOR
                && speech_power[channel] >= CHANNEL_POWER_FLOOR
        })
        .collect();
    if heard.is_empty() {
        return Outcome::Fail(vec![
            format!(
                "beam(s) {} heard the speech but no capture channel's level rose with it",
                index_list(&risen)
            ),
            energy_line,
            capture_line,
            format!(
                "a rise is {CHANNEL_RISE_FACTOR}× the quiet reading and an rms of at least {:.0}, \
                 below which a channel is silence rather than a quiet room",
                CHANNEL_POWER_FLOOR.sqrt()
            ),
            "the control interface and the audio stream are different interfaces on one board, \
             so a chip that hears what the stream does not is a card-selection or routing finding"
                .to_string(),
            relationship_line,
            routing_line,
        ]);
    }

    let matrix = correlation_matrix(speech);
    let mapped: Vec<Option<ChannelMapping>> = matrix.iter().map(best_beam).collect();
    let incoherent: Vec<usize> = (0..CHANNELS)
        .filter(|&channel| {
            mapped[channel].is_none_or(|mapping| mapping.correlation < CORRELATION_FLOOR)
        })
        .collect();
    if !incoherent.is_empty() {
        let mut lines = vec![format!(
            "capture channel(s) {} do not follow the telemetry the gate is driven by: {}",
            index_list(&incoherent),
            render_mapping(&mapped)
        )];
        lines.extend(render_matrix(&matrix));
        lines.push(format!(
            "a channel is coherent when its best-correlated beam reaches {CORRELATION_FLOOR} over \
             the speech window; there is no margin and no distinctness rule, because both \
             channels carry the same auto-selected look direction"
        ));
        lines.push(
            "what this asserts is what the pipeline depends on: when the chip's energy \
                    opens the gate, the audio streamed under it is the speech the chip gated on"
                .to_string(),
        );
        lines.push(relationship_line);
        lines.push(routing_line);
        lines.push(energy_line);
        lines.push(capture_line);
        return Outcome::Fail(lines);
    }

    if let Ok(routing) = routing
        && speech.channels_sample_identical()
        && routing.channels_differ()
    {
        return Outcome::Fail(vec![
            "the capture channels delivered identical samples while the chip says its two \
             outputs carry different sources"
                .to_string(),
            routing_line,
            relationship_line,
            "one of three is wrong and the bench cannot say which: the registers' readback, the \
             firmware's output routing, or this pod's capture path"
                .to_string(),
            "nothing in-tree has written or exercised these registers on this firmware fork, so \
             the readback is itself unreviewed"
                .to_string(),
            energy_line,
            capture_line,
        ]);
    }

    let mut detail = vec![render_mapping(&mapped)];
    detail.extend(render_matrix(&matrix));
    detail.push(relationship_line);
    detail.push(routing_line);
    detail.push(energy_line);
    detail.push(tracking_line);
    detail.push(capture_line);
    Outcome::Pass(detail.join("; "))
}

/// Indices as a human reads them — beams in one reading, channels in another.
fn index_list(indices: &[usize]) -> String {
    let rendered: Vec<String> = indices.iter().map(|i| i.to_string()).collect();
    rendered.join(", ")
}

/// How often each beam resolved a direction, as percentages.
fn beams_as_percent(window: &BeamWindow) -> String {
    let rendered: Vec<String> = (0..BEAMS)
        .map(|beam| format!("{:.0}%", window.azimuth_tracking(beam) * 100.0))
        .collect();
    format!("[{}]", rendered.join(", "))
}

/// Channel powers as amplitudes, which is the scale a reviewer reads a capture
/// level in.
fn render_rms(power: &[f32; CHANNELS]) -> String {
    let rendered: Vec<String> = power.iter().map(|p| format!("{:.0}", p.sqrt())).collect();
    format!("[{}]", rendered.join(", "))
}

/// What each channel follows best — reported whether or not it clears the floor.
fn render_mapping(mapped: &[Option<ChannelMapping>]) -> String {
    let rendered: Vec<String> = mapped
        .iter()
        .enumerate()
        .map(|(channel, mapping)| match mapping {
            Some(m) => format!("ch{channel} → beam {} (r={:.2})", m.beam, m.correlation),
            None => format!("ch{channel} → nothing (no level to correlate)"),
        })
        .collect();
    rendered.join(" | ")
}

/// How the capture channels relate to each other over the speech window — a reading
/// on every run, and the half of the routing question the audio itself answers.
fn render_relationship(speech: &BeamWindow) -> String {
    let correlation = match speech.cross_channel_correlation() {
        Some(r) => format!("r={r:+.2}"),
        None => "r=— (a channel with no level to correlate)".to_string(),
    };
    let samples = if speech.channels_sample_identical() {
        "sample-identical"
    } else {
        "not sample-identical"
    };
    format!("ch0↔ch1 {correlation}, {samples}")
}

/// What the chip says its outputs carry, or why it would not say.
fn render_routing(routing: &Result<OutputRouting, String>) -> String {
    match routing {
        Ok(routing) => format!("output routing {routing}"),
        Err(why) => format!("output routing unread: {why}"),
    }
}

/// Carry the routing reading into an outcome the collection produced.
///
/// The registers were read before either window, so a run that never got its
/// audio still has this reading — losing it costs a second bench session. There
/// is no channel relationship to carry alongside it: that one is in samples the
/// run never collected.
fn with_routing(outcome: Outcome, routing: &Result<OutputRouting, String>) -> Outcome {
    match outcome {
        Outcome::Fail(mut lines) => {
            lines.push(render_routing(routing));
            Outcome::Fail(lines)
        }
        other => other,
    }
}

/// The whole matrix, one line per channel — the reading every run is reviewed from.
fn render_matrix(matrix: &[[Option<f32>; BEAMS]; CHANNELS]) -> Vec<String> {
    matrix
        .iter()
        .enumerate()
        .map(|(channel, row)| {
            let cells: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(beam, r)| match r {
                    Some(r) => format!("b{beam} {r:+.2}"),
                    None => format!("b{beam}    —"),
                })
                .collect();
            format!("ch{channel}: {}", cells.join("  "))
        })
        .collect()
}

// ── Collection ────────────────────────────────────────────────────────────────

/// Take `ticks` ticks of the room: capture on one interface, telemetry on the other.
///
/// The loop is paced by the capture stream rather than by a sleep — a tick ends when
/// the card has delivered its [`TICK_SAMPLES`], which on a stream running at rate is
/// [`TICK_MS`] of room, near enough the pipeline's own poll interval to be the same
/// cadence and without a second clock to keep in step with the first.
///
/// A tick short of its samples ends the window rather than being recorded. Every
/// tick is one equally weighted point of the correlation, so a card running slow
/// would stretch the cadence tick by tick — the operator asked to speak for five
/// seconds while the window takes far longer — and pair each chip reading with
/// capture from a span of room it no longer covers. That is a reading nobody was
/// told was ragged, and this case's whole output is a characterization meant to be
/// baked in.
///
/// The error side is an [`Outcome`] rather than a message because every way this can
/// end is a finding the case reports verbatim.
pub fn collect_beam_window<T: ControlTransport, S: PeriodSource>(
    transport: &mut T,
    source: &mut S,
    ticks: usize,
    now: &dyn Fn() -> Instant,
) -> Result<BeamWindow, Outcome>
where
    T::Error: fmt::Display,
{
    let mut window = BeamWindow::default();
    for tick in 0..ticks {
        let mut heard: [Vec<i16>; CHANNELS] = Default::default();
        let deadline = now() + TICK_TIMEOUT;
        let collected = {
            let mut fold = |period: &[i16]| append_channels(period, &mut heard);
            collect_window(source, TICK_SAMPLES, deadline, now, &mut fold)
                .map_err(|e| Outcome::fail(format!("capture stopped at tick {tick}: {e}")))?
        };
        if collected < TICK_SAMPLES {
            return Err(Outcome::fail(format!(
                "tick {tick} of {ticks}: the card delivered {collected} of {TICK_SAMPLES} samples \
                 inside its {TICK_TIMEOUT:?} bound"
            )));
        }
        let energy = read_f32x4(
            transport,
            AEC_RESID,
            AEC_SPENERGY_VALUES_CMD,
            AEC_SPENERGY_READ_LEN,
            "AEC_SPENERGY_VALUES (resid 33 cmd 80)",
        )?;
        let azimuth = read_f32x4(
            transport,
            AEC_RESID,
            AEC_AZIMUTH_VALUES_CMD,
            AEC_AZIMUTH_READ_LEN,
            "AEC_AZIMUTH_VALUES (resid 33 cmd 75)",
        )?;
        window.ticks.push(BeamTick {
            energy,
            azimuth,
            channel_power: std::array::from_fn(|channel| {
                // Exactly one tick's worth, however the periods divided it: an
                // overshoot on one tick and not the next would weight the two
                // differently in the correlation.
                WaveformStats::of(&heard[channel][..TICK_SAMPLES]).mean_square
            }),
            // Over the same span the power is taken from, so a tick that overshot
            // cannot answer this from samples no other reading of it covers.
            channels_identical: heard
                .iter()
                .all(|channel| channel[..TICK_SAMPLES] == heard[0][..TICK_SAMPLES]),
        });
    }
    Ok(window)
}

/// Speak at the array, and see what moves.
///
/// The one case with a human in it. Two windows are taken back to back so the
/// speech is measured against this room rather than against a threshold from
/// another one, and the routing registers are read once before either — they
/// describe the board, not the room, so every verdict carries them, including
/// the ones a window never got far enough to reach.
///
/// TODO(reachy-beam-mapping): bake the reviewed characterization in as expectations
/// once a bench run has produced it — what the two channels are (one source on both
/// versus the two processing flavors of one look direction), the `OP_L`/`OP_R`
/// values as identity assertions, and which channel `CHANNEL=` should default to.
/// Until then those are readings the case reports.
pub fn beam_energy_speech<T: ControlTransport, S: PeriodSource>(
    transport: &mut T,
    source: &mut S,
    out: &mut dyn io::Write,
    now: &dyn Fn() -> Instant,
) -> io::Result<Outcome>
where
    T::Error: fmt::Display,
{
    let routing = read_output_routing(transport);
    writeln!(
        out,
        "     beam_energy_speech: keep the room quiet — measuring it for {:?}",
        window_duration(QUIET_TICKS)
    )?;
    let quiet = match collect_beam_window(transport, source, QUIET_TICKS, now) {
        Ok(window) => window,
        Err(outcome) => return Ok(with_routing(outcome, &routing)),
    };
    writeln!(
        out,
        "     beam_energy_speech: speak toward the array now — for the next {:?}",
        window_duration(SPEECH_TICKS)
    )?;
    let speech = match collect_beam_window(transport, source, SPEECH_TICKS, now) {
        Ok(window) => window,
        Err(outcome) => return Ok(with_routing(outcome, &routing)),
    };
    writeln!(out, "     beam_energy_speech: that is enough, thank you")?;
    Ok(assess_beam_speech(&quiet, &speech, &routing))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Clock, Scripted, ScriptedCard, detail, f32x4_bytes};
    use xvf3800_ctrl::STATUS_DONE;

    /// A window whose beams and channels are written by hand as repeating patterns:
    /// `beams[beam]` and `channels[channel]` are cycled to `ticks` readings, with
    /// every beam resolving a direction.
    fn window(beams: [&[f32]; BEAMS], channels: [&[f32]; CHANNELS], ticks: usize) -> BeamWindow {
        let mut w = BeamWindow::default();
        for tick in 0..ticks {
            w.ticks.push(BeamTick {
                energy: std::array::from_fn(|beam| beams[beam][tick % beams[beam].len()]),
                azimuth: [0.5; BEAMS],
                channel_power: std::array::from_fn(|channel| {
                    channels[channel][tick % channels[channel].len()]
                }),
                channels_identical: false,
            });
        }
        w
    }

    /// `n` ticks of a room with nobody in it: a little energy on every beam, no
    /// direction resolved, and a quiet capture level.
    fn quiet_room(n: usize) -> BeamWindow {
        let mut w = BeamWindow::default();
        for tick in 0..n {
            let wobble = (tick % 3) as f32 * 0.01;
            w.ticks.push(BeamTick {
                energy: [0.02 + wobble, 0.03, 0.02, 0.04 - wobble],
                azimuth: [f32::NAN; BEAMS],
                channel_power: [400.0 + wobble, 380.0],
                channels_identical: false,
            });
        }
        w
    }

    /// `n` ticks of someone speaking, with ch0 following beam 1 and ch1 following
    /// beam 3 — the shape a working board is expected to produce.
    fn speech_room(n: usize) -> BeamWindow {
        let mut w = BeamWindow::default();
        for tick in 0..n {
            // Two envelopes that rise and fall differently, so a correlation can
            // tell one from the other.
            let a = 4.0 + (tick as f32 * 0.7).sin() * 3.0;
            let b = 4.0 + (tick as f32 * 0.31).cos() * 3.0;
            w.ticks.push(BeamTick {
                energy: [0.05, a, 0.06, b],
                azimuth: [f32::NAN, 0.8, f32::NAN, -0.9],
                channel_power: [a * 90_000.0, b * 90_000.0],
                channels_identical: false,
            });
        }
        w
    }

    /// Three envelopes over the same ticks: the two the beams carry, and one more
    /// the room has in it. Deliberately unlike each other, so a channel built by
    /// blending them correlates the way the blend says it should.
    fn envelopes(tick: usize) -> (f32, f32, f32) {
        let t = tick as f32;
        (
            4.0 + (t * 0.7).sin() * 3.0,
            4.0 + (t * 0.31).cos() * 3.0,
            4.0 + (t * 2.3 + 1.0).sin() * 3.0,
        )
    }

    /// A speech window whose channel levels are blends of those three: `mix[c]` is
    /// how much of beam 1 and of beam 3 channel `c` carries, and the remainder is
    /// the unrelated source. What that buys is a channel whose best correlation
    /// sits at a chosen height — the reading the coherence floor exists to judge —
    /// and, when both beams are mixed in, one whose top two are near enough
    /// together that a reinstated margin rule would reject it.
    fn blended(ticks: usize, mix: [(f32, f32); CHANNELS]) -> BeamWindow {
        let mut w = BeamWindow::default();
        for tick in 0..ticks {
            let (a, b, other) = envelopes(tick);
            w.ticks.push(BeamTick {
                energy: [0.05, a, 0.06, b],
                azimuth: [f32::NAN, 0.8, f32::NAN, -0.9],
                channel_power: std::array::from_fn(|channel| {
                    let (from_a, from_b) = mix[channel];
                    (from_a * a + from_b * b + (1.0 - from_a - from_b) * other) * 90_000.0
                }),
                channels_identical: false,
            });
        }
        w
    }

    /// What one channel of a window correlates best with — the reading a threshold
    /// test states the regime of before it asserts the verdict.
    fn mapping_of(window: &BeamWindow, channel: usize) -> ChannelMapping {
        best_beam(&correlation_matrix(window)[channel]).expect("a channel with a level")
    }

    /// The routing registers naming two different sources.
    const TWO_SOURCES: OutputRouting = OutputRouting {
        left: [1, 4],
        right: [1, 5],
    };

    /// And naming one, which is what a board routing a single source to both
    /// outputs is expected to report.
    const ONE_SOURCE: OutputRouting = OutputRouting {
        left: [1, 4],
        right: [1, 4],
    };

    /// Judge two windows against registers that name two sources — the reading
    /// under which nothing in the routing arms fires, so a test can be about the
    /// arm it is written for.
    fn assess(quiet: &BeamWindow, speech: &BeamWindow) -> Outcome {
        assess_beam_speech(quiet, speech, &Ok(TWO_SOURCES))
    }

    /// A speech window whose two channels carry the identical stream: same level
    /// tick by tick, and the sample-identity flag the collection sets when the two
    /// halves of a period are the same bytes.
    fn duplicated_channels(n: usize) -> BeamWindow {
        let mut w = speech_room(n);
        for tick in &mut w.ticks {
            tick.channel_power[1] = tick.channel_power[0];
            tick.channels_identical = true;
        }
        w
    }

    #[test]
    fn a_series_correlates_perfectly_with_itself_and_inversely_with_its_negation() {
        let a = [1.0, 3.0, 2.0, 7.0, 4.0];
        let negated: Vec<f32> = a.iter().map(|v| -v).collect();
        let scaled: Vec<f32> = a.iter().map(|v| v * 10.0 + 5.0).collect();
        assert!((pearson(&a, &a).expect("self") - 1.0).abs() < 1e-5);
        assert!((pearson(&a, &negated).expect("negated") + 1.0).abs() < 1e-5);
        // Correlation is scale- and offset-free, which is why a channel's power and
        // a beam's energy can be compared at all: they are in different units.
        assert!((pearson(&a, &scaled).expect("scaled") - 1.0).abs() < 1e-5);
    }

    #[test]
    fn a_series_with_nothing_to_correlate_reports_nothing_rather_than_zero() {
        let a = [1.0, 2.0, 3.0];
        assert_eq!(pearson(&a, &[1.0, 1.0, 1.0]), None, "a flat series");
        assert_eq!(pearson(&a, &[1.0, 2.0]), None, "different lengths");
        assert_eq!(pearson(&[1.0], &[2.0]), None, "one point");
        assert_eq!(pearson(&a, &[1.0, f32::NAN, 3.0]), None, "a NaN reading");
        assert_eq!(
            pearson(&a, &[1.0, f32::INFINITY, 3.0]),
            None,
            "an infinite reading"
        );
    }

    #[test]
    fn the_best_beam_is_the_top_one_and_a_tie_goes_to_the_lower_index() {
        let mapping = best_beam(&[Some(0.2), Some(0.9), None, Some(0.7)]).expect("a mapping");
        assert_eq!(mapping.beam, 1);
        assert!((mapping.correlation - 0.9).abs() < 1e-6);
        let lone = best_beam(&[None, None, Some(0.6), None]).expect("a mapping");
        assert_eq!(lone.beam, 2);
        // Two channels carrying one source produce two identical rows, and the
        // auto-select index duplicates whichever beam it has settled on — so ties
        // are the normal reading here, and both channels have to resolve one the
        // same way or the mapping invents a difference between them.
        let tied = best_beam(&[None, Some(0.8), None, Some(0.8)]).expect("a mapping");
        assert_eq!(tied.beam, 1);
        assert_eq!(best_beam(&[None; BEAMS]), None);
    }

    #[test]
    fn a_clean_bench_run_passes_and_carries_every_reading_a_reviewer_bakes_from() {
        let outcome = assess(&quiet_room(QUIET_TICKS), &speech_room(SPEECH_TICKS));
        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("ch0 → beam 1") && rendered.contains("ch1 → beam 3"),
            "what each channel follows: {rendered}"
        );
        for fact in [
            "beam energy quiet",
            "azimuths in speech",
            "capture rms",
            "ch0↔ch1 r=",
            "not sample-identical",
            "output routing OP_L (category 1, source 4)",
            "OP_R (category 1, source 5)",
        ] {
            assert!(rendered.contains(fact), "{fact} missing from {rendered}");
        }
        assert!(
            rendered.contains("ch0: b0") && rendered.contains("ch1: b0"),
            "the whole matrix is in a passing reading too, not only a failing one: {rendered}"
        );
    }

    #[test]
    fn a_window_too_short_to_judge_is_reported_rather_than_judged() {
        let outcome = assess(&quiet_room(MIN_TICKS - 1), &speech_room(SPEECH_TICKS));
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains(&format!("{MIN_TICKS} needed")),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_room_nobody_spoke_in_fails_with_both_energies_in_the_reading() {
        let outcome = assess(&quiet_room(QUIET_TICKS), &quiet_room(SPEECH_TICKS));
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("no beam's energy rose"), "{rendered}");
        assert!(rendered.contains("beam energy quiet"), "{rendered}");
    }

    #[test]
    fn a_rise_that_never_reaches_the_gates_level_is_not_a_rise() {
        // Twenty times the quiet reading, and still under the threshold the
        // pipeline opens a segment at: the pod would never hear this speaker.
        let mut speech = speech_room(SPEECH_TICKS);
        for tick in &mut speech.ticks {
            tick.energy = tick.energy.map(|e| e * BEAM_ENERGY_FLOOR / 20.0);
        }
        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains(&format!("at least {BEAM_ENERGY_FLOOR}")),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_beam_that_rose_without_resolving_a_direction_is_the_finding() {
        let mut speech = speech_room(SPEECH_TICKS);
        for tick in &mut speech.ticks {
            tick.azimuth = [f32::NAN; BEAMS];
        }
        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("resolved no direction"), "{rendered}");
        assert!(
            rendered.contains("beam(s) 1, 3"),
            "the risen beams are named: {rendered}"
        );

        // A direction held for less than half the window is not tracking either.
        let mut flickering = speech_room(SPEECH_TICKS);
        for (tick, reading) in flickering.ticks.iter_mut().enumerate() {
            if tick % 3 != 0 {
                reading.azimuth = [f32::NAN; BEAMS];
            }
        }
        assert!(!assess(&quiet_room(QUIET_TICKS), &flickering).passed());
    }

    #[test]
    fn a_chip_that_hears_what_the_capture_stream_does_not_is_a_routing_finding() {
        let mut speech = speech_room(SPEECH_TICKS);
        let quiet = quiet_room(QUIET_TICKS);
        for tick in &mut speech.ticks {
            tick.channel_power = quiet.ticks[0].channel_power;
        }
        let outcome = assess(&quiet, &speech);
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("no capture channel's level rose"),
            "{rendered}"
        );
        assert!(rendered.contains("capture rms quiet"), "{rendered}");
    }

    #[test]
    fn channels_that_track_the_same_beam_pass_because_that_is_what_this_board_does() {
        // Both channels carrying the same envelope. The two processed outputs are
        // two renderings of one auto-selected look direction, so this is the
        // expected reading.
        let mut speech = speech_room(SPEECH_TICKS);
        for tick in &mut speech.ticks {
            tick.channel_power[1] = tick.channel_power[0];
        }
        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("ch0 → beam 1") && rendered.contains("ch1 → beam 1"),
            "both channels naming one beam is the reading, not a failure: {rendered}"
        );
        assert!(
            rendered.contains("ch0↔ch1 r=+1.00"),
            "and how tightly they move together is the finding: {rendered}"
        );
    }

    #[test]
    fn identical_channels_whose_registers_agree_pass_and_report_the_duplication() {
        // The board routing one source to both outputs: the streams are the same
        // bytes and the two registers name the same source. Nothing contradicts
        // anything, and the case's job is to say so precisely enough that a human
        // can bake it in.
        let outcome = assess_beam_speech(
            &quiet_room(QUIET_TICKS),
            &duplicated_channels(SPEECH_TICKS),
            &Ok(ONE_SOURCE),
        );
        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("ch0↔ch1 r=+1.00, sample-identical"),
            "the relationship is the reading: {rendered}"
        );
        assert!(
            rendered.contains("OP_L (category 1, source 4) | OP_R (category 1, source 4)"),
            "with the registers that agree with it: {rendered}"
        );
    }

    #[test]
    fn identical_channels_whose_registers_differ_name_the_contradiction_and_its_suspects() {
        let outcome = assess_beam_speech(
            &quiet_room(QUIET_TICKS),
            &duplicated_channels(SPEECH_TICKS),
            &Ok(TWO_SOURCES),
        );
        assert!(!outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("identical samples") && rendered.contains("different sources"),
            "both halves of the contradiction: {rendered}"
        );
        for suspect in ["readback", "firmware's output routing", "capture path"] {
            assert!(
                rendered.contains(suspect),
                "the bench cannot adjudicate, so all three go to the human: {suspect} missing \
                 from {rendered}"
            );
        }
        assert!(
            rendered.contains("OP_L (category 1, source 4) | OP_R (category 1, source 5)"),
            "with the register values behind it: {rendered}"
        );
    }

    #[test]
    fn channels_that_are_not_identical_are_no_contradiction_however_the_registers_read() {
        // The registers say two sources and the streams differ, which agrees; and
        // the registers saying one source while the streams differ is the
        // Conference/ASR pair, two renderings of one look direction. Neither is a
        // finding — only sample-identity against differing registers is.
        for routing in [TWO_SOURCES, ONE_SOURCE] {
            let outcome = assess_beam_speech(
                &quiet_room(QUIET_TICKS),
                &speech_room(SPEECH_TICKS),
                &Ok(routing),
            );
            assert!(outcome.passed(), "{routing:?}: {}", detail(&outcome));
        }
    }

    #[test]
    fn a_routing_read_that_failed_is_reported_without_deciding_the_verdict() {
        // The reading the consistency check needs is missing, so the check is
        // skipped — including on the one window shape that would otherwise fail it.
        let outcome = assess_beam_speech(
            &quiet_room(QUIET_TICKS),
            &duplicated_channels(SPEECH_TICKS),
            &Err("AUDIO_MGR_OP_L (resid 35 cmd 15) read failed: pipe error".to_string()),
        );
        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(
                "output routing unread: AUDIO_MGR_OP_L (resid 35 cmd 15) read \
                               failed: pipe error"
            ),
            "the failure is carried verbatim: {rendered}"
        );
        assert!(
            rendered.contains("sample-identical"),
            "and the reading that had nothing to be checked against is still reported: {rendered}"
        );
    }

    #[test]
    fn every_failing_arm_carries_the_relationship_and_the_routing_too() {
        // However a run ends, it is still the run whose channel identity a human is
        // trying to establish — and both of those readings describe the board, not
        // the room, so they survive a room that nobody spoke in. Losing them on an
        // early arm costs a second bench session for a reading already taken.
        let implausible = {
            let mut w = speech_room(SPEECH_TICKS);
            w.ticks[2].energy[0] = -1.0;
            w
        };
        let directionless = {
            let mut w = speech_room(SPEECH_TICKS);
            for tick in &mut w.ticks {
                tick.azimuth = [f32::NAN; BEAMS];
            }
            w
        };
        let unheard = {
            let mut w = speech_room(SPEECH_TICKS);
            for tick in &mut w.ticks {
                tick.channel_power = quiet_room(1).ticks[0].channel_power;
            }
            w
        };
        // Every arm that can fail, in the order `assess_beam_speech` reaches them.
        // The length arm is the one exception to the relationship reading: a window
        // too short to judge is too short to correlate.
        let arms: [(String, BeamWindow, BeamWindow, bool); 6] = [
            (
                format!("{MIN_TICKS} needed"),
                quiet_room(MIN_TICKS - 1),
                speech_room(SPEECH_TICKS),
                false,
            ),
            (
                "not a value the chip should ever report".to_string(),
                quiet_room(QUIET_TICKS),
                implausible,
                true,
            ),
            (
                "no beam's energy rose".to_string(),
                quiet_room(QUIET_TICKS),
                quiet_room(SPEECH_TICKS),
                true,
            ),
            (
                "resolved no direction".to_string(),
                quiet_room(QUIET_TICKS),
                directionless,
                true,
            ),
            (
                "no capture channel's level rose".to_string(),
                quiet_room(QUIET_TICKS),
                unheard,
                true,
            ),
            (
                "do not follow the telemetry".to_string(),
                quiet_room(QUIET_TICKS),
                blended(SPEECH_TICKS, [(0.25, 0.0), (0.0, 1.0)]),
                true,
            ),
        ];
        for (arm, quiet, speech, relationship) in arms {
            let outcome = assess_beam_speech(&quiet, &speech, &Ok(ONE_SOURCE));
            assert!(!outcome.passed(), "{arm}: {}", detail(&outcome));
            let rendered = detail(&outcome);
            assert!(
                rendered.contains(&arm),
                "the case reaches the arm the reading is written for: {rendered}"
            );
            assert!(
                rendered.contains("output routing OP_L (category 1, source 4)"),
                "{arm}: the registers were read before either window: {rendered}"
            );
            assert_eq!(
                rendered.contains("ch0↔ch1 r="),
                relationship,
                "{arm}: {rendered}"
            );
        }
    }

    #[test]
    fn the_routing_registers_are_read_by_name_and_reported_raw() {
        let mut board =
            Scripted::sequenced(vec![(STATUS_DONE, vec![1, 4]), (STATUS_DONE, vec![2, 7])]);
        let routing = read_output_routing(&mut board).expect("a board that answers");
        assert_eq!(routing.left, [1, 4]);
        assert_eq!(routing.right, [2, 7]);
        assert!(routing.channels_differ());
        assert_eq!(
            board.registers,
            vec![
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD),
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD),
            ],
            "left then right, off the audio manager"
        );

        let mut dead = Scripted::failing("pipe error");
        let why = read_output_routing(&mut dead).expect_err("a board that does not");
        assert!(
            why.contains("resid 35 cmd 15") && why.contains("pipe error"),
            "{why}"
        );
    }

    #[test]
    fn a_routing_register_that_answers_a_bad_status_is_no_reading_of_it() {
        // A board that ACKs the transfer and answers a status that is not DONE
        // has told us nothing about its routing, and the payload buffer still
        // holds the zeros it was handed. Accepted, that reads as one source on
        // both outputs — which suppresses the one contradiction arm the case
        // has, and prints a fabricated (0, 0) pair for a human under
        // instruction to bake these values in as identity assertions.
        let mut refusing = Scripted::sequenced(vec![(0x02, vec![0, 0])]);
        let why = read_output_routing(&mut refusing).expect_err("a status that is not DONE");
        assert!(
            why.contains("resid 35 cmd 15") && why.contains("0x02"),
            "the register and the status it answered: {why}"
        );
        assert_eq!(refusing.reads, 1, "a fatal status is not retried");

        // And a board that answers the first register and not the second names
        // the second: sending the operator to cmd 15 when cmd 19 is what would
        // not answer is the wrong-interface error one register down.
        let mut half = Scripted::sequenced(vec![(STATUS_DONE, vec![1, 4]), (0x02, vec![0, 0])]);
        let why = read_output_routing(&mut half).expect_err("a board that answers halfway");
        assert!(
            why.contains("resid 35 cmd 19"),
            "the register that would not answer is the finding: {why}"
        );
    }

    #[test]
    fn a_capture_channel_that_is_silence_has_not_heard_anything_however_it_grew() {
        // Nothing is twice nothing, so a growth factor on its own accepts a dead
        // capture path as having heard the speech — and the run then goes on to
        // blame the correlation for a reading the routing arm exists to name.
        let mut quiet = quiet_room(QUIET_TICKS);
        let mut speech = speech_room(SPEECH_TICKS);
        for tick in &mut quiet.ticks {
            tick.channel_power = [0.0; CHANNELS];
        }
        for tick in &mut speech.ticks {
            tick.channel_power = [0.0; CHANNELS];
        }
        let outcome = assess(&quiet, &speech);
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("no capture channel's level rose"),
            "silence is the routing finding, not a correlation one: {rendered}"
        );

        // Nor does a channel that grew a hundredfold and is still under the level
        // at which the shared classifier calls a window silent.
        for tick in &mut quiet.ticks {
            tick.channel_power = [1.0; CHANNELS];
        }
        for tick in &mut speech.ticks {
            tick.channel_power = [CHANNEL_POWER_FLOOR / 2.0; CHANNELS];
        }
        let outcome = assess(&quiet, &speech);
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("no capture channel's level rose"),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_beam_loud_enough_but_barely_risen_is_not_a_rise() {
        // The one reading only the rise factor can reject: well past the level the
        // pipeline's gate opens at, and only twice what the same room was reading
        // with nobody in it. A room that loud all along is not a room this speaker
        // was heard in.
        let mut quiet = quiet_room(QUIET_TICKS);
        for (tick, reading) in quiet.ticks.iter_mut().enumerate() {
            reading.energy = [0.6 + (tick % 3) as f32 * 0.01; BEAMS];
        }
        let mut speech = speech_room(SPEECH_TICKS);
        for reading in &mut speech.ticks {
            reading.energy = reading.energy.map(|e| 1.2 + e * 0.01);
        }
        assert!(
            speech.mean_energy()[1] >= BEAM_ENERGY_FLOOR,
            "the energy floor must not be what rejects this one: {:?}",
            speech.mean_energy()
        );

        let outcome = assess(&quiet, &speech);
        assert!(!outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(rendered.contains("no beam's energy rose"), "{rendered}");
        assert!(
            rendered.contains(&format!("{BEAM_RISE_FACTOR}×")),
            "{rendered}"
        );
    }

    #[test]
    fn a_channel_that_follows_its_best_beam_too_loosely_fails_the_coherence_arm() {
        // Mostly something else in the room, with a little of beam 1 in it: the
        // channel's level and the energy the gate is driven by are barely the same
        // signal, so audio streamed under an open gate is not what opened it.
        let speech = blended(SPEECH_TICKS, [(0.25, 0.0), (0.0, 1.0)]);
        let ch0 = mapping_of(&speech, 0);
        assert!(
            ch0.correlation < CORRELATION_FLOOR,
            "the floor is what rejects this one: {ch0:?}"
        );

        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(!outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("capture channel(s) 0 do not follow"),
            "the channel is named: {rendered}"
        );
        assert!(
            rendered.contains(&format!("reaches {CORRELATION_FLOOR}")),
            "the reading must name the height it fell short of: {rendered}"
        );
    }

    #[test]
    fn a_channel_that_follows_two_beams_almost_equally_is_still_coherent() {
        // Half of each: strongly correlated with both, which is what the chip's own
        // auto-select index produces on every run — it mirrors whichever fixed beam
        // it has settled on, so the top two entries of a row are near-duplicates.
        // Only the height is asserted, so this passes.
        let speech = blended(SPEECH_TICKS, [(0.52, 0.48), (0.0, 1.0)]);
        let row = correlation_matrix(&speech)[0];
        let ch0 = mapping_of(&speech, 0);
        let runner_up = row
            .iter()
            .flatten()
            .filter(|r| **r < ch0.correlation)
            .fold(f32::MIN, |best, r| best.max(*r));
        assert!(
            ch0.correlation >= CORRELATION_FLOOR && ch0.correlation - runner_up < 0.1,
            "the two readings this case must not separate on: {ch0:?} against {runner_up}"
        );

        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(outcome.passed(), "{}", detail(&outcome));
    }

    #[test]
    fn a_channel_whose_level_never_moves_fails_the_coherence_arm_by_name() {
        let flat = window(
            [
                &[0.05],
                &[9.0, 2.0, 8.0, 3.0],
                &[0.05],
                &[2.0, 9.0, 3.0, 8.0],
            ],
            [&[900_000.0, 200_000.0, 800_000.0, 300_000.0], &[7.0]],
            MIN_TICKS,
        );
        let outcome = assess(&quiet_room(MIN_TICKS), &flat);
        assert!(!outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("capture channel(s) 1 do not follow"),
            "a channel with no level to correlate fails the coherence arm by name: {rendered}"
        );
        assert!(rendered.contains("ch1 → nothing"), "{rendered}");
        // And the channel relationship says the same thing rather than a number.
        // A dead channel is the routing finding this case exists to catch, and
        // "ch0↔ch1 r=+0.00" would tell a reviewer the two channels are
        // uncorrelated when one of them has no signal at all.
        assert!(
            rendered.contains("ch0↔ch1 r=— (a channel with no level to correlate)"),
            "the relationship reading reports nothing rather than zero: {rendered}"
        );
    }

    #[test]
    fn a_window_with_no_ticks_in_it_is_no_reading_of_identical_channels() {
        // Every tick of nothing is identical, so without the emptiness guard a
        // window that never happened reads as one source routed to both
        // outputs — which is the reading the contradiction arm turns on and the
        // one a human is asked to bake in.
        assert!(!BeamWindow::default().channels_sample_identical());
    }

    #[test]
    fn an_implausible_reading_is_named_before_anything_is_concluded_from_it() {
        let mut speech = speech_room(SPEECH_TICKS);
        speech.ticks[7].energy[2] = -1.0;
        let outcome = assess(&quiet_room(QUIET_TICKS), &speech);
        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("beam 2 reported energy -1"), "{rendered}");
        assert!(rendered.contains("tick 7"), "{rendered}");

        let mut spun = speech_room(SPEECH_TICKS);
        spun.ticks[2].azimuth[0] = 99.0;
        let outcome = assess(&quiet_room(QUIET_TICKS), &spun);
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("beam 0 reported azimuth 99"),
            "{}",
            detail(&outcome)
        );
    }

    /// A card delivering `count` periods of `frames` frames, where ch0 is loud and
    /// ch1 is quiet, then idle.
    fn card(count: usize, frames: usize) -> ScriptedCard {
        let periods: Vec<Vec<i16>> = (0..count)
            .map(|p| {
                (0..frames)
                    .flat_map(|i| {
                        let phase = (p * frames + i) as f32 * std::f32::consts::TAU / 32.0;
                        [
                            (phase.sin() * 8_000.0) as i16,
                            (phase.sin() * 1_000.0) as i16,
                        ]
                    })
                    .collect()
            })
            .collect();
        ScriptedCard::delivering(periods).quiet_when_drained()
    }

    /// A transport answering SPENERGY then DoA, over and over.
    fn telemetry(energy: [f32; BEAMS], azimuth: [f32; BEAMS]) -> Scripted {
        Scripted::sequenced(vec![
            (STATUS_DONE, f32x4_bytes(energy)),
            (STATUS_DONE, f32x4_bytes(azimuth)),
            (STATUS_DONE, f32x4_bytes(energy)),
            (STATUS_DONE, f32x4_bytes(azimuth)),
        ])
    }

    /// A board whose registers answer independently and whose room changes once
    /// the quiet window is behind it.
    ///
    /// A bench run takes two windows back to back — quiet then speech — so the
    /// board must answer by register and take its reading from the tick it is on,
    /// not from a flat script that cannot express a room that changes. It also
    /// answers the two output-routing registers, which the case reads once before
    /// either window.
    struct BenchBoard {
        tick: usize,
        quiet_ticks: usize,
        routing: OutputRouting,
    }

    impl BenchBoard {
        /// Quiet for `quiet_ticks`, then someone speaking into beams 1 and 3, with
        /// the two outputs routed from different sources.
        fn speaking_after(quiet_ticks: usize) -> Self {
            Self {
                tick: 0,
                quiet_ticks,
                routing: TWO_SOURCES,
            }
        }

        /// The same board reporting whatever routing a test needs.
        fn routed(mut self, routing: OutputRouting) -> Self {
            self.routing = routing;
            self
        }

        /// A room that never changes, whatever anyone does in it.
        fn steady() -> Self {
            Self::speaking_after(usize::MAX)
        }

        fn speaking(&self) -> bool {
            self.tick >= self.quiet_ticks
        }

        fn energy(&self) -> [f32; BEAMS] {
            if !self.speaking() {
                return [0.02, 0.03, 0.02, 0.04];
            }
            let (a, b, _) = envelopes(self.tick - self.quiet_ticks);
            [0.05, a, 0.06, b]
        }

        fn azimuth(&self) -> [f32; BEAMS] {
            if !self.speaking() {
                return [f32::NAN; BEAMS];
            }
            [f32::NAN, 0.8, f32::NAN, -0.9]
        }
    }

    impl ControlTransport for BenchBoard {
        type Error = &'static str;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            let values = match (resid, cmd) {
                (AEC_RESID, AEC_SPENERGY_VALUES_CMD) => self.energy(),
                (AEC_RESID, AEC_AZIMUTH_VALUES_CMD) => {
                    // The direction is the tick's second read, so the tick is over.
                    let azimuth = self.azimuth();
                    self.tick += 1;
                    azimuth
                }
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD) => {
                    payload.copy_from_slice(&self.routing.left);
                    return Ok(STATUS_DONE);
                }
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD) => {
                    payload.copy_from_slice(&self.routing.right);
                    return Ok(STATUS_DONE);
                }
                other => panic!("the bench case reads no other register: {other:?}"),
            };
            payload.copy_from_slice(&f32x4_bytes(values));
            Ok(STATUS_DONE)
        }

        fn control_write_once(
            &mut self,
            _resid: u8,
            _cmd: u8,
            _payload: &[u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            unreachable!("no case writes")
        }

        fn delay_ms(&mut self, _ms: u32) {}
    }

    /// A card whose channels carry the same envelopes that board reports: ch0
    /// follows beam 1, ch1 follows beam 3, and both are near silence until the
    /// speech window starts.
    ///
    /// Amplitudes are chosen so a channel's mean square comes out proportional to
    /// its beam's energy, which is exactly the relationship the case is looking
    /// for — and a whole number of sine cycles fits in a tick, so the proportion
    /// holds tick by tick rather than on average.
    fn bench_card(quiet_ticks: usize, speech_ticks: usize) -> ScriptedCard {
        bench_card_with(quiet_ticks, speech_ticks, false)
    }

    /// The same card, optionally delivering one source on both channels — a board
    /// whose two USB channels carry identical samples.
    fn bench_card_with(quiet_ticks: usize, speech_ticks: usize, duplicate: bool) -> ScriptedCard {
        let frames = PERIOD_FRAMES as usize;
        let mut periods: Vec<Vec<i16>> = Vec::new();
        for tick in 0..(quiet_ticks + speech_ticks) {
            let (left, mut right) = if tick < quiet_ticks {
                (30.0, 28.0)
            } else {
                let (a, b, _) = envelopes(tick - quiet_ticks);
                ((2.0 * a * 90_000.0).sqrt(), (2.0 * b * 90_000.0).sqrt())
            };
            if duplicate {
                right = left;
            }
            for period in 0..TICK_PERIODS {
                let base = (tick * TICK_PERIODS + period) * frames;
                periods.push(
                    (0..frames)
                        .flat_map(|i| {
                            let phase = (base + i) as f32 * std::f32::consts::TAU / 32.0;
                            [(phase.sin() * left) as i16, (phase.sin() * right) as i16]
                        })
                        .collect(),
                );
            }
        }
        ScriptedCard::delivering(periods).quiet_when_drained()
    }

    #[test]
    fn a_tick_pairs_the_chips_reading_with_the_audio_that_arrived_beside_it() {
        let frames = TICK_SAMPLES / 2;
        let mut card = card(8, frames);
        let mut transport = telemetry([1.0, 2.0, 3.0, 4.0], [0.1, 0.2, 0.3, 0.4]);
        let clock = Clock::new();
        let window = collect_beam_window(&mut transport, &mut card, 2, &|| clock.now())
            .expect("two ticks of a healthy board");

        assert_eq!(window.ticks.len(), 2);
        assert_eq!(window.ticks[0].energy, [1.0, 2.0, 3.0, 4.0]);
        assert_eq!(window.ticks[0].azimuth, [0.1, 0.2, 0.3, 0.4]);
        assert!(
            window.ticks[0].channel_power[0] > window.ticks[0].channel_power[1] * 10.0,
            "each channel's own power: {:?}",
            window.ticks[0].channel_power
        );
        assert_eq!(
            transport.registers,
            vec![
                (AEC_RESID, AEC_SPENERGY_VALUES_CMD),
                (AEC_RESID, AEC_AZIMUTH_VALUES_CMD),
                (AEC_RESID, AEC_SPENERGY_VALUES_CMD),
                (AEC_RESID, AEC_AZIMUTH_VALUES_CMD),
            ],
            "one energy and one direction per tick, in that order"
        );
        assert_eq!(
            card.remaining(),
            4,
            "a tick takes the audio it needs and no more"
        );
        assert!(
            !window.ticks[0].channels_identical,
            "the two channels of this card carry different levels"
        );

        // The same collection over a card whose two channels are the same bytes.
        let period: Vec<i16> = (0..frames)
            .flat_map(|f| [(f % 97) as i16; CHANNELS])
            .collect();
        let mut duplicating =
            ScriptedCard::delivering(vec![period.clone(), period]).quiet_when_drained();
        let mut transport = telemetry([1.0; BEAMS], [0.1; BEAMS]);
        let window = collect_beam_window(&mut transport, &mut duplicating, 1, &|| clock.now())
            .expect("one tick of a board that duplicates its source");
        assert!(
            window.ticks[0].channels_identical,
            "identical samples are noticed where they arrive, not inferred from a level"
        );
    }

    #[test]
    fn a_card_that_stops_delivering_ends_the_window_within_the_tick_bound() {
        let mut card = ScriptedCard::stalled();
        let mut transport = telemetry([1.0; BEAMS], [0.1; BEAMS]);
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(100));
            now
        };
        let outcome = collect_beam_window(&mut transport, &mut card, QUIET_TICKS, &advancing)
            .expect_err("a card that delivers nothing cannot be measured");
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(&format!("{TICK_TIMEOUT:?} bound")),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!("tick 0 of {QUIET_TICKS}")),
            "the reading says how far into the window it got: {rendered}"
        );
        assert!(
            rendered.contains(&format!("0 of {TICK_SAMPLES} samples")),
            "and how little the tick itself carried: {rendered}"
        );
        assert!(
            card.waits.iter().all(|w| *w <= TICK_TIMEOUT),
            "no wait outlasts the tick it is bounded by: {:?}",
            card.waits
        );
    }

    #[test]
    fn a_control_interface_that_stops_answering_names_the_register() {
        let mut card = card(8, TICK_SAMPLES);
        let mut transport = Scripted::failing("pipe error");
        let clock = Clock::new();
        let outcome = collect_beam_window(&mut transport, &mut card, 2, &|| clock.now())
            .expect_err("a dead control interface cannot be measured");
        let rendered = detail(&outcome);
        assert!(rendered.contains("resid 33 cmd 80"), "{rendered}");
        assert!(rendered.contains("pipe error"), "{rendered}");
    }

    #[test]
    fn a_capture_error_mid_window_ends_it_on_the_streams_own_reason() {
        // Two periods and then nothing, with no idle behavior scripted: the third
        // read is the stream's own failure.
        let mut card = ScriptedCard::delivering(vec![vec![0i16; 4], vec![0i16; 4]]);
        let mut transport = telemetry([1.0; BEAMS], [0.1; BEAMS]);
        let clock = Clock::new();
        let outcome = collect_beam_window(&mut transport, &mut card, 2, &|| clock.now())
            .expect_err("a stream that stops cannot be measured");
        assert!(
            detail(&outcome).contains("the card stopped delivering"),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_tick_is_whole_periods_covering_the_pipelines_own_poll_interval() {
        // The collection reads whole periods, so a tick that is not a whole number
        // of them takes more audio than it asked for, and every duration derived
        // from the tick count is wrong by the difference.
        assert_eq!(TICK_SAMPLES % PERIOD_FRAMES as usize, 0);
        let interval = SAMPLE_RATE_HZ as usize / VAD_POLL_HZ as usize;
        assert!(
            TICK_SAMPLES >= interval && TICK_SAMPLES - interval < PERIOD_FRAMES as usize,
            "a tick covers the poll interval, and overshoots by under one period: \
             {TICK_SAMPLES} against {interval}"
        );
        assert_eq!(TICK_MS, TICK_SAMPLES * 1_000 / SAMPLE_RATE_HZ as usize);
    }

    #[test]
    fn the_windows_are_the_room_they_claim_and_a_bench_operators_patience() {
        // The prompts are printed from these, so the operator has to be asked to
        // hold the room for as long as the window will actually take.
        for (ticks, wanted) in [
            (QUIET_TICKS, Duration::from_secs(2)),
            (SPEECH_TICKS, Duration::from_secs(5)),
            (MIN_TICKS, Duration::from_secs(1)),
        ] {
            let taken = window_duration(ticks);
            assert!(
                taken >= wanted && taken - wanted < Duration::from_millis(TICK_MS as u64),
                "{ticks} ticks is {taken:?}, which must cover {wanted:?} by under one tick"
            );
        }
        const {
            assert!(
                MIN_TICKS <= QUIET_TICKS && MIN_TICKS <= SPEECH_TICKS,
                "a full run has to be judgeable"
            )
        };
        assert!(
            TICK_TIMEOUT.as_secs_f32() > TICK_SAMPLES as f32 / SAMPLE_RATE_HZ as f32,
            "the tick bound must outlast the audio it waits for"
        );
    }

    #[test]
    fn a_tick_the_card_came_up_short_on_ends_the_window() {
        // The realistic USB-audio failure is a card that is late, not one that is
        // dead: it delivers, just not a tick's worth inside a tick's bound. Recorded
        // rather than reported, that tick would enter the correlation as an
        // equal-weight point covering a shorter span of room than its neighbours.
        let half = TICK_SAMPLES / 2;
        let mut card = card(3, half);
        let mut transport = telemetry([1.0; BEAMS], [0.1; BEAMS]);
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(250));
            now
        };
        let outcome = collect_beam_window(&mut transport, &mut card, 2, &advancing)
            .expect_err("a tick that came up short is a finding, not a data point");
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("tick 1 of 2"),
            "the short tick is named: {rendered}"
        );
        assert!(
            rendered.contains(&format!("{half} of {TICK_SAMPLES} samples")),
            "with what it carried against what a tick is: {rendered}"
        );
    }

    #[test]
    fn a_bench_run_on_a_working_board_passes_and_reports_the_pairing_it_found() {
        // The whole case end to end, off hardware: two windows collected from a
        // board and a card that agree with each other, then judged. Without this
        // the collection could hand `assess_beam_speech` its windows in either
        // order and every other test in the file would still pass.
        let mut board = BenchBoard::speaking_after(QUIET_TICKS);
        let mut card = bench_card(QUIET_TICKS, SPEECH_TICKS);
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = beam_energy_speech(&mut board, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("ch0 → beam 1") && rendered.contains("ch1 → beam 3"),
            "what each channel follows is the reading this case produces: {rendered}"
        );
        assert!(
            rendered.contains("output routing OP_L (category 1, source 4)")
                && rendered.contains("not sample-identical"),
            "read off the board once per run, before either window: {rendered}"
        );
        assert_eq!(
            card.remaining(),
            0,
            "both windows are collected from the one stream, in order"
        );
    }

    #[test]
    fn a_board_that_duplicates_one_source_passes_and_says_so() {
        // Both USB channels carrying the same samples, and both registers naming
        // one source. The end-to-end reading a human bakes the channel identity
        // from, produced by the collection rather than written into a window.
        let mut board = BenchBoard::speaking_after(QUIET_TICKS).routed(ONE_SOURCE);
        let mut card = bench_card_with(QUIET_TICKS, SPEECH_TICKS, true);
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = beam_energy_speech(&mut board, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("sample-identical") && !rendered.contains("not sample-identical"),
            "the collection is what notices the duplication: {rendered}"
        );
        assert!(
            rendered.contains("ch0 → beam 1") && rendered.contains("ch1 → beam 1"),
            "and both channels follow the one beam: {rendered}"
        );
    }

    #[test]
    fn a_board_that_duplicates_one_source_while_its_registers_disagree_fails() {
        let mut board = BenchBoard::speaking_after(QUIET_TICKS).routed(TWO_SOURCES);
        let mut card = bench_card_with(QUIET_TICKS, SPEECH_TICKS, true);
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = beam_energy_speech(&mut board, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(!outcome.passed(), "{}", detail(&outcome));
        assert!(
            detail(&outcome).contains("identical samples"),
            "{}",
            detail(&outcome)
        );
    }

    #[test]
    fn a_run_whose_audio_never_arrived_still_carries_the_routing_it_read() {
        // The registers are read before either window precisely so the earliest
        // failures — the likeliest bring-up outcomes — still carry the reading a
        // human came to the bench for. Both collection arms, since either can be
        // the one that ends the run: a card that never delivered at all, and one
        // that delivered the quiet room and then stopped.
        for (arm, quiet_ticks, ticks) in [
            ("the quiet window", 0, QUIET_TICKS),
            ("the speech window", QUIET_TICKS, SPEECH_TICKS),
        ] {
            let mut board = BenchBoard::speaking_after(QUIET_TICKS);
            let mut card = bench_card(quiet_ticks, 0);
            let clock = Clock::new();
            let advancing = || {
                let now = clock.now();
                clock.advance(Duration::from_millis(100));
                now
            };
            let mut out = Vec::new();
            let outcome = beam_energy_speech(&mut board, &mut card, &mut out, &advancing)
                .expect("a Vec never fails to write");

            assert!(!outcome.passed(), "{arm}: {}", detail(&outcome));
            let rendered = detail(&outcome);
            assert!(
                rendered.contains(&format!("tick 0 of {ticks}")),
                "{arm} is where it ended: {rendered}"
            );
            assert!(
                rendered.contains("output routing OP_L (category 1, source 4)")
                    && rendered.contains("OP_R (category 1, source 5)"),
                "{arm}: the reading was taken before the audio was asked for, so it survives \
                 the audio never arriving: {rendered}"
            );
        }
    }

    #[test]
    fn the_prompts_tell_the_operator_when_to_speak_and_when_to_stop() {
        // A room that never changes: the case is expected to say so rather than to
        // conclude a mapping from two windows that read the same.
        let mut card = bench_card(QUIET_TICKS + SPEECH_TICKS, 0);
        let mut board = BenchBoard::steady();
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = beam_energy_speech(&mut board, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(!outcome.passed(), "{}", detail(&outcome));
        assert!(
            detail(&outcome).contains("no beam's energy rose"),
            "and say which assertion it was: {}",
            detail(&outcome)
        );
        let printed = String::from_utf8(out).expect("utf8");
        assert!(printed.contains("keep the room quiet"), "{printed}");
        assert!(printed.contains("speak toward the array now"), "{printed}");
        assert!(
            printed.find("keep the room quiet") < printed.find("speak toward the array now"),
            "the quiet window is measured before the speech one: {printed}"
        );
        for window in [QUIET_TICKS, SPEECH_TICKS] {
            assert!(
                printed.contains(&format!("{:?}", window_duration(window))),
                "the operator is asked for as long as the window takes: {printed}"
            );
        }
    }
}
