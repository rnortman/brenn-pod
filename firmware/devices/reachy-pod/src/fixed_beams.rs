//! The XVF3800's fixed-beam mode, as the self-test registry exercises it.
//!
//! Fixed-beam mode pins both of the chip's focused beams to a commanded azimuth
//! and elevation instead of letting them track the loudest talker. This module is
//! the self-test registry's only writer of it; nothing in the pipeline writes it.
//!
//! Its two cases answer three unknowns: whether the board accepts the writes,
//! whether they read back, and where the chip then places a talker standing
//! straight ahead. The registry runs only while the pod is stopped, and the pod
//! reboots the chip on every start, so nothing written here outlives the next pod
//! start.

use std::fmt;
use std::io;
use std::time::Instant;

use xvf3800_ctrl::{
    AEC_FIXEDBEAMS_PAIR_LEN, AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD, AEC_FIXEDBEAMSAZIMUTH_VALUES_LABEL,
    AEC_FIXEDBEAMSELEVATION_VALUES_CMD, AEC_FIXEDBEAMSELEVATION_VALUES_LABEL,
    AEC_FIXEDBEAMSGATING_CMD, AEC_FIXEDBEAMSGATING_LABEL, AEC_FIXEDBEAMSGATING_LEN,
    AEC_FIXEDBEAMSONOFF_CMD, AEC_FIXEDBEAMSONOFF_LABEL, AEC_RESID, ControlTransport,
    SCALAR_READ_LEN, USB_RETRY, decode_f32x2, decode_i32, encode_f32x2, encode_i32,
};

use crate::beam::{
    BEAM_ENERGY_FLOOR, BeamWindow, MIN_TICKS, SPEECH_TICKS, collect_beam_window, window_duration,
};
use crate::regs::{read_register, write_register};
use crate::run::PeriodSource;
use crate::selftest::Outcome;

/// Both focused beams' fixed direction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FixedBeams {
    /// Per-beam azimuth, radians, in the chip's convention: the angle between the
    /// source and the array axis, π/2 broadside.
    pub azimuth: [f32; 2],
    /// Per-beam elevation, radians, in the chip's own convention — which neither
    /// case establishes; they assert what reads back and where the chip then
    /// places a talker, not what the elevation means.
    pub elevation: [f32; 2],
}

/// Broadside — straight out of the array, which on the robot is the way the head
/// faces.
pub const BROADSIDE_RAD: f32 = core::f32::consts::FRAC_PI_2;

/// The elevation both cases fix the beams at: a standing talker's mouth seen from a
/// table-top array at a metre or less.
pub const TALKER_ELEVATION_DEG: f32 = 27.0;

/// [`TALKER_ELEVATION_DEG`] in radians, as the chip takes it.
pub const TALKER_ELEVATION_RAD: f32 = TALKER_ELEVATION_DEG * (core::f32::consts::PI / 180.0);

/// What both cases fix: both beams at broadside, at the talker elevation.
pub const BROADSIDE: FixedBeams = FixedBeams {
    azimuth: [BROADSIDE_RAD; 2],
    elevation: [TALKER_ELEVATION_RAD; 2],
};

/// How far a talker straight ahead may read from broadside under fixed beams: 10°.
pub const BROADSIDE_TOLERANCE_DEG: f32 = 10.0;

/// The line that follows a release that did not take.
const MAY_STILL_BE_FIXED: &str = "the focused beams may still be fixed until the chip reboots";

// ── The register operations ───────────────────────────────────────────────────

/// Write both angle pairs, then switch the mode on. The angles go first so the
/// mode never comes on at the chip's (0, 0) defaults, even for one transaction.
///
/// # Errors
/// The first write that was not taken, as its one-line reading; nothing after it
/// is written.
fn fix<T: ControlTransport>(transport: &mut T, beams: &FixedBeams) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    write_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD,
        &encode_f32x2(beams.azimuth),
        AEC_FIXEDBEAMSAZIMUTH_VALUES_LABEL,
    )?;
    write_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSELEVATION_VALUES_CMD,
        &encode_f32x2(beams.elevation),
        AEC_FIXEDBEAMSELEVATION_VALUES_LABEL,
    )?;
    write_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSONOFF_CMD,
        &encode_i32(1),
        AEC_FIXEDBEAMSONOFF_LABEL,
    )
}

/// Return both focused beams to tracking.
///
/// # Errors
/// The write not being taken, as its one-line reading.
fn release<T: ControlTransport>(transport: &mut T) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    write_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSONOFF_CMD,
        &encode_i32(0),
        AEC_FIXEDBEAMSONOFF_LABEL,
    )
}

/// The mode switch as the chip reports it.
///
/// # Errors
/// The read failing, as its one-line reading.
fn read_onoff<T: ControlTransport>(transport: &mut T) -> Result<i32, String>
where
    T::Error: fmt::Display,
{
    let mut payload = [0u8; SCALAR_READ_LEN];
    read_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSONOFF_CMD,
        &mut payload,
        AEC_FIXEDBEAMSONOFF_LABEL,
    )?;
    Ok(decode_i32(&payload))
}

/// One angle pair as the chip reports it.
///
/// # Errors
/// The read failing, as its one-line reading.
fn read_pair<T: ControlTransport>(
    transport: &mut T,
    cmd: u8,
    label: &str,
) -> Result<[f32; 2], String>
where
    T::Error: fmt::Display,
{
    let mut payload = [0u8; AEC_FIXEDBEAMS_PAIR_LEN];
    read_register(transport, USB_RETRY, AEC_RESID, cmd, &mut payload, label)?;
    Ok(decode_f32x2(&payload))
}

/// The gating switch as the chip reports it.
///
/// # Errors
/// The read failing, as its one-line reading.
fn read_gating<T: ControlTransport>(transport: &mut T) -> Result<u8, String>
where
    T::Error: fmt::Display,
{
    let mut payload = [0u8; AEC_FIXEDBEAMSGATING_LEN];
    read_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_FIXEDBEAMSGATING_CMD,
        &mut payload,
        AEC_FIXEDBEAMSGATING_LABEL,
    )?;
    Ok(payload[0])
}

/// An angle pair in Rust's shortest round-trip form, so a readback that differs
/// from what was written in the last bit renders differently from it.
fn render_pair(v: [f32; 2]) -> String {
    format!("[{:?}, {:?}]", v[0], v[1])
}

/// `outcome`, unless the release after it was not taken — then the case fails
/// whatever it found, with the release's reading first and what it found after.
fn released(outcome: Outcome, freed: Result<(), String>) -> Outcome {
    let Err(line) = freed else {
        return outcome;
    };
    let mut lines = vec![format!("{line}: {MAY_STILL_BE_FIXED}")];
    match outcome {
        Outcome::Pass(d) | Outcome::NotRun(d) => lines.push(d),
        Outcome::Fail(found) => lines.extend(found),
    }
    Outcome::Fail(lines)
}

/// Whether two angle pairs are the same bits.
fn same_bits(a: [f32; 2], b: [f32; 2]) -> bool {
    a.map(f32::to_bits) == b.map(f32::to_bits)
}

// ── The cases ─────────────────────────────────────────────────────────────────

/// The board takes fixed-beam mode and reports it back as written.
///
/// Asserts that the board takes both angle pairs and the switch, that all three
/// read back exactly — bit for bit — as written, and that releasing the mode reads
/// back 0. Gating is read and recorded, never written. An inexact readback, such
/// as a chip that quantises the angles, fails and goes to a human rather than
/// being accepted.
///
/// The angles stay written, with the mode off, until the next chip reboot. The
/// chip uses them only while the mode is on, so the case does not restore them.
pub fn ctrl_fixedbeams<T: ControlTransport>(transport: &mut T) -> Outcome
where
    T::Error: fmt::Display,
{
    let gating = match read_gating(transport) {
        Ok(g) => g,
        Err(line) => return Outcome::fail(line),
    };
    let before = match read_onoff(transport) {
        Ok(v) => v,
        Err(line) => return Outcome::fail(line),
    };

    let mut lines = Vec::new();
    let readback = fix(transport, &BROADSIDE).and_then(|()| {
        let onoff = read_onoff(transport)?;
        let azimuth = read_pair(
            transport,
            AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD,
            AEC_FIXEDBEAMSAZIMUTH_VALUES_LABEL,
        )?;
        let elevation = read_pair(
            transport,
            AEC_FIXEDBEAMSELEVATION_VALUES_CMD,
            AEC_FIXEDBEAMSELEVATION_VALUES_LABEL,
        )?;
        Ok((onoff, azimuth, elevation))
    });
    match readback {
        Err(line) => lines.push(line),
        Ok((onoff, azimuth, elevation)) => {
            if onoff != 1 {
                lines.push(format!(
                    "{AEC_FIXEDBEAMSONOFF_LABEL} was written 1 and reads back {onoff}"
                ));
            }
            for (label, written, read) in [
                (
                    AEC_FIXEDBEAMSAZIMUTH_VALUES_LABEL,
                    BROADSIDE.azimuth,
                    azimuth,
                ),
                (
                    AEC_FIXEDBEAMSELEVATION_VALUES_LABEL,
                    BROADSIDE.elevation,
                    elevation,
                ),
            ] {
                if !same_bits(written, read) {
                    lines.push(format!(
                        "{label} was written {} and reads back {}",
                        render_pair(written),
                        render_pair(read)
                    ));
                }
            }
        }
    }

    match release(transport).and_then(|()| read_onoff(transport)) {
        Ok(0) => {}
        Ok(v) => lines.push(format!(
            "{AEC_FIXEDBEAMSONOFF_LABEL} was written 0 and reads back {v}: {MAY_STILL_BE_FIXED}"
        )),
        Err(line) => lines.push(format!("{line}: {MAY_STILL_BE_FIXED}")),
    }

    let reading = format!("GATING {gating} (read, never written); ONOFF {before} before the case");
    if lines.is_empty() {
        return Outcome::Pass(format!(
            "{reading}; fixed at azimuths {} rad, elevations {} rad, read back exactly; ONOFF 0 \
             after release",
            render_pair(BROADSIDE.azimuth),
            render_pair(BROADSIDE.elevation)
        ));
    }
    lines.push(reading);
    lines.push(
        "an unexpected readback is a reading to review, not one to accept: review it before this \
         case is changed to pass"
            .to_string(),
    );
    Outcome::Fail(lines)
}

/// Stand straight in front of the array with the beams fixed at broadside, and see
/// where the chip places you.
///
/// Asserts that, with both focused beams fixed at broadside and a talker straight
/// ahead, beams 0 and 1 place the talker within [`BROADSIDE_TOLERANCE_DEG`] of
/// broadside on every tick on which that beam heard speech. A failure is the
/// reading that says whether the array has an axis offset or the talker stood
/// off-centre. Whether the audio is better with the beams fixed is judged by ear
/// during the same session, not here.
///
/// The beams are released before the case returns, whatever happened, and before
/// any transcript write can end it early.
pub fn fixed_beams_broadside<T: ControlTransport, S: PeriodSource>(
    transport: &mut T,
    source: &mut S,
    out: &mut dyn io::Write,
    now: &dyn Fn() -> Instant,
) -> io::Result<Outcome>
where
    T::Error: fmt::Display,
{
    if let Err(line) = fix(transport, &BROADSIDE) {
        return Ok(released(Outcome::fail(line), release(transport)));
    }
    let prompted = writeln!(
        out,
        "     fixed_beams_broadside: both focused beams are fixed at broadside — stand straight \
         in front of the array, on the side the robot faces, and speak toward it for the next \
         {:?}",
        window_duration(SPEECH_TICKS)
    );
    let window = if prompted.is_ok() {
        collect_beam_window(transport, source, SPEECH_TICKS, now)
    } else {
        Err(Outcome::NotRun(
            "the prompt could not be written".to_string(),
        ))
    };
    let freed = release(transport);
    prompted?;
    writeln!(out, "     fixed_beams_broadside: that is enough, thank you")?;
    let outcome = match window {
        Ok(w) => assess_broadside(&w),
        Err(o) => o,
    };
    Ok(released(outcome, freed))
}

/// Judge one window taken with both focused beams fixed at broadside.
///
/// A chip that reports the fixed azimuth back verbatim passes with a spread of
/// zero (`90.0°..90.0°`). The detail shows that, and it is still a reading worth
/// having: it says the chip's DoA under fixed beams is the commanded direction,
/// not a measurement.
pub fn assess_broadside(window: &BeamWindow) -> Outcome {
    if let Some((tick, beam, what, value)) = window.implausible() {
        return Outcome::fail(format!(
            "tick {tick}: beam {beam} {what} {value} is not a value the chip should report"
        ));
    }
    let total = window.ticks.len();
    let tolerance = BROADSIDE_TOLERANCE_DEG.to_radians();
    let mut failures = Vec::new();
    let mut summaries = Vec::new();
    for beam in 0..2 {
        // The pipeline's own gate level: a tick that is quiet on this beam says
        // nothing about where the talker is.
        let heard: Vec<f32> = window
            .ticks
            .iter()
            .filter(|t| t.energy[beam] >= BEAM_ENERGY_FLOOR)
            .map(|t| t.azimuth[beam])
            .collect();
        let finite: Vec<f32> = heard.iter().copied().filter(|v| v.is_finite()).collect();
        summaries.push(if finite.is_empty() {
            format!(
                "beam {beam}: {} of {total} ticks heard speech, none with a direction",
                heard.len()
            )
        } else {
            let min = finite.iter().copied().fold(f32::INFINITY, f32::min);
            let max = finite.iter().copied().fold(f32::NEG_INFINITY, f32::max);
            let mean = finite.iter().sum::<f32>() / finite.len() as f32;
            format!(
                "beam {beam}: {} of {total} ticks heard speech, {} with a direction, \
                 {:.1}°..{:.1}° (mean {:.1}°)",
                heard.len(),
                finite.len(),
                min.to_degrees(),
                max.to_degrees(),
                mean.to_degrees()
            )
        });
        if finite.len() < MIN_TICKS {
            failures.push(format!(
                "beam {beam} placed the talker on {} ticks, fewer than {MIN_TICKS}: not heard \
                 well enough to judge",
                finite.len()
            ));
            continue;
        }
        let off = finite
            .iter()
            .filter(|v| (**v - BROADSIDE_RAD).abs() > tolerance)
            .count();
        if off > 0 {
            failures.push(format!(
                "beam {beam}: {off} of {} readings lie more than {BROADSIDE_TOLERANCE_DEG}° from \
                 broadside",
                finite.len()
            ));
        }
    }
    let others = window.mean_finite_azimuths();
    let trailing = format!(
        "free-running {:.1}°, auto-select {:.1}° (mean, not judged)",
        others[2].to_degrees(),
        others[3].to_degrees()
    );
    if failures.is_empty() {
        return Outcome::Pass(format!(
            "fixed at azimuth {:.1}°, elevation {:.1}°; {}; {trailing}",
            BROADSIDE_RAD.to_degrees(),
            TALKER_ELEVATION_DEG,
            summaries.join("; ")
        ));
    }
    let mut lines = failures;
    lines.extend(summaries);
    lines.push(trailing);
    lines.push(
        "a talker straight ahead read off broadside is the array's own offset or the talker's \
         placement: review the reading before this case is changed to pass"
            .to_string(),
    );
    Outcome::Fail(lines)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::alsa_capture::PERIOD_FRAMES;
    use crate::beam::{BEAMS, BeamTick, TICK_PERIODS};
    use crate::config::CHANNELS;
    use crate::test_support::{
        Clock, RegisterBank, Scripted, ScriptedCard, detail, f32x4_bytes, fixed_beam_answers,
    };
    use xvf3800_ctrl::{AEC_AZIMUTH_VALUES_CMD, AEC_SPENERGY_VALUES_CMD, STATUS_DONE};

    const FRAC_PI_2: f32 = core::f32::consts::FRAC_PI_2;

    fn onoff_write(v: i32) -> (u8, u8, Vec<u8>) {
        (AEC_RESID, AEC_FIXEDBEAMSONOFF_CMD, encode_i32(v).to_vec())
    }

    fn fix_writes() -> Vec<(u8, u8, Vec<u8>)> {
        vec![
            (
                AEC_RESID,
                AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD,
                encode_f32x2(BROADSIDE.azimuth).to_vec(),
            ),
            (
                AEC_RESID,
                AEC_FIXEDBEAMSELEVATION_VALUES_CMD,
                encode_f32x2(BROADSIDE.elevation).to_vec(),
            ),
            onoff_write(1),
        ]
    }

    #[test]
    fn a_board_that_takes_the_fixed_beams_passes_with_every_reading() {
        let mut t = Scripted::sequenced(fixed_beam_answers(0));
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(outcome.passed(), "{}", detail(&outcome));
        let mut expected = fix_writes();
        expected.push(onoff_write(0));
        assert_eq!(
            t.writes, expected,
            "angles, then the switch, then the release"
        );
        assert_eq!(
            t.registers,
            vec![
                (AEC_RESID, AEC_FIXEDBEAMSGATING_CMD),
                (AEC_RESID, AEC_FIXEDBEAMSONOFF_CMD),
                (AEC_RESID, AEC_FIXEDBEAMSONOFF_CMD),
                (AEC_RESID, AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD),
                (AEC_RESID, AEC_FIXEDBEAMSELEVATION_VALUES_CMD),
                (AEC_RESID, AEC_FIXEDBEAMSONOFF_CMD),
            ]
        );
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("GATING 0 (read, never written)"),
            "{rendered}"
        );
        assert!(
            rendered.contains(&render_pair(BROADSIDE.azimuth)),
            "{rendered}"
        );
    }

    #[test]
    fn gating_is_recorded_and_never_written() {
        let mut t = Scripted::sequenced(fixed_beam_answers(1));
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(outcome.passed(), "{}", detail(&outcome));
        assert!(
            detail(&outcome).contains("GATING 1"),
            "{}",
            detail(&outcome)
        );
        assert!(
            t.writes
                .iter()
                .all(|(_, cmd, _)| *cmd != AEC_FIXEDBEAMSGATING_CMD),
            "{:?}",
            t.writes
        );
    }

    #[test]
    fn a_readback_that_did_not_take_fails_by_register_and_still_releases() {
        let mut answers = fixed_beam_answers(0);
        answers[3] = (STATUS_DONE, encode_f32x2([0.0, 0.0]).to_vec());
        let mut t = Scripted::sequenced(answers);
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("AEC_FIXEDBEAMSAZIMUTH_VALUES (resid 33 cmd 81) was written"),
            "{rendered}"
        );
        assert!(rendered.contains("reads back [0.0, 0.0]"), "{rendered}");
        assert!(rendered.contains("a reading to review"), "{rendered}");
        assert_eq!(t.writes.last(), Some(&onoff_write(0)));
    }

    #[test]
    fn a_readback_off_by_one_bit_is_a_reading_to_review_not_a_pass() {
        let nudged = f32::from_bits(TALKER_ELEVATION_RAD.to_bits() + 1);
        let mut answers = fixed_beam_answers(0);
        answers[4] = (STATUS_DONE, encode_f32x2([nudged; 2]).to_vec());
        let mut t = Scripted::sequenced(answers);
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(AEC_FIXEDBEAMSELEVATION_VALUES_LABEL),
            "{rendered}"
        );
        assert!(
            !rendered.contains(AEC_FIXEDBEAMSAZIMUTH_VALUES_LABEL),
            "{rendered}"
        );
    }

    #[test]
    fn a_release_that_does_not_take_fails_the_case() {
        let mut answers = fixed_beam_answers(0);
        answers[5] = (STATUS_DONE, 1i32.to_le_bytes().to_vec());
        let mut t = Scripted::sequenced(answers);
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(
            rendered.contains("was written 0 and reads back 1"),
            "{rendered}"
        );
        assert!(rendered.contains("may still be fixed"), "{rendered}");
    }

    #[test]
    fn writes_that_fail_still_attempt_the_release() {
        let mut bank = RegisterBank::new().failing_writes("pipe error");
        let outcome = ctrl_fixedbeams(&mut bank);

        assert!(!outcome.passed());
        assert_eq!(
            bank.writes,
            vec![
                (
                    AEC_RESID,
                    AEC_FIXEDBEAMSAZIMUTH_VALUES_CMD,
                    encode_f32x2(BROADSIDE.azimuth).to_vec()
                ),
                onoff_write(0),
            ],
            "the first failure stops the fix, and the release is tried anyway"
        );
        let rendered = detail(&outcome);
        assert!(
            rendered.contains(
                "AEC_FIXEDBEAMSAZIMUTH_VALUES (resid 33 cmd 81) write failed: pipe error"
            ),
            "{rendered}"
        );
        assert!(rendered.contains("may still be fixed"), "{rendered}");
    }

    #[test]
    fn a_board_that_will_not_answer_is_left_untouched() {
        let mut t = Scripted::failing("no such device");
        let outcome = ctrl_fixedbeams(&mut t);

        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("AEC_FIXEDBEAMSGATING (resid 33 cmd 83)"),
            "{}",
            detail(&outcome)
        );
        assert!(t.writes.is_empty(), "{:?}", t.writes);
    }

    fn tick(energy: [f32; BEAMS], azimuth: [f32; BEAMS]) -> BeamTick {
        BeamTick {
            energy,
            azimuth,
            channel_power: [0.0; CHANNELS],
            channels_identical: false,
        }
    }

    fn broadside_window(n: usize) -> BeamWindow {
        BeamWindow {
            ticks: (0..n)
                .map(|i| {
                    let wobble = if i % 2 == 0 { 0.05 } else { -0.05 };
                    tick(
                        [2.0; BEAMS],
                        [FRAC_PI_2 + wobble, FRAC_PI_2, 0.3, FRAC_PI_2],
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn a_talker_at_broadside_passes_with_both_beams_reported() {
        let outcome = assess_broadside(&broadside_window(SPEECH_TICKS));

        assert!(outcome.passed(), "{}", detail(&outcome));
        let rendered = detail(&outcome);
        for part in ["beam 0:", "beam 1:", "free-running", "elevation 27.0°"] {
            assert!(rendered.contains(part), "{part}: {rendered}");
        }
    }

    #[test]
    fn a_beam_off_broadside_fails_naming_the_beam() {
        let mut window = broadside_window(SPEECH_TICKS);
        for t in &mut window.ticks {
            t.azimuth[1] = 105f32.to_radians();
        }
        let outcome = assess_broadside(&window);

        assert!(!outcome.passed());
        let rendered = detail(&outcome);
        assert!(rendered.contains("beam 1: "), "{rendered}");
        assert!(
            rendered.contains("more than 10° from broadside"),
            "{rendered}"
        );
        match &outcome {
            Outcome::Fail(lines) => assert!(
                !lines
                    .iter()
                    .any(|l| l.starts_with("beam 0") && l.contains("from broadside")),
                "{lines:?}"
            ),
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[test]
    fn a_reading_on_a_quiet_beam_is_not_judged() {
        let mut ticks = vec![tick([2.0; BEAMS], [FRAC_PI_2; BEAMS]); MIN_TICKS];
        ticks.extend(vec![
            tick([0.2; BEAMS], [30f32.to_radians(); BEAMS]);
            MIN_TICKS
        ]);
        let outcome = assess_broadside(&BeamWindow { ticks });

        assert!(outcome.passed(), "{}", detail(&outcome));
    }

    #[test]
    fn too_few_heard_ticks_is_a_failure_not_a_pass() {
        let quiet = BeamWindow {
            ticks: vec![tick([0.2; BEAMS], [FRAC_PI_2; BEAMS]); SPEECH_TICKS],
        };
        let outcome = assess_broadside(&quiet);
        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("fewer than"),
            "{}",
            detail(&outcome)
        );

        let undirected = BeamWindow {
            ticks: vec![
                tick([2.0; BEAMS], [f32::NAN, FRAC_PI_2, FRAC_PI_2, FRAC_PI_2]);
                SPEECH_TICKS
            ],
        };
        let outcome = assess_broadside(&undirected);
        assert!(!outcome.passed());
        match &outcome {
            Outcome::Fail(lines) => {
                assert!(
                    lines
                        .iter()
                        .any(|l| l.starts_with("beam 0") && l.contains("fewer than")),
                    "{lines:?}"
                );
                assert!(
                    !lines
                        .iter()
                        .any(|l| l.starts_with("beam 1") && l.contains("fewer than")),
                    "{lines:?}"
                );
            }
            other => panic!("expected a failure, got {other:?}"),
        }
    }

    #[test]
    fn an_implausible_reading_is_named_by_tick() {
        let mut window = broadside_window(SPEECH_TICKS);
        window.ticks[3].azimuth[0] = f32::INFINITY;
        let outcome = assess_broadside(&window);

        assert!(!outcome.passed());
        assert!(detail(&outcome).contains("tick 3"), "{}", detail(&outcome));
    }

    #[test]
    fn the_bench_case_fixes_before_it_asks_and_releases_after() {
        let mut bank = RegisterBank::new();
        let mut card = ScriptedCard::stalled();
        let clock = Clock::new();
        let advancing = || {
            let now = clock.now();
            clock.advance(Duration::from_millis(250));
            now
        };
        let mut out = Vec::new();
        let outcome = fixed_beams_broadside(&mut bank, &mut card, &mut out, &advancing)
            .expect("a Vec never fails to write");

        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("the card delivered"),
            "{}",
            detail(&outcome)
        );
        let mut expected = fix_writes();
        expected.push(onoff_write(0));
        assert_eq!(bank.writes, expected);
        let transcript = String::from_utf8(out).expect("utf-8");
        assert!(
            transcript.contains("stand straight in front of the array"),
            "{transcript}"
        );
        assert!(transcript.contains("that is enough"), "{transcript}");
    }

    #[test]
    fn a_fix_that_fails_asks_nobody_to_speak_and_still_releases() {
        let mut bank = RegisterBank::new().failing_writes("pipe error");
        let mut card = ScriptedCard::stalled();
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = fixed_beams_broadside(&mut bank, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(!outcome.passed());
        assert!(
            detail(&outcome).contains("may still be fixed"),
            "{}",
            detail(&outcome)
        );
        let transcript = String::from_utf8(out).expect("utf-8");
        assert!(!transcript.contains("stand straight"), "{transcript}");
        assert_eq!(bank.writes.last(), Some(&onoff_write(0)));
        assert!(card.waits.is_empty(), "{:?}", card.waits);
    }

    #[test]
    fn a_bench_run_at_broadside_passes_end_to_end() {
        let mut bank = RegisterBank::new();
        bank.set(AEC_RESID, AEC_SPENERGY_VALUES_CMD, f32x4_bytes([2.0; 4]));
        bank.set(
            AEC_RESID,
            AEC_AZIMUTH_VALUES_CMD,
            f32x4_bytes([FRAC_PI_2, FRAC_PI_2, 0.3, FRAC_PI_2]),
        );
        let mut card =
            ScriptedCard::delivering(vec![
                vec![100i16; PERIOD_FRAMES as usize * CHANNELS];
                SPEECH_TICKS * TICK_PERIODS
            ])
            .quiet_when_drained();
        let clock = Clock::new();
        let mut out = Vec::new();
        let outcome = fixed_beams_broadside(&mut bank, &mut card, &mut out, &|| clock.now())
            .expect("a Vec never fails to write");

        assert!(outcome.passed(), "{}", detail(&outcome));
        assert_eq!(
            bank.reads_of(AEC_RESID, AEC_AZIMUTH_VALUES_CMD),
            SPEECH_TICKS
        );
        assert_eq!(bank.writes.last(), Some(&onoff_write(0)));
    }
}
