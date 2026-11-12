//! The command line: six flags, every one required, each at most once, in any
//! order. No subcommand and no mode flag: the binary has one job.

use std::ffi::OsString;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::IntoDeserializer;

use crate::granted::SampleFormat;

pub const USAGE: &str = "usage: pod-capture --card <NAME> --rate <HZ> \
    --format <s16_le|s32_le> --layout <a,b,c,...> --pod-id <NAME> --out <PATH>";

/// The lowest rate accepted: the period is a 20 ms slice of the rate, and below
/// 50 Hz that slice holds no frame.
pub const MIN_RATE_HZ: u32 = 50;

/// The most bytes a pod id holds: what the hello's `pod_id` carries.
pub const MAX_POD_ID_BYTES: usize = 32;

/// What the command line asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    /// Matched case-insensitively against each card's name and long name; exactly
    /// one card must match.
    pub card: String,
    pub rate_hz: u32,
    pub format: SampleFormat,
    /// One name per channel, in interleave order. Its length is the channel count
    /// demanded of the device.
    pub layout: Vec<String>,
    /// The stream's source, as the hello names it: 1 to
    /// [`MAX_POD_ID_BYTES`] bytes.
    pub pod_id: String,
    /// Must not exist.
    pub out: PathBuf,
}

/// Read the flags out of the arguments, which the caller has already stripped of
/// the program name. `Err` says what was not understood.
///
/// Each flag takes the next word as its value, which must not itself start with
/// `--`; `--flag=value` is not a spelling this accepts.
pub fn parse(args: &[OsString]) -> Result<Args, String> {
    let mut card = None;
    let mut rate = None;
    let mut format = None;
    let mut layout = None;
    let mut pod_id = None;
    let mut out = None;
    let mut words = args.iter();
    while let Some(flag) = words.next() {
        let flag = flag
            .to_str()
            .ok_or_else(|| format!("{flag:?} is not a flag this understands"))?;
        let slot: &mut Option<OsString> = match flag {
            "--card" => &mut card,
            "--rate" => &mut rate,
            "--format" => &mut format,
            "--layout" => &mut layout,
            "--pod-id" => &mut pod_id,
            "--out" => &mut out,
            other => return Err(format!("{other:?} is not a flag this understands")),
        };
        if slot.is_some() {
            return Err(format!("{flag} is given more than once"));
        }
        let value = words
            .next()
            .filter(|v| !v.to_string_lossy().starts_with("--"))
            .ok_or_else(|| format!("{flag} needs a value"))?;
        *slot = Some(value.clone());
    }
    let required =
        |slot: Option<OsString>, flag: &str| slot.ok_or_else(|| format!("{flag} is required"));
    let text = |value: OsString, flag: &str| {
        value
            .into_string()
            .map_err(|v| format!("{flag} {v:?} is not UTF-8"))
    };

    let card = text(required(card, "--card")?, "--card")?;
    if card.is_empty() {
        return Err("--card is empty".to_owned());
    }
    let rate = text(required(rate, "--rate")?, "--rate")?;
    let rate_hz: u32 = rate
        .parse()
        .map_err(|_| format!("--rate {rate:?} is not a whole number of hertz"))?;
    if rate_hz < MIN_RATE_HZ {
        return Err(format!("--rate {rate_hz} is under {MIN_RATE_HZ} Hz"));
    }
    let format = text(required(format, "--format")?, "--format")?;
    // The event lines' names for the formats, so the flag takes exactly what
    // `capture_started` can say.
    let format = SampleFormat::deserialize(format.as_str().into_deserializer())
        .map_err(|e: serde::de::value::Error| format!("--format {format:?}: {e}"))?;
    let layout = parse_layout(&text(required(layout, "--layout")?, "--layout")?)?;
    let pod_id = text(required(pod_id, "--pod-id")?, "--pod-id")?;
    if pod_id.is_empty() || pod_id.len() > MAX_POD_ID_BYTES {
        return Err(format!(
            "--pod-id {pod_id:?} is {} bytes; it must be 1 to {MAX_POD_ID_BYTES}",
            pod_id.len()
        ));
    }
    let out = PathBuf::from(required(out, "--out")?);
    Ok(Args {
        card,
        rate_hz,
        format,
        layout,
        pod_id,
        out,
    })
}

/// Comma-separated channel names: non-empty and distinct.
fn parse_layout(value: &str) -> Result<Vec<String>, String> {
    let names: Vec<String> = value.split(',').map(str::to_owned).collect();
    for (i, name) in names.iter().enumerate() {
        if name.is_empty() {
            return Err(format!(
                "--layout {value:?} has an empty name at channel {i}"
            ));
        }
        if names[..i].contains(name) {
            return Err(format!("--layout {value:?} names {name:?} twice"));
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<OsString> {
        line.split_whitespace().map(OsString::from).collect()
    }

    const FULL: &str = "--card Flex --rate 16000 --format s16_le --layout a,b,c --pod-id bench --out /tmp/x.framelog";

    #[test]
    fn every_flag_in_any_order() {
        let want = Args {
            card: "Flex".to_owned(),
            rate_hz: 16_000,
            format: SampleFormat::S16Le,
            layout: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            pod_id: "bench".to_owned(),
            out: PathBuf::from("/tmp/x.framelog"),
        };
        assert_eq!(parse(&words(FULL)), Ok(want.clone()));
        assert_eq!(
            parse(&words(
                "--out /tmp/x.framelog --pod-id bench --layout a,b,c --format s16_le --rate 16000 --card Flex"
            )),
            Ok(want)
        );
        let s32 = parse(&words(&FULL.replace("s16_le", "s32_le"))).unwrap();
        assert_eq!(s32.format, SampleFormat::S32Le);
    }

    #[test]
    fn each_flag_is_required() {
        for flag in [
            "--card", "--rate", "--format", "--layout", "--pod-id", "--out",
        ] {
            let mut w = words(FULL);
            let at = w.iter().position(|x| x == flag).unwrap();
            w.drain(at..at + 2);
            let err = parse(&w).unwrap_err();
            assert_eq!(err, format!("{flag} is required"));
        }
    }

    #[test]
    fn no_flag_is_repeated() {
        let err = parse(&words(&format!("{FULL} --rate 48000"))).unwrap_err();
        assert_eq!(err, "--rate is given more than once");
    }

    #[test]
    fn a_flag_needs_a_value_that_is_not_a_flag() {
        assert_eq!(
            parse(&words("--card --rate 16000")).unwrap_err(),
            "--card needs a value"
        );
        assert_eq!(
            parse(&words(&format!("{FULL} --bogus"))).unwrap_err(),
            "\"--bogus\" is not a flag this understands"
        );
        assert!(parse(&words(&FULL.replace(" /tmp/x.framelog", ""))).is_err());
    }

    #[test]
    fn bad_values_are_refused() {
        for (from, to) in [
            ("16000", "16k"),
            ("16000", "49"),
            ("s16_le", "s24_le"),
            ("Flex", "--"),
        ] {
            assert!(parse(&words(&FULL.replace(from, to))).is_err(), "{to}");
        }
        assert!(parse(&words(&FULL.replace("16000", "50"))).is_ok());
    }

    #[test]
    fn an_unknown_format_is_refused_with_the_line_s_names() {
        let err = parse(&words(&FULL.replace("s16_le", "s24_le"))).unwrap_err();
        assert!(err.starts_with("--format \"s24_le\""), "{err}");
        assert!(
            err.contains("`s16_le`") && err.contains("`s32_le`"),
            "{err}"
        );
    }

    #[test]
    fn bad_layout_names_are_refused() {
        for layout in ["a,,c", ",a", "a,", "a,b,a"] {
            let err = parse(&words(&FULL.replace("a,b,c", layout))).unwrap_err();
            assert!(err.starts_with("--layout"), "{layout}: {err}");
        }
        let one = parse(&words(&FULL.replace("a,b,c", "mono"))).unwrap();
        assert_eq!(one.layout, vec!["mono".to_owned()]);
    }

    #[test]
    fn a_pod_id_is_1_to_32_bytes() {
        let at_most = "p".repeat(MAX_POD_ID_BYTES);
        let ok = parse(&words(&FULL.replace("bench", &at_most))).unwrap();
        assert_eq!(ok.pod_id, at_most);
        let err = parse(&words(&FULL.replace("bench", &format!("{at_most}p")))).unwrap_err();
        assert!(err.starts_with("--pod-id"), "{err}");
        // Words split on whitespace cannot carry an empty value; build it.
        let mut w = words(FULL);
        let at = w.iter().position(|x| x == "bench").unwrap();
        w[at] = OsString::new();
        assert!(parse(&w).unwrap_err().starts_with("--pod-id"));
    }

    #[test]
    fn an_empty_command_line_is_refused() {
        assert_eq!(parse(&[]).unwrap_err(), "--card is required");
    }
}
