//! The command line, and what the process's exit status means.
//!
//! There is one invocation — the daemon, given its configuration file — so the
//! parsing is small. It lives here rather than in `main` because the exit status
//! does not: an operator reading a number off a terminal after a supervised run
//! is reading the one thing this process says after it has stopped saying
//! anything, and that mapping is worth a test.

use std::io::Write;
use std::path::PathBuf;

use serde_json::json;

use crate::motion::Outcome;
use crate::report::Sink;

/// Exit status of a run an operator ended and the machine released cleanly.
pub const RELEASED: u8 = 0;

/// Exit status of a daemon that never took the machine: the configuration did
/// not resolve, a gate refused, the token file is not readable, or something
/// else holds the serial port.
///
/// [`brenn_bridge::exit::HARD_FAILURE`]'s number and its meaning — this process,
/// as configured, is wrong, and a restart into the same configuration reaches
/// the same verdict.
pub const STARTUP_REFUSED: u8 = brenn_bridge::exit::HARD_FAILURE;

/// Exit status of a daemon whose machine stopped taking commands. Torque was
/// left on and the servos are holding where they were; go and look at the
/// machine before restarting anything.
///
/// Deliberately not 1, 3, 4 or 5: those numbers are already spoken for by the
/// bridge library and by the speech surface's tools, and an operator reading a
/// code off a unit should not have to ask which binary produced it.
pub const FAULTED: u8 = 6;

/// Exit status of a daemon that stowed and left torque on because it had nothing
/// left to obey. Nothing is wrong with the machine; the attachment ended.
pub const PARKED: u8 = 7;

/// What this invocation asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// Run the daemon against this configuration file.
    Run(PathBuf),
    /// Print what the arguments are. Asked for, so not a failure.
    Help,
    /// The arguments say something this binary does not offer.
    Unrecognized,
}

/// Read the arguments, which are the program's own arguments without `argv[0]`.
#[must_use]
pub fn parse(args: &[String]) -> Invocation {
    match args {
        [one] if one == "-h" || one == "--help" => Invocation::Help,
        [one] if !one.starts_with('-') => Invocation::Run(PathBuf::from(one)),
        _ => Invocation::Unrecognized,
    }
}

/// Say what the arguments are, and answer with the status that saying it earns.
///
/// Zero when it was asked for, a refusal when it was not: a service that started
/// with the wrong arguments must not look to a supervisor like one that ran.
pub fn describe(out: &mut dyn Write, asked: bool) -> u8 {
    let _ = writeln!(
        out,
        "usage: reachy-motiond <config.toml>\n\
         \n\
         The head-presence motion daemon. It runs the same two standing gates the\n\
         bench does, arms the machine, stows it, and then holds it at whatever\n\
         posture the presence channel asks for until an operator signals it.\n\
         \n\
         Run it in the foreground with somebody watching. SIGINT or SIGTERM stows\n\
         the head, verifies it is there, and releases torque; every other ending\n\
         leaves the servos holding."
    );
    if asked { RELEASED } else { STARTUP_REFUSED }
}

/// The exit status an ending earns.
#[must_use]
pub fn exit_code(outcome: &Outcome) -> u8 {
    match outcome {
        Outcome::Released => RELEASED,
        Outcome::Parked => PARKED,
        Outcome::Faulted(_) => FAULTED,
    }
}

/// Say why the daemon never started, and answer with its status.
///
/// Both streams: a run that refused before it took the machine is as much a fact
/// of a capture as one that faulted holding it, and a supervisor reading only
/// JSONL should not have to infer a refusal from the absence of everything else.
pub fn refuse_startup(sink: &dyn Sink, error: &dyn std::fmt::Display) -> u8 {
    let detail = format!("{error:#}");
    sink.line(&format!("reachy-motiond: {detail}"));
    sink.event(
        "daemon_startup_refused",
        &json!({ "detail": detail, "code": STARTUP_REFUSED }),
    );
    STARTUP_REFUSED
}

#[cfg(test)]
mod tests {
    use crate::cells::{FaultReport, FaultStage};

    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    #[test]
    fn one_path_is_the_configuration_to_run_against() {
        assert_eq!(
            parse(&args(&["/etc/brenn/motiond.toml"])),
            Invocation::Run(PathBuf::from("/etc/brenn/motiond.toml"))
        );
    }

    #[test]
    fn help_is_asked_for_either_way_and_nothing_else_is_offered() {
        assert_eq!(parse(&args(&["-h"])), Invocation::Help);
        assert_eq!(parse(&args(&["--help"])), Invocation::Help);
        assert_eq!(parse(&args(&[])), Invocation::Unrecognized);
        assert_eq!(parse(&args(&["--verbose"])), Invocation::Unrecognized);
        assert_eq!(
            parse(&args(&["a.toml", "b.toml"])),
            Invocation::Unrecognized
        );
    }

    #[test]
    fn usage_earns_zero_when_it_was_asked_for_and_a_refusal_when_it_was_not() {
        let mut asked = Vec::new();
        assert_eq!(describe(&mut asked, true), RELEASED);
        let mut told = Vec::new();
        assert_eq!(describe(&mut told, false), STARTUP_REFUSED);
        assert_eq!(asked, told, "the text does not depend on who asked");
        assert!(
            String::from_utf8_lossy(&asked).contains("reachy-motiond <config.toml>"),
            "the usage names the one argument"
        );
    }

    /// A refusal before the machine was taken is on both streams, and the event
    /// carries the status the process is about to exit with.
    #[test]
    fn a_startup_refusal_is_narrated_and_captured() {
        let sink = crate::report::Collect::default();

        let code = refuse_startup(
            &sink,
            &"cannot read the daemon configuration at /etc/absent",
        );

        assert_eq!(code, STARTUP_REFUSED);
        let said = sink.said();
        assert!(said.lines[0].contains("/etc/absent"), "{:?}", said.lines);
        let fields = sink.fields("daemon_startup_refused").expect("reported");
        assert_eq!(fields["code"], json!(STARTUP_REFUSED));
        assert!(
            fields["detail"]
                .as_str()
                .is_some_and(|d| d.contains("/etc/absent")),
            "{fields}"
        );
    }

    #[test]
    fn every_ending_but_the_released_one_is_a_nonzero_status_of_its_own() {
        assert_eq!(exit_code(&Outcome::Released), 0);
        assert_ne!(exit_code(&Outcome::Parked), 0);
        assert_ne!(
            exit_code(&Outcome::Faulted(FaultReport::new(
                FaultStage::Presence,
                "read loss"
            ))),
            0
        );
        assert_ne!(
            exit_code(&Outcome::Parked),
            exit_code(&Outcome::Faulted(FaultReport::new(
                FaultStage::Presence,
                "read loss"
            ))),
            "a machine to go and look at is not the same ending as a bus that went away"
        );
    }
}
