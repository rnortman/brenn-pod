//! The command line: what the arguments select, and what the exit status says
//! about a run.
//!
//! Both live in the library rather than in `main.rs` because the exit status is
//! the machine-readable half of the bring-up contract — the self-test target runs
//! this binary over SSH and reads the status to decide whether the bench is green
//! — so the mapping is asserted rather than assumed. A status that said 0 over a
//! transcript full of FAIL lines is exactly the kind of laundering the bring-up
//! doctrine forbids, and nobody re-reads a transcript the status called good.

use std::io;

use crate::logging;
use crate::selftest::Report;

/// Every case ran and passed.
pub const EXIT_OK: u8 = 0;
/// A case failed, or could not be attempted.
pub const EXIT_FAILED: u8 = 1;
/// The command line was not one this binary understands.
pub const EXIT_USAGE: u8 = 2;

/// What the arguments selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Bring the pipeline up and hold it there.
    Run,
    /// Run the bring-up registry and report.
    Selftest,
    /// Run the bench registry — the cases that need someone at the array.
    SelftestManual,
    /// Nothing this binary understands.
    Unrecognized,
}

/// Read the command out of the arguments, which the caller has already stripped of
/// the program name.
///
/// Only recognized words, and nothing beyond them: a trailing argument is a command
/// line whose intent is unclear, and a self-test run started on a guess is worse
/// than one refused.
pub fn parse(args: &[String]) -> Command {
    match args {
        [command] if command == "run" => Command::Run,
        [command] if command == "selftest" => Command::Selftest,
        [command, flag] if command == "selftest" && flag == "--manual" => Command::SelftestManual,
        _ => Command::Unrecognized,
    }
}

/// Exit status for a completed self-test run.
///
/// Anything short of every case passing is a failure: a case that could not be
/// attempted asserted nothing, and a run that attempted nothing least of all.
pub fn report_exit(result: io::Result<Report>, err: &mut dyn io::Write) -> u8 {
    match result {
        Ok(report) if report.all_passed() => EXIT_OK,
        Ok(_) => EXIT_FAILED,
        // The transcript is the report; if it cannot be written there is nothing to
        // say about the run beyond why.
        Err(e) => {
            let _ = writeln!(err, "reachy-pod: cannot write the self-test report: {e}");
            EXIT_FAILED
        }
    }
}

/// Print what this binary accepts, naming the arguments that were not understood.
pub fn usage(err: &mut dyn io::Write, given: &[String]) -> u8 {
    if !given.is_empty() {
        let _ = writeln!(err, "reachy-pod: unrecognized command: {}", given.join(" "));
    }
    let _ = writeln!(err, "usage: reachy-pod run | selftest [--manual]");
    let _ = writeln!(err);
    let _ = writeln!(
        err,
        "  run                 capture, gate, stream to the audio host, and play back"
    );
    let _ = writeln!(
        err,
        "  selftest            run the unattended hardware bring-up registry and report"
    );
    let _ = writeln!(
        err,
        "  selftest --manual   run the bench registry, which asks you to speak at the array"
    );
    let _ = writeln!(err);
    let _ = writeln!(
        err,
        "{}=<error|warn|info|debug|trace> sets the log level (default {}).",
        logging::LEVEL_ENV,
        logging::DEFAULT_LEVEL
    );
    EXIT_USAGE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selftest::Outcome;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    /// A report holding exactly the outcomes given, recorded the way a run records
    /// them.
    fn report_of(outcomes: Vec<Outcome>) -> Report {
        let mut sink = Vec::new();
        let mut report = Report::default();
        for outcome in outcomes {
            report
                .record(&mut sink, "case", outcome)
                .expect("a Vec never fails to write");
        }
        report
    }

    #[test]
    fn only_the_exact_subcommand_selects_a_job() {
        assert_eq!(parse(&args(&["selftest"])), Command::Selftest);
        assert_eq!(parse(&args(&["run"])), Command::Run);
        assert_eq!(
            parse(&args(&["selftest", "--manual"])),
            Command::SelftestManual,
            "the bench cases are asked for, never included in an unattended run"
        );
        assert_eq!(parse(&args(&[])), Command::Unrecognized);
        assert_eq!(parse(&args(&["stream"])), Command::Unrecognized);
        assert_eq!(
            parse(&args(&["selftest", "--all"])),
            Command::Unrecognized,
            "a self-test run must not start on a command line half of which was ignored"
        );
        assert_eq!(
            parse(&args(&["selftest", "--manual", "--again"])),
            Command::Unrecognized
        );
        assert_eq!(
            parse(&args(&["run", "--manual"])),
            Command::Unrecognized,
            "the flag belongs to the registry, not to the pipeline"
        );
        assert_eq!(
            parse(&args(&["run", "--channel=1"])),
            Command::Unrecognized,
            "the pipeline takes its settings from audio.conf, so an argument here \
             was meant to do something this binary will not do"
        );
    }

    #[test]
    fn only_a_run_whose_every_case_passed_exits_zero() {
        let mut err = Vec::new();
        assert_eq!(
            report_exit(
                Ok(report_of(vec![Outcome::Pass("38fb:1001".into())])),
                &mut err
            ),
            EXIT_OK
        );
        assert_eq!(
            report_exit(Ok(report_of(vec![Outcome::fail("status 0x02")])), &mut err),
            EXIT_FAILED
        );
        assert_eq!(
            report_exit(
                Ok(report_of(vec![
                    Outcome::Pass("38fb:1001".into()),
                    Outcome::NotRun("no open board to talk to".into()),
                ])),
                &mut err
            ),
            EXIT_FAILED,
            "a case that did not run asserted nothing, so the run is not green"
        );
        assert_eq!(
            report_exit(Ok(Report::default()), &mut err),
            EXIT_FAILED,
            "a run that attempted nothing is not a passing run"
        );
        assert!(
            err.is_empty(),
            "a completed run says everything in its transcript"
        );
    }

    #[test]
    fn a_report_that_could_not_be_written_fails_and_says_why() {
        let mut err = Vec::new();
        let code = report_exit(Err(io::Error::from(io::ErrorKind::BrokenPipe)), &mut err);
        assert_eq!(code, EXIT_FAILED);
        let printed = String::from_utf8(err).expect("utf8");
        assert!(
            printed.contains("cannot write the self-test report"),
            "{printed}"
        );
    }

    #[test]
    fn an_unrecognized_command_is_its_own_status_and_names_itself() {
        let mut err = Vec::new();
        assert_eq!(usage(&mut err, &args(&["stream", "--now"])), EXIT_USAGE);
        let printed = String::from_utf8(err).expect("utf8");
        assert!(
            printed.contains("unrecognized command: stream --now"),
            "{printed}"
        );
        assert!(
            printed.contains("usage: reachy-pod run | selftest [--manual]"),
            "{printed}"
        );
        assert!(
            printed.contains(logging::LEVEL_ENV),
            "the usage text names the log-level variable: {printed}"
        );

        // No arguments at all is a bare usage request, not a mistake to quote back.
        let mut err = Vec::new();
        assert_eq!(usage(&mut err, &args(&[])), EXIT_USAGE);
        let printed = String::from_utf8(err).expect("utf8");
        assert!(!printed.contains("unrecognized"), "{printed}");
        assert!(
            printed.contains("usage: reachy-pod run | selftest [--manual]"),
            "{printed}"
        );
    }
}
