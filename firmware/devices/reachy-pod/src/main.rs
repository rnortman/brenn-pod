//! The Reachy pod's entry point.
//!
//! One process per job, chosen by a subcommand: `run` is the pipeline the payload's
//! launcher starts, `reboot-chip` resets the audio chip and waits for it to come
//! back, `selftest` runs the unattended bring-up registry against the hardware and
//! reports, and `selftest --manual` runs the bench cases that need someone standing
//! at the array.
//!
//! `reboot-chip` is its own job because the reset costs more than this process's
//! audio: the board leaves the USB bus, and while it is away the servo bus on the
//! same board answers nothing in short bursts. A composition that commissions
//! servos runs this step to completion before it starts any app, and then starts
//! the pipeline as `run --chip-rebooted`, which attaches to the board without
//! resetting it again.
//!
//! Dispatch and the exit-status mapping live in `reachy_pod::cli`, where they are
//! testable; what is left here is the part that cannot be: the real streams.

use std::io;
use std::process::ExitCode;

use reachy_pod::{cli, logging, run, selftest};

fn main() -> ExitCode {
    logging::init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match cli::parse(&args) {
        // Returns only on a failure, and every ending is one: with the code in
        // hand, returning from `main` takes the remaining threads down with the
        // process, which is what the service unit restarts.
        cli::Command::Run { chip_rebooted } => run::run(chip_rebooted),
        cli::Command::RebootChip => run::reboot_chip(),
        cli::Command::Selftest => run_selftest(selftest::run),
        cli::Command::SelftestManual => run_selftest(selftest::run_manual),
        cli::Command::Unrecognized => cli::usage(&mut io::stderr(), &args),
    };
    ExitCode::from(code)
}

fn run_selftest(registry: fn(&mut dyn io::Write) -> io::Result<selftest::Report>) -> u8 {
    // Locked once: the registry prints a line per case, and stdout's per-write lock
    // would let a warning from a worker thread land inside one of them.
    let stdout = io::stdout();
    let mut out = stdout.lock();
    cli::report_exit(registry(&mut out), &mut io::stderr())
}
