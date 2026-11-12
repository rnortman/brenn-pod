//! `pod-capture`: record every channel of one ALSA capture device to a local
//! frame log until told to stop.
//!
//! ```text
//! pod-capture --card <NAME> --rate <HZ> --format <s16_le|s32_le> --layout <a,b,c,...> \
//!     --pod-id <NAME> --out <PATH>
//! ```
//!
//! Event lines on stdout, prose on stderr. Exit status 0 after `capture_stopped`,
//! 1 after `capture_failed`, 2 for a command line it does not understand.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ExitCode::from(pod_capture::main_with(&args, std::io::stdout()))
}
