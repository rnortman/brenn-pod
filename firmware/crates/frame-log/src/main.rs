//! `frame-log`: read a frame log, a capture's or the voice host's.
//!
//! ```text
//! frame-log summary <log>
//! frame-log export-wav <log> <out.wav>
//! ```
//!
//! Each prints one JSON object on stdout: the summary, or the export map.
//! Exit status 0 when that output was produced, whatever the log's tail says;
//! 1 when the header is unreadable, the log has no hello, a read or write
//! failed, or the export was refused; 2 for a command line this does not
//! understand. An export whose map cannot be written removes its WAV file,
//! which is not to be used without the map that says which of its silence is
//! estimated.

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

const EXIT_OK: u8 = 0;
const EXIT_FAILED: u8 = 1;
const EXIT_USAGE: u8 = 2;

const USAGE: &str = "usage: frame-log summary <log>\n       frame-log export-wav <log> <out.wav>";

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    ExitCode::from(run(&args))
}

fn run(args: &[OsString]) -> u8 {
    let result = match args {
        [cmd, log] if cmd == "summary" => frame_log::summarize(Path::new(log))
            .map_err(|e| e.to_string())
            .and_then(|s| print_json(&s)),
        [cmd, log, out] if cmd == "export-wav" => export_wav(Path::new(log), Path::new(out)),
        _ => {
            eprintln!("{USAGE}");
            return EXIT_USAGE;
        }
    };
    match result {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("frame-log: {e}");
            EXIT_FAILED
        }
    }
}

/// Export `log` to `out` and print the map; when the map cannot be printed,
/// remove `out`.
fn export_wav(log: &Path, out: &Path) -> Result<(), String> {
    let map = frame_log::export_wav(log, out).map_err(|e| e.to_string())?;
    print_json(&map).map_err(|e| match std::fs::remove_file(out) {
        Ok(()) => format!("{e}; removed {}", out.display()),
        Err(remove) => format!(
            "{e}; {} could not be removed and must not be used without its map: {remove}",
            out.display()
        ),
    })
}

/// Print `value` as one JSON line on stdout.
fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    let line = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{line}")
        .and_then(|()| stdout.flush())
        .map_err(|e| format!("writing stdout: {e}"))
}
