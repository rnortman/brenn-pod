//! `capture-record`: read a capture record.
//!
//! ```text
//! capture-record summary <record>
//! capture-record export-wav <record> <out.wav>
//! ```
//!
//! Each prints one JSON object on stdout: the summary, or the export map.
//! Exit status 0 when that output was produced, whatever the record's tail
//! says; 1 when the header is unreadable, a read or write failed, or the
//! export was refused; 2 for a command line this does not understand. An
//! export whose map cannot be written removes its WAV file, which is not to be
//! used without the map that says which of its silence is estimated.

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

const EXIT_OK: u8 = 0;
const EXIT_FAILED: u8 = 1;
const EXIT_USAGE: u8 = 2;

const USAGE: &str =
    "usage: capture-record summary <record>\n       capture-record export-wav <record> <out.wav>";

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    ExitCode::from(run(&args))
}

fn run(args: &[OsString]) -> u8 {
    let result = match args {
        [cmd, record] if cmd == "summary" => capture_record::summarize(Path::new(record))
            .map_err(|e| e.to_string())
            .and_then(|s| print_json(&s)),
        [cmd, record, out] if cmd == "export-wav" => export_wav(Path::new(record), Path::new(out)),
        _ => {
            eprintln!("{USAGE}");
            return EXIT_USAGE;
        }
    };
    match result {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("capture-record: {e}");
            EXIT_FAILED
        }
    }
}

/// Export `record` to `out` and print the map; when the map cannot be printed,
/// remove `out`.
fn export_wav(record: &Path, out: &Path) -> Result<(), String> {
    let map = capture_record::export_wav(record, out).map_err(|e| e.to_string())?;
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
