//! `frame-log`: read a frame log, a capture's or the voice host's, and write a
//! capture log from given samples.
//!
//! ```text
//! frame-log summary <log>
//! frame-log export-wav <log> <out.wav>
//! frame-log write-capture <spec.json> <in.wav> <out.framelog>
//! ```
//!
//! Each prints one JSON object on stdout: the summary, the export map, or the
//! written log's record id and frames. Exit status 0 when that output was
//! produced, whatever a read log's tail says; 1 when the header is
//! unreadable, the log has no hello, a read or write failed, or the export,
//! the spec or the WAV file was refused; 2 for a command line this does not
//! understand. A command that wrote a file and cannot print its object
//! removes the file and exits 1: an export's WAV file is not to be used
//! without the map that says which of its silence is estimated, and a log is
//! left behind only by a write that succeeded.

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;

const EXIT_OK: u8 = 0;
const EXIT_FAILED: u8 = 1;
const EXIT_USAGE: u8 = 2;

const USAGE: &str = "usage: frame-log summary <log>\n       frame-log export-wav <log> <out.wav>\n       frame-log write-capture <spec.json> <in.wav> <out.framelog>";

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    ExitCode::from(run(&args))
}

fn run(args: &[OsString]) -> u8 {
    let result = match args {
        [cmd, log] if cmd == "summary" => frame_log::summarize(Path::new(log))
            .map_err(|e| e.to_string())
            .and_then(|s| print_json(&s)),
        [cmd, log, out] if cmd == "export-wav" => {
            let out = Path::new(out);
            frame_log::export_wav(Path::new(log), out)
                .map_err(|e| e.to_string())
                .and_then(|map| print_or_remove(&map, out))
        }
        [cmd, spec, wav, out] if cmd == "write-capture" => {
            let out = Path::new(out);
            frame_log::write_capture(Path::new(spec), Path::new(wav), out)
                .map_err(|e| e.to_string())
                .and_then(|w| print_or_remove(&w, out))
        }
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

/// Print `value`, the result of writing `out`; when it cannot be printed,
/// remove `out`.
fn print_or_remove(value: &impl serde::Serialize, out: &Path) -> Result<(), String> {
    print_json(value).map_err(|e| match std::fs::remove_file(out) {
        Ok(()) => format!("{e}; removed {}", out.display()),
        Err(remove) => format!(
            "{e}; {} could not be removed and must not be used: {remove}",
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
