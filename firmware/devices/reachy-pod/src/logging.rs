//! Logging to stderr, which is where the service manager collects it.
//!
//! No timestamps and no colour: the journal stamps and files every line it captures,
//! so a logger that adds its own duplicates one and invents the other. Level comes
//! from the environment because the payload's configuration is not read until after
//! the process is already logging.

use std::io::Write;

use log::{LevelFilter, Log, Metadata, Record};

/// Environment variable naming the maximum level, e.g. `REACHY_POD_LOG=debug`.
pub const LEVEL_ENV: &str = "REACHY_POD_LOG";

/// Level when nothing is configured. Info: a voice node that logs nothing about a
/// dropped connection is a voice node nobody can debug in the field.
pub const DEFAULT_LEVEL: LevelFilter = LevelFilter::Info;

/// The level named by `value`, or [`DEFAULT_LEVEL`] when it is absent or is not a
/// level. An unparseable value falls back rather than refusing to start: a typo in a
/// unit's environment must not cost the pipeline.
pub fn level_from(value: Option<&str>) -> LevelFilter {
    match value.map(str::trim) {
        None | Some("") => DEFAULT_LEVEL,
        Some(name) => name.parse().unwrap_or_else(|_| {
            eprintln!("{LEVEL_ENV}={name:?} is not a log level; using {DEFAULT_LEVEL}");
            DEFAULT_LEVEL
        }),
    }
}

/// The one logger, installed by reference: nothing about it is per-run, so it needs
/// no allocation and `log`'s `std` feature stays unrequested.
static LOGGER: StderrLogger = StderrLogger;

struct StderrLogger;

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        // One write per record: two would interleave with another thread's line.
        let line = format!(
            "{:<5} {}: {}\n",
            record.level(),
            record.target(),
            record.args()
        );
        let _ = std::io::stderr().write_all(line.as_bytes());
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Install the stderr logger at the level [`LEVEL_ENV`] names.
///
/// Idempotent by failure: a second call leaves the first logger in place, which is
/// what a test binary that touches this more than once needs.
pub fn init() {
    let level = level_from(std::env::var(LEVEL_ENV).ok().as_deref());
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_or_empty_setting_takes_the_default() {
        assert_eq!(level_from(None), DEFAULT_LEVEL);
        assert_eq!(level_from(Some("")), DEFAULT_LEVEL);
        assert_eq!(level_from(Some("   ")), DEFAULT_LEVEL);
    }

    #[test]
    fn a_named_level_is_taken_in_any_case_and_a_typo_falls_back() {
        assert_eq!(level_from(Some("debug")), LevelFilter::Debug);
        assert_eq!(level_from(Some("WARN")), LevelFilter::Warn);
        assert_eq!(level_from(Some(" trace ")), LevelFilter::Trace);
        assert_eq!(level_from(Some("off")), LevelFilter::Off);
        assert_eq!(level_from(Some("verbose")), DEFAULT_LEVEL);
    }
}
