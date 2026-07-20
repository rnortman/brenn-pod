//! `speech-surface`: the daemon. Loads the TOML config, applies CLI overrides,
//! opens the JSONL sink, binds the accept loop, and runs it until a SIGINT or
//! SIGTERM triggers a graceful shutdown (open segments finalize truncated, logs
//! flush, the pipeline drains, a final `stage_health` line is emitted).
//!
//! The CLI takes an optional subcommand: no subcommand — or an explicit `run` —
//! starts the daemon. `run` requires `--config <path>` and accepts `--listen`,
//! `--record-dir`, and `--jsonl <path|->` overrides onto the parsed config. The
//! `pin <framelog-path>` subcommand sets the log's sidecar `pinned` flag (the
//! keep-always retention mechanism) — a separate tool, safe to run against a
//! live log: it and the daemon serialize their sidecar read-modify-writes on an
//! exclusive store-directory lock, so a pin can never be lost to a concurrent
//! rewrite.

use std::future::Future;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use serde_json::json;

use speech_surface::server::Server;
use speech_surface::{jsonl, set_pinned, sidecar_path, Config, JsonlSink};

/// The daemon and its (future) sibling subcommands.
#[derive(Parser)]
#[command(
    name = "speech-surface",
    about = "Speech-focused audio I/O surface: ingest, record, and observe pod audio streams",
    // No subcommand runs the daemon with the top-level run args; an explicit
    // subcommand cannot be mixed with the bare-run args. `--config` is optional
    // at the clap layer (so it can appear either at top level or under `run`
    // without a duplicate-required conflict) and enforced in `load_config`.
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(flatten)]
    run: RunArgs,
    #[command(subcommand)]
    command: Option<Command>,
}

impl Cli {
    /// The run arguments, whether given bare or under an explicit `run`. Only
    /// called once the `pin` subcommand has been ruled out, so a `Pin` command
    /// never reaches here with meaningful run args.
    fn into_run(self) -> RunArgs {
        match self.command {
            Some(Command::Run(args)) => args,
            // `main` handles `Pin` before this is ever reached; enumerate every
            // variant with no wildcard so a future subcommand added to `Command`
            // forces a decision here instead of being silently swallowed into the
            // run path.
            Some(Command::Pin(_)) => self.run,
            None => self.run,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon (the default when no subcommand is given).
    Run(RunArgs),
    /// Pin a frame log so retention never prunes it (sets `pinned` in its sidecar).
    Pin(PinArgs),
}

/// Arguments for the `pin` subcommand: the frame log to keep always.
#[derive(Args, Clone)]
struct PinArgs {
    /// Path to the `.framelog` whose sidecar should be pinned.
    framelog: PathBuf,
}

/// Daemon arguments: the config path (required, enforced in `load_config`) plus
/// optional overrides.
#[derive(Args, Clone)]
struct RunArgs {
    /// Path to the TOML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override `listen_addr` — the LAN address the accept loop binds.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Override `record.dir` — the frame-log store directory.
    #[arg(long)]
    record_dir: Option<PathBuf>,
    /// Override `jsonl.sink` — a file path, `stdout`/`-`, or `none`.
    #[arg(long)]
    jsonl: Option<String>,
}

/// Apply CLI overrides onto a parsed config. Each override wins over the file.
fn apply_overrides(config: &mut Config, args: &RunArgs) {
    if let Some(listen) = args.listen {
        config.listen_addr = listen;
    }
    if let Some(dir) = &args.record_dir {
        config.record.dir = dir.clone();
    }
    if let Some(sink) = &args.jsonl {
        config.jsonl.sink = JsonlSink::from(sink.clone());
    }
}

/// Load the config file, apply overrides, and re-validate — an override (e.g.
/// `--listen 0.0.0.0`) must be rejected just like a bad file value.
fn load_config(args: &RunArgs) -> Result<Config> {
    let config_path = args
        .config
        .as_ref()
        .context("--config <path> is required")?;
    let mut config = Config::load(config_path)
        .with_context(|| format!("loading config {}", config_path.display()))?;
    apply_overrides(&mut config, args);
    config
        .validate()
        .map_err(|message| anyhow::anyhow!("invalid configuration after overrides: {message}"))?;
    Ok(config)
}

/// Report a handler-install failure on the event stream. `missing` names the
/// unavailable delivery, `fallback` what the daemon does instead, `detail` the
/// OS error text.
fn emit_signal_degraded(jsonl: &jsonl::JsonlHandle, missing: &str, fallback: &str, detail: String) {
    jsonl.emit(
        "signal_handler_failed",
        &json!({
            "missing": missing,
            "fallback": fallback,
            "detail": detail,
        }),
    );
}

/// Resolve when a real SIGINT or SIGTERM is delivered. Signal *streams* are used
/// (not `ctrl_c()`), so a handler-install failure is caught at setup and logged
/// rather than resolving the future immediately — a failed-to-arm signal source
/// must not masquerade as a delivered signal and trigger a phantom shutdown
/// moments after startup.
///
/// A handler-install failure is reported as a `signal_handler_failed` event on
/// the JSONL sink (the daemon's observability surface, watched by monitoring),
/// not just stderr: the `failed` token makes the console tee render it loud, so
/// an operator sees that graceful stop is degraded or unavailable.
async fn shutdown_signal(jsonl: jsonl::JsonlHandle) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) {
            (Ok(mut sigint), Ok(mut sigterm)) => {
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }
            }
            (Ok(mut sigint), Err(e)) => {
                emit_signal_degraded(&jsonl, "sigterm", "sigint_only", e.to_string());
                sigint.recv().await;
            }
            (Err(e), Ok(mut sigterm)) => {
                emit_signal_degraded(&jsonl, "sigint", "sigterm_only", e.to_string());
                sigterm.recv().await;
            }
            (Err(ei), Err(et)) => {
                // Neither handler could be armed: rather than resolve now (a
                // phantom shutdown), park forever so the daemon keeps serving and
                // must be stopped externally.
                emit_signal_degraded(
                    &jsonl,
                    "all",
                    "none",
                    format!("SIGINT: {ei}; SIGTERM: {et}"),
                );
                std::future::pending::<()>().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            emit_signal_degraded(&jsonl, "all", "none", e.to_string());
            std::future::pending::<()>().await;
        }
    }
}

/// Open the JSONL sink, bind and run the server until `shutdown` resolves, then
/// drain the sink. `shutdown` is a factory, not a bare future: it is handed a
/// live [`jsonl::JsonlHandle`] clone once the sink is open, so the signal path
/// can report a handler-install failure on the event stream. Split from `main`
/// so a test can drive it with its own shutdown future instead of a process
/// signal.
async fn serve<F, Fut>(config: Config, shutdown: F) -> Result<()>
where
    F: FnOnce(jsonl::JsonlHandle) -> Fut,
    Fut: Future<Output = ()>,
{
    // The console is always stderr; its color tracks whether that stderr is a
    // terminal, chosen here next to the writer it describes.
    let (jsonl, sinks) = jsonl::spawn(
        &config.jsonl.sink,
        tokio::io::stderr(),
        std::io::stderr().is_terminal(),
    )
    .await
    .context("opening JSONL sink")?;

    let config = Arc::new(config);
    jsonl.emit(
        "daemon_start",
        &json!({
            "listen_addr": config.listen_addr.to_string(),
            "record_enabled": config.record.enabled,
            "record_dir": config.record.dir.display().to_string(),
            "max_connections": config.max_connections,
            "jsonl_sink": config.jsonl.sink.label(),
        }),
    );

    let server = Server::bind(config.clone(), jsonl.clone())
        .await
        .with_context(|| format!("binding {}", config.listen_addr))?;
    let shutdown = shutdown(jsonl.clone());
    server.run(shutdown).await.context("running server")?;

    // Drop the emit handle so both writer tasks see their channels close, then
    // await them so buffered lines flush before exit. Console is joined before
    // file (it holds a file-sender clone); each writer self-reports its own I/O
    // errors, and a panicked task is surfaced on stderr rather than vanishing
    // with a possibly-truncated event log.
    drop(jsonl);
    sinks.join().await;
    Ok(())
}

/// Set the `pinned` flag on a frame log's sidecar, creating a minimal sidecar if
/// none exists. The whole read-modify-write runs under the store lock (see
/// [`speech_surface::set_pinned`]), so it is safe to run against a log the daemon
/// is still writing: the pin cannot be lost to a concurrent rewrite, and it does
/// not clobber a segment the daemon appended. A present-but-unreadable sidecar
/// surfaces as an error rather than being clobbered, so a corrupt file never
/// silently drops an existing pin.
fn pin(framelog: &Path) -> Result<()> {
    let path = sidecar_path(framelog);
    set_pinned(&path).with_context(|| format!("pinning sidecar {}", path.display()))?;
    println!("pinned {}", path.display());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(Command::Pin(args)) = &cli.command {
        return pin(&args.framelog);
    }
    let config = load_config(&cli.into_run())?;
    serve(config, shutdown_signal).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use speech_surface::Sidecar;

    fn base_config() -> Config {
        Config::parse("listen_addr = \"127.0.0.1:7380\"").expect("parse")
    }

    #[test]
    fn no_subcommand_defaults_to_run() {
        let cli = Cli::parse_from(["speech-surface", "--config", "/etc/speech.toml"]);
        let run = cli.into_run();
        assert_eq!(run.config, Some(PathBuf::from("/etc/speech.toml")));
    }

    #[test]
    fn explicit_run_subcommand_parses() {
        let cli = Cli::parse_from(["speech-surface", "run", "--config", "/etc/speech.toml"]);
        let run = cli.into_run();
        assert_eq!(run.config, Some(PathBuf::from("/etc/speech.toml")));
    }

    #[test]
    fn missing_config_is_a_load_error() {
        // `--config` parses as absent, and `load_config` turns that into an error.
        let run = Cli::parse_from(["speech-surface"]).into_run();
        assert!(run.config.is_none());
        let err = load_config(&run).unwrap_err();
        assert!(err.to_string().contains("--config"), "message: {err}");
    }

    #[test]
    fn overrides_win_over_file_values() {
        let mut config = base_config();
        let args = RunArgs {
            config: Some(PathBuf::from("unused")),
            listen: Some("10.0.0.9:9000".parse().unwrap()),
            record_dir: Some(PathBuf::from("/data/logs")),
            jsonl: Some("/var/log/speech.jsonl".to_string()),
        };
        apply_overrides(&mut config, &args);
        assert_eq!(config.listen_addr, "10.0.0.9:9000".parse().unwrap());
        assert_eq!(config.record.dir, PathBuf::from("/data/logs"));
        assert_eq!(
            config.jsonl.sink,
            JsonlSink::File(PathBuf::from("/var/log/speech.jsonl"))
        );
    }

    #[test]
    fn no_overrides_leaves_config_untouched() {
        let mut config = base_config();
        let args = RunArgs {
            config: Some(PathBuf::from("unused")),
            listen: None,
            record_dir: None,
            jsonl: None,
        };
        apply_overrides(&mut config, &args);
        assert_eq!(config.listen_addr, "127.0.0.1:7380".parse().unwrap());
        assert_eq!(config.jsonl.sink, JsonlSink::None);
    }

    #[test]
    fn load_config_rejects_override_binding_all_interfaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("speech.toml");
        std::fs::write(&path, "listen_addr = \"127.0.0.1:7380\"").unwrap();
        let args = RunArgs {
            config: Some(path),
            listen: Some("0.0.0.0:7380".parse().unwrap()),
            record_dir: None,
            jsonl: None,
        };
        let err = load_config(&args).unwrap_err();
        assert!(
            err.to_string().contains("invalid configuration"),
            "message: {err}"
        );
    }

    #[test]
    fn load_config_reports_missing_file() {
        let args = RunArgs {
            config: Some(PathBuf::from("/nonexistent/speech-surface.toml")),
            listen: None,
            record_dir: None,
            jsonl: None,
        };
        let err = load_config(&args).unwrap_err();
        assert!(err.to_string().contains("loading config"), "message: {err}");
    }

    /// Full daemon wiring: open the sink, bind an ephemeral port, run to an
    /// immediate shutdown, and drain — the final `stage_health` line proves the
    /// server ran and the JSONL sink drained.
    #[tokio::test(flavor = "multi_thread")]
    async fn serve_starts_and_drains_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [jsonl]\nsink = {:?}\n\
             [record]\nenabled = false\n",
            jsonl_path.to_str().unwrap()
        );
        let config = Config::parse(&text).expect("parse");

        serve(config, |_| async {}).await.expect("serve");

        let contents = std::fs::read_to_string(&jsonl_path).unwrap();
        assert!(
            has_event(&contents, "daemon_start"),
            "daemon_start emitted: {contents}"
        );
        assert!(
            has_event(&contents, "stage_health"),
            "final stage_health emitted: {contents}"
        );
    }

    /// The shutdown seam is a factory handed a live `JsonlHandle`: an event
    /// emitted through that handle must reach the durable JSONL file, and the
    /// captured clone must not stall the sink drain (the test completing proves
    /// it). This is the exact property the signal-degradation fix depends on.
    #[tokio::test(flavor = "multi_thread")]
    async fn serve_shutdown_factory_gets_a_live_draining_handle() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let text = format!(
            "listen_addr = \"127.0.0.1:0\"\n\
             [jsonl]\nsink = {:?}\n\
             [record]\nenabled = false\n",
            jsonl_path.to_str().unwrap()
        );
        let config = Config::parse(&text).expect("parse");

        serve(config, |handle: jsonl::JsonlHandle| async move {
            handle.emit("shutdown_marker", &json!({ "via": "factory_handle" }));
        })
        .await
        .expect("serve");

        let contents = std::fs::read_to_string(&jsonl_path).unwrap();
        assert!(
            has_event(&contents, "shutdown_marker"),
            "marker emitted through the factory handle reached the file: {contents}"
        );
    }

    /// The default (`None`) sink path — the config every deployment without a
    /// `[jsonl]` section hits: `serve` must start, run to shutdown, and drain
    /// cleanly with no file writer task and no hang.
    #[tokio::test(flavor = "multi_thread")]
    async fn serve_with_no_sink_starts_and_drains() {
        let text = "listen_addr = \"127.0.0.1:0\"\n\
                    [record]\nenabled = false\n";
        let config = Config::parse(text).expect("parse");
        assert_eq!(config.jsonl.sink, JsonlSink::None);
        serve(config, |_| async {}).await.expect("serve");
    }

    /// Pin the `signal_handler_failed` schema the monitoring layer consumes:
    /// the exact field keys (`missing`/`fallback`/`detail`) and a representative
    /// `(missing, fallback)` value pairing. A rename or typo in a key or token
    /// would compile and pass every other test while silently breaking the
    /// signal the whole change exists to deliver; this asserts the contract.
    #[tokio::test(flavor = "multi_thread")]
    async fn signal_degraded_event_has_the_monitored_schema() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (jsonl, sinks) = jsonl::spawn(
            &JsonlSink::File(jsonl_path.clone()),
            tokio::io::stderr(),
            false,
        )
        .await
        .expect("spawn sink");

        emit_signal_degraded(&jsonl, "sigterm", "sigint_only", "boom".to_string());

        drop(jsonl);
        sinks.join().await;

        let contents = std::fs::read_to_string(&jsonl_path).unwrap();
        let event = contents
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .find(|v| v["event"] == "signal_handler_failed")
            .unwrap_or_else(|| panic!("signal_handler_failed emitted: {contents}"));

        assert_eq!(event["missing"], "sigterm", "in {event}");
        assert_eq!(event["fallback"], "sigint_only", "in {event}");
        assert_eq!(event["detail"], "boom", "in {event}");
    }

    fn has_event(contents: &str, event: &str) -> bool {
        contents
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .any(|v| v["event"] == event)
    }

    #[test]
    fn pin_subcommand_parses_the_framelog_path() {
        let cli = Cli::parse_from(["speech-surface", "pin", "/store/pod-x_0.framelog"]);
        match cli.command {
            Some(Command::Pin(args)) => {
                assert_eq!(args.framelog, PathBuf::from("/store/pod-x_0.framelog"));
            }
            _ => panic!("expected a pin subcommand"),
        }
    }

    #[test]
    fn pin_sets_the_flag_on_an_existing_sidecar_preserving_segments() {
        let dir = tempfile::tempdir().unwrap();
        let framelog = dir.path().join("pod-x_0.framelog");
        let side = sidecar_path(&framelog);

        // A daemon-written sidecar with a segment, not yet pinned.
        let mut existing = Sidecar::new("pod-x");
        existing.push(speech_surface::SidecarSegment {
            segment_id: 4,
            part: 0,
            wake: speech_surface::WakeClass::Ungated,
            start_epoch_us: 10,
            end_epoch_us: 20,
            end_cause: speech_pipeline::SegmentEndCause::VadRelease,
            truncated: false,
            resumed: false,
            gap_count: 0,
            samples: 16_000,
        });
        existing.write_atomic(&side).unwrap();

        pin(&framelog).unwrap();

        let read = Sidecar::read(&side).unwrap();
        assert!(read.pinned, "pin flag set");
        assert_eq!(read.pod_id, "pod-x", "identity preserved");
        assert_eq!(read.segments.len(), 1, "segments preserved");
        assert_eq!(read.segments[0].segment_id, 4);
    }

    #[test]
    fn pin_creates_a_minimal_sidecar_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let framelog = dir.path().join("pod-x_0.framelog");
        let side = sidecar_path(&framelog);
        assert!(!side.exists(), "no sidecar to start");

        pin(&framelog).unwrap();

        let read = Sidecar::read(&side).unwrap();
        assert!(read.pinned);
        assert!(read.segments.is_empty(), "minimal sidecar has no segments");
    }

    #[test]
    fn pin_does_not_clobber_a_corrupt_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let framelog = dir.path().join("pod-x_0.framelog");
        let side = sidecar_path(&framelog);
        std::fs::write(&side, b"{ not valid json").unwrap();

        // A present-but-unreadable sidecar errors rather than being overwritten,
        // so a corrupt file never silently drops a pin it might have carried.
        let err = pin(&framelog).unwrap_err();
        assert!(
            err.to_string().contains("pinning sidecar"),
            "message: {err}"
        );
        assert_eq!(
            std::fs::read(&side).unwrap(),
            b"{ not valid json",
            "corrupt file left untouched"
        );
    }
}
