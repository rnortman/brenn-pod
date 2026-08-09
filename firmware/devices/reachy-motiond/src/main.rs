//! The head-presence daemon's entry point.
//!
//! Everything decidable from text happens before anything is acquired: the
//! daemon's configuration, the machine's, and the bridge's token file. Only then
//! does the serial port open, the bus thread start, and the machine get
//! commissioned.
//!
//! What is here is the part that cannot be tested without a machine and a
//! socket — the ordering, the two threads, and the exit status. The decisions
//! they carry out live in the library, where they are asserted with neither.

use std::io;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;

use brenn_bridge::{Bridge, BridgeOutcome};
use reachy_motiond::bus::{self, Listener};
use reachy_motiond::cells::Shared;
use reachy_motiond::cli::{self, Invocation};
use reachy_motiond::config::Config;
use reachy_motiond::motion::{self, Machine, Timing};
use reachy_motiond::report::{Sink, Streams};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match cli::parse(&args) {
        Invocation::Run(path) => run(&path),
        Invocation::Help => cli::describe(&mut io::stdout(), true),
        Invocation::Unrecognized => cli::describe(&mut io::stderr(), false),
    };
    ExitCode::from(code)
}

/// Rest the machine, and take hold of it for as long as a script asks the head
/// to be up.
fn run(path: &Path) -> u8 {
    let sink = Streams;

    // Text only, and in this order: the daemon's own file, then the machine's,
    // then the token the attachment presents. A refusal here costs nothing — no
    // port is open, no servo is torqued, and nothing has been told anything.
    let config = match Config::load(path) {
        Ok(config) => config,
        Err(error) => return cli::refuse_startup(&sink, &error),
    };
    let machine = match Machine::resolve(&config.motion_config, config.durations()) {
        Ok(machine) => machine,
        Err(error) => return cli::refuse_startup(&sink, &error),
    };
    let (bridge, handle, events) = match Bridge::new(&config.bridge) {
        Ok(parts) => parts,
        Err(error) => return cli::refuse_startup(&sink, &error),
    };
    sink.line(&format!(
        "resolved: machine {} from {}, obeying {} on {}",
        machine.device(),
        config.motion_config.display(),
        config.pod,
        config.channel
    ));
    // Both moves and both files, before anything moves. A head that raises at a
    // pace nobody expects is otherwise a question two configurations and a guess
    // are needed to answer, and the override is exactly the thing a reader of
    // the bench file cannot see.
    sink.line(&format!("durations: {}", machine.clocks()));
    sink.event("motion_durations", &machine.clocks().json());

    // The port before the bus: a second speaker on the servo chain is the one
    // failure that cannot be recovered from by trying again, so the refusal that
    // names who holds it comes before anything else is started.
    let port = match machine.open() {
        Ok(port) => port,
        Err(error) => return cli::refuse_startup(&sink, &error),
    };

    let shared = Arc::new(Shared::new(&config.pod));
    let bus = spawn_bus(bridge, handle, events, &config, Arc::clone(&shared));

    // Commissioning touches no torque in either direction, so a refusal here
    // leaves the machine exactly as limp as it was found.
    let resting = match machine.commission(port, &mut |text| sink.line(text)) {
        Ok(resting) => resting,
        Err(refusal) => {
            let outcome = motion::commission_failed(&shared, refusal, &sink);
            return finish(bus, &sink, &outcome);
        }
    };

    let timing = Timing {
        dwell: config.hold_dwell(),
        rest_poll: config.rest_poll(),
        rest_delay: config.rest_delay(),
    };
    let outcome = motion::run(resting, &shared, timing, &sink);
    finish(bus, &sink, &outcome)
}

/// The bus thread: a current-thread runtime, the attachment, and the path into
/// the schedule.
///
/// Its own runtime rather than the main thread's, because the other thread
/// blocks on a serial port for seconds at a time and an attachment that stopped
/// being driven for that long would be dropped by its peer.
fn spawn_bus<C>(
    bridge: Bridge<C>,
    handle: brenn_bridge::BridgeHandle,
    events: tokio::sync::mpsc::Receiver<brenn_bridge::BridgeEvent>,
    config: &Config,
    shared: Arc<Shared>,
) -> thread::JoinHandle<Option<BridgeOutcome>>
where
    C: brenn_bridge::TransportConnector + Send + 'static,
    C::Conn: Send,
{
    let channel = config.channel.clone();
    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                bus::no_runtime(&shared, &Streams, &error);
                return None;
            }
        };
        let sink = Streams;
        let listener = Listener::new(shared, channel, &sink);
        Some(runtime.block_on(bus::serve(bridge, &handle, events, listener)))
    })
}

/// Wait for the bus thread, say how the run ended, and answer with the status.
fn finish(
    bus: thread::JoinHandle<Option<BridgeOutcome>>,
    sink: &Streams,
    outcome: &motion::Outcome,
) -> u8 {
    match bus.join() {
        Ok(Some(bridge)) => sink.line(&format!("bus: {bridge}")),
        Ok(None) => {}
        // A panicked bus thread is worth a line of its own: scripts stopped
        // arriving at that moment, which is why the head came down.
        Err(_) => sink.line("bus: the bus thread panicked"),
    }

    let code = cli::exit_code(outcome);
    sink.line(&format!("exit {code}: {outcome}"));
    sink.event(
        "daemon_exit",
        &serde_json::json!({ "outcome": outcome.to_string(), "code": code }),
    );
    code
}
