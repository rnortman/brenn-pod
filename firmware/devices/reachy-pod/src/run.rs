//! The pipeline: four threads, what they share, and what happens when one ends.
//!
//! ```text
//! capture     ALSA period → the configured channel → the capture ring
//! telemetry   USB control: SPENERGY 20 Hz, DoA 10 Hz → the VAD gate → the streamer
//! streamer    idle/segment loop → TLS-PSK → the audio host
//! playback    the inbound ring → mono→stereo → the board's playback stream
//! ```
//!
//! Only the first two and the last are this pod's; the streamer thread is the
//! shared engine — the same code the ESP32 pod runs — reached with the ring,
//! the channel, the sink and the clocks this module wires up.
//!
//! The supervision policy is crash-only, which the device's service unit is built
//! for: a thread that ends has lost the hardware or the gate, and the recovery is
//! the process exiting non-zero and being restarted five seconds later — not a
//! re-probe state machine here, which would be a second, less-tested copy of what
//! systemd already does. So there is no success exit: every path out of [`run`]
//! is a failure, and the first thread to end names it.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, Sender, channel, sync_channel};
use std::time::{Duration, Instant};

use audio_pipeline::inbound::{InboundObserver, InboundWaypoint};
use audio_pipeline::ring::{CaptureRing, RING_CAPACITY_SAMPLES, RingIndex};
use audio_pipeline::wire::PlaybackFormat;
use pod_streamer::idle::should_log_provisioning_failure;
use pod_streamer::run::{StreamerExit, StreamerRuntime, run_streamer_loop};
use pod_streamer::segment::{ObsEvent, STREAMER_CHAN_CAPACITY, StreamerMsg};
use pod_streamer::telemetry::{
    DOA_POLL_HZ, Reading, TelemetryBus, TelemetryCore, TelemetryCtx, VAD_POLL_HZ,
    read_f32x4_reading, run_telemetry_loop,
};
use xvf3800_ctrl::{ControlTransport, USB_RETRY};

use crate::alsa_capture::{
    CaptureStream, PcmError, append_channel, enumerate_cards, monotonic_us, new_ring, open_capture,
    select_card,
};
use crate::chip::{self, Routing, StateLineCadence};
use crate::cli::EXIT_FAILED;
use crate::config::{Config, ConfigError, RECHECK_INTERVAL};
use crate::playback::{AlsaOut, open_playback_on, playback_pair, run_drain_loop};
use crate::usb_ctrl::{Board, UsbControl, find_boards, select_board};
use crate::{config, tls};

/// The capture ring, shared by the thread that fills it and the two that read it.
pub type SharedRing = Arc<Mutex<Option<CaptureRing<Box<[i16]>>>>>;

/// The capture thread's share of the wiring.
pub struct CaptureWiring {
    /// The ring this thread fills.
    pub ring: SharedRing,
    /// Which of the board's two capture channels is written into it.
    pub channel: usize,
}

/// The telemetry thread's share of the wiring.
pub struct TelemetryWiring {
    /// Read at onset, for the pre-roll cursor the gate publishes.
    pub ring: SharedRing,
    /// Set on a gate release, cleared on onset.
    pub vad_closed: Arc<AtomicBool>,
    /// The sending end of the gate's messages to the streamer.
    pub msg_tx: std::sync::mpsc::SyncSender<StreamerMsg>,
    /// Speech-energy level above which the chip's telemetry counts as speech.
    pub threshold: f32,
    /// How long the gate stays open after the energy drops below it.
    pub hangover_ms: u32,
}

/// The streamer thread's share of the wiring.
pub struct StreamerWiring {
    /// Read per frame, for the audio a segment carries.
    pub ring: SharedRing,
    /// The lossless path when a `VadClosed` message is dropped.
    pub vad_closed: Arc<AtomicBool>,
    /// Published for the span of an onset. Nothing on this pod reads it; the
    /// shared loop publishes it regardless.
    pub segment_active: Arc<AtomicBool>,
    /// The receiving end of the gate's messages.
    pub msg_rx: Receiver<StreamerMsg>,
    /// Where inbound playback audio is banked.
    pub sink: crate::playback::RingSink,
    /// This pod's name on the wire, and its TLS-PSK identity.
    pub pod_id: String,
    /// Where the audio host is and which key to present.
    pub config: Config,
}

/// The playback thread's share of the wiring.
pub struct PlaybackWiring {
    /// The read end of the ring the streamer banks into.
    pub drain: crate::playback::PlaybackDrain,
}

/// Everything the four threads share, split into what each one gets.
///
/// Built in one place and handed out whole rather than cloned at four spawn sites,
/// because which handle reaches which thread is precisely what a spawn site can get
/// wrong in silence: the two flags have the same type, and swapping them compiles.
/// A pod wired that way connects, handshakes and streams nothing but fragments.
pub struct Wiring {
    pub capture: CaptureWiring,
    pub telemetry: TelemetryWiring,
    pub streamer: StreamerWiring,
    pub playback: PlaybackWiring,
}

/// Create the shared state and divide it among the threads.
///
/// No hardware: everything here is a ring, a flag, a channel or a value off the
/// configuration, so the hand-off is decidable in a test.
///
/// `channel` rather than `config.channel`, because the chip's routing has the last
/// word on it: a board that refused the ASR routing is streamed on the
/// post-processed channel whatever the configuration asked for.
pub fn wire(config: &Config, pod_id: String, channel: usize) -> Wiring {
    let ring: SharedRing = Arc::new(Mutex::new(Some(new_ring())));
    let vad_closed = Arc::new(AtomicBool::new(false));
    let segment_active = Arc::new(AtomicBool::new(false));
    let (msg_tx, msg_rx) = sync_channel::<StreamerMsg>(STREAMER_CHAN_CAPACITY);
    let (sink, drain) = playback_pair();
    Wiring {
        capture: CaptureWiring {
            ring: Arc::clone(&ring),
            channel,
        },
        telemetry: TelemetryWiring {
            ring: Arc::clone(&ring),
            vad_closed: Arc::clone(&vad_closed),
            msg_tx,
            threshold: config.vad_threshold,
            hangover_ms: config.vad_hangover_ms,
        },
        streamer: StreamerWiring {
            ring,
            vad_closed,
            segment_active,
            msg_rx,
            sink,
            pod_id,
            config: config.clone(),
        },
        playback: PlaybackWiring { drain },
    }
}

/// The four threads that are the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Worker {
    /// Reads ALSA periods into the capture ring.
    Capture,
    /// Polls the chip's control plane and drives the VAD gate.
    Telemetry,
    /// Runs the shared idle/segment loop against the audio host.
    Streamer,
    /// Plays banked inbound audio out of the board.
    Playback,
}

impl Worker {
    /// The thread's name, as it appears in a log line and in `ps`.
    pub const fn name(self) -> &'static str {
        match self {
            Worker::Capture => "capture",
            Worker::Telemetry => "telemetry",
            Worker::Streamer => "streamer",
            Worker::Playback => "playback",
        }
    }
}

/// A worker thread ending, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadExit {
    /// Which thread, or `None` when every thread went away without saying.
    pub worker: Option<Worker>,
    /// What it reported on the way out.
    pub cause: String,
}

impl fmt::Display for ThreadExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.worker {
            Some(worker) => write!(f, "the {} thread ended: {}", worker.name(), self.cause),
            None => write!(f, "{}", self.cause),
        }
    }
}

/// Reports its thread's ending exactly once, on every path out — including a
/// panic, which unwinds through this drop.
///
/// A thread that panicked reports nothing but its own existence, which is enough:
/// the panic itself is already in the journal above this line, and what the
/// supervisor needs is that *a* thread is gone. Without this, a panicking
/// telemetry thread would leave the pod running with a dead gate and a live
/// process, which is the one failure mode systemd cannot see.
pub struct ExitReporter {
    tx: Sender<ThreadExit>,
    worker: Worker,
    cause: Option<String>,
}

impl ExitReporter {
    /// A reporter for `worker`, publishing on `tx`.
    pub fn new(tx: Sender<ThreadExit>, worker: Worker) -> Self {
        Self {
            tx,
            worker,
            cause: None,
        }
    }

    /// Record why this thread is ending. The last cause set is the one reported.
    pub fn set(&mut self, cause: impl Into<String>) {
        self.cause = Some(cause.into());
    }
}

impl Drop for ExitReporter {
    fn drop(&mut self) {
        let cause = self.cause.take().unwrap_or_else(|| {
            "it panicked; the panic itself is logged above this line".to_string()
        });
        // A send failure means the supervisor is already gone, i.e. the process is
        // on its way out for a reason someone else reported.
        let _ = self.tx.send(ThreadExit {
            worker: Some(self.worker),
            cause,
        });
    }
}

/// Wait for the first thread to end.
///
/// There is nothing to wait *for* beyond that: no thread has a successful ending,
/// so the first report is the run's verdict. A channel that closes with nothing on
/// it means every reporter was destroyed without sending, which the reporters
/// themselves make impossible short of the whole process unwinding.
pub fn supervise(rx: &Receiver<ThreadExit>) -> ThreadExit {
    match rx.recv() {
        Ok(exit) => exit,
        Err(_) => ThreadExit {
            worker: None,
            cause: "every worker thread went away without reporting".to_string(),
        },
    }
}

/// Read the configuration, waiting for it to appear.
///
/// A pod that boots before its credentials are placed parks and re-reads rather
/// than exiting: a restart loop against a file that is simply not there yet says
/// nothing an operator can act on. The cause is logged on the first failure and
/// on every change of cause, so a wait of hours is a line or two rather than a
/// wall.
pub fn wait_for_config(
    load: &mut dyn FnMut() -> Result<Config, ConfigError>,
    sleep: &dyn Fn(Duration),
) -> Config {
    let mut last: Option<String> = None;
    loop {
        match load() {
            Ok(config) => {
                if last.is_some() {
                    log::info!("config: readable now, continuing");
                }
                return config;
            }
            Err(e) => {
                let cause = e.to_string();
                if should_log_provisioning_failure(last.as_deref(), &cause) {
                    log::warn!(
                        "config: {cause} — waiting, re-reading every {}s",
                        RECHECK_INTERVAL.as_secs()
                    );
                    last = Some(cause);
                }
                sleep(RECHECK_INTERVAL);
            }
        }
    }
}

/// Where captured periods come from. The production source is an ALSA stream; the
/// seam is what lets the ring bookkeeping of the capture loop and the window
/// collection of the waveform self-test be asserted without a sound card.
pub trait PeriodSource {
    /// Read one period of interleaved frames, recovering from an overrun.
    ///
    /// Blocks until the stream delivers, so a caller that needs a bound waits with
    /// [`wait_ready`](Self::wait_ready) first.
    fn read_period(&mut self) -> Result<&[i16], PcmError>;

    /// Wait up to `timeout` for a period to be ready, reporting whether one is.
    ///
    /// `Ok(false)` is a timeout — or a recovered xrun, which reaches the caller the
    /// same way. No default: a source that answered "ready" without waiting would
    /// turn every deadline built on this into one that cannot fire, which is the
    /// failure the deadline exists to report.
    fn wait_ready(&mut self, timeout: Duration) -> Result<bool, PcmError>;

    /// Overruns recovered from since the stream opened — reported alongside a
    /// reading, because a window that needed several is worth seeing.
    fn recoveries(&self) -> u64;
}

impl PeriodSource for CaptureStream<'_> {
    fn read_period(&mut self) -> Result<&[i16], PcmError> {
        CaptureStream::read_period(self)
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<bool, PcmError> {
        CaptureStream::wait_ready(self, timeout)
    }

    fn recoveries(&self) -> u64 {
        CaptureStream::recoveries(self)
    }
}

/// Read periods into the ring until the stream stops delivering.
///
/// Returns the error that ended it. Nothing here retries past what
/// [`CaptureStream::read_period`] already does: a stream that will not come back
/// after its recovery budget is a board that has gone away, and this pod's answer
/// to that is to exit.
pub fn run_capture_loop<S: PeriodSource>(
    source: &mut S,
    ring: &Mutex<Option<CaptureRing<Box<[i16]>>>>,
    channel: usize,
    now_us: &dyn Fn() -> u64,
) -> PcmError {
    loop {
        match source.read_period() {
            Ok(period) => {
                // Dated before the lock: the reading names when the audio arrived,
                // not when a contended mutex let go of it.
                let arrived = now_us();
                let mut guard = ring.lock().unwrap_or_else(|_| {
                    panic!("capture ring mutex poisoned in the capture thread")
                });
                let ring = guard
                    .as_mut()
                    .expect("capture ring is wired before the capture thread starts");
                append_channel(ring, period, channel, arrived);
            }
            Err(e) => return e,
        }
    }
}

/// The ring's current write head, which the gate publishes at onset so the
/// streamer can place its pre-roll cursor.
pub fn ring_write_head(ring: &Mutex<Option<CaptureRing<Box<[i16]>>>>) -> u64 {
    ring.lock()
        .unwrap_or_else(|_| panic!("capture ring mutex poisoned reading the write head"))
        .as_ref()
        .map_or(0, |ring| ring.write_head)
}

/// The chip's control plane over USB, as the telemetry loop wants it.
///
/// One reading per call over a handle held for the life of the thread. Nothing
/// else on this transport contends for it, so the handle is simply owned here.
pub struct UsbTelemetryBus<T> {
    transport: T,
}

impl<T> UsbTelemetryBus<T> {
    /// A bus over an open control transport.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// The transport itself, for the control-plane reads that are not telemetry.
    ///
    /// This thread owns the pod's only control handle, so the chip's state line is
    /// read here or nowhere.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

impl<T: ControlTransport> TelemetryBus for UsbTelemetryBus<T>
where
    T::Error: fmt::Debug,
{
    fn read(&mut self, reading: Reading) -> Option<[f32; 4]> {
        read_f32x4_reading(&mut self.transport, USB_RETRY, reading)
    }
}

/// The chip's state line, when the gate says it is due: this pod's after-tick work
/// on the shared telemetry loop.
///
/// The state line rides that thread because that thread holds the control handle —
/// and it follows the gate because the chip's post-processing state is worth a line
/// while someone is speaking and worth nothing while the room is quiet.
pub fn state_line_tick<T>(
    core: &TelemetryCore,
    bus: &mut UsbTelemetryBus<T>,
    cadence: &mut StateLineCadence,
    now: &dyn Fn() -> Instant,
    say: &mut dyn FnMut(String),
) where
    T: ControlTransport,
    T::Error: fmt::Debug + fmt::Display,
{
    if cadence.tick(core.segment_open(), now()) {
        say(chip::state_line(bus.transport_mut(), now));
    }
}

/// The segment engine's waypoints, as log lines.
///
/// Most are progress markers; the interesting one is the dropped telemetry — that
/// is the engine saying its outbound direction is stalled long enough to be
/// losing readings.
pub fn log_segment_obs(segment_id: u32, event: ObsEvent) {
    match event {
        ObsEvent::TelemetryDropped { cap } => log::warn!(
            "seg={segment_id} {} (queue cap {cap} reached with the socket stalled)",
            event.as_str()
        ),
        _ => log::debug!("seg={segment_id} {}", event.as_str()),
    }
}

/// The inbound path's waypoints, as log lines — records that playback is
/// arriving and was accepted.
pub struct LogInboundObs;

impl InboundObserver for LogInboundObs {
    fn waypoint(&mut self, site: InboundWaypoint, frame: u32) {
        log::debug!("inbound {} (frame {frame})", site.as_str());
    }

    fn hello_ok(&mut self, format: PlaybackFormat) {
        log::info!(
            "inbound: host stream accepted — {} Hz, {} bit, {} ch",
            format.sample_rate_hz,
            format.bits_per_sample,
            format.channels
        );
    }
}

/// A per-pod reconnect jitter seed, derived from the pod's identity.
///
/// The point is that a fleet coming back after a host outage does not converge on
/// one retry beat, which needs the seeds to *differ* between pods rather than to
/// be unpredictable. Deriving it from the name rather than from randomness makes a
/// pod's retry pattern the same across restarts, which is one less thing that
/// varies when a reconnect problem is being watched.
pub fn jitter_seed(pod_id: &str) -> u32 {
    // FNV-1a: a few lines, no dependency, and well-spread over short ASCII names.
    let mut hash: u32 = 0x811c_9dc5;
    for byte in pod_id.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// The one board on the bus, or why there is not one.
fn the_board() -> Result<Board, String> {
    find_boards()
        .map_err(|e| format!("cannot enumerate USB: {e}"))
        .and_then(|found| select_board(&found))
}

/// Whether the kernel is presenting the board's sound card yet.
///
/// The USB device and its ALSA card come back at different moments after a reboot,
/// and a capture opened between the two fails on a card that is not there.
fn card_present() -> Result<(), String> {
    let cards = enumerate_cards().map_err(|e| format!("cannot list sound cards: {e}"))?;
    select_card(&cards).map(|_| ())
}

/// The chip's bring-up, on this unit's real bus, card and clock.
fn bring_up_chip() -> Result<(UsbControl, Routing), String> {
    chip::bring_up(
        &|board| UsbControl::open(board).map_err(|e| format!("{board} will not open: {e}")),
        &the_board,
        &card_present,
        &Instant::now,
        &std::thread::sleep,
    )
}

/// Bring up the pipeline and run it until a thread ends.
///
/// Hardware is opened here, on the main thread, before anything is spawned: a
/// board that is absent or a card that refuses the pipeline's parameters is a
/// startup failure with one clear line, not four threads racing to report it.
/// The chip comes first of all, because bringing it up reboots it and the sound
/// card goes away with it. Configuration is the exception — a missing `audio.conf`
/// is waited for, because it arrives per unit and may simply not be placed yet.
pub fn run() -> u8 {
    let config = wait_for_config(&mut Config::load, &std::thread::sleep);
    let pod_id = config::hostname();
    if let Err(why) = config::check_pod_id(&pod_id) {
        log::error!("startup: {why}");
        return EXIT_FAILED;
    }

    let (control, routing) = match bring_up_chip() {
        Ok(brought_up) => brought_up,
        Err(e) => {
            log::error!("startup: {e}");
            return EXIT_FAILED;
        }
    };
    let capture_channel = routing.channel(config.channel);
    log::info!(
        "startup: pod_id={pod_id} host={} {} vad_threshold={} vad_hangover_ms={}",
        config.addr,
        routing.channel_note(config.channel),
        config.vad_threshold,
        config.vad_hangover_ms
    );

    // Both directions of the same resolved card, so the echo canceller is
    // cancelling audio this pod actually played. The open is retried while the
    // node access budget lasts: the card comes back with the reboot and its
    // permissions land a moment after it.
    let (card, capture_pcm) = match chip::keep_trying(
        &|| open_capture().map_err(|e| e.to_string()),
        chip::NODE_ACCESS_TIMEOUT,
        &Instant::now,
        &std::thread::sleep,
    ) {
        Ok(open) => open,
        Err(e) => {
            log::error!("startup: no capture stream: {e}");
            return EXIT_FAILED;
        }
    };
    let playback_pcm = match open_playback_on(&card) {
        Ok(pcm) => pcm,
        Err(e) => {
            log::error!("startup: no playback stream: {e}");
            return EXIT_FAILED;
        }
    };

    let wiring = wire(&config, pod_id, capture_channel);
    let (exit_tx, exit_rx) = channel::<ThreadExit>();

    spawn_capture(&exit_tx, capture_pcm, wiring.capture);
    spawn_telemetry(&exit_tx, control, wiring.telemetry);
    spawn_streamer(&exit_tx, wiring.streamer);
    spawn_playback(&exit_tx, playback_pcm, wiring.playback);
    // The last handle the main thread holds: with it gone, a channel that closes
    // means the threads are gone rather than that nobody has spoken yet.
    drop(exit_tx);

    let exit = supervise(&exit_rx);
    log::error!("{exit} — exiting so the service restarts the pipeline");
    // Returning from `main` ends the process, and with it the threads still
    // running. That is the intent: the pipeline is only whole with all four.
    EXIT_FAILED
}

/// Spawn the thread that fills the capture ring.
fn spawn_capture(exit_tx: &Sender<ThreadExit>, pcm: alsa::pcm::PCM, wiring: CaptureWiring) {
    let CaptureWiring { ring, channel } = wiring;
    spawn(exit_tx, Worker::Capture, move |reporter| {
        // Built inside the thread because the reader borrows the PCM, and the PCM
        // is owned here for the life of the thread.
        match CaptureStream::new(&pcm) {
            Ok(mut stream) => {
                let cause = run_capture_loop(&mut stream, &ring, channel, &monotonic_us);
                reporter.set(cause.to_string());
            }
            Err(e) => reporter.set(e.to_string()),
        }
    });
}

/// Spawn the thread that polls the chip and drives the VAD gate.
fn spawn_telemetry<T>(exit_tx: &Sender<ThreadExit>, control: T, wiring: TelemetryWiring)
where
    T: ControlTransport + Send + 'static,
    T::Error: fmt::Debug + fmt::Display,
{
    let TelemetryWiring {
        ring,
        vad_closed,
        msg_tx,
        threshold,
        hangover_ms,
    } = wiring;
    log::info!("telemetry: polling SPENERGY at {VAD_POLL_HZ} Hz, DoA at {DOA_POLL_HZ} Hz");
    spawn(exit_tx, Worker::Telemetry, move |_reporter| {
        let mut bus = UsbTelemetryBus::new(control);
        let core = TelemetryCore::new(threshold, hangover_ms);
        let ctx = TelemetryCtx {
            tx: &msg_tx,
            vad_closed_flag: &vad_closed,
            write_head: &|| ring_write_head(&ring),
            now_us: &monotonic_us,
            // Nothing quiesces capture on this pod; the capture thread runs for
            // the life of the process.
            capture_quiesced: &|| false,
        };
        let mut cadence = StateLineCadence::new();
        let mut say = |line: String| log::info!("{line}");
        run_telemetry_loop(
            core,
            &mut bus,
            &ctx,
            &|ms| std::thread::sleep(Duration::from_millis(u64::from(ms))),
            &mut |core, bus| state_line_tick(core, bus, &mut cadence, &Instant::now, &mut say),
        );
    });
}

/// Spawn the thread that runs the shared idle/segment loop.
fn spawn_streamer(exit_tx: &Sender<ThreadExit>, wiring: StreamerWiring) {
    let StreamerWiring {
        ring,
        vad_closed,
        segment_active,
        msg_rx,
        mut sink,
        pod_id,
        config,
    } = wiring;
    spawn(exit_tx, Worker::Streamer, move |reporter| {
        let seed = jitter_seed(&pod_id);
        let platform = tls::platform(pod_id, &config);
        let ridx = RingIndex::new(RING_CAPACITY_SAMPLES);
        let mut obs = log_segment_obs;
        let mut inbound_obs = LogInboundObs;
        let mut runtime = StreamerRuntime {
            rx: &msg_rx,
            ring: &ring,
            ridx: &ridx,
            vad_closed_flag: &vad_closed,
            segment_active_flag: &segment_active,
            inbound_sink: &mut sink,
            now_us: &monotonic_us,
            now_instant: &Instant::now,
            obs: &mut obs,
            inbound_obs: &mut inbound_obs,
            jitter_seed: seed,
        };
        match run_streamer_loop(&platform, &mut runtime) {
            StreamerExit::ChannelDisconnected => reporter
                .set("the telemetry channel disconnected, so no utterance can ever be gated again"),
        }
    });
}

/// Spawn the thread that plays banked inbound audio.
fn spawn_playback(exit_tx: &Sender<ThreadExit>, pcm: alsa::pcm::PCM, wiring: PlaybackWiring) {
    let PlaybackWiring { mut drain } = wiring;
    spawn(
        exit_tx,
        Worker::Playback,
        move |reporter| match AlsaOut::new(&pcm) {
            Ok(mut out) => {
                let fault = run_drain_loop(&mut drain, &mut out);
                reporter.set(fault.to_string());
            }
            Err(e) => reporter.set(e.to_string()),
        },
    );
}

/// Spawn one named worker with its own exit reporter.
///
/// The reporter is constructed before the body and dropped after it, so a body
/// that returns and a body that panics both reach the supervisor.
fn spawn<F>(exit_tx: &Sender<ThreadExit>, worker: Worker, body: F)
where
    F: FnOnce(&mut ExitReporter) + Send + 'static,
{
    let tx = exit_tx.clone();
    let spawned = std::thread::Builder::new()
        .name(worker.name().to_string())
        .spawn(move || {
            let mut reporter = ExitReporter::new(tx, worker);
            body(&mut reporter);
        });
    if let Err(e) = spawned {
        // Nothing recovers from this: a pipeline missing a thread is not a
        // pipeline. The reporter never existed, so say it here.
        panic!("cannot spawn the {} thread: {e}", worker.name());
    }
    log::info!("{} thread spawned", worker.name());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ASR_OUTPUT_CHANNEL, POST_PROCESSED_CHANNEL};
    use crate::test_support::{Clock, RegisterBank};
    use audio_pipeline::playback::PlaybackSink;
    use audio_pipeline::ring::SAMPLE_RATE_HZ;
    use std::collections::VecDeque;

    const KEY_HEX: &str = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

    #[test]
    fn the_first_thread_to_end_is_the_run_s_verdict() {
        let (tx, rx) = channel::<ThreadExit>();
        {
            let mut first = ExitReporter::new(tx.clone(), Worker::Capture);
            first.set("the card stopped delivering");
        }
        {
            let mut second = ExitReporter::new(tx.clone(), Worker::Playback);
            second.set("the device went away");
        }
        let exit = supervise(&rx);
        assert_eq!(exit.worker, Some(Worker::Capture));
        assert_eq!(exit.cause, "the card stopped delivering");
        assert_eq!(
            exit.to_string(),
            "the capture thread ended: the card stopped delivering"
        );
    }

    #[test]
    fn a_thread_that_said_nothing_still_reports_itself() {
        let (tx, rx) = channel::<ThreadExit>();
        // A panicking body unwinds through the reporter's drop without ever
        // reaching a `set`. Without this, a dead gate would leave a live process.
        drop(ExitReporter::new(tx, Worker::Telemetry));
        let exit = supervise(&rx);
        assert_eq!(exit.worker, Some(Worker::Telemetry));
        assert!(exit.cause.contains("panicked"), "{}", exit.cause);
    }

    #[test]
    fn a_panicking_body_reaches_the_supervisor_through_the_unwind() {
        let (tx, rx) = channel::<ThreadExit>();
        let handle = std::thread::spawn(move || {
            let _reporter = ExitReporter::new(tx, Worker::Streamer);
            panic!("the body gave up");
        });
        assert!(handle.join().is_err(), "the thread is expected to panic");
        let exit = supervise(&rx);
        assert_eq!(exit.worker, Some(Worker::Streamer));
    }

    #[test]
    fn the_last_cause_set_is_the_one_reported() {
        let (tx, rx) = channel::<ThreadExit>();
        {
            let mut reporter = ExitReporter::new(tx, Worker::Playback);
            reporter.set("recovering");
            reporter.set("the device went away for good");
        }
        assert_eq!(supervise(&rx).cause, "the device went away for good");
    }

    #[test]
    fn a_channel_that_closes_silently_is_still_a_failure_with_a_reason() {
        let (tx, rx) = channel::<ThreadExit>();
        drop(tx);
        let exit = supervise(&rx);
        assert_eq!(exit.worker, None);
        assert!(exit.to_string().contains("without reporting"), "{exit}");
    }

    #[test]
    fn every_worker_has_its_own_name() {
        let names: Vec<&str> = [
            Worker::Capture,
            Worker::Telemetry,
            Worker::Streamer,
            Worker::Playback,
        ]
        .iter()
        .map(|w| w.name())
        .collect();
        assert_eq!(names, ["capture", "telemetry", "streamer", "playback"]);
    }

    #[test]
    fn a_missing_file_is_waited_for_rather_than_fatal() {
        let mut attempts = 0;
        let mut load = || {
            attempts += 1;
            if attempts < 4 {
                Err(ConfigError::Missing { key: "ADDR" })
            } else {
                Config::parse(&format!("ADDR=198.51.100.7:5555\nPSK={KEY_HEX}\n"))
            }
        };
        let slept = std::cell::RefCell::new(Vec::new());
        let config = wait_for_config(&mut load, &|d| slept.borrow_mut().push(d));
        assert_eq!(config.addr.port(), 5555);
        assert_eq!(
            slept.into_inner(),
            vec![RECHECK_INTERVAL; 3],
            "one wait per failed read, at the shared cadence"
        );
    }

    #[test]
    fn a_readable_file_is_taken_without_waiting() {
        let mut load = || Config::parse(&format!("ADDR=198.51.100.7:5555\nPSK={KEY_HEX}\n"));
        let slept = std::cell::Cell::new(0);
        wait_for_config(&mut load, &|_| slept.set(slept.get() + 1));
        assert_eq!(slept.get(), 0);
    }

    /// Periods handed out in order; the queue running dry ends the stream.
    struct ScriptedPeriods {
        periods: VecDeque<Vec<i16>>,
        current: Vec<i16>,
    }

    impl ScriptedPeriods {
        fn new(periods: Vec<Vec<i16>>) -> Self {
            Self {
                periods: periods.into(),
                current: Vec::new(),
            }
        }
    }

    impl PeriodSource for ScriptedPeriods {
        fn read_period(&mut self) -> Result<&[i16], PcmError> {
            match self.periods.pop_front() {
                Some(period) => {
                    self.current = period;
                    Ok(&self.current)
                }
                None => Err(PcmError::Stream {
                    reason: "the script ran out".to_string(),
                }),
            }
        }

        fn wait_ready(&mut self, _timeout: Duration) -> Result<bool, PcmError> {
            Ok(!self.periods.is_empty())
        }

        fn recoveries(&self) -> u64 {
            0
        }
    }

    /// One interleaved stereo period whose two channels are told apart by sign.
    fn stereo(frames: usize, base: i16) -> Vec<i16> {
        (0..frames)
            .flat_map(|i| [base + i as i16, -(base + i as i16)])
            .collect()
    }

    fn empty_ring() -> Mutex<Option<CaptureRing<Box<[i16]>>>> {
        Mutex::new(Some(new_ring()))
    }

    #[test]
    fn the_capture_loop_lands_only_the_configured_channel_in_the_ring() {
        let ring = empty_ring();
        let mut source = ScriptedPeriods::new(vec![stereo(4, 100), stereo(4, 200)]);
        let clock = std::cell::Cell::new(0u64);
        let err = run_capture_loop(&mut source, &ring, 1, &|| {
            clock.set(clock.get() + 20_000);
            clock.get()
        });
        assert!(matches!(err, PcmError::Stream { .. }));

        let guard = ring.lock().expect("ring");
        let ring = guard.as_ref().expect("wired");
        assert_eq!(ring.write_head, 8, "both periods, four frames each");
        assert_eq!(
            &ring.samples[..8],
            &[-100, -101, -102, -103, -200, -201, -202, -203],
            "channel 1 is the negated one"
        );
        assert_eq!(ring.anchor_sample, 7, "the anchor names the last sample");
        assert_eq!(
            ring.anchor_ts_us, 40_000,
            "dated by the arrival of the chunk that carried it"
        );
    }

    #[test]
    fn the_capture_loop_ends_on_the_error_the_stream_gave_it() {
        let ring = empty_ring();
        let mut source = ScriptedPeriods::new(vec![]);
        let err = run_capture_loop(&mut source, &ring, 0, &|| 0);
        assert!(
            err.to_string().contains("the script ran out"),
            "the loop reports the stream's own cause: {err}"
        );
        assert_eq!(
            ring_write_head(&ring),
            0,
            "a stream that never delivered leaves the ring where it started"
        );
    }

    #[test]
    fn the_write_head_the_gate_publishes_is_the_one_capture_advanced() {
        let ring = empty_ring();
        let mut source = ScriptedPeriods::new(vec![stereo(SAMPLE_RATE_HZ as usize / 50, 1)]);
        let _ = run_capture_loop(&mut source, &ring, 0, &|| 1_000);
        assert_eq!(
            ring_write_head(&ring),
            320,
            "one 20 ms period of mono samples"
        );
    }

    #[test]
    fn an_unwired_ring_reads_as_a_write_head_of_zero() {
        // Reachable only between the ring being taken and the pod exiting; the gate
        // must not panic there, since a zero pre-roll cursor is simply no history.
        let ring: Mutex<Option<CaptureRing<Box<[i16]>>>> = Mutex::new(None);
        assert_eq!(ring_write_head(&ring), 0);
    }

    /// A control transport that answers every read with the same payload, and
    /// records what it was asked for.
    struct ScriptedControl {
        payload: [u8; 16],
        status: u8,
        reads: Vec<(u8, u8)>,
    }

    impl ControlTransport for ScriptedControl {
        type Error = String;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.reads.push((resid, cmd));
            payload.copy_from_slice(&self.payload[..payload.len()]);
            Ok(self.status)
        }

        fn control_write_once(
            &mut self,
            _resid: u8,
            _cmd: u8,
            _payload: &[u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            unreachable!("the telemetry bus never writes")
        }

        fn delay_ms(&mut self, _ms: u32) {}
    }

    fn f32x4_bytes(values: [f32; 4]) -> [u8; 16] {
        let mut out = [0u8; 16];
        for (slot, value) in out.chunks_exact_mut(4).zip(values) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
        out
    }

    #[test]
    fn the_usb_bus_reads_each_register_and_decodes_it() {
        let mut bus = UsbTelemetryBus::new(ScriptedControl {
            payload: f32x4_bytes([1.5, 2.5, 3.5, 4.5]),
            status: xvf3800_ctrl::STATUS_DONE,
            reads: Vec::new(),
        });
        assert_eq!(bus.read(Reading::SpEnergy), Some([1.5, 2.5, 3.5, 4.5]));
        assert_eq!(bus.read(Reading::Azimuths), Some([1.5, 2.5, 3.5, 4.5]));
        assert_eq!(
            bus.transport.reads,
            vec![
                (
                    xvf3800_ctrl::AEC_RESID,
                    xvf3800_ctrl::AEC_SPENERGY_VALUES_CMD
                ),
                (
                    xvf3800_ctrl::AEC_RESID,
                    xvf3800_ctrl::AEC_AZIMUTH_VALUES_CMD
                ),
            ],
            "each reading goes to its own register"
        );
    }

    #[test]
    fn a_reading_the_chip_refused_is_no_reading_at_all() {
        let mut bus = UsbTelemetryBus::new(ScriptedControl {
            payload: f32x4_bytes([9.0; 4]),
            status: 0x02,
            reads: Vec::new(),
        });
        assert_eq!(
            bus.read(Reading::SpEnergy),
            None,
            "a fatal status must not be decoded into an energy the gate then believes"
        );
    }

    /// Drive `ticks` telemetry ticks at the poll cadence over a chip whose speech
    /// energy is `energy`, returning the state lines said and the gate's state.
    fn ticks_at(
        energy: f32,
        ticks: u32,
    ) -> (
        Vec<String>,
        TelemetryCore,
        UsbTelemetryBus<RegisterBank>,
        StateLineCadence,
        Clock,
    ) {
        let mut bank = RegisterBank::new();
        bank.set(
            xvf3800_ctrl::AEC_RESID,
            xvf3800_ctrl::AEC_SPENERGY_VALUES_CMD,
            f32x4_bytes([energy; 4]).to_vec(),
        );
        let mut bus = UsbTelemetryBus::new(bank);
        let mut core = TelemetryCore::new(1.0, 0);
        let mut cadence = StateLineCadence::new();
        let clock = Clock::new();
        let mut said = Vec::new();
        let (tx, _rx) = sync_channel::<StreamerMsg>(STREAMER_CHAN_CAPACITY);
        let closed = AtomicBool::new(false);
        let ctx = TelemetryCtx {
            tx: &tx,
            vad_closed_flag: &closed,
            write_head: &|| 0,
            now_us: &|| 0,
            capture_quiesced: &|| false,
        };
        for _ in 0..ticks {
            core.poll_tick(&mut bus, &ctx);
            state_line_tick(
                &core,
                &mut bus,
                &mut cadence,
                &|| clock.now(),
                &mut |line| said.push(line),
            );
            clock.advance(Duration::from_millis(u64::from(
                pod_streamer::telemetry::VAD_POLL_INTERVAL_MS,
            )));
        }
        (said, core, bus, cadence, clock)
    }

    /// The gate open, the state line said once for the opening and once more per
    /// interval — and the registers it reads are the chip's, not the gate's.
    #[test]
    fn the_state_line_follows_the_gate_and_not_the_poll() {
        // Fifty seconds of speech: the line the gate's opening says, and one more
        // when the first interval is up.
        let ticks = VAD_POLL_HZ * 50;
        let (said, core, mut bus, _cadence, _clock) = ticks_at(5.0, ticks);
        assert!(core.segment_open(), "the gate is open on this energy");
        assert_eq!(said.len(), 2, "{said:?}");
        assert!(said[0].starts_with("chip state: OP_L="), "{said:?}");
        assert_eq!(
            bus.transport_mut()
                .reads_of(xvf3800_ctrl::PP_RESID, xvf3800_ctrl::PP_DTSENSITIVE_CMD),
            2,
            "one post-processing readback per state line, no more"
        );
    }

    #[test]
    fn a_gate_that_never_opens_says_nothing_about_the_chip() {
        let (said, core, _bus, _cadence, _clock) = ticks_at(0.0, VAD_POLL_HZ * 60);
        assert!(!core.segment_open());
        assert!(said.is_empty(), "{said:?}");
    }

    // ── The wiring ────────────────────────────────────────────────────────────

    fn wired() -> Wiring {
        let config = Config::parse(&format!(
            "ADDR=198.51.100.7:5555\nPSK={KEY_HEX}\nCHANNEL=1\nVAD_THRESHOLD=2.5\n\
             VAD_HANGOVER_MS=1200\n"
        ))
        .expect("parse");
        wire(&config, "reachy00".to_string(), config.channel)
    }

    /// The routing has the last word over the configuration, and this is where
    /// that word reaches the hardware: the capture thread's channel. A unit
    /// whose tuning fragment asks for the ASR channel still streams the
    /// post-processed one when the chip would not take the routing.
    #[test]
    fn a_refused_routing_wires_capture_to_the_post_processed_channel() {
        let config = Config::parse(&format!(
            "ADDR=198.51.100.7:5555\nPSK={KEY_HEX}\nCHANNEL=1\n"
        ))
        .expect("parse");
        assert_eq!(config.channel, ASR_OUTPUT_CHANNEL);

        let applied = Routing::Applied;
        let w = wire(
            &config,
            "reachy00".to_string(),
            applied.channel(config.channel),
        );
        assert_eq!(w.capture.channel, ASR_OUTPUT_CHANNEL);

        let refused = Routing::Refused(vec!["AUDIO_MGR_OP_R reads back (8, 0)".to_string()]);
        let w = wire(
            &config,
            "reachy00".to_string(),
            refused.channel(config.channel),
        );
        assert_eq!(w.capture.channel, POST_PROCESSED_CHANNEL);
    }

    #[test]
    fn all_three_threads_that_touch_the_ring_are_handed_the_same_one() {
        // A second `new_ring()` anywhere in the hand-off compiles and type-checks,
        // and leaves the streamer reading a ring capture never fills.
        let w = wired();
        assert!(Arc::ptr_eq(&w.capture.ring, &w.telemetry.ring));
        assert!(Arc::ptr_eq(&w.capture.ring, &w.streamer.ring));

        // And it is one ring in fact, not merely by pointer: what capture appends
        // is what the streamer's write-head reader sees.
        let mut source = ScriptedPeriods::new(vec![stereo(4, 100)]);
        let _ = run_capture_loop(&mut source, &w.capture.ring, w.capture.channel, &|| 1_000);
        assert_eq!(ring_write_head(&w.streamer.ring), 4);
        assert_eq!(ring_write_head(&w.telemetry.ring), 4);
    }

    #[test]
    fn the_gate_flag_the_telemetry_thread_sets_is_the_one_the_streamer_reads() {
        let w = wired();
        assert!(Arc::ptr_eq(&w.telemetry.vad_closed, &w.streamer.vad_closed));
        // The two flags are the same type, so swapping them compiles clean and
        // passes every other test: telemetry's gate-close would be published to a
        // flag nobody reads, and the streamer would read its own segment-active
        // publication as "VAD closed" and tear down each segment as it opened.
        assert!(!Arc::ptr_eq(
            &w.telemetry.vad_closed,
            &w.streamer.segment_active
        ));
        w.telemetry
            .vad_closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(
            w.streamer
                .vad_closed
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(
            !w.streamer
                .segment_active
                .load(std::sync::atomic::Ordering::SeqCst),
            "the segment flag is the streamer's to publish, and starts clear"
        );
    }

    #[test]
    fn the_gates_messages_reach_the_streamers_own_receiver() {
        let w = wired();
        w.telemetry
            .msg_tx
            .send(StreamerMsg::VadOpened { write_head: 42 })
            .expect("the receiver is held by the streamer's wiring");
        assert!(matches!(
            w.streamer.msg_rx.try_recv(),
            Ok(StreamerMsg::VadOpened { write_head: 42 })
        ));
    }

    #[test]
    fn every_configured_value_reaches_the_thread_that_acts_on_it() {
        // Each of these is a plain number handed across a spawn boundary, so a
        // transposition — the channel where a threshold was meant — type-checks.
        let w = wired();
        assert_eq!(w.capture.channel, 1);
        assert_eq!(w.telemetry.threshold, 2.5);
        assert_eq!(w.telemetry.hangover_ms, 1200);
        assert_eq!(w.streamer.pod_id, "reachy00");
        assert_eq!(w.streamer.config.addr.port(), 5555);
    }

    #[test]
    fn what_the_streamer_banks_is_what_the_playback_thread_drains() {
        let mut w = wired();
        let audio: Vec<u8> = (0..64u8).collect();
        assert_eq!(
            w.streamer.sink.accept(&audio),
            audio_pipeline::playback::Accepted::Enqueued
        );
        let mut out = CountingOut::default();
        assert!(
            matches!(
                w.playback.drain.pass(&mut out, Instant::now()),
                Ok(crate::playback::PassOutcome::Filling)
            ),
            "the drain sees the sink's audio, held while its cushion fills"
        );
    }

    /// A sink for stereo frames that only counts them.
    #[derive(Default)]
    struct CountingOut(usize);

    impl crate::playback::StereoOut for CountingOut {
        fn write_frames(
            &mut self,
            samples: &[i16],
        ) -> Result<usize, crate::playback::PlaybackFault> {
            let frames = samples.len() / crate::config::CHANNELS;
            self.0 += frames;
            Ok(frames)
        }
    }

    #[test]
    fn two_pods_draw_different_jitter_and_one_pod_draws_the_same_one_twice() {
        assert_ne!(jitter_seed("reachy00"), jitter_seed("reachy01"));
        assert_eq!(jitter_seed("reachy00"), jitter_seed("reachy00"));
        assert_ne!(jitter_seed(""), jitter_seed("reachy00"));
    }
}
