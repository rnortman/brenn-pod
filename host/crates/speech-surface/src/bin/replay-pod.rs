//! `replay-pod`: replay a recorded frame log as a fake pod over TCP against a
//! running `speech-surface`. Each record's payload is the exact framed wire
//! bytes and is sent verbatim — no decode, no session FSM, no re-encode — so
//! replay reproduces the original traffic (including undecodable frames) rather
//! than laundering it.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use audio_pipeline::wire::{StreamFrame, decode_frame};
use clap::{Parser, ValueEnum};
use openssl::ssl::{Ssl, SslContext, SslStream};
use pod_ingest::{FrameLogError, FrameLogReader, HostMicros, LogItem};
use serde_json::json;
use speech_surface::emit_line as emit;
use speech_surface::exit;
use speech_surface::psk::{client_context, parse_psk_hex};

/// Send pacing between records.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Pace {
    /// Sleep the `host_rx` delta between consecutive records — faithful
    /// reproduction. This is the tool's stated purpose, hence the default.
    Realtime,
    /// Send with no sleeps. Tests opt into this.
    Fast,
}

#[derive(Parser)]
#[command(
    name = "replay-pod",
    about = "Replay recorded frame logs as a fake pod over TCP against a running speech-surface"
)]
struct Cli {
    /// The daemon's ingest address, `host:port`.
    #[arg(long)]
    connect: String,
    /// PSK identity to authenticate as. Must match the `Hello.pod_id` the
    /// replayed log carries; a mismatch is a fatal connection error.
    #[arg(long)]
    pod_id: String,
    /// File holding this pod's 64-hex-character audio-link key. A file, never an
    /// argument: keys do not belong in shell history or `ps` output.
    #[arg(long)]
    psk_file: PathBuf,
    /// Pacing between records.
    #[arg(long, value_enum, default_value_t = Pace::Realtime)]
    pace: Pace,
    /// Cap any single inter-record sleep, in milliseconds. Unclamped when
    /// absent — a long between-segment silence replays faithfully by default;
    /// the clamp exists for interactive use against long captures.
    #[arg(long)]
    max_gap_ms: Option<u64>,
    /// Stay connected past end-of-log until the daemon's playback `EndOfAudio`
    /// is observed (or the drain closes, or `--linger-timeout-ms` elapses),
    /// then send FIN. Off by default: without it, FIN fires at end-of-log
    /// exactly as before. Only affects the clean `Done` replay path.
    #[arg(long)]
    linger_until_eoa: bool,
    /// Liveness bound for `--linger-until-eoa`, in milliseconds. A timed-out
    /// linger is reported and never the success path. Requires
    /// `--linger-until-eoa`; a usage error given alone.
    #[arg(
        long,
        default_value_t = 30000,
        value_parser = clap::value_parser!(u64).range(1..),
        requires = "linger_until_eoa"
    )]
    linger_timeout_ms: u64,
    /// Frame logs to replay, in order. Each replays on its own TCP connection.
    #[arg(required = true)]
    framelogs: Vec<PathBuf>,
}

/// Pacing policy: the sleep to insert before sending each record.
///
/// - First record: zero.
/// - Rolled log (`meta().rolled_from` is `Some`): zero before the *second*
///   record too. A rolled log's first record is the re-emitted `Hello`
///   carrying the original connection's early `host_rx`; the delta between it
///   and the first genuine post-roll record spans everything from connection
///   start to the roll point — a roll artifact that never existed on the wire.
///   The record's `host_rx` still becomes `prev`, so pacing resumes with the
///   true wire delta from the third record on.
/// - Negative delta (host clock step during capture): zero.
/// - `max_gap` caps every result when set.
struct Pacer {
    prev: Option<HostMicros>,
    skip_next_delta: bool,
    max_gap: Option<Duration>,
}

impl Pacer {
    fn new(rolled: bool, max_gap: Option<Duration>) -> Self {
        Pacer {
            prev: None,
            skip_next_delta: rolled,
            max_gap,
        }
    }

    /// The delay to sleep before sending the record arriving at `host_rx`.
    /// Advances internal state; call once per record in send order.
    fn delay_before(&mut self, host_rx: HostMicros) -> Duration {
        let raw = match self.prev {
            None => Duration::ZERO,
            Some(prev) => {
                if self.skip_next_delta {
                    self.skip_next_delta = false;
                    Duration::ZERO
                } else {
                    // `checked_delta` is `None` on a backward clock step; a
                    // negative delta paces as zero.
                    Duration::from_micros(host_rx.checked_delta(prev).unwrap_or(0))
                }
            }
        };
        self.prev = Some(host_rx);
        match self.max_gap {
            Some(cap) => raw.min(cap),
            None => raw,
        }
    }
}

/// Outcome of replaying one frame log. The outer driver maps these to the
/// shared exit codes ("worst thing that happened"); `ConnectRefused` also aborts
/// the whole run, since a down target fails every remaining log identically.
enum LogOutcome {
    /// Fully replayed, torn tail included.
    Done,
    /// Input file absent — a pruned/never-existed miss, already reported.
    Missing,
    /// The daemon closed the connection mid-replay (write error / reset).
    PeerClosed,
    /// Unreadable header or a corrupt/errored record; already reported.
    Failed,
    /// Connect refused — the target is down; the caller aborts the run.
    ConnectRefused,
}

/// One log's replay result: its outcome, the frame count written to the socket
/// (summed into the run's `replay_complete` total), and the tally of server→device
/// frames the drain decoded on the way back.
struct LogReplay {
    outcome: LogOutcome,
    frames: u64,
    rx: PlaybackRx,
    /// The linger result on this connection, `Some` only when `--linger-until-eoa`
    /// was set and the replay reached the clean `Done` path; `None` otherwise.
    linger: Option<LingerReport>,
}

/// Per-connection tally of the server→device (playback) frames the drain decoded.
/// The daemon sends one `Hello`, then `Audio` frames, then one `EndOfAudio`; `other`
/// and `decode_errors` stay zero on a healthy connection and make any wire mismatch
/// visible rather than silently discarded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PlaybackRx {
    hello: u64,
    audio: u64,
    end_of_audio: u64,
    /// Decoded, but not one of the three expected server→device variants.
    other: u64,
    /// A full-length frame whose payload failed to decode — a version/format mismatch.
    decode_errors: u64,
}

impl PlaybackRx {
    /// Fold another connection's tally into this running run-level total.
    fn add(&mut self, o: PlaybackRx) {
        self.hello += o.hello;
        self.audio += o.audio;
        self.end_of_audio += o.end_of_audio;
        self.other += o.other;
        self.decode_errors += o.decode_errors;
    }
}

/// Sticky cross-thread signal from the drain thread to the replay thread for
/// `--linger-until-eoa`. The drain sets `eoa` the moment it decodes an
/// `EndOfAudio` and sets `exited` when it returns; both flags are sticky (never
/// cleared) so the replay thread's wait is a level check, not an edge — an
/// `EndOfAudio` decoded before end-of-log already satisfies the wait.
#[derive(Default)]
struct LingerFlags {
    eoa: bool,
    exited: bool,
}

struct LingerSignal {
    flags: Mutex<LingerFlags>,
    cv: Condvar,
}

/// How a `--linger-until-eoa` connection settled. The wait's three exits (first
/// wins): an observed `EndOfAudio`, the drain thread closing (daemon gone, no
/// `EndOfAudio` can ever arrive), or the liveness timeout. `NoDrain` is the
/// off-nominal fourth: the linger path was reached but the drain clone had
/// failed, so nothing could ever be observed — reported (not skipped) so an
/// unverifiable connection fails the run-level fold rather than vanishing from
/// it. Only `Eoa` is the success path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LingerOutcome {
    Eoa,
    DrainClosed,
    Timeout,
    NoDrain,
}

impl LingerOutcome {
    fn as_str(self) -> &'static str {
        match self {
            LingerOutcome::Eoa => "eoa",
            LingerOutcome::DrainClosed => "drain_closed",
            LingerOutcome::Timeout => "timeout",
            LingerOutcome::NoDrain => "no_drain",
        }
    }
}

impl LingerSignal {
    fn new() -> Self {
        LingerSignal {
            flags: Mutex::new(LingerFlags::default()),
            cv: Condvar::new(),
        }
    }

    /// Mark that an `EndOfAudio` was decoded (idempotent). Notifies only on the
    /// first transition to avoid redundant wakeups.
    fn signal_eoa(&self) {
        let mut flags = self.flags.lock().expect("linger mutex poisoned");
        if !flags.eoa {
            flags.eoa = true;
            drop(flags);
            self.cv.notify_all();
        }
    }

    /// Mark that the drain thread has exited (EOF or socket error).
    fn signal_exit(&self) {
        let mut flags = self.flags.lock().expect("linger mutex poisoned");
        flags.exited = true;
        drop(flags);
        self.cv.notify_all();
    }

    /// Block until an `EndOfAudio` is observed, the drain exits, or `timeout`
    /// elapses — whichever comes first. `Eoa` outranks `DrainClosed` when both
    /// flags are set: the wait is a level check, not an edge, so an `EndOfAudio`
    /// decoded before the wait began still counts as success even if the drain
    /// has since closed.
    fn wait(&self, timeout: Duration) -> LingerOutcome {
        let deadline = Instant::now() + timeout;
        let mut flags = self.flags.lock().expect("linger mutex poisoned");
        loop {
            if flags.eoa {
                return LingerOutcome::Eoa;
            }
            if flags.exited {
                return LingerOutcome::DrainClosed;
            }
            let now = Instant::now();
            if now >= deadline {
                return LingerOutcome::Timeout;
            }
            let (guard, res) = self
                .cv
                .wait_timeout(flags, deadline - now)
                .expect("linger mutex poisoned");
            flags = guard;
            if res.timed_out() && !flags.eoa && !flags.exited {
                return LingerOutcome::Timeout;
            }
        }
    }
}

/// Per-connection linger result, present only when `--linger-until-eoa` reached
/// the clean (`Done`) replay path; emitted on the `replay_log_done` line and
/// folded into the run-level report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LingerReport {
    outcome: LingerOutcome,
    waited_ms: u64,
}

impl LingerReport {
    /// The success predicate, derived from `outcome` so the two can never
    /// disagree: an `EndOfAudio` was observed iff the wait released on it.
    fn eoa_observed(&self) -> bool {
        matches!(self.outcome, LingerOutcome::Eoa)
    }
}

/// Run-level fold of every lingered connection's `LingerReport`. Present in
/// `replay_complete` whenever the flag is set — even with zero lingered
/// connections, where it reports `eoa_observed: false` so a harness assertion
/// can never vacuously pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LingerRunReport {
    /// AND across all lingered connections; false when none lingered.
    eoa_observed: bool,
    /// Max `waited_ms` across lingered connections.
    waited_ms: u64,
}

/// The whole run's result: how many logs were attempted, the total frames
/// written across them, the folded device-side `playback_rx` tally, the folded
/// linger report (`Some` whenever `--linger-until-eoa` was set), and the
/// per-log exit codes. A named struct rather than a positional tuple so a call
/// site cannot transpose the two `u64`s or misplace a field.
struct RunSummary {
    logs: u64,
    frames: u64,
    rx: PlaybackRx,
    linger: Option<LingerRunReport>,
    codes: Vec<u8>,
}

/// Pop every complete `[u16 LE len][postcard payload]` frame from the front of `buf`,
/// tallying each into `rx`; any trailing partial frame is left in `buf` for the next
/// read. A full-length frame that fails to decode is counted and skipped by its
/// declared length so the parse stays byte-synchronized.
fn consume_frames(buf: &mut Vec<u8>, rx: &mut PlaybackRx) {
    let mut off = 0;
    while buf.len() - off >= 2 {
        let payload_len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
        let end = off + 2 + payload_len;
        if buf.len() < end {
            break; // frame not fully arrived yet
        }
        match decode_frame(&buf[off..end]) {
            Ok(StreamFrame::Hello(_)) => rx.hello += 1,
            Ok(StreamFrame::Audio(_)) => rx.audio += 1,
            Ok(StreamFrame::EndOfAudio(_)) => rx.end_of_audio += 1,
            Ok(_) => rx.other += 1,
            Err(_) => rx.decode_errors += 1,
        }
        off = end;
    }
    buf.drain(..off);
}

/// Drain `r` to EOF, decoding server→device length-prefixed frames and tallying them
/// by variant. Trailing bytes shorter than a full frame at EOF are a normal torn tail
/// and left uncounted. Signals `signal` when an `EndOfAudio` is first decoded and when
/// the drain exits — the two release conditions the `--linger-until-eoa` wait checks.
fn drain_and_count<R: Read>(mut r: R, signal: Arc<LingerSignal>) -> PlaybackRx {
    let mut rx = PlaybackRx::default();
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut eoa_signaled = false;
    loop {
        match r.read(&mut chunk) {
            Ok(0) => break, // clean EOF: daemon closed
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                consume_frames(&mut buf, &mut rx);
                // Signal only on the first transition; the flag is sticky, so a
                // per-chunk re-signal would just re-lock the mutex to no effect.
                if !eoa_signaled && rx.end_of_audio > 0 {
                    signal.signal_eoa();
                    eoa_signaled = true;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                // A torn-down socket is a different exit than a clean daemon FIN;
                // both release the linger as `DrainClosed`, so name the error here
                // to keep an errored teardown distinguishable in the logs.
                eprintln!("replay-pod: drain read error ({}): {e}", e.kind());
                break;
            }
        }
    }
    signal.signal_exit();
    rx
}

/// The `--pace` value's canonical name for JSONL output, taken from the
/// `ValueEnum` derive so CLI spelling and reported spelling share one source.
fn pace_str(pace: Pace) -> String {
    pace.to_possible_value()
        .expect("Pace variants are all reachable")
        .get_name()
        .to_owned()
}

/// How long the socket waits for bytes before the drain releases the session
/// lock. Short enough that a paced write is never held up materially, long
/// enough that idle polling stays cheap.
const READ_TIMEOUT: Duration = Duration::from_millis(20);

/// The shared TLS session. One session serves both directions — TLS record
/// framing and sequence numbers are session state, so there is no `try_clone`
/// equivalent — and the drain thread holds the lock only for one read at a time,
/// bounded by [`READ_TIMEOUT`].
type SharedSession = Arc<Mutex<SslStream<TcpStream>>>;

/// A `Read` view onto the shared session, so the drain keeps its `R: Read` shape.
/// A receive timeout with no plaintext ready yields the lock and retries rather
/// than reporting an error.
struct SessionReader {
    session: SharedSession,
}

impl Read for SessionReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let result = {
                let mut session = self.session.lock().expect("tls session poisoned");
                session.read(buf)
            };
            match result {
                Err(ref e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    thread::sleep(Duration::from_millis(1));
                }
                other => return other,
            }
        }
    }
}

/// Read a key file holding the 64 hex characters of one pod key. The key never
/// reaches an error message.
fn read_psk_file(path: &Path) -> Result<[u8; 32], String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("reading psk file {}: {e}", path.display()))?;
    parse_psk_hex(&path.display().to_string(), &text)
}

/// Connect and complete the TLS-PSK handshake. The socket carries a receive
/// timeout from the start (the drain depends on it), so a timed-out read during
/// the handshake is a retry, not a failure.
fn connect_session(connect: &str, ctx: &SslContext) -> Result<SharedSession, String> {
    let tcp = TcpStream::connect(connect).map_err(|e| e.to_string())?;
    // Paced per-frame writes must not be quantized by Nagle — the pacing is the
    // measurement. A failure here degrades that measurement silently, so warn.
    if let Err(e) = tcp.set_nodelay(true) {
        eprintln!(
            "replay-pod: warning: TCP_NODELAY not set on {connect}: {e}; \
             paced timings may be Nagle-quantized"
        );
    }
    tcp.set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| format!("set read timeout: {e}"))?;
    let session = Ssl::new(ctx).map_err(|e| format!("ssl session: {e}"))?;
    let mut stream = SslStream::new(session, tcp).map_err(|e| format!("ssl stream: {e}"))?;
    loop {
        match stream.connect() {
            Ok(()) => break,
            Err(e)
                if matches!(
                    e.io_error().map(std::io::Error::kind),
                    Some(std::io::ErrorKind::WouldBlock) | Some(std::io::ErrorKind::TimedOut)
                ) => {}
            Err(e) => return Err(format!("tls handshake: {e}")),
        }
    }
    Ok(Arc::new(Mutex::new(stream)))
}

/// Replay one frame log as a fake pod over its own TLS-PSK connection: each
/// record's payload is written to the session verbatim (no decode, no re-encode),
/// paced by `Pacer` in `realtime` mode. The connection is closed with a
/// close_notify and a `Write` shutdown.
fn replay_log(
    path: &Path,
    connect: &str,
    ctx: &SslContext,
    pace: Pace,
    max_gap: Option<Duration>,
    linger: Option<Duration>,
) -> LogReplay {
    let log_name = path.display().to_string();

    let reader = match FrameLogReader::open(path) {
        Ok(r) => r,
        Err(FrameLogError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            emit(
                "input_missing",
                json!({ "log": log_name, "detail": "pruned or never existed" }),
            );
            return LogReplay {
                outcome: LogOutcome::Missing,
                frames: 0,
                rx: PlaybackRx::default(),
                linger: None,
            };
        }
        Err(e) => {
            emit(
                "log_corrupt",
                json!({ "log": log_name, "detail": e.to_string() }),
            );
            return LogReplay {
                outcome: LogOutcome::Failed,
                frames: 0,
                rx: PlaybackRx::default(),
                linger: None,
            };
        }
    };

    let stream = match connect_session(connect, ctx) {
        Ok(s) => s,
        Err(detail) => {
            // Report on the structured channel like every other terminal
            // outcome, not stderr alone, so a JSONL-only consumer sees the abort.
            // A refused TCP connect and a refused handshake (unknown identity,
            // wrong key) are the same terminal outcome: no session, no replay.
            emit(
                "connect_refused",
                json!({ "log": log_name, "connect": connect, "detail": detail }),
            );
            eprintln!("replay-pod: cannot connect to {connect}: {detail}");
            return LogReplay {
                outcome: LogOutcome::ConnectRefused,
                frames: 0,
                rx: PlaybackRx::default(),
                linger: None,
            };
        }
    };
    let addr = stream
        .lock()
        .expect("tls session poisoned")
        .get_ref()
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| connect.to_string());
    emit(
        "replay_connected",
        json!({ "log": log_name, "addr": addr, "pace": pace_str(pace) }),
    );

    // Drain the daemon's server→device stream to EOF through a shared view of the
    // session, decoding and tallying frames by variant. A peer that never reads
    // would deadlock the daemon once it writes. The drain signals `linger_signal`
    // on a decoded `EndOfAudio` and on its own exit — the two release conditions
    // of the linger wait below.
    let linger_signal = Arc::new(LingerSignal::new());
    let drain = {
        let signal = Arc::clone(&linger_signal);
        let reader = SessionReader {
            session: Arc::clone(&stream),
        };
        Some(thread::spawn(move || drain_and_count(reader, signal)))
    };

    let mut pacer = Pacer::new(reader.meta().rolled_from.is_some(), max_gap);
    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut first_rx: Option<HostMicros> = None;
    let mut last_rx: Option<HostMicros> = None;
    let mut torn = false;
    let mut result = LogOutcome::Done;
    let start = Instant::now();

    for item in reader {
        match item {
            Ok(LogItem::Record { host_rx, payload }) => {
                // Realtime sleeps the wire delta between records; fast mode skips
                // pacing entirely.
                if pace == Pace::Realtime {
                    thread::sleep(pacer.delay_before(host_rx));
                }
                let sent = {
                    let mut session = stream.lock().expect("tls session poisoned");
                    session.write_all(&payload).and_then(|()| session.flush())
                };
                if let Err(e) = sent {
                    emit(
                        "replay_peer_closed",
                        json!({ "log": log_name, "frames_sent": frames, "detail": e.to_string() }),
                    );
                    result = LogOutcome::PeerClosed;
                    break;
                }
                frames += 1;
                bytes += payload.len() as u64;
                first_rx.get_or_insert(host_rx);
                last_rx = Some(host_rx);
            }
            Ok(LogItem::TornTail) => {
                torn = true;
                break;
            }
            Err(e) => {
                emit(
                    "log_corrupt",
                    json!({ "log": log_name, "detail": e.to_string() }),
                );
                result = LogOutcome::Failed;
                break;
            }
        }
    }

    // Sample send-loop wall time before teardown: `wall_us` measures pacing drift
    // against `capture_span_us`, so it must exclude the daemon's post-EOF
    // finalize/flush that the `join` below waits on.
    let wall = start.elapsed();

    // Linger: on a clean replay with `--linger-until-eoa` set, hold the write half
    // open past end-of-log until the daemon's playback `EndOfAudio` is observed (or
    // the drain closes, or the timeout elapses), so the daemon's abort-on-disconnect
    // teardown never beats the async pipeline to the utterance. Only the `Done` path
    // lingers; every other exit already knows its outcome and FINs immediately.
    let linger_report = match (linger, &result) {
        (Some(timeout), LogOutcome::Done) if drain.is_some() => {
            let wait_start = Instant::now();
            Some(LingerReport {
                outcome: linger_signal.wait(timeout),
                waited_ms: wait_start.elapsed().as_millis() as u64,
            })
        }
        // The linger path was reached on a clean replay, but the drain clone
        // failed at connect time — nothing can observe an `EndOfAudio`. Report
        // it (rather than leaving `None`) so this unverifiable connection folds
        // into the run-level result as a failure instead of silently dropping
        // out and letting the AND-fold pass vacuously.
        (Some(_), LogOutcome::Done) => Some(LingerReport {
            outcome: LingerOutcome::NoDrain,
            waited_ms: 0,
        }),
        _ => None,
    };

    // close_notify then FIN; the drain thread sees EOF and exits. Both are
    // ignored on an already-broken session (peer-closed path).
    {
        let mut session = stream.lock().expect("tls session poisoned");
        let _ = session.shutdown();
        let _ = session.get_ref().shutdown(Shutdown::Write);
    }
    // Join the drain and carry its per-connection frame tally out as the device-side
    // record of what actually crossed the wire back; `run_all` folds it into the
    // run's `replay_complete` report. A panicked drain must not read as a clean
    // all-zero tally (indistinguishable from "the daemon sent nothing", which is
    // exactly what the tally exists to disprove) — surface it loudly and keep the
    // default so the zeroed tally is at least explained.
    let rx = match drain {
        Some(h) => match h.join() {
            Ok(rx) => rx,
            Err(_) => {
                emit(
                    "drain_panicked",
                    json!({ "log": log_name, "detail": "playback drain thread panicked; tally is unreliable" }),
                );
                eprintln!(
                    "replay-pod: drain thread panicked on {log_name}; playback tally unreliable"
                );
                PlaybackRx::default()
            }
        },
        None => PlaybackRx::default(),
    };

    if matches!(result, LogOutcome::Done) {
        if torn {
            emit(
                "replay_torn_tail",
                json!({ "log": log_name, "frames": frames }),
            );
        }
        let capture_span_us = match (first_rx, last_rx) {
            (Some(f), Some(l)) => l.checked_delta(f).unwrap_or(0),
            _ => 0,
        };
        let mut done = json!({
            "log": log_name,
            "frames": frames,
            "bytes": bytes,
            "capture_span_us": capture_span_us,
            "wall_us": wall.as_micros() as u64,
            "pace": pace_str(pace),
        });
        if let Some(l) = &linger_report {
            done["linger"] = json!({
                "eoa_observed": l.eoa_observed(),
                "outcome": l.outcome.as_str(),
                "waited_ms": l.waited_ms,
            });
        }
        emit("replay_log_done", done);
    }
    LogReplay {
        outcome: result,
        frames,
        rx,
        linger: linger_report,
    }
}

/// Map one log's outcome to its exit-code contribution. Hard failures (unreadable
/// header, corrupt record, connect refused) are code 1; a peer close is 4; a
/// missing input is 3; a clean or torn-tail replay is 0.
fn outcome_code(outcome: &LogOutcome) -> u8 {
    match outcome {
        LogOutcome::Done => 0,
        LogOutcome::Missing => exit::MISSING_INPUT,
        LogOutcome::PeerClosed => exit::PEER_CLOSED,
        LogOutcome::Failed | LogOutcome::ConnectRefused => exit::HARD_FAILURE,
    }
}

/// Severity rank for exit-code aggregation — higher is worse. The code numbers
/// are not their own severity order, so the ranking is explicit. Unknown nonzero
/// codes rank below every known code; among themselves the lowest number wins.
fn severity(code: u8) -> (u8, std::cmp::Reverse<u8>) {
    let rank = match code {
        exit::HARD_FAILURE => 3,
        exit::PEER_CLOSED => 2,
        exit::MISSING_INPUT => 1,
        _ => 0,
    };
    (rank, std::cmp::Reverse(code))
}

/// Aggregate per-log codes into the run's exit code: the most severe nonzero
/// code wins (hard failure > peer-closed > missing input), matching the "worst
/// thing that happened" reading a script wants. All-clean → 0.
fn aggregate_exit_code(codes: impl IntoIterator<Item = u8>) -> u8 {
    codes
        .into_iter()
        .filter(|&c| c != 0)
        .max_by_key(|&c| severity(c))
        .unwrap_or(0)
}

/// Replay every log in argument order, returning the run's [`RunSummary`]. A
/// refused connect aborts the run — a down target fails every remaining log
/// identically, so the remaining logs are not attempted. `linger` is `Some`
/// whenever `--linger-until-eoa` was set, folding every lingered connection:
/// `eoa_observed` ANDs across them (false with none), `waited_ms` is the max.
/// Kept separate from `main` so its accumulation and early-abort are
/// unit-testable without `process::exit`.
fn run_all(
    framelogs: &[PathBuf],
    connect: &str,
    ctx: &SslContext,
    pace: Pace,
    max_gap: Option<Duration>,
    linger: Option<Duration>,
) -> RunSummary {
    let mut codes: Vec<u8> = Vec::new();
    let mut total_frames = 0u64;
    let mut rx = PlaybackRx::default();
    let mut logs = 0u64;
    let mut linger_any = false;
    let mut linger_eoa_and = true;
    let mut linger_max_ms = 0u64;

    for path in framelogs {
        let replay = replay_log(path, connect, ctx, pace, max_gap, linger);
        logs += 1;
        total_frames += replay.frames;
        rx.add(replay.rx);
        if let Some(l) = replay.linger {
            linger_any = true;
            linger_eoa_and &= l.eoa_observed();
            linger_max_ms = linger_max_ms.max(l.waited_ms);
        }
        codes.push(outcome_code(&replay.outcome));
        if matches!(replay.outcome, LogOutcome::ConnectRefused) {
            break;
        }
    }

    let run_linger = linger.map(|_| LingerRunReport {
        eoa_observed: linger_any && linger_eoa_and,
        waited_ms: linger_max_ms,
    });
    RunSummary {
        logs,
        frames: total_frames,
        rx,
        linger: run_linger,
        codes,
    }
}

fn main() {
    // clap exits 2 by default on a usage error; the tool's exit contract maps a
    // usage error to the hard-failure code. Help/version requests still exit 0.
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            let _ = e.print();
            let code = if e.use_stderr() {
                exit::HARD_FAILURE
            } else {
                0
            };
            std::process::exit(code as i32);
        }
    };
    let max_gap = cli.max_gap_ms.map(Duration::from_millis);
    let linger = cli
        .linger_until_eoa
        .then(|| Duration::from_millis(cli.linger_timeout_ms));

    // Key and context first: a bad key file or identity is a usage error, and
    // must not cost a connection attempt to discover.
    let ctx = match read_psk_file(&cli.psk_file).and_then(|key| client_context(&cli.pod_id, key)) {
        Ok(ctx) => ctx,
        Err(detail) => {
            emit("psk_unusable", json!({ "detail": detail }));
            eprintln!("replay-pod: {detail}");
            std::process::exit(exit::HARD_FAILURE as i32);
        }
    };

    let summary = run_all(
        &cli.framelogs,
        &cli.connect,
        &ctx,
        cli.pace,
        max_gap,
        linger,
    );

    let rx = summary.rx;
    let mut complete = json!({
        "logs": summary.logs,
        "frames": summary.frames,
        "playback_rx": {
            "hello": rx.hello,
            "audio": rx.audio,
            "end_of_audio": rx.end_of_audio,
            "other": rx.other,
            "decode_errors": rx.decode_errors,
        },
    });
    if let Some(l) = &summary.linger {
        complete["linger"] = json!({
            "eoa_observed": l.eoa_observed,
            "waited_ms": l.waited_ms,
        });
    }
    emit("replay_complete", complete);
    std::process::exit(aggregate_exit_code(summary.codes) as i32);
}

#[cfg(test)]
mod tests {
    use super::*;
    use audio_pipeline::wire::{
        AUDIO_PROTOCOL_VERSION, AudioFrame, ChannelSource, Codec, EndOfAudio, FlushPlayback, Hello,
        MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES, encode_frame,
    };
    use openssl::ssl::SslMethod;
    use pod_ingest::{FrameLogWriter, LogMeta};
    use std::net::TcpListener;

    /// The exact on-wire framing (`[u16 len][postcard]`) the drain decodes.
    fn frame_bytes(frame: &StreamFrame) -> Vec<u8> {
        let mut buf = [0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(frame, &mut buf).expect("encode");
        buf[..n].to_vec()
    }

    fn hello_frame() -> StreamFrame {
        StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::try_from("speech-surface").unwrap(),
            sample_rate_hz: 16_000,
            bits_per_sample: 16,
            channels: 1,
            codec: Codec::S16Le,
            channel_source: ChannelSource::CommunicationBeam,
        })
    }

    fn audio_frame() -> StreamFrame {
        let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
        for i in 0..640u32 {
            pcm.push(i as u8).unwrap();
        }
        StreamFrame::Audio(AudioFrame {
            segment_id: 0,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        })
    }

    fn write_framelog(path: &Path, rolled: bool, records: &[(u64, &[u8])]) {
        let mut w = FrameLogWriter::create(
            path,
            LogMeta {
                build_id: "replay-test".into(),
                created_epoch_us: HostMicros(1_700_000_000_000_000),
                conn_seq: 1,
                rolled_from: rolled.then(|| "prev.framelog".to_string()),
            },
        )
        .expect("create framelog");
        for (host_rx, payload) in records {
            w.append(HostMicros(*host_rx), payload).expect("append");
        }
        // Drop flushes the writer's buffer.
    }

    /// The key and identity every test in this module authenticates with. The
    /// stand-in daemons below hold the same key, so the handshake is real even
    /// though the fixture fleet is one pod.
    const TEST_PSK: [u8; 32] = [0x5a; 32];
    const TEST_POD_ID: &str = "pod-replay";

    /// The client context `replay_log` takes — what `main` builds from
    /// `--pod-id`/`--psk-file`.
    fn test_ctx() -> SslContext {
        client_context(TEST_POD_ID, TEST_PSK).expect("client context")
    }

    /// A stand-in daemon's TLS-PSK server context: same suite, same key.
    fn psk_server_context() -> SslContext {
        let mut builder = SslContext::builder(SslMethod::tls_server()).expect("server context");
        speech_surface::psk::pin_link_params(&mut builder).expect("tls parameters");
        builder.set_psk_server_callback(|_ssl, identity, secret| {
            assert_eq!(
                identity,
                Some(TEST_POD_ID.as_bytes()),
                "client presents its configured identity"
            );
            secret[..TEST_PSK.len()].copy_from_slice(&TEST_PSK);
            Ok(TEST_PSK.len())
        });
        builder.build()
    }

    /// Accept one connection and complete the server-side handshake — every
    /// stand-in daemon below starts here, because the tool has no plaintext mode.
    fn accept_tls(listener: &TcpListener) -> SslStream<TcpStream> {
        let (sock, _) = listener.accept().expect("accept");
        let ctx = psk_server_context();
        let mut stream =
            SslStream::new(Ssl::new(&ctx).expect("server ssl"), sock).expect("wrap socket");
        stream.accept().expect("server handshake");
        stream
    }

    #[test]
    fn replays_records_verbatim_to_a_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        let payloads: Vec<Vec<u8>> = vec![vec![1, 2, 3, 4], vec![5, 6], vec![7, 8, 9]];
        write_framelog(
            &path,
            false,
            &[(10, &payloads[0]), (20, &payloads[1]), (30, &payloads[2])],
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
            got
        });

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert!(matches!(replay.outcome, LogOutcome::Done));
        assert_eq!(replay.frames, 3, "all three records replay");

        let got = server.join().unwrap();
        assert_eq!(
            got,
            payloads.concat(),
            "payloads must arrive verbatim and concatenated"
        );
    }

    #[test]
    fn missing_input_returns_missing() {
        // Opens the reader first and never connects.
        let replay = replay_log(
            Path::new("/nonexistent/does-not-exist.framelog"),
            "127.0.0.1:9",
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert!(matches!(replay.outcome, LogOutcome::Missing));
        assert_eq!(replay.frames, 0, "no frames sent on a missing input");
    }

    #[test]
    fn connect_refused_returns_connect_refused() {
        // Bind then drop to obtain an address with nothing listening.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        write_framelog(&path, false, &[(1, &[1, 2, 3])]);

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert!(matches!(replay.outcome, LogOutcome::ConnectRefused));
        assert_eq!(replay.frames, 0, "no frames sent when connect is refused");
    }

    /// Accept one connection, drain it to EOF, and return the bytes received.
    fn accept_and_collect(listener: TcpListener) -> thread::JoinHandle<Vec<u8>> {
        thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
            got
        })
    }

    #[test]
    fn torn_tail_is_normal_completion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("torn.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);
        // Append a partial record header (fewer than the 10-byte header) so the
        // reader yields a terminal `TornTail` after the one good record.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0xAA, 0xBB, 0xCC])
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = accept_and_collect(listener);

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert!(
            matches!(replay.outcome, LogOutcome::Done),
            "a torn tail is normal completion, not a failure"
        );
        assert_eq!(
            server.join().unwrap(),
            vec![1, 2, 3, 4],
            "the readable record still replays before the torn tail"
        );
    }

    #[test]
    fn corrupt_length_is_hard_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);
        // Append a full record header claiming a zero-length payload — a corrupt
        // length the reader rejects mid-log.
        let mut extra = Vec::new();
        extra.extend_from_slice(&20u64.to_le_bytes()); // host_rx
        extra.extend_from_slice(&0u16.to_le_bytes()); // len = 0 → CorruptLength
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&extra)
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = accept_and_collect(listener);

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert!(
            matches!(replay.outcome, LogOutcome::Failed),
            "a corrupt record length is a hard failure"
        );
        assert_eq!(
            server.join().unwrap(),
            vec![1, 2, 3, 4],
            "records before the corruption still replay"
        );
    }

    #[test]
    fn first_record_is_zero() {
        let mut p = Pacer::new(false, None);
        assert_eq!(p.delay_before(HostMicros(5_000)), Duration::ZERO);
    }

    #[test]
    fn positive_delta_paces_by_delta() {
        let mut p = Pacer::new(false, None);
        p.delay_before(HostMicros(1_000_000));
        assert_eq!(
            p.delay_before(HostMicros(1_040_000)),
            Duration::from_micros(40_000)
        );
    }

    #[test]
    fn negative_delta_is_zero() {
        // NTP step backward mid-capture: pace as zero, never panic.
        let mut p = Pacer::new(false, None);
        p.delay_before(HostMicros(2_000_000));
        assert_eq!(p.delay_before(HostMicros(1_999_000)), Duration::ZERO);
    }

    #[test]
    fn max_gap_clamps_every_result() {
        let mut p = Pacer::new(false, Some(Duration::from_millis(10)));
        p.delay_before(HostMicros(0));
        // 50 ms wire gap clamps to the 10 ms cap.
        assert_eq!(
            p.delay_before(HostMicros(50_000)),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn rolled_log_suppresses_giant_second_delta() {
        // Rolled log: record 1 is the re-emitted `Hello` with the original
        // connection's early `host_rx`; record 2 is the first genuine
        // post-roll frame, minutes later on the log timeline. The giant delta
        // between them is a roll artifact and must not become a sleep. The true
        // wire delta resumes from record 3.
        let mut p = Pacer::new(true, None);
        assert_eq!(p.delay_before(HostMicros(1_000)), Duration::ZERO); // Hello
        assert_eq!(
            p.delay_before(HostMicros(600_000_000)), // ~10 min later
            Duration::ZERO
        );
        assert_eq!(
            p.delay_before(HostMicros(600_020_000)),
            Duration::from_micros(20_000)
        );
    }

    #[test]
    fn non_rolled_second_record_paces_normally() {
        // The rolled suppression must not leak into ordinary logs.
        let mut p = Pacer::new(false, None);
        p.delay_before(HostMicros(1_000));
        assert_eq!(
            p.delay_before(HostMicros(31_000)),
            Duration::from_micros(30_000)
        );
    }

    #[test]
    fn outcome_codes_match_the_contract() {
        assert_eq!(outcome_code(&LogOutcome::Done), 0);
        assert_eq!(outcome_code(&LogOutcome::Missing), exit::MISSING_INPUT);
        assert_eq!(outcome_code(&LogOutcome::PeerClosed), exit::PEER_CLOSED);
        assert_eq!(outcome_code(&LogOutcome::Failed), 1);
        assert_eq!(outcome_code(&LogOutcome::ConnectRefused), 1);
    }

    #[test]
    fn aggregate_all_clean_is_zero() {
        assert_eq!(aggregate_exit_code([0, 0, 0]), 0);
        assert_eq!(aggregate_exit_code([]), 0);
    }

    #[test]
    fn aggregate_severity_order_wins() {
        // A hard failure (1) dominates a missing input (3) and a peer close (4).
        assert_eq!(aggregate_exit_code([0, 4, 1, 3]), 1);
        assert_eq!(aggregate_exit_code([1, 4, 3]), 1);
        // With no hard failure, peer-closed (4) beats missing input (3).
        assert_eq!(aggregate_exit_code([0, 4, 3]), 4);
        // Lone codes pass through.
        assert_eq!(aggregate_exit_code([0, 4, 0]), 4);
        assert_eq!(aggregate_exit_code([0, 3, 0]), 3);
        // Unknown codes: lowest-numbered wins, and never outrank a known code.
        assert_eq!(aggregate_exit_code([7, 9]), 7);
        assert_eq!(aggregate_exit_code([7, 3]), 3);
    }

    #[test]
    fn run_all_accumulates_missing_inputs() {
        // Two absent inputs: both `Missing`, neither aborts the run, and the
        // per-log codes and counts accumulate across iterations.
        let summary = run_all(
            &[
                PathBuf::from("/nonexistent/a.framelog"),
                PathBuf::from("/nonexistent/b.framelog"),
            ],
            "127.0.0.1:9",
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert_eq!(summary.logs, 2);
        assert_eq!(summary.frames, 0);
        assert_eq!(
            summary.rx,
            PlaybackRx::default(),
            "no connection, no playback tally"
        );
        assert_eq!(
            summary.codes,
            vec![exit::MISSING_INPUT, exit::MISSING_INPUT]
        );
    }

    #[test]
    fn run_all_aborts_on_connect_refused() {
        // A down target aborts the run: the first log's refused connect breaks
        // the loop, so the second (present) log is never attempted.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.framelog");
        let b = dir.path().join("b.framelog");
        write_framelog(&a, false, &[(1, &[1, 2, 3])]);
        write_framelog(&b, false, &[(1, &[4, 5, 6])]);

        let summary = run_all(
            &[a, b],
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        assert_eq!(
            summary.logs, 1,
            "the run aborts after the first refused connect"
        );
        assert_eq!(summary.frames, 0);
        assert_eq!(summary.codes, vec![exit::HARD_FAILURE]);
    }

    #[test]
    fn run_all_accumulates_frames_across_logs() {
        // Two clean logs on two sequential connections: frame counts sum and
        // both contribute a 0 code.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.framelog");
        let b = dir.path().join("b.framelog");
        write_framelog(&a, false, &[(1, &[1, 2]), (2, &[3, 4])]);
        write_framelog(&b, false, &[(1, &[5]), (2, &[6]), (3, &[7])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            // One connection per log, in order; write one Hello back on each, then
            // drain to EOF — so the run-level `playback_rx` tally sums to two Hellos.
            for _ in 0..2 {
                let mut sock = accept_tls(&listener);
                sock.write_all(&frame_bytes(&hello_frame())).unwrap();
                let mut got = Vec::new();
                sock.read_to_end(&mut got).unwrap();
            }
        });

        let summary = run_all(
            &[a, b],
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        server.join().unwrap();
        assert_eq!(summary.logs, 2);
        assert_eq!(summary.frames, 5, "2 frames from log A + 3 from log B");
        assert_eq!(
            summary.rx.hello, 2,
            "one Hello per connection folds into the run total"
        );
        assert_eq!(summary.codes, vec![0, 0]);
    }

    #[test]
    fn cli_defaults_realtime_unclamped() {
        let cli = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "127.0.0.1:9",
            "a.framelog",
        ])
        .expect("parse");
        assert_eq!(cli.connect, "127.0.0.1:9");
        assert_eq!(cli.pace, Pace::Realtime);
        assert_eq!(cli.max_gap_ms, None);
        assert!(!cli.linger_until_eoa);
        assert_eq!(cli.linger_timeout_ms, 30000);
        assert_eq!(cli.framelogs, vec![PathBuf::from("a.framelog")]);
    }

    #[test]
    fn cli_linger_flag_parses() {
        let cli = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--linger-until-eoa",
            "--linger-timeout-ms",
            "500",
            "x.framelog",
        ])
        .expect("parse linger");
        assert!(cli.linger_until_eoa);
        assert_eq!(cli.linger_timeout_ms, 500);
    }

    #[test]
    fn cli_linger_timeout_zero_rejected() {
        let bad = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--linger-until-eoa",
            "--linger-timeout-ms",
            "0",
            "x.framelog",
        ]);
        assert!(bad.is_err(), "zero linger timeout must be rejected");
    }

    #[test]
    fn cli_linger_timeout_requires_linger_flag() {
        let bad = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--linger-timeout-ms",
            "500",
            "x.framelog",
        ]);
        assert!(
            bad.is_err(),
            "--linger-timeout-ms without --linger-until-eoa is a usage error"
        );
    }

    #[test]
    fn cli_pace_values_parse() {
        let realtime = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--pace",
            "realtime",
            "x.framelog",
        ])
        .expect("parse realtime");
        assert_eq!(realtime.pace, Pace::Realtime);

        let fast = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--pace",
            "fast",
            "x.framelog",
        ])
        .expect("parse fast");
        assert_eq!(fast.pace, Pace::Fast);

        let bad = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--pace",
            "slow",
            "x.framelog",
        ]);
        assert!(bad.is_err(), "unknown pace value must be rejected");
    }

    #[test]
    fn cli_connect_required() {
        let missing = Cli::try_parse_from(["replay-pod", "a.framelog"]);
        assert!(missing.is_err(), "--connect is required");
    }

    #[test]
    fn cli_at_least_one_framelog_required() {
        let none = Cli::try_parse_from(["replay-pod", "--connect", "h:1"]);
        assert!(none.is_err(), "at least one framelog is required");
    }

    #[test]
    fn drain_and_count_tallies_by_variant() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&frame_bytes(&hello_frame()));
        bytes.extend_from_slice(&frame_bytes(&audio_frame()));
        bytes.extend_from_slice(&frame_bytes(&audio_frame()));
        bytes.extend_from_slice(&frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {})));

        let rx = drain_and_count(std::io::Cursor::new(bytes), Arc::new(LingerSignal::new()));
        assert_eq!(
            rx,
            PlaybackRx {
                hello: 1,
                audio: 2,
                end_of_audio: 1,
                other: 0,
                decode_errors: 0,
            }
        );
    }

    #[test]
    fn consume_frames_holds_a_partial_frame_until_complete() {
        let full = frame_bytes(&hello_frame());
        let mut buf = full[..full.len() - 1].to_vec();
        let mut rx = PlaybackRx::default();
        consume_frames(&mut buf, &mut rx);
        assert_eq!(rx, PlaybackRx::default(), "a partial frame counts nothing");
        assert_eq!(buf.len(), full.len() - 1, "partial bytes are retained");

        buf.push(*full.last().unwrap());
        consume_frames(&mut buf, &mut rx);
        assert_eq!(rx.hello, 1, "the completed frame is counted");
        assert!(buf.is_empty(), "consumed bytes are drained");
    }

    #[test]
    fn drain_counts_undecodable_frame_and_stays_synced() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&frame_bytes(&hello_frame()));
        // A full-length frame (len = 1) whose single payload byte (0x63) is not a
        // valid `StreamFrame` discriminant: decodes to an error, but its declared
        // length keeps the parse aligned for the trailing valid frame.
        bytes.extend_from_slice(&[0x01, 0x00, 0x63]);
        bytes.extend_from_slice(&frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {})));

        let rx = drain_and_count(std::io::Cursor::new(bytes), Arc::new(LingerSignal::new()));
        assert_eq!(
            rx,
            PlaybackRx {
                hello: 1,
                audio: 0,
                end_of_audio: 1,
                other: 0,
                decode_errors: 1,
            }
        );
    }

    #[test]
    fn consume_frames_buckets_unexpected_variant_as_other() {
        let mut buf = frame_bytes(&StreamFrame::FlushPlayback(FlushPlayback {}));
        let mut rx = PlaybackRx::default();
        consume_frames(&mut buf, &mut rx);
        assert_eq!(
            rx.other, 1,
            "a FlushPlayback is not one of the three expected"
        );
        assert_eq!(rx.hello + rx.audio + rx.end_of_audio + rx.decode_errors, 0);
    }

    #[test]
    fn drain_reports_server_to_device_frames_over_socket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // The daemon stand-in writes the playback stream (Hello, two Audio,
        // EndOfAudio) back down the connection, then drains the client's replay to
        // EOF and closes so the client's drain sees EOF.
        let server = thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            sock.write_all(&frame_bytes(&hello_frame())).unwrap();
            sock.write_all(&frame_bytes(&audio_frame())).unwrap();
            sock.write_all(&frame_bytes(&audio_frame())).unwrap();
            sock.write_all(&frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {})))
                .unwrap();
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
        });

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            None,
        );
        server.join().unwrap();
        assert!(matches!(replay.outcome, LogOutcome::Done));
        assert_eq!(
            replay.rx,
            PlaybackRx {
                hello: 1,
                audio: 2,
                end_of_audio: 1,
                other: 0,
                decode_errors: 0,
            },
            "the drain decoded and tallied the server→device playback stream"
        );
    }

    #[test]
    fn cli_framelogs_collected_in_order() {
        let cli = Cli::try_parse_from([
            "replay-pod",
            "--pod-id",
            "pod-replay",
            "--psk-file",
            "/dev/null",
            "--connect",
            "h:1",
            "--max-gap-ms",
            "500",
            "first.framelog",
            "second.framelog",
            "third.framelog",
        ])
        .expect("parse");
        assert_eq!(cli.max_gap_ms, Some(500));
        assert_eq!(
            cli.framelogs,
            vec![
                PathBuf::from("first.framelog"),
                PathBuf::from("second.framelog"),
                PathBuf::from("third.framelog"),
            ]
        );
    }

    #[test]
    fn linger_releases_on_end_of_audio() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Daemon stand-in: write the playback stream (Hello, Audio, EndOfAudio) back,
        // then wait for the client's FIN — which the linger releases only on the
        // observed EndOfAudio.
        let server = thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            sock.write_all(&frame_bytes(&hello_frame())).unwrap();
            sock.write_all(&frame_bytes(&audio_frame())).unwrap();
            sock.write_all(&frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {})))
                .unwrap();
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
        });

        // Generous timeout: the release must come from the observed EndOfAudio, so
        // `outcome == Eoa` proves it did not fall through to the timeout.
        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            Some(Duration::from_secs(10)),
        );
        server.join().unwrap();
        assert!(matches!(replay.outcome, LogOutcome::Done));
        let linger = replay.linger.expect("flag set on a Done replay");
        assert_eq!(linger.outcome, LingerOutcome::Eoa);
        assert!(linger.eoa_observed());
        assert_eq!(
            replay.rx.end_of_audio, 1,
            "the drain decoded the EndOfAudio"
        );
    }

    #[test]
    fn linger_times_out_when_no_end_of_audio() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Daemon stand-in: never writes anything back and holds the connection open,
        // so neither an EndOfAudio nor a drain close ever releases the wait — only
        // the timeout can. `read_to_end` returns once the client FINs at the timeout.
        let server = thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
        });

        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            Some(Duration::from_millis(150)),
        );
        server.join().unwrap();
        assert!(
            matches!(replay.outcome, LogOutcome::Done),
            "a timed-out linger is still a clean replay"
        );
        let linger = replay.linger.expect("flag set on a Done replay");
        assert_eq!(linger.outcome, LingerOutcome::Timeout);
        assert!(!linger.eoa_observed());
    }

    #[test]
    fn linger_releases_when_drain_closes_without_eoa() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cap.framelog");
        write_framelog(&path, false, &[(10, &[1, 2, 3, 4])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Daemon stand-in: read the client's replay, then close the socket without
        // ever writing EndOfAudio. The client's drain sees EOF and exits, releasing
        // the wait promptly with `DrainClosed` — well under the generous timeout.
        let server = thread::spawn(move || {
            let mut sock = accept_tls(&listener);
            let mut got = [0u8; 64];
            let _ = sock.read(&mut got);
            // Dropping `sock` closes the connection.
        });

        let start = Instant::now();
        let replay = replay_log(
            &path,
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            Some(Duration::from_secs(10)),
        );
        let elapsed = start.elapsed();
        server.join().unwrap();
        assert!(matches!(replay.outcome, LogOutcome::Done));
        let linger = replay.linger.expect("flag set on a Done replay");
        assert_eq!(linger.outcome, LingerOutcome::DrainClosed);
        assert!(!linger.eoa_observed());
        assert!(
            elapsed < Duration::from_secs(5),
            "released on the drain close, not the timeout"
        );
    }

    #[test]
    fn run_all_linger_fold_is_and_across_connections() {
        // Two logs → two sequential connections. The first stand-in writes a full
        // playback stream ending in `EndOfAudio` (linger releases on `Eoa`); the
        // second reads the replay and closes without ever writing `EndOfAudio`
        // (the drain sees EOF, releasing as `DrainClosed`). The run-level fold
        // ANDs the two: one saw its `EndOfAudio`, one did not, so `eoa_observed`
        // is false. A `||` regression would wrongly report true — a slip no
        // single-connection test can catch.
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.framelog");
        let b = dir.path().join("b.framelog");
        write_framelog(&a, false, &[(1, &[1, 2, 3, 4])]);
        write_framelog(&b, false, &[(1, &[5, 6, 7, 8])]);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            // Connection A: full playback stream, then drain the client's replay
            // to EOF. The inner scope drops the socket once the client FINs, so
            // the client's drain thread sees EOF and `replay_log` can join it and
            // move on to connection B.
            {
                let mut ca = accept_tls(&listener);
                ca.write_all(&frame_bytes(&hello_frame())).unwrap();
                ca.write_all(&frame_bytes(&audio_frame())).unwrap();
                ca.write_all(&frame_bytes(&StreamFrame::EndOfAudio(EndOfAudio {})))
                    .unwrap();
                let mut got = Vec::new();
                ca.read_to_end(&mut got).unwrap();
            }
            // Connection B: read the replay, then close without any `EndOfAudio`
            // (the scope drop closes the socket → the client's drain sees EOF and
            // releases the linger as `DrainClosed`).
            {
                let mut cb = accept_tls(&listener);
                let mut buf = [0u8; 64];
                let _ = cb.read(&mut buf);
            }
        });

        // Generous timeout: both connections release on an observed wire event
        // (`Eoa` then `DrainClosed`), never the timeout.
        let summary = run_all(
            &[a, b],
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            Some(Duration::from_secs(10)),
        );
        server.join().unwrap();
        let linger = summary
            .linger
            .expect("run-level report present when the flag is set");
        assert!(
            !linger.eoa_observed,
            "AND-fold: one connection missed its EndOfAudio, so the run did not observe all"
        );
        assert!(
            linger.waited_ms < 10_000,
            "neither connection burned the timeout: {}",
            linger.waited_ms
        );
    }

    #[test]
    fn run_all_linger_reports_false_with_no_lingered_connections() {
        // Flag set, but the only target refuses the connect — no connection ever
        // reaches the lingering `Done` path. The run-level report must still be
        // present and report `eoa_observed: false`, never a vacuous `true` folded
        // over an empty set.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.framelog");
        write_framelog(&a, false, &[(1, &[1, 2, 3])]);

        let summary = run_all(
            &[a],
            &addr.to_string(),
            &test_ctx(),
            Pace::Fast,
            None,
            Some(Duration::from_millis(500)),
        );
        let linger = summary
            .linger
            .expect("run-level report present whenever the flag is set");
        assert!(
            !linger.eoa_observed,
            "zero lingered connections must not fold to a vacuous true"
        );
    }
}
