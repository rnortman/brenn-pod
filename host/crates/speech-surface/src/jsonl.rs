//! JSONL observability sink: the `{"ts_ms", "event", ...fields}` envelope and
//! the writer task that drains it to stdout or a file.
//!
//! Emit is non-blocking: [`JsonlHandle::emit`] serializes an envelope and
//! `try_send`s it onto a bounded channel; on a full (or closed) channel the
//! line is dropped and a counter bumped, so the data plane never blocks on
//! observability. The drop count is surfaced via [`JsonlHandle::dropped`] for a
//! later `stage_health` line. A single writer task owns the sink, coalescing a
//! burst into one flush; if the sink breaks it writes the reason to stderr (a
//! distinct fd, still alive when the JSONL sink is not) and stops the task
//! rather than aborting the process.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pod_ingest::HostMicros;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::JsonlSink;
use crate::console;

/// Bounded emit channel depth. A full channel drops the incoming line (the
/// newest, via `try_send`); the already-queued lines are untouched. The data
/// plane wins over observability, and drops are counted, not blocked on.
const CHANNEL_CAPACITY: usize = 1024;

/// Bounded console channel depth. Smaller than the file channel: the console
/// carries human-rate narration, not the full event stream.
const CONSOLE_CAPACITY: usize = 256;

/// Grace period for the console drain at shutdown. A stderr consumer that is not
/// reading (a terminal stopped with `^S`, a wedged pipe) would otherwise pend
/// the drain forever; after this the console task is aborted so the file drain —
/// the part that guards durable data — proceeds regardless.
const CONSOLE_DRAIN_GRACE: Duration = Duration::from_secs(5);

/// Cloneable emit handle feeding two sinks: the full JSONL event stream (a
/// file, stdout, or nowhere) and the human console. Cheap to clone (two mpsc
/// senders plus two `Arc` counters), so every task that needs to log holds one.
#[derive(Clone)]
pub struct JsonlHandle {
    /// `None` when the configured sink is [`JsonlSink::None`]: no event stream.
    file_tx: Option<mpsc::Sender<String>>,
    console_tx: mpsc::Sender<ConsoleMsg>,
    dropped: Arc<AtomicU64>,
    console_dropped: Arc<AtomicU64>,
}

/// One event handed to the console task: the shared stamp, the event name, and
/// the fields as a `Value` for the renderer's tolerant field access. Rendering
/// happens in the console task, not at the emit site, so the data plane never
/// pays formatting cost.
pub(crate) struct ConsoleMsg {
    ts_ms: u64,
    event: String,
    fields: Value,
}

/// The two writer tasks a [`spawn`] starts. Await [`SinkTasks::join`] at
/// shutdown to drain and flush both. Console is joined before file because the
/// console task holds a clone of the file sender (to leave a
/// `console_sink_failed` obituary in the durable log); joining file first would
/// deadlock on the never-closing channel.
pub struct SinkTasks {
    console: JoinHandle<()>,
    file: Option<JoinHandle<()>>,
}

impl SinkTasks {
    /// Drain and flush both sinks: console first, then file. An abnormal exit
    /// (a panicked writer) is surfaced on stderr rather than vanishing with a
    /// possibly-truncated log. The console drain is bounded: an unconsumable
    /// stderr must not hold the file flush hostage, so on timeout the console
    /// task is aborted (dropping its `file_tx` clone) and the file drain
    /// proceeds.
    pub async fn join(self) {
        let mut console = self.console;
        match tokio::time::timeout(CONSOLE_DRAIN_GRACE, &mut console).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("console sink task exited abnormally: {e}"),
            Err(_) => {
                console.abort();
                eprintln!("console sink drain timed out; aborting to protect the file flush");
            }
        }
        if let Some(file) = self.file {
            if let Err(e) = file.await {
                eprintln!("JSONL sink writer task exited abnormally: {e}");
            }
        }
    }
}

/// The wire envelope: `ts_ms` and `event` first, then the caller's fields
/// flattened in. `fields` must serialize to a JSON object.
#[derive(Serialize)]
struct Envelope<'a, T: Serialize> {
    ts_ms: u64,
    event: &'a str,
    #[serde(flatten)]
    fields: T,
}

/// Serialize one event into the `{"ts_ms", "event", ...fields}` envelope at a
/// caller-supplied `ts_ms`. The single owner of the wire envelope, split from
/// [`format_line`] so a caller feeding two sinks can stamp once and hand the
/// same `ts_ms` to both. A serialization failure yields a self-describing
/// `jsonl_encode_error` line (built through serde, since interpolating a serde
/// error into a hand-written literal could itself emit malformed JSON) so the
/// miss is visible rather than silent.
pub fn format_line_at<T: Serialize>(ts_ms: u64, event: &str, fields: &T) -> String {
    match serde_json::to_string(&Envelope {
        ts_ms,
        event,
        fields,
    }) {
        Ok(line) => line,
        Err(err) => serde_json::json!({
            "ts_ms": ts_ms,
            "event": "jsonl_encode_error",
            "target": event,
            "detail": err.to_string(),
        })
        .to_string(),
    }
}

/// Serialize one event into the envelope, stamping `ts_ms` from the current host
/// clock. The self-stamping entry point for callers that own one line: the
/// offline tools' [`emit_line`] and today's [`JsonlHandle::emit`].
pub fn format_line<T: Serialize>(event: &str, fields: &T) -> String {
    format_line_at(HostMicros::now().0 / 1000, event, fields)
}

/// Print one event as a JSONL line on stdout in the shared envelope, locking
/// stdout for the write. The synchronous offline tools (`segments-export`,
/// `replay-pod`) call this directly rather than through the async sink, so both
/// build the line via the one [`format_line`] and stay a single dialect. A write
/// error (the consumer closed the pipe, e.g. `… | head`) is ignored: the tools
/// have already flushed their real side effects.
pub fn emit_line(event: &str, fields: serde_json::Value) {
    use std::io::Write;
    let line = format_line(event, &fields);
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
}

impl JsonlHandle {
    /// Stamp `ts_ms` once, then tee the event to both sinks. Never blocks: each
    /// sink `try_send`s independently and drop-counts a full or closed channel.
    /// The file line is built exactly as before — same envelope, same struct
    /// serialization, no round-trip through `Value`. The console side pays only
    /// one string comparison for events it does not want (the high-rate
    /// `tracking` stream fails that filter before any serialization).
    pub fn emit<T: Serialize>(&self, event: &str, fields: &T) {
        let ts_ms = HostMicros::now().0 / 1000;

        if let Some(tx) = &self.file_tx {
            if tx.try_send(format_line_at(ts_ms, event, fields)).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        if console::wants(event) {
            // A `to_value` failure sends a synthetic loud message so the miss is
            // visible on the console, mirroring `format_line`'s fallback.
            let msg = match serde_json::to_value(fields) {
                Ok(fields) => ConsoleMsg {
                    ts_ms,
                    event: event.to_string(),
                    fields,
                },
                Err(err) => ConsoleMsg {
                    ts_ms,
                    event: "jsonl_encode_error".to_string(),
                    fields: serde_json::json!({ "target": event, "detail": err.to_string() }),
                },
            };
            if self.console_tx.try_send(msg).is_err() {
                self.console_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Number of file-stream events dropped so far (channel full or closed).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of console lines dropped so far (channel full or closed). Feeds the
    /// `stage_health` line so the counter-delta backstop can surface console
    /// overflow even when the dropped lines themselves are gone.
    pub fn console_dropped(&self) -> u64 {
        self.console_dropped.load(Ordering::Relaxed)
    }
}

/// Open the configured event-stream sink, spawn its writer task, and spawn the
/// console writer over `console_out`. Returns the emit handle and both writer
/// tasks: drop every handle, then await [`SinkTasks::join`] to drain and flush
/// at shutdown. A file sink that cannot be opened is a fatal startup error. The
/// console destination and its `color` flag are injected together so tests can
/// capture the output instead of writing to the process's real stderr; the
/// production binary passes `tokio::io::stderr()` and its `is_terminal()`.
pub async fn spawn<W: AsyncWrite + Send + Unpin + 'static>(
    sink: &JsonlSink,
    console_out: W,
    color: bool,
) -> io::Result<(JsonlHandle, SinkTasks)> {
    let (file_tx, file_join) = match sink {
        // No configured sink: the event stream is discarded, no writer task.
        JsonlSink::None => (None, None),
        JsonlSink::Stdout => {
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            (
                Some(tx),
                Some(tokio::spawn(run_writer(rx, tokio::io::stdout()))),
            )
        }
        JsonlSink::File(path) => {
            let mut opts = tokio::fs::OpenOptions::new();
            opts.create(true).append(true);
            // Owner-only at creation: the stream is a room-level speech-activity
            // timeline, not fit for a world-readable default. Pre-existing files
            // keep their own permissions.
            #[cfg(unix)]
            opts.mode(0o600);
            let file = opts.open(path).await?;
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            (Some(tx), Some(tokio::spawn(run_writer(rx, file))))
        }
    };

    let dropped = Arc::new(AtomicU64::new(0));
    let (console_tx, console_rx) = mpsc::channel(CONSOLE_CAPACITY);
    let renderer = console::Renderer::new(color);
    let console_join = tokio::spawn(run_console(
        console_rx,
        renderer,
        console_out,
        file_tx.clone(),
        dropped.clone(),
    ));

    let handle = JsonlHandle {
        file_tx,
        console_tx,
        dropped,
        console_dropped: Arc::new(AtomicU64::new(0)),
    };
    Ok((
        handle,
        SinkTasks {
            console: console_join,
            file: file_join,
        },
    ))
}

/// Quiet `spawn` for the crate's tests: discards the console to `tokio::io::sink`
/// so `cargo test` output stays clean, and collapses both writer tasks into one
/// `JoinHandle` so existing call sites keep their `(handle, join)` shape and
/// `join.await` drain.
#[cfg(test)]
pub async fn spawn_quiet(sink: &JsonlSink) -> io::Result<(JsonlHandle, JoinHandle<()>)> {
    let (handle, tasks) = spawn(sink, tokio::io::sink(), false).await?;
    Ok((handle, tokio::spawn(tasks.join())))
}

/// Drain the console channel, rendering each event and writing the resulting
/// lines with one flush per drained batch (mirrors [`run_writer`]). On a write
/// failure (stderr closed) it leaves one `console_sink_failed` event in the
/// durable stream via its `file_tx` clone, then stops; further console sends
/// then drop-count. The obituary needs a file sink to land: under the default
/// [`JsonlSink::None`] the console is the only surface, so its death has no
/// durable record (a stderr write to report it would hit the same broken fd). A
/// lost obituary — file channel closed or full — bumps `file_dropped` so a dead
/// console is never fully silent when a file sink exists.
async fn run_console<W: AsyncWrite + Unpin>(
    mut rx: mpsc::Receiver<ConsoleMsg>,
    mut renderer: console::Renderer,
    mut writer: W,
    file_tx: Option<mpsc::Sender<String>>,
    file_dropped: Arc<AtomicU64>,
) {
    let mut batch: Vec<String> = Vec::new();
    while let Some(first) = rx.recv().await {
        if let Some(line) = renderer.render(first.ts_ms, &first.event, &first.fields) {
            batch.push(line);
        }
        while let Ok(msg) = rx.try_recv() {
            if let Some(line) = renderer.render(msg.ts_ms, &msg.event, &msg.fields) {
                batch.push(line);
            }
        }
        if batch.is_empty() {
            continue;
        }
        let result = write_batch(&mut writer, &batch).await;
        batch.clear();
        if let Err(err) = result {
            if let Some(tx) = &file_tx {
                if tx
                    .try_send(format_line(
                        "console_sink_failed",
                        &serde_json::json!({ "detail": err.to_string() }),
                    ))
                    .is_err()
                {
                    file_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            return;
        }
    }
    if let Err(err) = writer.flush().await {
        eprintln!("console sink final flush failed: {err}");
    }
}

/// Drain the channel, writing each line and flushing once per drained batch so
/// a burst pays one flush syscall rather than one per line. A write error
/// (broken stdout/file) is reported to stderr and stops the task; further emits
/// then drop-count.
async fn run_writer<W: AsyncWrite + Unpin>(mut rx: mpsc::Receiver<String>, mut writer: W) {
    let mut batch: Vec<String> = Vec::new();
    while let Some(first) = rx.recv().await {
        batch.push(first);
        // Coalesce everything already queued into this flush.
        while let Ok(line) = rx.try_recv() {
            batch.push(line);
        }
        let result = write_batch(&mut writer, &batch).await;
        batch.clear();
        if let Err(err) = result {
            eprintln!("jsonl sink write failed, stopping writer task: {err}");
            return;
        }
    }
    if let Err(err) = writer.flush().await {
        eprintln!("jsonl sink final flush failed: {err}");
    }
}

/// Write every entry in `batch` (newline-terminated) and flush once. An entry
/// is one [`Renderer::render`] return, which is usually one line but may itself
/// be a newline-separated pair (the shutdown health summary + delta); it is
/// written verbatim, so those land as two terminated lines.
async fn write_batch<W: AsyncWrite + Unpin>(writer: &mut W, batch: &[String]) -> io::Result<()> {
    for line in batch {
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use std::sync::Mutex;

    /// An `AsyncWrite` that fails every write — drives the sink-error path.
    struct FailingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::other("sink broke")))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// An `AsyncWrite` that captures every byte, so a test can assert what the
    /// console task wrote without touching the process's real stderr.
    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl CaptureWriter {
        fn new() -> Self {
            CaptureWriter(Arc::new(Mutex::new(Vec::new())))
        }
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl AsyncWrite for CaptureWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Build a handle with live file and console channels; tests drive both
    /// receivers directly. Mirrors what `spawn` wires internally.
    fn channel() -> (
        JsonlHandle,
        mpsc::Receiver<String>,
        mpsc::Receiver<ConsoleMsg>,
    ) {
        let (file_tx, file_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (console_tx, console_rx) = mpsc::channel(CONSOLE_CAPACITY);
        (
            JsonlHandle {
                file_tx: Some(file_tx),
                console_tx,
                dropped: Arc::new(AtomicU64::new(0)),
                console_dropped: Arc::new(AtomicU64::new(0)),
            },
            file_rx,
            console_rx,
        )
    }

    #[test]
    fn envelope_carries_ts_event_and_flattened_fields() {
        let (handle, mut rx, _console) = channel();
        handle.emit(
            "segment_opened",
            &serde_json::json!({ "segment_id": 7, "is_resume": false }),
        );
        let line = rx.try_recv().expect("one queued line");
        let value: Value = serde_json::from_str(&line).expect("valid json");
        assert_eq!(value["event"], "segment_opened");
        assert!(value["ts_ms"].as_u64().is_some(), "ts_ms present: {value}");
        assert_eq!(value["segment_id"], 7);
        assert_eq!(value["is_resume"], false);
    }

    #[test]
    fn flattens_a_typed_struct() {
        #[derive(Serialize)]
        struct Fields<'a> {
            pod_id: &'a str,
            room: &'a str,
        }
        let (handle, mut rx, _console) = channel();
        handle.emit(
            "conn_hello",
            &Fields {
                pod_id: "pod-a1b2c3",
                room: "kitchen",
            },
        );
        let value: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(value["event"], "conn_hello");
        assert_eq!(value["pod_id"], "pod-a1b2c3");
        assert_eq!(value["room"], "kitchen");
    }

    #[test]
    fn format_line_at_stamps_the_given_ts() {
        // The shared-timestamp seam: whatever ts_ms the caller supplies lands in
        // the envelope verbatim, so two sinks fed one stamp correlate exactly.
        let line = format_line_at(1_700_000_000_123, "segment_opened", &serde_json::json!({}));
        let value: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["ts_ms"], 1_700_000_000_123u64);
        assert_eq!(value["event"], "segment_opened");
    }

    #[test]
    fn closed_channel_drops_and_counts() {
        let (handle, rx, _console) = channel();
        drop(rx);
        handle.emit("x", &serde_json::json!({}));
        handle.emit("y", &serde_json::json!({}));
        assert_eq!(handle.dropped(), 2);
    }

    #[test]
    fn overflow_drops_and_counts() {
        let (handle, _rx, _console) = channel();
        // Fill the channel exactly, then overflow it; the receiver never reads.
        for _ in 0..CHANNEL_CAPACITY {
            handle.emit("fill", &serde_json::json!({}));
        }
        assert_eq!(handle.dropped(), 0);
        handle.emit("overflow", &serde_json::json!({}));
        assert_eq!(handle.dropped(), 1);
    }

    #[test]
    fn encode_error_yields_visible_line() {
        // A bare (non-object) value cannot be `flatten`ed into the envelope.
        let (handle, mut rx, _console) = channel();
        handle.emit("bad", &serde_json::json!(42));
        let value: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(value["event"], "jsonl_encode_error");
        assert_eq!(value["target"], "bad");
    }

    #[tokio::test]
    async fn writer_stops_on_sink_error_without_hanging() {
        let (handle, rx, _console) = channel();
        handle.emit("boom", &serde_json::json!({ "n": 1 }));
        // The sender stays alive: run_writer must still return because the write
        // fails, not because the channel closed. Reaching the assert past the
        // await proves it did not hang or panic on the error path.
        run_writer(rx, FailingWriter).await;
        let _ = handle;
    }

    #[tokio::test]
    async fn spawn_file_in_missing_dir_is_error() {
        // The `?` after `OpenOptions::open` must propagate as a fatal startup
        // error when the directory does not exist.
        let sink = JsonlSink::File(PathBuf::from("/nonexistent-dir-xyz/events.jsonl"));
        assert!(spawn_quiet(&sink).await.is_err());
    }

    #[tokio::test]
    async fn file_sink_writes_lines_and_drains_at_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let sink = JsonlSink::File(path.clone());
        let (handle, join) = spawn_quiet(&sink).await.unwrap();
        handle.emit("first", &serde_json::json!({ "n": 1 }));
        handle.emit("second", &serde_json::json!({ "n": 2 }));
        drop(handle);
        join.await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["event"], "first");
        assert_eq!(first["n"], 1);
        let second: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["event"], "second");
        assert_eq!(second["n"], 2);
    }

    #[test]
    fn tee_routes_console_mapped_event_to_both_sinks() {
        let (handle, mut file_rx, mut console_rx) = channel();
        handle.emit("segment_opened", &serde_json::json!({ "segment_id": 7 }));
        let line = file_rx.try_recv().expect("file line");
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["event"],
            "segment_opened"
        );
        let msg = console_rx.try_recv().expect("console message");
        assert_eq!(msg.event, "segment_opened");
        assert_eq!(msg.fields["segment_id"], 7);
    }

    #[test]
    fn tracking_reaches_only_the_file_sink() {
        let (handle, mut file_rx, mut console_rx) = channel();
        handle.emit("tracking", &serde_json::json!({ "doa": [1, 2, 3] }));
        assert!(file_rx.try_recv().is_ok(), "file keeps full fidelity");
        assert!(
            console_rx.try_recv().is_err(),
            "console never sees high-rate tracking"
        );
    }

    #[tokio::test]
    async fn none_sink_has_no_file_channel_but_console_still_receives() {
        let (handle, tasks) = spawn(&JsonlSink::None, tokio::io::sink(), false)
            .await
            .unwrap();
        assert!(handle.file_tx.is_none(), "no event-stream sink");
        // A console-mapped event does not drop-count against either counter.
        handle.emit("segment_opened", &serde_json::json!({ "segment_id": 1 }));
        assert_eq!(handle.dropped(), 0);
        assert_eq!(handle.console_dropped.load(Ordering::Relaxed), 0);
        drop(handle);
        tasks.join().await;
    }

    #[test]
    fn console_channel_overflow_counts_independently() {
        let (handle, _file_rx, _console_rx) = channel();
        // The console channel (256) fills well before the file channel (1024),
        // so console_dropped moves while the file counter stays put.
        for _ in 0..CONSOLE_CAPACITY {
            handle.emit("segment_opened", &serde_json::json!({}));
        }
        assert_eq!(handle.console_dropped.load(Ordering::Relaxed), 0);
        assert_eq!(handle.dropped(), 0);
        handle.emit("segment_opened", &serde_json::json!({}));
        assert_eq!(handle.console_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(handle.dropped(), 0);
    }

    #[tokio::test]
    async fn console_task_writes_lines_and_drains_on_close() {
        let writer = CaptureWriter::new();
        let (tx, rx) = mpsc::channel(CONSOLE_CAPACITY);
        let join = tokio::spawn(run_console(
            rx,
            console::Renderer::new(false),
            writer.clone(),
            None,
            Arc::new(AtomicU64::new(0)),
        ));
        tx.try_send(ConsoleMsg {
            ts_ms: 0,
            event: "segment_opened".to_string(),
            fields: serde_json::json!({ "segment_id": 7 }),
        })
        .unwrap();
        drop(tx);
        join.await.unwrap();
        let out = writer.contents();
        assert!(out.contains("segment 7 opened"), "{out}");
        assert!(out.ends_with('\n'), "line terminated: {out:?}");
    }

    #[tokio::test]
    async fn console_write_failure_leaves_obituary_in_file_stream() {
        let (file_tx, mut file_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (tx, rx) = mpsc::channel(CONSOLE_CAPACITY);
        let join = tokio::spawn(run_console(
            rx,
            console::Renderer::new(false),
            FailingWriter,
            Some(file_tx),
            Arc::new(AtomicU64::new(0)),
        ));
        tx.try_send(ConsoleMsg {
            ts_ms: 0,
            event: "stt_failed".to_string(),
            fields: serde_json::json!({}),
        })
        .unwrap();
        drop(tx);
        join.await.unwrap();
        let line = file_rx.try_recv().expect("obituary line");
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["event"],
            "console_sink_failed"
        );
    }

    #[tokio::test]
    async fn console_write_failure_drop_counts_a_lost_obituary() {
        // File channel closed at the moment the console dies: the obituary cannot
        // land, so the loss must be counted rather than vanishing silently.
        let (file_tx, file_rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);
        drop(file_rx);
        let dropped = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel(CONSOLE_CAPACITY);
        let join = tokio::spawn(run_console(
            rx,
            console::Renderer::new(false),
            FailingWriter,
            Some(file_tx),
            dropped.clone(),
        ));
        tx.try_send(ConsoleMsg {
            ts_ms: 0,
            event: "stt_failed".to_string(),
            fields: serde_json::json!({}),
        })
        .unwrap();
        drop(tx);
        join.await.unwrap();
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn console_encode_failure_sends_visible_message() {
        // A value whose `Serialize` fails: the file line falls back through
        // `format_line_at`, and the console tee must independently surface the
        // miss as a loud `jsonl_encode_error` naming the original event.
        struct AlwaysErr;
        impl Serialize for AlwaysErr {
            fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("unserializable"))
            }
        }
        let (handle, _file, mut console_rx) = channel();
        handle.emit("stt_failed", &AlwaysErr);
        let msg = console_rx.try_recv().expect("console message");
        assert_eq!(msg.event, "jsonl_encode_error");
        assert_eq!(msg.fields["target"], "stt_failed");
    }

    #[tokio::test]
    async fn spawn_drains_both_sinks_in_order_without_hang() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let console = CaptureWriter::new();
        let (handle, tasks) = spawn(&JsonlSink::File(path.clone()), console.clone(), false)
            .await
            .unwrap();
        handle.emit("segment_opened", &serde_json::json!({ "segment_id": 7 }));
        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), tasks.join())
            .await
            .expect("drain completed without hanging");

        let file = std::fs::read_to_string(&path).unwrap();
        let file_has = file
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .any(|v| v["event"] == "segment_opened");
        assert!(file_has, "file drained the event: {file}");
        let out = console.contents();
        assert!(out.contains("segment 7 opened"), "console drained: {out}");
    }
}
