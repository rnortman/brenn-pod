//! `bridge-probe`: the operator's smoke tool for a pod's bus attachment.
//!
//! It does exactly what the crate does and nothing an application would do:
//! dial the configured server, subscribe the channels named on the command
//! line, print every attachment fact and delivery as a JSONL event line, and
//! publish whatever arrives on stdin. That makes it the answer to "is the token
//! right, is the ACL right, is anything actually coming down this pipe" without
//! a Brain in the way — and, because it drives the real library over the real
//! socket, a run of it is evidence about the bridge and not about a mock.
//!
//! ```text
//! bridge-probe --config /etc/brenn/bridge.toml \
//!     --subscribe-cursorless brenn:chat.app.home.roster=1/1 \
//!     --subscribe brenn:chat.app.home.out.42
//! ```
//!
//! Publishing is one JSON object per stdin line:
//!
//! ```text
//! {"channel":"brenn:chat.app.home.in.42","body":{"v":1,"cmd":"send","text":"hi"}}
//! ```
//!
//! A `body` given as a JSON string is published verbatim; any other JSON value
//! is published as its compact text, so a chat command needs no hand-escaping.
//! There is no attribution field: the remote route admits publishes from the
//! attacher itself only, and answers an attributed one by closing the socket,
//! so offering the knob could only earn an operator a protocol violation
//! against their own pod.
//!
//! Output is the `{"ts_ms", "event", ...}` line shape the rest of the host
//! workspace emits. Lines are written synchronously under a stdout
//! lock — this is a diagnostic tool whose output *is* its product, so a blocked
//! consumer should slow the probe rather than lose lines.
//!
//! Stderr carries the attachment stack's own `tracing` output at `warn` and
//! above, `RUST_LOG` overriding. That is where a failed dial's actual cause
//! lives — a refused connect, an unresolvable host, a TLS failure, and a
//! rejected upgrade with its HTTP status line all render as the same
//! `bridge_connect_failed` event, and only the log text tells them apart.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use brenn_bridge::{
    Bridge, BridgeEvent, BridgeHandle, BridgeOutcome, Config, DetachReason, GapReason,
    PublishError, PublishOutcome, PublishRequest, ResumePolicy, SubscriptionDepths, Urgency, exit,
};
use clap::Parser;
use pod_jsonl::{format_line_at, now_ms};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

/// Separator between a subscription's channel and its depths. Not `:`, which
/// every channel address already carries as its scheme separator.
const DEPTH_SEP: char = '=';

/// Separator between the two depths of one subscription spec.
const DEPTH_PAIR_SEP: char = '/';

#[derive(Parser)]
#[command(
    name = "bridge-probe",
    about = "Attach to a brenn bus, print what arrives, publish what stdin says"
)]
struct Cli {
    /// Bridge config file — the same TOML the daemon would be started with.
    #[arg(long)]
    config: PathBuf,
    /// Channel to hold, resuming from its held cursor across reconnects.
    /// `CHANNEL` or `CHANNEL=<push>/<retain>`; repeatable.
    #[arg(long = "subscribe", value_name = "SPEC")]
    subscribe: Vec<String>,
    /// As `--subscribe`, but presents no resume claim, so every attachment
    /// replays the retained window. What a channel carrying state — a roster —
    /// wants, since a resumed cursor would be answered with nothing and the
    /// state would never be re-applied. Repeatable.
    #[arg(long = "subscribe-cursorless", value_name = "SPEC")]
    subscribe_cursorless: Vec<String>,
    /// Push depth for a spec that names no depths.
    #[arg(long, default_value_t = 8)]
    push_depth: u64,
    /// Retain depth for a spec that names no depths.
    #[arg(long, default_value_t = 64)]
    retain_depth: u64,
    /// Shut down when stdin reaches EOF. Off by default: a probe started with
    /// stdin on `/dev/null` is a watcher, and exiting instantly would be a
    /// surprising answer to `--subscribe`.
    #[arg(long)]
    stop_on_eof: bool,
}

/// One channel this probe holds.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Subscription {
    channel: String,
    depths: SubscriptionDepths,
    resume: ResumePolicy,
}

/// One publish, as a stdin line states it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishLine {
    channel: String,
    /// A string is the body; anything else is published as its compact JSON
    /// text, so a structured command needs no escaping by hand.
    body: Value,
    #[serde(default = "normal_urgency")]
    urgency: Urgency,
}

fn normal_urgency() -> Urgency {
    Urgency::Normal
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing();
    match run(cli).await {
        Ok(code) => ExitCode::from(code),
        Err(detail) => {
            emit("probe_failed", json!({ "detail": detail }));
            ExitCode::from(exit::HARD_FAILURE)
        }
    }
}

/// Send the attachment stack's `tracing` output to stderr, so a dial's real
/// cause reaches the operator.
///
/// Stderr, not stdout: the JSONL event stream is this tool's product and stays
/// machine-readable. `warn` by default; `RUST_LOG` raises or lowers it.
fn install_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Start everything, then be the event pump until the bridge ends.
///
/// The pump is the foreground because it is what the probe is *for*, and
/// because the event channel closing is how the bridge announces that its task
/// is finishing — waiting on the task instead would race the last few lines out
/// of the run.
async fn run(cli: Cli) -> Result<u8, String> {
    let default_depths = SubscriptionDepths {
        push_depth: cli.push_depth,
        retain_depth: cli.retain_depth,
    };
    let mut subscriptions = Vec::new();
    for spec in &cli.subscribe {
        subscriptions.push(parse_subscription(
            spec,
            ResumePolicy::Resume,
            default_depths,
        )?);
    }
    for spec in &cli.subscribe_cursorless {
        subscriptions.push(parse_subscription(
            spec,
            ResumePolicy::Cursorless,
            default_depths,
        )?);
    }
    let subscriptions = fold_subscriptions(subscriptions)?;

    let config = Config::load(&cli.config).map_err(|err| err.to_string())?;
    let (bridge, handle, mut events) = Bridge::new(&config).map_err(|err| err.to_string())?;
    emit(
        "probe_started",
        json!({
            "config": cli.config.display().to_string(),
            "server_url": config.server_url,
            "subscriptions": subscriptions.len(),
            "stop_on_eof": cli.stop_on_eof,
        }),
    );

    let task = tokio::spawn(bridge.run());
    // Stated before anything is attached: the subscription plane holds them and
    // sends the whole set at each attachment, so there is no window to lose one
    // in and no attachment to wait for.
    for subscription in subscriptions {
        handle
            .subscribe(
                subscription.channel,
                subscription.depths,
                subscription.resume,
            )
            .await
            .map_err(|err| err.to_string())?;
    }
    tokio::spawn(read_stdin(handle.clone(), cli.stop_on_eof));
    tokio::spawn(watch_signal(handle));

    while let Some(event) = events.recv().await {
        let (name, fields) = render_event(&event);
        emit(name, fields);
    }
    let outcome = task
        .await
        .map_err(|err| format!("the bridge task did not finish: {err}"))?;
    let (name, fields) = render_outcome(&outcome);
    emit(name, fields);
    Ok(outcome.exit_code())
}

/// Read publish lines from stdin until it ends.
async fn read_stdin(handle: BridgeHandle, stop_on_eof: bool) {
    pump_publishes(
        BufReader::new(tokio::io::stdin()),
        handle,
        stop_on_eof,
        emit,
    )
    .await
}

/// Turn each line of `reader` into a publish, reporting through `sink`.
///
/// Each publish is awaited before the next line is read: an operator typing at
/// a terminal wants the answer under the line that produced it, and a scripted
/// feed of a few lines is not a throughput problem. Line numbers count blank
/// and malformed lines, so a reported number is the one an editor shows.
async fn pump_publishes<R, S>(reader: R, handle: BridgeHandle, stop_on_eof: bool, mut sink: S)
where
    R: tokio::io::AsyncBufRead + Unpin,
    S: FnMut(&str, Value),
{
    let mut lines = reader.lines();
    let mut number: u64 = 0;
    loop {
        number += 1;
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(err) => {
                sink("probe_stdin_failed", json!({ "detail": err.to_string() }));
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let request = match parse_publish_line(&line) {
            Ok(request) => request,
            Err(detail) => {
                sink(
                    "probe_stdin_invalid",
                    json!({ "line": number, "detail": detail }),
                );
                continue;
            }
        };
        let channel = request.channel.clone();
        match handle.publish(request).await {
            Ok(outcome) => sink("bridge_published", publish_fields(&channel, outcome)),
            Err(err) => {
                sink(
                    "bridge_publish_failed",
                    json!({ "channel": channel, "detail": err.to_string() }),
                );
                // The task is gone; every later line would answer the same, and
                // the run is ending anyway.
                if err == PublishError::Gone {
                    return;
                }
            }
        }
    }
    if stop_on_eof {
        sink("probe_stdin_eof", json!({}));
        let _ = handle.shutdown().await;
    }
}

/// Turn the first interrupt into an orderly shutdown, so the run ends with its
/// outcome line rather than with a signal; make a second one immediate.
///
/// The second is not a nicety. An orderly shutdown waits for the run loop, and
/// the run loop serves commands only between connect attempts — so a probe
/// pointed at a blackholed host takes up to `reconnect.connect_timeout_ms` to
/// notice. Installing this handler at all has also taken SIGINT's default
/// disposition away for the life of the process, so without a second listener a
/// reflexive second ctrl-C would do nothing at all and the only way out would be
/// SIGKILL.
async fn watch_signal(handle: BridgeHandle) {
    if tokio::signal::ctrl_c().await.is_err() {
        return;
    }
    emit("probe_interrupted", json!({}));
    let _ = handle.shutdown().await;
    if tokio::signal::ctrl_c().await.is_ok() {
        emit("probe_interrupt_forced", json!({}));
        // Lines are written under a stdout lock as they are produced, so there
        // is nothing buffered for this to lose.
        std::process::exit(i32::from(exit::HARD_FAILURE));
    }
}

/// Parse one `CHANNEL` or `CHANNEL=<push>/<retain>` spec.
fn parse_subscription(
    spec: &str,
    resume: ResumePolicy,
    default_depths: SubscriptionDepths,
) -> Result<Subscription, String> {
    let (channel, depths) = match spec.split_once(DEPTH_SEP) {
        None => (spec, default_depths),
        Some((channel, depths)) => {
            let Some((push, retain)) = depths.split_once(DEPTH_PAIR_SEP) else {
                return Err(format!(
                    "subscription {spec:?}: depths are written <push>{DEPTH_PAIR_SEP}<retain>"
                ));
            };
            let push_depth = push
                .parse::<u64>()
                .map_err(|err| format!("subscription {spec:?}: push depth {push:?}: {err}"))?;
            let retain_depth = retain
                .parse::<u64>()
                .map_err(|err| format!("subscription {spec:?}: retain depth {retain:?}: {err}"))?;
            (
                channel,
                SubscriptionDepths {
                    push_depth,
                    retain_depth,
                },
            )
        }
    };
    if channel.is_empty() {
        return Err(format!("subscription {spec:?} names no channel"));
    }
    // The subscription plane asserts on both of these rather than answering an
    // error, and the assert fires on the bridge task — so an operator typo would
    // surface as a panic backtrace and a join error instead of a refusal naming
    // the flag that caused it.
    if brenn_envelope::is_local_channel(channel) {
        return Err(format!(
            "subscription {spec:?}: a local: address is confined to this process and never \
             reaches the bus"
        ));
    }
    if depths.push_depth == 0 && depths.retain_depth == 0 {
        return Err(format!(
            "subscription {spec:?}: states no depth on either knob, so it asks for nothing; \
             give at least one of <push>{DEPTH_PAIR_SEP}<retain> a non-zero value"
        ));
    }
    Ok(Subscription {
        channel: channel.to_string(),
        depths,
        resume,
    })
}

/// Fold the command line's subscriptions into the set the probe will hold.
///
/// A channel named twice is fine when both statements agree — a repeated flag is
/// an easy thing to write — and a refusal when they do not: the plane asserts
/// that every acquisition of a live channel states the identical depths and
/// resume policy, and that assert fires on the bridge task.
fn fold_subscriptions(stated: Vec<Subscription>) -> Result<Vec<Subscription>, String> {
    let mut held: Vec<Subscription> = Vec::new();
    for subscription in stated {
        match held
            .iter()
            .find(|other| other.channel == subscription.channel)
        {
            None => held.push(subscription),
            Some(other) if *other == subscription => {}
            Some(other) => {
                return Err(format!(
                    "subscription {:?} is stated twice and differently: {}/{} {:?} against \
                     {}/{} {:?}",
                    subscription.channel,
                    other.depths.push_depth,
                    other.depths.retain_depth,
                    other.resume,
                    subscription.depths.push_depth,
                    subscription.depths.retain_depth,
                    subscription.resume,
                ));
            }
        }
    }
    Ok(held)
}

/// Parse one stdin line into the publish it states.
fn parse_publish_line(line: &str) -> Result<PublishRequest, String> {
    let parsed: PublishLine =
        serde_json::from_str(line).map_err(|err| format!("not a publish line: {err}"))?;
    if parsed.channel.is_empty() {
        return Err("channel is empty".to_string());
    }
    let body = match parsed.body {
        Value::String(text) => text,
        other => other.to_string(),
    };
    Ok(PublishRequest {
        channel: parsed.channel,
        attribution: None,
        body,
        urgency: parsed.urgency,
    })
}

/// The event name and fields one bridge event renders as.
fn render_event(event: &BridgeEvent) -> (&'static str, Value) {
    match event {
        BridgeEvent::Attached(facts) => (
            "bridge_attached",
            json!({
                "version": facts.version,
                "participant_id": facts.participant_id,
                "session_id": facts.session_id,
                "heartbeat_secs": facts.heartbeat_secs,
                "max_body_bytes": facts.max_body_bytes,
                "max_frame_bytes": facts.max_frame_bytes,
                "alert_granted": facts.alert_granted,
            }),
        ),
        BridgeEvent::Detached { reason } => ("bridge_detached", detach_fields(reason)),
        BridgeEvent::ConnectFailed { timed_out } => {
            ("bridge_connect_failed", json!({ "timed_out": timed_out }))
        }
        BridgeEvent::Subscribed {
            channel,
            replay_count,
            gap,
        } => (
            "bridge_subscribed",
            json!({
                "channel": channel,
                "replay_count": replay_count,
                "gap": gap.map(|gap| match gap.reason {
                    GapReason::EpochChanged => "epoch_changed",
                    GapReason::BeyondRetained => "beyond_retained",
                }),
            }),
        ),
        BridgeEvent::Unavailable { channel } => {
            ("bridge_unavailable", json!({ "channel": channel }))
        }
        // The envelope goes out whole: a probe that summarized it would be
        // lossy exactly when an operator is asking what the peer actually sent.
        BridgeEvent::Delivered(delivery) => (
            "bridge_delivered",
            json!({
                "channel": delivery.channel,
                "seq": delivery.seq,
                "dropped": delivery.dropped,
                "envelope": serde_json::to_value(&delivery.envelope)
                    .unwrap_or_else(|err| json!({ "render_error": err.to_string() })),
            }),
        ),
    }
}

fn detach_fields(reason: &DetachReason) -> Value {
    match reason {
        DetachReason::LivenessTimeout => json!({ "reason": "liveness_timeout" }),
        DetachReason::TransportClosed { code, reason } => json!({
            "reason": "transport_closed",
            "code": code,
            "detail": reason,
        }),
    }
}

/// The terminal line. `detail` is the outcome's own operator sentence; the rest
/// is what a script would branch on.
fn render_outcome(outcome: &BridgeOutcome) -> (&'static str, Value) {
    let mut fields = match outcome {
        BridgeOutcome::Closed => json!({ "cause": "closed" }),
        BridgeOutcome::EmbedderGone => json!({ "cause": "embedder_gone" }),
        BridgeOutcome::Fatal { .. } => json!({ "cause": "fatal" }),
        BridgeOutcome::Incompatible { ours, theirs } => json!({
            "cause": "incompatible",
            "ours": { "min": ours.min, "max": ours.max },
            "theirs": { "min": theirs.min, "max": theirs.max },
        }),
        BridgeOutcome::PeerClosedTerminal { code, .. } => json!({
            "cause": "peer_closed_terminal",
            "code": code,
        }),
        BridgeOutcome::Futile { attachments } => json!({
            "cause": "futile",
            "attachments": attachments,
        }),
    };
    if let Some(map) = fields.as_object_mut() {
        map.insert("detail".to_string(), json!(outcome.to_string()));
        map.insert("exit_code".to_string(), json!(outcome.exit_code()));
    }
    ("bridge_stopped", fields)
}

fn publish_fields(channel: &str, outcome: PublishOutcome) -> Value {
    match outcome {
        PublishOutcome::Ok => json!({ "channel": channel, "outcome": "ok" }),
        PublishOutcome::RateLimited => json!({ "channel": channel, "outcome": "rate_limited" }),
        PublishOutcome::BodyTooLarge { len, max } => json!({
            "channel": channel,
            "outcome": "body_too_large",
            "len": len,
            "max": max,
        }),
        PublishOutcome::Failed => json!({ "channel": channel, "outcome": "failed" }),
    }
}

/// Write one event line. A write failure is ignored: the usual cause is a
/// consumer that closed the pipe (`… | head`), and the probe's job is to keep
/// driving the attachment either way.
fn emit(event: &str, fields: Value) {
    let line = format_line_at(now_ms(), event, &fields);
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use brenn_bridge::{
        AttachmentFacts, Bridge, ConnConfig, Delivery, GapInfo, MessageEnvelope,
        TransportConnection, TransportConnector, TransportError, TransportEvent, VersionRange,
    };
    use tokio::task::JoinHandle;

    /// Long enough that reaching it means something is wedged, not slow.
    const WAIT: Duration = Duration::from_secs(10);

    /// A connector whose every dial fails. The bridge it drives is alive and
    /// answering commands but never attached — which is both the state a probe
    /// starts in and the one that makes a publish's answer deterministic.
    struct DeadConnector;

    struct NeverConnected;

    impl TransportConnection for NeverConnected {
        async fn send_text(&mut self, _text: String) -> Result<(), TransportError> {
            unreachable!("no connection is ever handed out")
        }
        async fn next_event(&mut self) -> TransportEvent {
            unreachable!("no connection is ever handed out")
        }
        async fn close(&mut self) {}
    }

    impl TransportConnector for DeadConnector {
        type Conn = NeverConnected;
        async fn connect(&mut self, _url: &str) -> Result<NeverConnected, TransportError> {
            Err(TransportError::new("bridge-probe test: no peer"))
        }
    }

    /// A peer that completes the handshake and answers every publish `Ok`, so
    /// the probe's success path — stdin line to `bridge_published` — runs
    /// against a live attachment rather than only against a refusal.
    struct AnsweringConnector;

    struct Answering {
        inbound: tokio::sync::mpsc::UnboundedSender<TransportEvent>,
        outbound: tokio::sync::mpsc::UnboundedReceiver<TransportEvent>,
    }

    impl Answering {
        fn say(&self, frame: Value) {
            self.inbound
                .send(TransportEvent::Text(frame.to_string()))
                .expect("this connection still owns its own receiver");
        }
    }

    impl TransportConnector for AnsweringConnector {
        type Conn = Answering;
        async fn connect(&mut self, _url: &str) -> Result<Answering, TransportError> {
            let (inbound, outbound) = tokio::sync::mpsc::unbounded_channel();
            Ok(Answering { inbound, outbound })
        }
    }

    impl TransportConnection for Answering {
        async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
            let frame: Value =
                serde_json::from_str(&text).expect("the bridge writes parseable frames");
            match frame["type"].as_str() {
                Some("Hello") => {
                    self.say(json!({
                        "type": "Hello",
                        "versions": {
                            "min": brenn_attach_proto::SUPPORTED_VERSIONS.min,
                            "max": brenn_attach_proto::SUPPORTED_VERSIONS.max,
                        },
                        "ident": "answering-peer",
                    }));
                    self.say(json!({
                        "type": "Welcome",
                        "version": brenn_attach_proto::SUPPORTED_VERSIONS.max,
                        "participant_id": "remote:pod-kitchen",
                        "session_id": "sess-1",
                        "heartbeat_secs": 20,
                        "max_body_bytes": 65_536,
                        "max_frame_bytes": 532_480,
                        "alert_granted": true,
                    }));
                }
                Some("Publish") => self.say(json!({
                    "type": "PublishResult",
                    "correlation": frame["correlation"],
                    "outcome": {"kind": "Ok"},
                })),
                _ => {}
            }
            Ok(())
        }

        async fn next_event(&mut self) -> TransportEvent {
            match self.outbound.recv().await {
                Some(event) => event,
                None => TransportEvent::Failed("the answering peer is gone".to_string()),
            }
        }

        async fn close(&mut self) {}
    }

    fn test_conn() -> ConnConfig {
        ConnConfig {
            url: "wss://peer.example.net/remote/pod-kitchen/ws".to_string(),
            ident: "bridge-probe/test".to_string(),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(5),
            liveness_multiplier: 3,
            backoff_jitter_seed: 0,
            terminal_close_code: None,
        }
    }

    /// A bridge already attached to a peer that answers, plus the event stream
    /// the caller must keep alive — dropping it winds the bridge down.
    async fn attached_bridge() -> (
        BridgeHandle,
        tokio::sync::mpsc::Receiver<BridgeEvent>,
        JoinHandle<BridgeOutcome>,
    ) {
        let (bridge, handle, mut events) =
            Bridge::with_connector(test_conn(), 3, AnsweringConnector);
        let task = tokio::spawn(bridge.run());
        match tokio::time::timeout(WAIT, events.recv())
            .await
            .expect("the attachment came up before the timeout")
            .expect("the bridge is still running")
        {
            BridgeEvent::Attached(_) => {}
            other => panic!("expected an attachment, got {other:?}"),
        }
        (handle, events, task)
    }

    /// A running bridge that will never attach, and its task.
    fn detached_bridge() -> (BridgeHandle, JoinHandle<BridgeOutcome>) {
        let (bridge, handle, events) = Bridge::with_connector(test_conn(), 3, DeadConnector);
        // The receiver rides along: dropping it would end the run as
        // `EmbedderGone` before the test said anything.
        let task = tokio::spawn(async move {
            let _events = events;
            bridge.run().await
        });
        (handle, task)
    }

    async fn joined(task: JoinHandle<BridgeOutcome>) -> BridgeOutcome {
        tokio::time::timeout(WAIT, task)
            .await
            .expect("the task ended before the timeout")
            .expect("the task joins")
    }

    fn defaults() -> SubscriptionDepths {
        SubscriptionDepths {
            push_depth: 8,
            retain_depth: 64,
        }
    }

    fn parsed(line: &str) -> Value {
        serde_json::from_str(line).expect("the emitted line is JSON")
    }

    #[test]
    fn a_bare_spec_takes_the_default_depths() {
        let subscription = parse_subscription(
            "brenn:chat.app.home.roster",
            ResumePolicy::Resume,
            defaults(),
        )
        .unwrap();
        assert_eq!(
            subscription,
            Subscription {
                channel: "brenn:chat.app.home.roster".to_string(),
                depths: defaults(),
                resume: ResumePolicy::Resume,
            }
        );
    }

    #[test]
    fn a_spec_may_state_its_own_depths() {
        let subscription = parse_subscription(
            "brenn:chat.app.home.roster=1/2",
            ResumePolicy::Cursorless,
            defaults(),
        )
        .unwrap();
        assert_eq!(subscription.channel, "brenn:chat.app.home.roster");
        assert_eq!(
            subscription.depths,
            SubscriptionDepths {
                push_depth: 1,
                retain_depth: 2,
            }
        );
        assert_eq!(subscription.resume, ResumePolicy::Cursorless);
    }

    #[test]
    fn a_scheme_separator_is_not_a_depth_separator() {
        // The whole reason the depth separator is not `:`: every address
        // carries one already, and splitting on it would eat the scheme.
        let subscription = parse_subscription(
            "ephemeral:chat.app.home.stream.42",
            ResumePolicy::Resume,
            defaults(),
        )
        .unwrap();
        assert_eq!(subscription.channel, "ephemeral:chat.app.home.stream.42");
    }

    #[test]
    fn a_half_stated_depth_pair_is_refused() {
        let err = parse_subscription("brenn:x=4", ResumePolicy::Resume, defaults()).unwrap_err();
        assert!(err.contains("<push>/<retain>"), "{err}");
    }

    #[test]
    fn an_unparseable_depth_is_refused_by_name() {
        let err =
            parse_subscription("brenn:x=4/lots", ResumePolicy::Resume, defaults()).unwrap_err();
        assert!(err.contains("retain depth"), "{err}");
        assert!(err.contains("lots"), "{err}");
    }

    #[test]
    fn a_channelless_spec_is_refused() {
        let err = parse_subscription("=1/1", ResumePolicy::Resume, defaults()).unwrap_err();
        assert!(err.contains("names no channel"), "{err}");
    }

    #[test]
    fn a_confined_address_is_refused_at_the_flag() {
        // The plane asserts on it, and the assert fires on the bridge task — so
        // without this an operator typo is a panic backtrace and a join error
        // rather than a refusal naming the flag.
        let err =
            parse_subscription("local:anything", ResumePolicy::Resume, defaults()).unwrap_err();
        assert!(err.contains("local:"), "{err}");
        assert!(err.contains("never reaches the bus"), "{err}");
    }

    #[test]
    fn a_subscription_that_states_no_depth_is_refused() {
        let stated =
            parse_subscription("brenn:x=0/0", ResumePolicy::Resume, defaults()).unwrap_err();
        assert!(stated.contains("no depth"), "{stated}");

        // And the same pair reached through the defaults, which is the shape
        // `--push-depth 0 --retain-depth 0` produces for every bare spec.
        let zero = SubscriptionDepths {
            push_depth: 0,
            retain_depth: 0,
        };
        let defaulted = parse_subscription("brenn:x", ResumePolicy::Resume, zero).unwrap_err();
        assert!(defaulted.contains("no depth"), "{defaulted}");

        // One knob is enough: a pull-only subscription is legal.
        parse_subscription(
            "brenn:x=0/1",
            ResumePolicy::Resume,
            SubscriptionDepths {
                push_depth: 8,
                retain_depth: 64,
            },
        )
        .expect("one non-zero depth is a subscription");
    }

    #[test]
    fn one_channel_stated_twice_the_same_way_is_held_once() {
        let spec = || {
            parse_subscription("brenn:x=1/1", ResumePolicy::Resume, defaults())
                .expect("the fixture parses")
        };
        let held = fold_subscriptions(vec![spec(), spec()]).expect("agreeing statements fold");
        assert_eq!(held, vec![spec()]);
    }

    #[test]
    fn one_channel_stated_twice_differently_is_refused() {
        // The plane asserts that every acquisition of a live channel states the
        // identical depths and resume policy; a second, differing statement
        // panics the bridge task.
        let depths = fold_subscriptions(vec![
            parse_subscription("brenn:x=1/1", ResumePolicy::Resume, defaults()).unwrap(),
            parse_subscription("brenn:x=2/2", ResumePolicy::Resume, defaults()).unwrap(),
        ])
        .expect_err("conflicting depths are refused");
        assert!(depths.contains("stated twice and differently"), "{depths}");

        // Same depths, different resume policy — the shape `--subscribe` and
        // `--subscribe-cursorless` naming one channel produces.
        let resume = fold_subscriptions(vec![
            parse_subscription("brenn:x", ResumePolicy::Resume, defaults()).unwrap(),
            parse_subscription("brenn:x", ResumePolicy::Cursorless, defaults()).unwrap(),
        ])
        .expect_err("conflicting resume policies are refused");
        assert!(resume.contains("brenn:x"), "{resume}");
    }

    #[test]
    fn a_minimal_publish_line_is_unattributed_and_normal() {
        let request =
            parse_publish_line(r#"{"channel":"brenn:chat.app.home.in.42","body":"hi"}"#).unwrap();
        assert_eq!(
            request,
            PublishRequest {
                channel: "brenn:chat.app.home.in.42".to_string(),
                attribution: None,
                body: "hi".to_string(),
                urgency: Urgency::Normal,
            }
        );
    }

    #[test]
    fn a_structured_body_is_published_as_its_compact_text() {
        let request =
            parse_publish_line(r#"{"channel":"brenn:x","body":{"v":1,"cmd":"send"}}"#).unwrap();
        assert_eq!(request.body, r#"{"cmd":"send","v":1}"#);
    }

    #[test]
    fn urgency_is_read_in_the_wire_spelling() {
        let request =
            parse_publish_line(r#"{"channel":"brenn:x","body":"hi","urgency":"very-low"}"#)
                .unwrap();
        assert_eq!(request.urgency, Urgency::VeryLow);
    }

    #[test]
    fn an_attribution_is_not_a_field_this_probe_offers() {
        // Not merely unread — refused, so an operator learns it at the line
        // rather than by watching the socket close.
        let err = parse_publish_line(r#"{"channel":"brenn:x","body":"hi","attribution":"pod"}"#)
            .unwrap_err();
        assert!(err.contains("attribution"), "{err}");
    }

    #[test]
    fn a_channelless_publish_line_is_refused() {
        let err = parse_publish_line(r#"{"channel":"","body":"hi"}"#).unwrap_err();
        assert!(err.contains("channel is empty"), "{err}");
    }

    #[test]
    fn a_line_that_is_not_json_is_refused_without_panicking() {
        let err = parse_publish_line("brenn:x hello").unwrap_err();
        assert!(err.contains("not a publish line"), "{err}");
    }

    #[test]
    fn a_rendered_event_composes_into_the_workspace_envelope() {
        // The envelope itself is `pod-jsonl`'s, tested there; what this pins is
        // that a renderer's fields flatten into it rather than nesting.
        let (name, fields) = render_event(&BridgeEvent::Unavailable {
            channel: "brenn:chat.app.home.out.42".to_string(),
        });
        let line = parsed(&format_line_at(1_700_000_000_123, name, &fields));
        assert_eq!(line["ts_ms"], 1_700_000_000_123u64);
        assert_eq!(line["event"], "bridge_unavailable");
        assert_eq!(line["channel"], "brenn:chat.app.home.out.42");
    }

    #[test]
    fn a_failed_dial_renders_whether_it_timed_out() {
        let (name, fields) = render_event(&BridgeEvent::ConnectFailed { timed_out: true });
        assert_eq!(name, "bridge_connect_failed");
        assert_eq!(fields["timed_out"], true);
        assert_eq!(
            render_event(&BridgeEvent::ConnectFailed { timed_out: false }).1["timed_out"],
            false
        );
    }

    #[test]
    fn attach_facts_render_whole() {
        let (name, fields) = render_event(&BridgeEvent::Attached(AttachmentFacts {
            version: 3,
            participant_id: "remote:pod-kitchen".to_string(),
            session_id: "s-1".to_string(),
            heartbeat_secs: 20,
            max_body_bytes: 65_536,
            max_frame_bytes: 520_192,
            alert_granted: true,
        }));
        assert_eq!(name, "bridge_attached");
        assert_eq!(fields["participant_id"], "remote:pod-kitchen");
        assert_eq!(fields["version"], 3);
        assert_eq!(fields["max_body_bytes"], 65_536);
        assert_eq!(fields["alert_granted"], true);
    }

    #[test]
    fn a_gapless_subscribe_renders_a_null_gap() {
        let (name, fields) = render_event(&BridgeEvent::Subscribed {
            channel: "brenn:x".to_string(),
            replay_count: 3,
            gap: None,
        });
        assert_eq!(name, "bridge_subscribed");
        assert_eq!(fields["replay_count"], 3);
        assert!(fields["gap"].is_null(), "{fields}");
    }

    #[test]
    fn a_gap_renders_its_reason() {
        let (_, fields) = render_event(&BridgeEvent::Subscribed {
            channel: "brenn:x".to_string(),
            replay_count: 0,
            gap: Some(GapInfo {
                reason: GapReason::BeyondRetained,
            }),
        });
        assert_eq!(fields["gap"], "beyond_retained");
    }

    #[test]
    fn a_transport_close_renders_its_code_and_text() {
        let (name, fields) = render_event(&BridgeEvent::Detached {
            reason: DetachReason::TransportClosed {
                code: Some(1011),
                reason: "server error".to_string(),
            },
        });
        assert_eq!(name, "bridge_detached");
        assert_eq!(fields["reason"], "transport_closed");
        assert_eq!(fields["code"], 1011);
        assert_eq!(fields["detail"], "server error");
    }

    #[test]
    fn a_liveness_timeout_renders_no_code() {
        let (_, fields) = render_event(&BridgeEvent::Detached {
            reason: DetachReason::LivenessTimeout,
        });
        assert_eq!(fields["reason"], "liveness_timeout");
        assert!(fields.get("code").is_none(), "{fields}");
    }

    #[test]
    fn a_delivery_renders_the_whole_envelope_beside_its_wire_facts() {
        let envelope: MessageEnvelope = serde_json::from_value(json!({
            "message_id": "11111111-2222-3333-4444-555555555555",
            "source": "bus",
            "channel": "brenn:chat.app.home.out.42",
            "sender": "app:home",
            "publish_ts": "2026-08-02T00:00:00Z",
            "body": "{\"v\":1}",
            "urgency": "normal",
            "envelope_type": "brenn",
        }))
        .expect("envelope fixture");
        let (name, fields) = render_event(&BridgeEvent::Delivered(Delivery {
            channel: "brenn:chat.app.home.out.42".to_string(),
            envelope,
            seq: 7,
            dropped: 2,
        }));
        assert_eq!(name, "bridge_delivered");
        assert_eq!(fields["channel"], "brenn:chat.app.home.out.42");
        assert_eq!(fields["seq"], 7);
        assert_eq!(fields["dropped"], 2);
        assert_eq!(fields["envelope"]["sender"], "app:home");
        assert_eq!(fields["envelope"]["body"], "{\"v\":1}");
    }

    #[test]
    fn an_incompatible_outcome_names_both_ranges_and_its_exit_code() {
        let outcome = BridgeOutcome::Incompatible {
            ours: VersionRange { min: 3, max: 3 },
            theirs: VersionRange { min: 4, max: 5 },
        };
        let (name, fields) = render_outcome(&outcome);
        assert_eq!(name, "bridge_stopped");
        assert_eq!(fields["cause"], "incompatible");
        assert_eq!(fields["ours"]["max"], 3);
        assert_eq!(fields["theirs"]["min"], 4);
        assert_eq!(fields["exit_code"], exit::VERSION_INCOMPATIBLE);
        assert!(
            fields["detail"]
                .as_str()
                .expect("a detail sentence")
                .contains("no wire version in common"),
            "{fields}"
        );
    }

    #[test]
    fn an_orderly_shutdown_renders_exit_zero() {
        let (_, fields) = render_outcome(&BridgeOutcome::Closed);
        assert_eq!(fields["cause"], "closed");
        assert_eq!(fields["exit_code"], 0);
    }

    #[test]
    fn a_terminal_close_renders_its_code() {
        // `bridge_stopped` is the line an operator greps after a bridge dies,
        // and this arm is the only place the close code is surfaced.
        let (_, fields) = render_outcome(&BridgeOutcome::PeerClosedTerminal {
            code: 4001,
            reason: "this build is not welcome here".to_string(),
        });
        assert_eq!(fields["cause"], "peer_closed_terminal");
        assert_eq!(fields["code"], 4001);
        assert_eq!(fields["exit_code"], exit::HARD_FAILURE);
        assert!(
            fields["detail"]
                .as_str()
                .expect("a detail sentence")
                .contains("4001"),
            "{fields}"
        );
    }

    #[test]
    fn a_protocol_error_renders_as_a_hard_failure_naming_itself() {
        // The "better dead than wrong" exit the whole design turns on.
        let (_, fields) = render_outcome(&BridgeOutcome::Fatal {
            detail: "a deferred view on brenn:x for a bridge that parks nothing".to_string(),
        });
        assert_eq!(fields["cause"], "fatal");
        assert_eq!(fields["exit_code"], exit::HARD_FAILURE);
        assert!(
            fields["detail"]
                .as_str()
                .expect("a detail sentence")
                .contains("deferred view"),
            "{fields}"
        );
    }

    #[test]
    fn an_embedder_that_went_away_renders_exit_zero() {
        let (_, fields) = render_outcome(&BridgeOutcome::EmbedderGone);
        assert_eq!(fields["cause"], "embedder_gone");
        assert_eq!(fields["exit_code"], 0);
    }

    #[test]
    fn a_futile_run_renders_its_count_and_the_hard_failure_code() {
        let (_, fields) = render_outcome(&BridgeOutcome::Futile { attachments: 3 });
        assert_eq!(fields["cause"], "futile");
        assert_eq!(fields["attachments"], 3);
        assert_eq!(fields["exit_code"], exit::HARD_FAILURE);
    }

    #[test]
    fn an_oversize_body_renders_both_numbers() {
        let fields = publish_fields(
            "brenn:x",
            PublishOutcome::BodyTooLarge { len: 100, max: 64 },
        );
        assert_eq!(fields["outcome"], "body_too_large");
        assert_eq!(fields["len"], 100);
        assert_eq!(fields["max"], 64);
    }

    #[test]
    fn a_refused_publish_renders_its_own_outcome_name() {
        assert_eq!(
            publish_fields("brenn:x", PublishOutcome::RateLimited)["outcome"],
            "rate_limited"
        );
        assert_eq!(
            publish_fields("brenn:x", PublishOutcome::Failed)["outcome"],
            "failed"
        );
    }

    #[tokio::test]
    async fn a_stdin_line_is_carried_to_the_bridge_and_its_answer_reported() {
        let (handle, task) = detached_bridge();
        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(
            br#"{"channel":"brenn:chat.app.home.in.42","body":"hi"}"#.as_slice(),
            handle.clone(),
            false,
            |event, fields| seen.push((event.to_string(), fields)),
        )
        .await;

        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].0, "bridge_publish_failed");
        assert_eq!(seen[0].1["channel"], "brenn:chat.app.home.in.42");

        // EOF alone leaves the attachment running: a probe fed from
        // `/dev/null` is a watcher, not a one-shot.
        handle
            .shutdown()
            .await
            .expect("the bridge is still running");
        assert_eq!(joined(task).await, BridgeOutcome::Closed);
    }

    #[tokio::test]
    async fn a_publish_that_lands_is_reported_with_its_channel_and_outcome() {
        // The probe's whole reason to exist — "is the token right, is the ACL
        // right" — is an operator publishing and seeing the answer.
        let (handle, events, task) = attached_bridge().await;
        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(
            concat!(
                r#"{"channel":"brenn:chat.app.home.in.42","body":"hi"}"#,
                "\n",
                r#"{"channel":"brenn:chat.app.home.wake.42","body":{"v":1}}"#,
                "\n",
            )
            .as_bytes(),
            handle.clone(),
            false,
            |event, fields| seen.push((event.to_string(), fields)),
        )
        .await;

        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_eq!(seen[0].0, "bridge_published");
        assert_eq!(seen[0].1["channel"], "brenn:chat.app.home.in.42");
        assert_eq!(seen[0].1["outcome"], "ok");
        assert_eq!(seen[1].0, "bridge_published");
        assert_eq!(seen[1].1["channel"], "brenn:chat.app.home.wake.42");
        assert_eq!(seen[1].1["outcome"], "ok");

        handle
            .shutdown()
            .await
            .expect("the bridge is still running");
        drop(events);
        assert_eq!(joined(task).await, BridgeOutcome::Closed);
    }

    #[tokio::test]
    async fn a_gone_bridge_ends_the_stdin_loop_rather_than_answering_every_line() {
        let (handle, task) = detached_bridge();
        handle
            .shutdown()
            .await
            .expect("the bridge is still running");
        assert_eq!(joined(task).await, BridgeOutcome::Closed);

        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(
            concat!(
                r#"{"channel":"brenn:chat.app.home.in.42","body":"one"}"#,
                "\n",
                r#"{"channel":"brenn:chat.app.home.in.42","body":"two"}"#,
                "\n",
            )
            .as_bytes(),
            handle,
            false,
            |event, fields| seen.push((event.to_string(), fields)),
        )
        .await;

        // One line, not two: the loop returns on `Gone` rather than reporting
        // the same failure for every remaining line.
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].0, "bridge_publish_failed");
    }

    #[tokio::test]
    async fn a_lost_publish_reports_and_keeps_reading() {
        // The other half of the early return: `Lost` is "not right now" — the
        // bridge is alive and reconnecting — so the operator's next line must
        // still be read. Only `Gone` ends the loop.
        let (handle, task) = detached_bridge();
        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(
            concat!(
                r#"{"channel":"brenn:chat.app.home.in.1","body":"one"}"#,
                "\n",
                r#"{"channel":"brenn:chat.app.home.in.2","body":"two"}"#,
                "\n",
            )
            .as_bytes(),
            handle.clone(),
            false,
            |event, fields| seen.push((event.to_string(), fields)),
        )
        .await;

        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_eq!(
            seen.iter()
                .map(|(_, fields)| fields["channel"].clone())
                .collect::<Vec<_>>(),
            vec!["brenn:chat.app.home.in.1", "brenn:chat.app.home.in.2"],
        );

        handle
            .shutdown()
            .await
            .expect("the bridge is still running");
        assert_eq!(joined(task).await, BridgeOutcome::Closed);
    }

    #[tokio::test]
    async fn blank_lines_are_skipped_and_a_bad_line_is_reported_by_its_number() {
        let (handle, task) = detached_bridge();
        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(
            b"\n   \nnot json\n".as_slice(),
            handle.clone(),
            false,
            |event, fields| seen.push((event.to_string(), fields)),
        )
        .await;

        assert_eq!(seen.len(), 1, "only the third line said anything: {seen:?}");
        assert_eq!(seen[0].0, "probe_stdin_invalid");
        assert_eq!(seen[0].1["line"], 3, "the number an editor would show");

        handle
            .shutdown()
            .await
            .expect("the bridge is still running");
        assert_eq!(joined(task).await, BridgeOutcome::Closed);
    }

    #[tokio::test]
    async fn stop_on_eof_ends_the_attachment_in_order() {
        let (handle, task) = detached_bridge();
        let mut seen: Vec<(String, Value)> = Vec::new();
        pump_publishes(b"".as_slice(), handle, true, |event, fields| {
            seen.push((event.to_string(), fields))
        })
        .await;

        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].0, "probe_stdin_eof");
        // Closed, not `EmbedderGone`: the shutdown went out before the last
        // handle dropped, and the first answer is the one that stands.
        assert_eq!(joined(task).await, BridgeOutcome::Closed);
    }
}
