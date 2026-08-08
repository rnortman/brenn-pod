//! The bridge-facing task: one select loop owning every bus interaction that
//! cannot happen on the pipeline's thread.
//!
//! Four things converge here because they all touch the same bridge: the event
//! pump that renders what the attachment did and routes response messages into
//! the brain, the forwarder that publishes the fire-and-forget notices the brain
//! queued, the retry timer for a response channel the peer would not serve, and
//! the exit path that decides whether the bridge's ending is a shutdown or a
//! fault.
//!
//! Two rules shape the loop, and both come from the bridge's event channel being
//! bounded and *awaited*: an embedder that stops draining it back-pressures the
//! socket read.
//!
//! - Nothing in the loop awaits a publish. `BridgeHandle::publish` waits for the
//!   peer's own answer, which has to arrive through the socket this loop is
//!   supposed to be draining — so every publish here is spawned, and nothing
//!   waits on the task.
//! - Handing a delivery to the brain is synchronous and cheap by the brain's own
//!   contract (a scan and a queue push), so routing never parks the pump either.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use brenn_bridge::render::{attached_fields, detached_fields, gap_reason};
use brenn_bridge::{
    BridgeEvent, BridgeHandle, BridgeOutcome, Delivery, PublishRequest, ResumePolicy,
    SubscriptionDepths, Urgency,
};
use serde_json::json;
use speech_pipeline::brenn_brain::{HelpChannels, response_contract_help};
use speech_pipeline::{BrennBrain, DeliverOutcome};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{Notice, publish_once};
use crate::config::BrennConfig;
use crate::jsonl::JsonlHandle;

/// Subscription statement for the response channel.
///
/// Responses are only meaningful live: a stale answer to a turn that has long
/// since ended is noise, so nothing is retained and no cursor is resumed. The
/// push depth satisfies the plane's "at least one non-zero" precondition with
/// headroom for a burst of continuation segments. Both are constants at the one
/// acquisition site, which is also how the plane's identical-depths-per-hold
/// precondition is met.
const RESPONSE_DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 0,
};

/// See [`RESPONSE_DEPTHS`]: a reattach replays nothing.
const RESPONSE_RESUME: ResumePolicy = ResumePolicy::Cursorless;

/// How long to wait before asking again for a response channel the peer said was
/// not there. Nothing else retries it — the hold is dropped with the refusal —
/// and a bus brain with no response path is dead, so the ask has to come back.
const RESUBSCRIBE_DELAY: Duration = Duration::from_secs(30);

/// The channels the driver's loop selects on, and the bridge task it ends by
/// joining. Handed over whole at spawn: keeping them out of the driver's state
/// is what lets the loop borrow them and the driver's own methods at once.
pub struct DriverIo {
    /// Every fact the bridge reports.
    pub events: mpsc::Receiver<BridgeEvent>,
    /// The link's fire-and-forget queue.
    pub notices: mpsc::Receiver<Notice>,
    /// The spawned `Bridge::run`.
    pub bridge: JoinHandle<BridgeOutcome>,
}

/// The three shutdown-related handles the driver holds.
pub struct DriverTokens {
    /// Cancelled to stop the driver. Deliberately *not* the server's shared
    /// shutdown token: that one fires before the pipeline drains, and a bridge
    /// torn down under draining turns would fail every one of them.
    pub teardown: CancellationToken,
    /// The server's shutdown token, cancelled when the bridge ends mid-run — a
    /// voice node whose brain has no transport is inert, and a crisp stop that a
    /// supervisor restarts beats a slow drip of failing turns.
    pub shutdown: CancellationToken,
    /// First-writer-wins slot for the fault detail, so the process's exit reason
    /// names the bridge outcome rather than the symptom it caused downstream.
    pub fatal: Arc<OnceLock<String>>,
}

/// The contract document and the state of getting it published.
struct HelpDoc {
    channel: String,
    /// Built once at construction: its only dynamic content is the channel names.
    body: String,
    /// Set by the publishing task on success. Shared because that task outlives
    /// the call that spawned it.
    published: Arc<AtomicBool>,
    /// The in-flight publish, so a reattach arriving while the first attempt is
    /// still waiting for its answer does not start a second one.
    task: Option<JoinHandle<()>>,
}

/// The bridge-facing task. Build it, then [`run`](BridgeDriver::run) it.
pub struct BridgeDriver {
    handle: BridgeHandle,
    brain: Arc<BrennBrain>,
    response_channel: String,
    publish_channel: String,
    wake_channel: Option<String>,
    attribution: Option<String>,
    help: Option<HelpDoc>,
    tokens: DriverTokens,
    jsonl: JsonlHandle,
    /// When to ask for the response channel again, or `None` when the current
    /// subscription is not in doubt.
    resubscribe_at: Option<Instant>,
}

impl BridgeDriver {
    /// Assemble the driver. The help document is rendered here, once per process
    /// run, so the loop never pays for it and both publishes of a retried
    /// document carry identical bytes.
    pub fn new(
        config: &BrennConfig,
        handle: BridgeHandle,
        brain: Arc<BrennBrain>,
        tokens: DriverTokens,
        jsonl: JsonlHandle,
    ) -> Self {
        let help = config.help_channel.as_ref().map(|channel| HelpDoc {
            channel: channel.clone(),
            body: response_contract_help(&HelpChannels {
                publish: config.publish_channel.clone(),
                response: config.response_channel.clone(),
                wake: config.wake_channel.clone(),
            }),
            published: Arc::new(AtomicBool::new(false)),
            task: None,
        });
        Self {
            handle,
            brain,
            response_channel: config.response_channel.clone(),
            publish_channel: config.publish_channel.clone(),
            wake_channel: config.wake_channel.clone(),
            attribution: config.attribution.clone(),
            help,
            tokens,
            jsonl,
            resubscribe_at: None,
        }
    }

    /// Drive the bridge until it ends or the teardown token fires.
    ///
    /// The response subscription is stated before the loop and before anything
    /// has attached: the subscription plane holds the statement and re-sends it
    /// at every attachment, so there is no attach to wait for and no window to
    /// lose it in.
    pub async fn run(mut self, io: DriverIo) {
        let DriverIo {
            mut events,
            mut notices,
            bridge,
        } = io;
        // Cloned out of `tokens`: the loop's own arm cannot hold a borrow of
        // `self` while the other arms' handlers take it mutably.
        let teardown = self.tokens.teardown.clone();
        self.subscribe_response().await;
        let asked_to_stop = loop {
            tokio::select! {
                // Teardown first: a flood of deliveries must not starve a stop.
                biased;
                () = teardown.cancelled() => break true,
                // The raw `Option` is matched rather than pattern-bound: a
                // `Some(event) = …` arm would disable itself when the channel
                // closes, and the close IS the bridge's exit signal.
                event = events.recv() => match event {
                    Some(event) => self.on_event(event),
                    None => break false,
                },
                Some(notice) = notices.recv() => self.publish_notice(notice),
                () = resubscribe_due(self.resubscribe_at) => {
                    self.resubscribe_at = None;
                    self.subscribe_response().await;
                }
            }
        };
        if asked_to_stop {
            // Best-effort: a bridge that already ended needs no telling.
            let _ = self.handle.shutdown().await;
        }
        self.finish(events, bridge).await;
    }

    /// Join the bridge, report its outcome, and decide whether that outcome is a
    /// fault.
    ///
    /// Events keep being drained while the join is awaited. They must be: the
    /// event channel is awaited by the bridge, so a driver that stopped reading
    /// it could park the very task it is waiting to finish.
    async fn finish(
        mut self,
        mut events: mpsc::Receiver<BridgeEvent>,
        bridge: JoinHandle<BridgeOutcome>,
    ) {
        let mut bridge = bridge;
        let mut draining = true;
        let joined = loop {
            tokio::select! {
                joined = &mut bridge => break joined,
                event = events.recv(), if draining => match event {
                    Some(event) => self.on_event(event),
                    None => draining = false,
                },
            }
        };
        let outcome = match joined {
            Ok(outcome) => outcome.to_string(),
            // A panicked bridge task is as terminal as any fatal outcome, and
            // silently reporting nothing would be the one way to lose it.
            Err(err) => format!("the bridge task did not finish: {err}"),
        };
        // Nobody asked it to stop, so it gave up: reconnection is internal to the
        // bridge and already exhausted by the time an outcome exists.
        let fatal = !self.tokens.teardown.is_cancelled();
        self.jsonl.emit(
            "brenn_bridge_exit",
            &json!({ "outcome": outcome, "fatal": fatal }),
        );
        if fatal {
            // First writer wins: whatever fault the process ultimately reports,
            // this is the root one, and the downstream symptom (a router seeing
            // its shutdown token) must not overwrite it.
            let _ = self
                .tokens
                .fatal
                .set(format!("brenn bridge exited: {outcome}"));
            self.tokens.shutdown.cancel();
        }
    }

    /// State the response-channel hold. A gone bridge is not reported here: the
    /// event channel closing says the same thing, and that path owns the line.
    async fn subscribe_response(&self) {
        let _ = self
            .handle
            .subscribe(
                self.response_channel.clone(),
                RESPONSE_DEPTHS,
                RESPONSE_RESUME,
            )
            .await;
    }

    /// Render one bridge event, and route a delivery. Nothing here awaits.
    fn on_event(&mut self, event: BridgeEvent) {
        match event {
            BridgeEvent::Attached(facts) => {
                self.jsonl.emit("brenn_attached", &attached_fields(&facts));
                self.publish_help();
            }
            BridgeEvent::Detached { reason } => {
                self.jsonl.emit("brenn_detached", &detached_fields(&reason));
            }
            BridgeEvent::ConnectFailed { timed_out } => {
                self.jsonl
                    .emit("brenn_connect_failed", &json!({ "timed_out": timed_out }));
            }
            BridgeEvent::Subscribed {
                channel,
                replay_count,
                gap,
            } => {
                self.jsonl.emit(
                    "brenn_subscribed",
                    &json!({
                        "channel": channel,
                        "replay_count": replay_count,
                        "gap": gap.as_ref().map(gap_reason),
                    }),
                );
            }
            BridgeEvent::Unavailable { channel } => self.on_unavailable(channel),
            BridgeEvent::Delivered(delivery) => self.on_delivery(delivery),
        }
    }

    /// The peer will not serve a channel. For the response channel that is fatal
    /// to every turn until it comes back, so the ask is scheduled again.
    fn on_unavailable(&mut self, channel: String) {
        let ours = channel == self.response_channel;
        if ours {
            self.resubscribe_at = Some(Instant::now() + RESUBSCRIBE_DELAY);
        }
        self.jsonl.emit(
            "brenn_channel_unavailable",
            &json!({
                "channel": channel,
                "retry_in_ms": ours.then(|| RESUBSCRIBE_DELAY.as_millis() as u64),
            }),
        );
    }

    /// Hand a response-channel message to the turn awaiting it, or say why it
    /// went nowhere. Every drop here concerns a message no turn owns, which is
    /// why it is a driver line and not a brain event.
    fn on_delivery(&self, delivery: Delivery) {
        if delivery.dropped > 0 {
            // Reported before the message's own fate, and whatever that fate is:
            // messages the bus lost ahead of this one are otherwise invisible when
            // the one carrying the count is accepted — a turn answered with its
            // middle missing and nothing in the stream to say so.
            self.jsonl.emit(
                "brenn_delivery_gap",
                &json!({
                    "channel": delivery.channel,
                    "seq": delivery.seq,
                    "dropped": delivery.dropped,
                }),
            );
        }
        let (reason, pending) = if delivery.channel == self.response_channel {
            match self.brain.deliver(&delivery.envelope.body) {
                DeliverOutcome::Delivered => return,
                DeliverOutcome::NoTurnPending => ("no_turn_pending", None),
                DeliverOutcome::ReplyMismatch { pending } => ("reply_mismatch", Some(pending.0)),
                DeliverOutcome::Backlogged => ("backlog", None),
            }
        } else {
            ("unexpected_channel", None)
        };
        let mut fields = json!({
            "reason": reason,
            "channel": delivery.channel,
            "seq": delivery.seq,
        });
        if delivery.dropped > 0 {
            fields["dropped"] = json!(delivery.dropped);
        }
        if let Some(pending) = pending {
            fields["pending"] = json!(pending);
        }
        self.jsonl.emit("brenn_delivery_dropped", &fields);
    }

    /// Publish a queued notice on a detached task.
    fn publish_notice(&self, notice: Notice) {
        let (body, channel, urgency, event) = match notice {
            Notice::Wake(body) => {
                let Some(channel) = self.wake_channel.clone() else {
                    // The brain nudges on every confirmed wake; whether a nudge
                    // has anywhere to go is configuration, and this is the first
                    // place that knows. Silent by intent: one line per wake word
                    // for a deliberately-unconfigured channel is noise.
                    return;
                };
                (body, channel, Urgency::Low, "brenn_wake_publish_failed")
            }
            // Conversation content, ordered with the utterances it sits between.
            Notice::Interruption(body) => (
                body,
                self.publish_channel.clone(),
                Urgency::High,
                "brenn_interruption_publish_failed",
            ),
        };
        let request = PublishRequest {
            channel,
            attribution: self.attribution.clone(),
            body,
            urgency,
        };
        let handle = self.handle.clone();
        let jsonl = self.jsonl.clone();
        tokio::spawn(async move {
            let channel = request.channel.clone();
            if let Err(detail) = publish_once(&handle, request).await {
                jsonl.emit(event, &json!({ "channel": channel, "detail": detail }));
            }
        });
    }

    /// Publish the contract document, once per process run, on a detached task.
    ///
    /// Called on every attachment because the channel is expected to be
    /// server-retained: until one publish is *accepted*, the harness has no
    /// instructions, and a refusal has no other retry.
    fn publish_help(&mut self) {
        let attribution = self.attribution.clone();
        let handle = self.handle.clone();
        let jsonl = self.jsonl.clone();
        let Some(help) = self.help.as_mut() else {
            return;
        };
        if help.published.load(Ordering::Relaxed) {
            return;
        }
        if help.task.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let request = PublishRequest {
            channel: help.channel.clone(),
            attribution,
            body: help.body.clone(),
            urgency: Urgency::Low,
        };
        let published = help.published.clone();
        help.task = Some(tokio::spawn(async move {
            let channel = request.channel.clone();
            match publish_once(&handle, request).await {
                Ok(()) => {
                    published.store(true, Ordering::Relaxed);
                    jsonl.emit("brenn_help_published", &json!({ "channel": channel }));
                }
                Err(detail) => jsonl.emit(
                    "brenn_help_publish_failed",
                    &json!({ "channel": channel, "detail": detail }),
                ),
            }
        }));
    }
}

/// The resubscribe arm's future: the deadline when one is set, never when it is
/// not. Takes the deadline by value so the arm holds no borrow of the driver.
async fn resubscribe_due(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use futures::channel::mpsc as futures_mpsc;
    use serde_json::Value;
    use speech_pipeline::{
        AudioSpan, Brain, BrainLink, BrainStats, DoaTrack, EndpointCause, PodId, ResponseSink,
        RoomId, SpeakBody, SpeakCmd, StageTimings, Transcript, Utterance, UtteranceId,
    };

    use super::*;
    use crate::brenn::BridgeLink;
    use crate::brenn::scripted::{Attempt, Peer, WAIT, scripted};
    use crate::config::JsonlSink;
    use crate::jsonl::probe::{expect_line, lines};

    const PUBLISH: &str = "brenn:pod.utterance";
    const RESPONSE: &str = "brenn:pod.speak";
    const WAKE: &str = "brenn:pod.wake";
    const HELP: &str = "brenn:pod.help";

    /// A `[brenn]` table built the way an operator writes one, so the tests
    /// exercise the same defaults and the same parse the daemon does. `extra`
    /// lands before the `[bridge]` table, since keys after a header belong to it.
    fn brenn_config(extra: &str) -> BrennConfig {
        let toml = format!(
            "publish_channel = \"{PUBLISH}\"\n\
             response_channel = \"{RESPONSE}\"\n\
             attribution = \"voice\"\n\
             {extra}\n\
             [bridge]\n\
             server_url = \"wss://peer.example.net/remote/pod-kitchen/ws\"\n\
             token_file = \"/nonexistent/pod.token\"\n\
             ident = \"speech-surface/test\"\n"
        );
        toml::from_str(&toml).expect("the test table parses")
    }

    /// Everything one driver test drives: the peers behind the scripted socket,
    /// the link the brain publishes through, and the JSONL file the driver's
    /// judgement lands in.
    struct Fixture {
        link: BridgeLink,
        brain: Arc<BrennBrain>,
        peers: std::collections::VecDeque<Peer>,
        driver: JoinHandle<()>,
        teardown: CancellationToken,
        shutdown: CancellationToken,
        fatal: Arc<OnceLock<String>>,
        path: PathBuf,
        _dir: tempfile::TempDir,
        _jsonl: JsonlHandle,
        _writer: JoinHandle<()>,
    }

    async fn fixture(script: &[Attempt], config: BrennConfig) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let (bridge, handle, events, peers) = scripted(script, 3);
        let bridge = tokio::spawn(bridge.run());
        let (link, notices) = BridgeLink::new(
            handle.clone(),
            config.publish_channel.clone(),
            config.attribution.clone(),
            jsonl.clone(),
        );
        let brain = Arc::new(BrennBrain::new(
            Arc::new(link.clone()),
            Duration::from_millis(config.response_timeout_ms),
            Duration::from_millis(config.continuation_timeout_ms),
            config.failure_message.clone(),
            Arc::new(|_| {}),
            Arc::new(BrainStats::default()),
        ));
        let teardown = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let fatal: Arc<OnceLock<String>> = Arc::new(OnceLock::new());
        let driver = BridgeDriver::new(
            &config,
            handle,
            brain.clone(),
            DriverTokens {
                teardown: teardown.clone(),
                shutdown: shutdown.clone(),
                fatal: fatal.clone(),
            },
            jsonl.clone(),
        );
        let driver = tokio::spawn(driver.run(DriverIo {
            events,
            notices,
            bridge,
        }));
        Fixture {
            link,
            brain,
            peers,
            driver,
            teardown,
            shutdown,
            fatal,
            path,
            _dir: dir,
            _jsonl: jsonl,
            _writer: writer,
        }
    }

    impl Fixture {
        fn peer(&mut self) -> Peer {
            self.peers.pop_front().expect("the script opens a socket")
        }

        /// Attach the next scripted socket and answer the response-channel
        /// subscription, which is the state every test starts from.
        async fn attached(&mut self) -> Peer {
            let mut peer = self.peer();
            peer.handshake().await;
            peer.answer_subscribe(RESPONSE, "Ok").await;
            peer
        }

        /// Stop the driver and wait for it to finish, so the assertions that
        /// follow see a settled JSONL file.
        async fn teardown(&mut self) {
            self.teardown.cancel();
            tokio::time::timeout(WAIT, &mut self.driver)
                .await
                .expect("the driver stops when told to")
                .expect("the driver task does not panic");
        }
    }

    fn events(path: &Path) -> Vec<String> {
        lines(path)
            .iter()
            .map(|line| line["event"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    fn utterance(id: u64) -> Utterance {
        Utterance {
            id: UtteranceId(id),
            pod: PodId("pod-kitchen".into()),
            room: RoomId("kitchen".into()),
            speaker: None,
            doa: DoaTrack(Vec::new()),
            audio_ref: AudioSpan {
                log: "pod-kitchen_0.framelog".into(),
                start_sample: 0,
                end_sample: 16_000,
                segments: Vec::new(),
            },
            transcript: Some(Transcript {
                text: "what time is it".into(),
                confidence: None,
            }),
            timings: StageTimings::default(),
            endpoint_cause: EndpointCause::SoftEndpoint,
            wake: None,
            barge_in: None,
        }
    }

    #[tokio::test]
    async fn a_response_reaches_the_pending_turn_and_is_spoken() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;

        let (speak_tx, mut speak_rx) = futures_mpsc::channel::<SpeakCmd>(4);
        let brain = fx.brain.clone();
        let turn = tokio::spawn(async move {
            brain
                .handle(utterance(7), ResponseSink::new(speak_tx))
                .await;
        });

        let published = peer.answer_publish("Ok").await;
        assert_eq!(published["channel"], PUBLISH);
        assert_eq!(published["urgency"], "high");
        assert_eq!(published["attribution"], "voice");
        let body: Value =
            serde_json::from_str(published["body"].as_str().expect("a string body")).unwrap();
        assert_eq!(body["type"], "utterance");
        assert_eq!(body["utterance"], 7);
        assert_eq!(body["text"], "what time is it");

        peer.deliver(RESPONSE, "<reply to=\"7\"/>It is half past four.", 1);
        let cmd = tokio::time::timeout(WAIT, futures::StreamExt::next(&mut speak_rx))
            .await
            .expect("the response was spoken before the timeout")
            .expect("the sink is open");
        assert_eq!(cmd.in_reply_to, Some(UtteranceId(7)));
        assert!(cmd.interruptible);
        match cmd.body {
            SpeakBody::Text(text) => assert_eq!(text, "It is half past four."),
            other => panic!("expected text, got {other:?}"),
        }
        tokio::time::timeout(WAIT, turn)
            .await
            .expect("the terminal message ends the turn")
            .expect("the turn task does not panic");

        fx.teardown().await;
        assert!(
            !events(&fx.path).contains(&"brenn_delivery_dropped".to_string()),
            "a delivery the brain accepted is not a drop: {:?}",
            events(&fx.path)
        );
    }

    #[tokio::test]
    async fn a_delivery_with_no_pending_turn_is_dropped_loudly() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let peer = fx.attached().await;

        peer.deliver(RESPONSE, "<reply to=\"7\"/>nobody asked", 3);
        let line = expect_line(&fx.path, "brenn_delivery_dropped").await;
        assert_eq!(line["reason"], "no_turn_pending");
        assert_eq!(line["channel"], RESPONSE);
        assert_eq!(line["seq"], 3);
        fx.teardown().await;
    }

    /// Start a turn and let the peer accept its publish, leaving the brain parked
    /// on its response slot. The sink is returned so a test can watch what the
    /// answer speaks, and the join handle so it can end the turn.
    async fn pending_turn(
        fx: &Fixture,
        peer: &mut Peer,
        id: u64,
    ) -> (
        JoinHandle<()>,
        futures_mpsc::Receiver<SpeakCmd>,
        futures_mpsc::Sender<SpeakCmd>,
    ) {
        let (speak_tx, speak_rx) = futures_mpsc::channel::<SpeakCmd>(4);
        let sink = ResponseSink::new(speak_tx.clone());
        let brain = fx.brain.clone();
        let turn = tokio::spawn(async move { brain.handle(utterance(id), sink).await });
        peer.answer_publish("Ok").await;
        (turn, speak_rx, speak_tx)
    }

    #[tokio::test]
    async fn a_mismatched_reply_id_is_dropped_and_names_the_pending_turn() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;
        let (turn, _speak_rx, _speak_tx) = pending_turn(&fx, &mut peer, 7).await;

        // A stale id echoed out of the peer's own context, behind two messages the
        // bus lost: the id is what makes it a drop, the count is what the operator
        // needs beside it.
        peer.deliver_after_gap(RESPONSE, "<reply to=\"9\"/>a stale answer", 4, 2);
        let line = expect_line(&fx.path, "brenn_delivery_dropped").await;
        assert_eq!(line["reason"], "reply_mismatch");
        assert_eq!(line["channel"], RESPONSE);
        assert_eq!(line["seq"], 4);
        assert_eq!(
            line["pending"], 7,
            "the line names the turn the id was measured against"
        );
        assert_eq!(line["dropped"], 2);

        turn.abort();
        fx.teardown().await;
    }

    #[tokio::test]
    async fn a_delivery_that_lost_messages_ahead_of_it_reports_the_gap() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;
        let (turn, mut speak_rx, _speak_tx) = pending_turn(&fx, &mut peer, 7).await;

        // The message itself is accepted and spoken, so nothing else in the stream
        // would ever say that the middle of the answer never arrived.
        peer.deliver_after_gap(RESPONSE, "<reply to=\"7\"/>…and clear.", 5, 3);
        let line = expect_line(&fx.path, "brenn_delivery_gap").await;
        assert_eq!(line["channel"], RESPONSE);
        assert_eq!(line["seq"], 5);
        assert_eq!(line["dropped"], 3);

        tokio::time::timeout(WAIT, futures::StreamExt::next(&mut speak_rx))
            .await
            .expect("the response was spoken before the timeout")
            .expect("the sink is open");
        tokio::time::timeout(WAIT, turn)
            .await
            .expect("the terminal message ends the turn")
            .expect("the turn task does not panic");

        fx.teardown().await;
        assert!(
            !events(&fx.path).contains(&"brenn_delivery_dropped".to_string()),
            "the message was accepted; only its gap is reported: {:?}",
            events(&fx.path)
        );
    }

    #[tokio::test]
    async fn a_wake_nudge_is_published_on_the_wake_channel_at_low_urgency() {
        let mut fx = fixture(
            &[Attempt::Open],
            brenn_config(&format!("wake_channel = \"{WAKE}\"")),
        )
        .await;
        let mut peer = fx.attached().await;

        fx.link.notify_wake("{\"type\":\"wake\"}".to_string());
        let published = peer.answer_publish("Ok").await;
        assert_eq!(published["channel"], WAKE);
        assert_eq!(published["urgency"], "low");
        assert_eq!(published["body"], "{\"type\":\"wake\"}");

        fx.teardown().await;
        assert!(
            !events(&fx.path).contains(&"brenn_wake_publish_failed".to_string()),
            "an accepted nudge is not a failure: {:?}",
            events(&fx.path)
        );
    }

    #[tokio::test]
    async fn a_refused_wake_nudge_reports_the_wake_failure_line() {
        // Each notice kind reports under its own name, and the wake one is the name
        // the console deliberately renders calm — so it has to be the name that
        // actually appears when a nudge is refused.
        let mut fx = fixture(
            &[Attempt::Open],
            brenn_config(&format!("wake_channel = \"{WAKE}\"")),
        )
        .await;
        let mut peer = fx.attached().await;

        fx.link.notify_wake("{\"type\":\"wake\"}".to_string());
        let published = peer.answer_publish("Failed").await;
        assert_eq!(published["channel"], WAKE);

        let line = expect_line(&fx.path, "brenn_wake_publish_failed").await;
        assert_eq!(line["channel"], WAKE);
        assert_eq!(
            line["detail"],
            "the peer accepted the frame but the publish failed"
        );
        fx.teardown().await;
    }

    #[tokio::test]
    async fn a_wake_nudge_with_no_configured_channel_publishes_nothing() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;

        fx.link.notify_wake("{\"type\":\"wake\"}".to_string());
        // Round-trip something the driver *does* publish, to prove the nudge was
        // not merely slower than the assertion.
        fx.link
            .notify_interruption("{\"type\":\"interruption\"}".to_string());
        let published = peer.answer_publish("Ok").await;
        assert_eq!(published["body"], "{\"type\":\"interruption\"}");
        assert!(peer.wrote_nothing(), "the nudge had nowhere to go");
        fx.teardown().await;
    }

    #[tokio::test]
    async fn an_interruption_notice_rides_the_publish_channel_at_high_urgency() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;

        fx.link
            .notify_interruption("{\"type\":\"interruption\"}".to_string());
        let published = peer.answer_publish("RateLimited").await;
        assert_eq!(published["channel"], PUBLISH);
        assert_eq!(published["urgency"], "high");

        let line = expect_line(&fx.path, "brenn_interruption_publish_failed").await;
        assert_eq!(line["channel"], PUBLISH);
        assert_eq!(line["detail"], "the peer rate-limited the publish");
        fx.teardown().await;
    }

    #[tokio::test]
    async fn a_notice_publish_never_parks_the_event_pump() {
        let mut fx = fixture(
            &[Attempt::Open],
            brenn_config(&format!("wake_channel = \"{WAKE}\"")),
        )
        .await;
        let mut peer = fx.attached().await;

        // Read the publish and deliberately never answer it: the publish future
        // is parked for the rest of the test.
        fx.link.notify_wake("{\"type\":\"wake\"}".to_string());
        let published = peer.expect_frame("Publish").await;
        assert_eq!(published["channel"], WAKE);

        peer.deliver(RESPONSE, "still listening", 1);
        let line = expect_line(&fx.path, "brenn_delivery_dropped").await;
        assert_eq!(line["reason"], "no_turn_pending");
        fx.teardown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn an_unavailable_response_channel_is_asked_for_again() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.peer();
        peer.handshake().await;
        peer.answer_subscribe(RESPONSE, "Unavailable").await;

        let line = expect_line(&fx.path, "brenn_channel_unavailable").await;
        assert_eq!(line["channel"], RESPONSE);
        assert_eq!(line["retry_in_ms"], 30_000);

        // Nothing retries a dropped hold on its own, so the driver's timer is the
        // only thing that can bring the response path back.
        assert!(peer.wrote_nothing(), "the retry waits out its delay");
        tokio::time::advance(RESUBSCRIBE_DELAY).await;
        peer.answer_subscribe(RESPONSE, "Ok").await;
        expect_line(&fx.path, "brenn_subscribed").await;
        fx.teardown().await;
    }

    #[tokio::test]
    async fn the_help_document_is_published_once_per_run() {
        let mut fx = fixture(
            &[Attempt::Open, Attempt::Open],
            brenn_config(&format!("help_channel = \"{HELP}\"")),
        )
        .await;
        let mut peer = fx.peer();
        peer.handshake().await;

        // Two frames are outstanding at the first attachment — the held
        // subscription and the help publish — in no guaranteed order.
        let mut published = None;
        for _ in 0..2 {
            let frame = peer.next_frame().await;
            match frame["type"].as_str() {
                Some("Subscribe") => {
                    peer.say(json!({
                        "type": "SubscribeResult",
                        "channel": RESPONSE,
                        "outcome": { "kind": "Ok" },
                        "replay_count": 0,
                    }));
                }
                Some("Publish") => {
                    peer.say(json!({
                        "type": "PublishResult",
                        "correlation": frame["correlation"].clone(),
                        "outcome": { "kind": "Ok" },
                    }));
                    published = Some(frame);
                }
                other => panic!("unexpected frame {other:?}"),
            }
        }
        let published = published.expect("the first attachment publishes the contract");
        assert_eq!(published["channel"], HELP);
        assert_eq!(published["urgency"], "low");
        let body = published["body"].as_str().expect("a string body");
        assert!(
            body.contains(RESPONSE) && body.contains(PUBLISH),
            "the document names the channels it is about: {body}"
        );
        expect_line(&fx.path, "brenn_help_published").await;

        // A reattach re-sends the subscription and nothing else: the document is
        // retained on the bus, so publishing it again would only be noise.
        peer.close();
        let mut second = fx.peer();
        second.handshake().await;
        second.answer_subscribe(RESPONSE, "Ok").await;
        assert!(
            second.wrote_nothing(),
            "an accepted document is not published twice"
        );
        fx.teardown().await;
        assert_eq!(
            events(&fx.path)
                .iter()
                .filter(|event| *event == "brenn_help_published")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_refused_help_document_is_published_again_at_the_next_attachment() {
        let mut fx = fixture(
            &[Attempt::Open, Attempt::Open],
            brenn_config(&format!("help_channel = \"{HELP}\"")),
        )
        .await;
        let mut peer = fx.peer();
        peer.handshake().await;
        peer.answer_subscribe(RESPONSE, "Ok").await;
        peer.answer_publish("RateLimited").await;
        let line = expect_line(&fx.path, "brenn_help_publish_failed").await;
        assert_eq!(line["channel"], HELP);
        assert_eq!(line["detail"], "the peer rate-limited the publish");

        peer.close();
        let mut second = fx.peer();
        second.handshake().await;
        second.answer_subscribe(RESPONSE, "Ok").await;
        let retried = second.answer_publish("Ok").await;
        assert_eq!(retried["channel"], HELP, "the refusal is retried");
        expect_line(&fx.path, "brenn_help_published").await;
        fx.teardown().await;
    }

    #[tokio::test]
    async fn no_help_channel_means_no_contract_publish() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let mut peer = fx.attached().await;
        expect_line(&fx.path, "brenn_attached").await;
        assert!(
            peer.wrote_nothing(),
            "an unconfigured help channel is published nothing"
        );
        fx.teardown().await;
        assert!(!events(&fx.path).contains(&"brenn_help_published".to_string()));
    }

    #[tokio::test]
    async fn a_terminal_bridge_exit_latches_the_fault_and_stops_the_server() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let peer = fx.attached().await;

        // A frame this bridge cannot own: it parks nothing, so the peer owes it
        // no parked-set mirror.
        peer.say(json!({ "type": "DeferredView", "channel": RESPONSE, "entries": [] }));

        tokio::time::timeout(WAIT, &mut fx.driver)
            .await
            .expect("the driver follows the bridge out")
            .expect("the driver task does not panic");
        let line = expect_line(&fx.path, "brenn_bridge_exit").await;
        assert_eq!(line["fatal"], true);
        let outcome = line["outcome"].as_str().unwrap();
        assert!(
            outcome.contains("attachment protocol error"),
            "the line carries the bridge's own account: {outcome}"
        );
        assert!(
            fx.shutdown.is_cancelled(),
            "a voice node with no brain transport does not keep serving"
        );
        assert_eq!(
            fx.fatal.get().map(String::as_str),
            Some(format!("brenn bridge exited: {outcome}").as_str()),
            "the fatal detail names the bridge, not the symptom downstream"
        );
    }

    #[tokio::test]
    async fn a_dial_that_reaches_no_socket_is_reported() {
        // A pod that never attaches must not look like a pod with nothing to say.
        let mut fx = fixture(&[Attempt::Fail, Attempt::Open], brenn_config("")).await;
        let line = expect_line(&fx.path, "brenn_connect_failed").await;
        assert_eq!(line["timed_out"], false);

        let _peer = fx.attached().await;
        expect_line(&fx.path, "brenn_attached").await;
        fx.teardown().await;
    }

    #[tokio::test]
    async fn a_detach_is_reported_with_what_the_loss_said() {
        let mut fx = fixture(&[Attempt::Open, Attempt::Open], brenn_config("")).await;
        let peer = fx.attached().await;
        peer.close();

        let line = expect_line(&fx.path, "brenn_detached").await;
        assert_eq!(line["reason"], "transport_closed");
        let _second = fx.attached().await;
        fx.teardown().await;
    }

    #[tokio::test]
    async fn a_graceful_teardown_closes_the_bridge_without_a_fault() {
        let mut fx = fixture(&[Attempt::Open], brenn_config("")).await;
        let _peer = fx.attached().await;

        fx.teardown().await;
        let line = expect_line(&fx.path, "brenn_bridge_exit").await;
        assert_eq!(line["fatal"], false);
        assert_eq!(line["outcome"], "the bridge was asked to shut down");
        assert!(!fx.shutdown.is_cancelled(), "the server was not stopped");
        assert!(fx.fatal.get().is_none());
    }
}
