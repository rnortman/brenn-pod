//! The bridge-facing task: one select loop owning every bus interaction that
//! cannot happen on the pipeline's thread.
//!
//! Four things converge here because they all touch the same bridge: the event
//! pump that renders what the attachment did and routes response messages into
//! the brain, the forwarder that publishes the fire-and-forget notices the brain
//! queued, the retry timer for any channel the peer would not serve, and the
//! exit path that decides whether the bridge's ending is a shutdown or a fault.
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
use crate::time::due;

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

/// Subscription statement for the motion intent channel.
///
/// Live only, and for a sharper reason than the response channel's: a script's
/// offsets run from the moment it arrives, so a raise published yesterday and
/// replayed at a reattach would pop the head up at three in the morning. The
/// push depth satisfies the plane's "at least one non-zero" precondition with
/// room for a burst; a missed script is repaired by its sender's next refresh,
/// and the standing script's own timeout covers the gap either way.
const MOTION_DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 0,
};

/// See [`MOTION_DEPTHS`]: a reattach replays no raise.
const MOTION_RESUME: ResumePolicy = ResumePolicy::Cursorless;

/// Where a motion intent body goes once the driver has it off the bus.
///
/// Opaque: the body is an unparsed channel payload, and what it compiles into
/// is the consumer's concern, not this loop's.
///
/// Staleness is the sink's judgement too. This loop reports what the bus lost
/// on the way here (the delivery-gap line) and nothing else; whether the intent
/// *in* a body is stale — a redelivery, an ordering number already spent — is a
/// statement about content only the consumer that decodes it can make, and a run
/// of such drops is watched where it is made.
///
/// Must not block: this runs on the loop draining the bridge's bounded event
/// channel, and an embedder that stops draining it back-pressures the socket
/// read.
pub trait IntentSink: Send + Sync + 'static {
    /// Take one body. `Err` carries the one word the driver's line reports the
    /// drop with — a sink that could not take it says why here rather than
    /// narrating on its own.
    ///
    /// `&'static str` because `reason` on `brenn_delivery_dropped` is a fixed
    /// vocabulary that log queries and alert rules are written against: a sink
    /// picks from words it declares, and whatever variable detail sits behind
    /// one of them belongs on the sink's own stream.
    fn deliver(&self, body: &str) -> Result<(), &'static str>;
}

/// How long to wait before asking again for a channel the peer said was not
/// there. It governs every hold: nothing else retries one — the hold is dropped
/// with the refusal — and each hold's own reason demands the ask come back. A
/// bus brain with no response path is dead, and a host that stopped hearing
/// remote intent looks exactly like one nobody is sending to.
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

/// One channel this driver holds, and the state of getting it held.
struct Hold {
    channel: String,
    depths: SubscriptionDepths,
    resume: ResumePolicy,
    /// When to ask for this channel again, or `None` while the subscription is
    /// not in doubt. Per hold rather than one shared slot: channels are refused
    /// independently, and a single deadline would let a later refusal move an
    /// earlier ask.
    resubscribe_at: Option<Instant>,
}

impl Hold {
    fn new(channel: String, depths: SubscriptionDepths, resume: ResumePolicy) -> Self {
        Self {
            channel,
            depths,
            resume,
            resubscribe_at: None,
        }
    }
}

/// The bridge-facing task. Build it, then [`run`](BridgeDriver::run) it.
pub struct BridgeDriver {
    handle: BridgeHandle,
    brain: Arc<BrennBrain>,
    response_channel: String,
    publish_channel: String,
    wake_channel: Option<String>,
    /// The motion intent channel the configuration named, if it named one. A
    /// subscription is stated for it only when a sink is wired too: both halves
    /// or neither, since deliveries nothing consumes would spend a hold and drop
    /// every message.
    motion_channel: Option<String>,
    intents: Option<Arc<dyn IntentSink>>,
    attribution: Option<String>,
    help: Option<HelpDoc>,
    tokens: DriverTokens,
    jsonl: JsonlHandle,
    /// Every channel this run holds. One rule per site: a channel that joins
    /// this list is stated at startup, gets its own refusal deadline, and is
    /// asked for again when that deadline passes, with no further code.
    holds: Vec<Hold>,
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
            motion_channel: config.motion_channel.clone(),
            intents: None,
            attribution: config.attribution.clone(),
            help,
            tokens,
            jsonl,
            holds: Vec::new(),
        }
    }

    /// Hear motion intent from the bus, delivering each body to `sink`.
    ///
    /// The channel is the one the configuration handed to `new` named, so a
    /// driver's channels all come from one config and no caller can assemble one
    /// out of two. A configuration naming no motion channel states no
    /// subscription and never calls the sink.
    #[must_use]
    pub fn with_intents(mut self, sink: Arc<dyn IntentSink>) -> Self {
        self.intents = Some(sink);
        self
    }

    /// Drive the bridge until it ends or the teardown token fires.
    ///
    /// Every hold is stated before the loop and before anything has attached:
    /// the subscription plane holds the statement and re-sends it at every
    /// attachment, so there is no attach to wait for and no window to lose it
    /// in.
    pub async fn run(mut self, io: DriverIo) {
        let DriverIo {
            mut events,
            mut notices,
            bridge,
        } = io;
        // Cloned out of `tokens`: the loop's own arm cannot hold a borrow of
        // `self` while the other arms' handlers take it mutably.
        let teardown = self.tokens.teardown.clone();
        self.state_holds();
        for hold in &self.holds {
            self.subscribe(hold.channel.clone(), hold.depths, hold.resume)
                .await;
        }
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
                () = due(self.next_resubscribe()) => self.resubscribe().await,
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

    /// Decide what this run holds: the response channel always, and the motion
    /// intent channel when a sink is wired to take what arrives on it.
    ///
    /// A configured channel with no sink is said out loud. Silently ignoring the
    /// key would leave an operator who set it — or a wiring regression that
    /// dropped the sink — with a head that never moves and a clean log, which is
    /// the one diagnosis this stream exists to prevent.
    fn state_holds(&mut self) {
        self.holds.push(Hold::new(
            self.response_channel.clone(),
            RESPONSE_DEPTHS,
            RESPONSE_RESUME,
        ));
        match (self.motion_channel.clone(), self.intents.is_some()) {
            (Some(channel), true) => {
                self.holds
                    .push(Hold::new(channel, MOTION_DEPTHS, MOTION_RESUME));
            }
            (Some(channel), false) => self.jsonl.emit(
                "brenn_motion_channel_unwired",
                &json!({ "channel": channel }),
            ),
            (None, _) => {}
        }
    }

    /// The earliest re-ask owed, or `None` when no hold is in doubt.
    fn next_resubscribe(&self) -> Option<Instant> {
        self.holds
            .iter()
            .filter_map(|hold| hold.resubscribe_at)
            .min()
    }

    /// State every hold whose re-ask deadline has passed.
    async fn resubscribe(&mut self) {
        let now = Instant::now();
        let owed: Vec<(String, SubscriptionDepths, ResumePolicy)> = self
            .holds
            .iter_mut()
            .filter(|hold| hold.resubscribe_at.is_some_and(|at| at <= now))
            .map(|hold| {
                hold.resubscribe_at = None;
                (hold.channel.clone(), hold.depths, hold.resume)
            })
            .collect();
        for (channel, depths, resume) in owed {
            self.subscribe(channel, depths, resume).await;
        }
    }

    /// State one channel hold. A gone bridge is not reported here: the event
    /// channel closing says the same thing, and that path owns the line.
    async fn subscribe(&self, channel: String, depths: SubscriptionDepths, resume: ResumePolicy) {
        let _ = self.handle.subscribe(channel, depths, resume).await;
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
    /// to every turn until it comes back, and for the motion channel it leaves
    /// remote intent with no way in, so either ask is scheduled again.
    fn on_unavailable(&mut self, channel: String) {
        let at = Instant::now() + RESUBSCRIBE_DELAY;
        let ours = match self.holds.iter_mut().find(|hold| hold.channel == channel) {
            Some(hold) => {
                hold.resubscribe_at = Some(at);
                true
            }
            // A channel this driver never asked for; nothing to schedule.
            None => false,
        };
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
        } else if let (Some(motion), Some(sink)) =
            (self.motion_channel.as_deref(), self.intents.as_ref())
            && motion == delivery.channel
        {
            // The sink screens the body; this loop only says a body it would not
            // take went nowhere. A body the sink accepted is not reported here at
            // all — what happens to it afterwards is the consumer's to narrate.
            match sink.deliver(&delivery.envelope.body) {
                Ok(()) => return,
                Err(reason) => (reason, None),
            }
        } else {
            // A channel this driver never asked for. Held rather than routed: a
            // subscription the peer invented is not a mandate to act on it.
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use futures::channel::mpsc as futures_mpsc;
    use serde_json::Value;
    use speech_pipeline::{
        AudioSpan, Brain, BrainLink, BrainStats, DoaTrack, EndpointCause, PodId, ResponseSink,
        RoomId, SpeakBody, SpeakCmd, StageTimings, Transcript, TurnEnd, Utterance, UtteranceId,
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
    const MOTION: &str = "brenn:pod.motion";

    /// A sink that keeps the bodies it took and refuses on demand, so a test can
    /// read both halves of the routing without a consumer behind it.
    #[derive(Default)]
    struct Intents {
        taken: std::sync::Mutex<Vec<String>>,
        refuse: std::sync::Mutex<Option<&'static str>>,
    }

    impl Intents {
        fn taken(&self) -> Vec<String> {
            self.taken.lock().unwrap().clone()
        }

        fn refusing(reason: &'static str) -> Arc<Self> {
            let sink = Self::default();
            *sink.refuse.lock().unwrap() = Some(reason);
            Arc::new(sink)
        }
    }

    impl IntentSink for Intents {
        fn deliver(&self, body: &str) -> Result<(), &'static str> {
            if let Some(reason) = *self.refuse.lock().unwrap() {
                return Err(reason);
            }
            self.taken.lock().unwrap().push(body.to_owned());
            Ok(())
        }
    }

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
        fixture_with(script, config, None).await
    }

    async fn fixture_with(
        script: &[Attempt],
        config: BrennConfig,
        intents: Option<Arc<dyn IntentSink>>,
    ) -> Fixture {
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
        let driver = match intents {
            Some(sink) => driver.with_intents(sink),
            None => driver,
        };
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

        /// The same, for a driver that also holds the motion intent channel:
        /// both statements answered, the motion one with `motion_kind`, and the
        /// motion frame handed back so a test can read the depths it stated.
        async fn attached_with_motion(&mut self, motion_kind: &'static str) -> (Peer, Value) {
            let mut peer = self.peer();
            peer.handshake().await;
            let pick = move |channel: &str| {
                if channel == MOTION { motion_kind } else { "Ok" }
            };
            let first = peer.answer_subscribe_with(pick).await;
            let second = peer.answer_subscribe_with(pick).await;
            let motion = if first["channel"] == MOTION {
                first
            } else {
                second
            };
            (peer, motion)
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
        JoinHandle<TurnEnd>,
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

    /// The whole seam in one pass: the statement goes out with the recorded
    /// depths and no resume, and a body delivered on that channel reaches the
    /// sink verbatim — the driver parses nothing.
    #[tokio::test]
    async fn motion_intent_is_subscribed_live_only_and_handed_to_the_sink() {
        let sink = Arc::new(Intents::default());
        let mut fx = fixture_with(
            &[Attempt::Open],
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
            Some(sink.clone()),
        )
        .await;
        let (peer, stated) = fx.attached_with_motion("Ok").await;
        assert_eq!(stated["push_depth"], 4);
        assert_eq!(stated["retain_depth"], 0);
        assert!(
            stated["cursor"].is_null(),
            "the first statement resumes from nothing: {stated}"
        );
        // The resume policy is invisible on a first statement — it decides what a
        // *reattach* replays — so it is pinned where it is stated instead. A
        // replayed raise would pop the head up at three in the morning.
        assert_eq!(MOTION_RESUME, ResumePolicy::Cursorless);

        let body = r#"{"type":"motion-script","pod":"pod-kitchen","seq":9,"steps":[]}"#;
        peer.deliver(MOTION, body, 1);
        let taken = tokio::time::timeout(WAIT, async {
            loop {
                let taken = sink.taken();
                if !taken.is_empty() {
                    return taken;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the delivery reaches the sink");
        assert_eq!(taken, vec![body.to_string()]);

        fx.teardown().await;
        assert!(
            !events(&fx.path).contains(&"brenn_delivery_dropped".to_string()),
            "a body the sink took is not a drop: {:?}",
            events(&fx.path)
        );
    }

    /// The sink's own word for why a body went nowhere is what the line says.
    /// Without it the operator's only evidence of a dropped intent is a head
    /// that did not move.
    #[tokio::test]
    async fn a_sink_that_refuses_a_body_names_its_reason_on_the_line() {
        let mut fx = fixture_with(
            &[Attempt::Open],
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
            Some(Intents::refusing("backlog")),
        )
        .await;
        let (peer, _) = fx.attached_with_motion("Ok").await;

        peer.deliver(MOTION, "{}", 4);
        let line = expect_line(&fx.path, "brenn_delivery_dropped").await;
        assert_eq!(line["reason"], "backlog");
        assert_eq!(line["channel"], MOTION);
        assert_eq!(line["seq"], 4);
        fx.teardown().await;
    }

    /// A peer that will not serve the motion channel gets asked again, on the
    /// motion channel's own deadline. Nothing else retries it, and a host that
    /// stopped hearing remote intent looks exactly like one nobody is sending to.
    #[tokio::test(start_paused = true)]
    async fn a_refused_motion_channel_is_asked_for_again() {
        let mut fx = fixture_with(
            &[Attempt::Open],
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
            Some(Arc::new(Intents::default())),
        )
        .await;
        let (mut peer, _) = fx.attached_with_motion("Unavailable").await;

        let line = expect_line(&fx.path, "brenn_channel_unavailable").await;
        assert_eq!(line["channel"], MOTION);
        assert_eq!(line["retry_in_ms"], RESUBSCRIBE_DELAY.as_millis() as u64);

        // And the ask actually comes back: the refusal dropped the hold, so the
        // driver's own timer is the only thing that can restore remote intent.
        assert!(peer.wrote_nothing(), "the retry waits out its delay");
        tokio::time::advance(RESUBSCRIBE_DELAY).await;
        peer.answer_subscribe(MOTION, "Ok").await;
        let subscribed = expect_line_on(&fx.path, "brenn_subscribed", MOTION).await;
        assert_eq!(subscribed["channel"], MOTION);
        fx.teardown().await;
    }

    /// Each hold owes its own re-ask. One shared deadline would let the later
    /// refusal move the earlier ask, and re-asking every hold when the earliest
    /// fires spends a round-trip on channels nobody refused.
    #[tokio::test(start_paused = true)]
    async fn two_refused_channels_keep_their_own_deadlines() {
        let mut fx = fixture_with(
            &[Attempt::Open],
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
            Some(Arc::new(Intents::default())),
        )
        .await;
        let mut peer = fx.peer();
        peer.handshake().await;
        // Both statements are already on the wire, in the plane's own order, so
        // they are read first and answered when this test wants them refused.
        let first = peer.expect_frame("Subscribe").await;
        let second = peer.expect_frame("Subscribe").await;
        let motion_first = first["channel"] == MOTION;
        assert_eq!(
            second["channel"],
            if motion_first { RESPONSE } else { MOTION },
            "the run holds exactly the response and motion channels"
        );

        refuse(&peer, RESPONSE);
        let refused = expect_line_on(&fx.path, "brenn_channel_unavailable", RESPONSE).await;
        assert_eq!(refused["retry_in_ms"], RESUBSCRIBE_DELAY.as_millis() as u64);

        let half = RESUBSCRIBE_DELAY / 2;
        tokio::time::advance(half).await;
        refuse(&peer, MOTION);
        expect_line_on(&fx.path, "brenn_channel_unavailable", MOTION).await;

        // The response channel's own deadline: its ask comes back and the motion
        // one, refused half a delay later, is still owed.
        tokio::time::advance(half).await;
        let again = peer.expect_frame("Subscribe").await;
        assert_eq!(again["channel"], RESPONSE);
        peer.say(subscribed_ok(RESPONSE));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            peer.wrote_nothing(),
            "the motion ask waits out its own delay"
        );

        tokio::time::advance(half).await;
        let motion_again = peer.expect_frame("Subscribe").await;
        assert_eq!(motion_again["channel"], MOTION);
        fx.teardown().await;
    }

    /// Refuse `channel` on the peer's behalf, for the tests that decide when each
    /// statement is answered rather than answering it where it is read.
    fn refuse(peer: &Peer, channel: &str) {
        peer.say(json!({
            "type": "SubscribeResult",
            "channel": channel,
            "outcome": { "kind": "Unavailable" },
            "replay_count": 0,
        }));
    }

    fn subscribed_ok(channel: &str) -> Value {
        json!({
            "type": "SubscribeResult",
            "channel": channel,
            "outcome": { "kind": "Ok" },
            "replay_count": 0,
        })
    }

    /// The first line naming `event` *and* `channel`, waited for. The plain
    /// [`expect_line`] cannot tell two channels' lines apart, and both of this
    /// driver's holds write the same events.
    async fn expect_line_on(path: &Path, event: &str, channel: &str) -> Value {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some(line) = lines(path)
                .into_iter()
                .find(|line| line["event"] == event && line["channel"] == channel)
            {
                return line;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no {event} line for {channel} arrived; got {:?}",
                lines(path)
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// A named channel with nothing wired to consume it is said out loud, once,
    /// at startup — and no hold is spent on it. Without the line the symptom is a
    /// head that never moves and a log with nothing in it about why, which is the
    /// shape an operator cannot diagnose.
    #[tokio::test]
    async fn a_motion_channel_with_no_sink_is_reported_and_not_subscribed() {
        let mut fx = fixture(
            &[Attempt::Open],
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
        )
        .await;
        let line = expect_line(&fx.path, "brenn_motion_channel_unwired").await;
        assert_eq!(line["channel"], MOTION);
        let _peer = fx.attached().await;
        expect_line(&fx.path, "brenn_subscribed").await;

        fx.teardown().await;
        let stated: Vec<String> = lines(&fx.path)
            .iter()
            .filter(|line| line["event"] == "brenn_subscribed")
            .map(|line| line["channel"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(stated, vec![RESPONSE.to_string()]);
    }

    /// Unconfigured, nothing changes: a sink handed to a driver whose config
    /// names no motion channel buys no subscription, so only the response hold
    /// is ever stated and nothing reaches the sink.
    #[tokio::test]
    async fn no_motion_channel_states_no_subscription() {
        let sink = Arc::new(Intents::default());
        let mut fx = fixture_with(&[Attempt::Open], brenn_config(""), Some(sink.clone())).await;
        let _peer = fx.attached().await;
        expect_line(&fx.path, "brenn_subscribed").await;

        fx.teardown().await;
        let stated: Vec<String> = lines(&fx.path)
            .iter()
            .filter(|line| line["event"] == "brenn_subscribed")
            .map(|line| line["channel"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(stated, vec![RESPONSE.to_string()]);
        assert!(sink.taken().is_empty());
    }

    /// A driver assembled and never run, with its holds stated, for the two arms
    /// a conformant peer cannot reach: the subscription plane refuses a
    /// `SubscribeResult` for a channel it has nothing pending on and a `Deliver`
    /// on a channel that was never active, so a socket cannot produce either. The
    /// arms exist because the driver may not trust that of a peer.
    async fn unrun(
        config: BrennConfig,
        intents: Option<Arc<dyn IntentSink>>,
    ) -> (BridgeDriver, PathBuf, tempfile::TempDir, JoinHandle<()>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let (_bridge, handle, _events, _peers) = scripted(&[Attempt::Open], 3);
        let (link, _notices) = BridgeLink::new(
            handle.clone(),
            config.publish_channel.clone(),
            config.attribution.clone(),
            jsonl.clone(),
        );
        let brain = Arc::new(BrennBrain::new(
            Arc::new(link),
            Duration::from_millis(config.response_timeout_ms),
            Duration::from_millis(config.continuation_timeout_ms),
            config.failure_message.clone(),
            Arc::new(|_| {}),
            Arc::new(BrainStats::default()),
        ));
        let driver = BridgeDriver::new(
            &config,
            handle,
            brain,
            DriverTokens {
                teardown: CancellationToken::new(),
                shutdown: CancellationToken::new(),
                fatal: Arc::new(OnceLock::new()),
            },
            jsonl,
        );
        let mut driver = match intents {
            Some(sink) => driver.with_intents(sink),
            None => driver,
        };
        driver.state_holds();
        (driver, path, dir, writer)
    }

    /// One delivery, envelope and all, as the bridge hands it over.
    fn delivery(channel: &str, body: &str) -> Delivery {
        Delivery {
            channel: channel.to_owned(),
            envelope: serde_json::from_value(json!({
                "message_id": "00000000-0000-0000-0000-000000000001",
                "source": "brenn",
                "channel": channel,
                "sender": "system:harness",
                "publish_ts": "2023-11-14T22:13:20Z",
                "body": body,
                "urgency": "high",
                "envelope_type": "brenn",
            }))
            .expect("the envelope shape the peer sends"),
            seq: 1,
            dropped: 0,
        }
    }

    /// A refusal for a channel no hold covers schedules nothing and says it
    /// scheduled nothing. Scheduling it anyway would put the driver in a re-ask
    /// loop for a channel it never held, and a `retry_in_ms` on the line would
    /// read to a log query as a channel this pod owns.
    #[tokio::test]
    async fn an_unheld_channel_refusal_schedules_no_re_ask() {
        let (mut driver, path, _dir, _writer) = unrun(brenn_config(""), None).await;

        driver.on_unavailable("brenn:pod.invented".to_string());
        assert!(
            driver.next_resubscribe().is_none(),
            "nothing is owed for a channel this driver never asked for"
        );
        let line = expect_line(&path, "brenn_channel_unavailable").await;
        assert_eq!(line["channel"], "brenn:pod.invented");
        assert!(
            line["retry_in_ms"].is_null(),
            "a channel nobody holds is not coming back: {line}"
        );

        // And the held channel still schedules its own, so the lookup is a match
        // rather than a blanket refusal to schedule.
        driver.on_unavailable(RESPONSE.to_string());
        assert!(driver.next_resubscribe().is_some());
    }

    /// A body arriving on the configured motion channel with no sink wired is a
    /// narrated drop, not a panic and not silence: the subscription was never
    /// stated, so a peer delivering there decided the routing on its own.
    #[tokio::test]
    async fn a_motion_delivery_with_no_sink_is_dropped_loudly() {
        let (driver, path, _dir, _writer) = unrun(
            brenn_config(&format!("motion_channel = \"{MOTION}\"")),
            None,
        )
        .await;

        driver.on_delivery(delivery(MOTION, "{\"type\":\"motion-script\"}"));
        let line = expect_line(&path, "brenn_delivery_dropped").await;
        assert_eq!(line["reason"], "unexpected_channel");
        assert_eq!(line["channel"], MOTION);
    }
}
