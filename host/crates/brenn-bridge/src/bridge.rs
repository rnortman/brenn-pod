//! The supervised attachment: one task owning the connection, the subscription
//! plane and the outstanding publishes, and the handle a pod application drives
//! it through.
//!
//! The [`Bridge`] task is the only thing that touches the driver, so nothing above
//! it has to reason about reconnects, resubscribes, or which frames may be
//! written right now. A pod application holds a [`BridgeHandle`] — cheap to
//! clone, safe to hold across a reconnect — and a stream of [`BridgeEvent`]s.
//!
//! **Commands are statements, not requests.** `subscribe` says "I want this
//! channel"; the answer arrives later as an event, possibly several reconnects
//! later, because a subscription made while the link is down is re-sent whole at
//! the next attachment. The one exception is `publish`, which answers its
//! caller: a publish is a single act with a single outcome, and an application
//! that cannot tell whether its message landed cannot retry honestly.
//!
//! **Better dead than wrong.** The task ends — it does not reconnect — on a
//! protocol error, on a version range with no overlap, and on a run of
//! attachments that achieved nothing. The last is the shape a rejected frame
//! produces: the peer answers an illegal frame by closing the socket, with
//! nothing on the wire to say why, so a bridge that keeps re-sending it would
//! reconnect into the same refusal until the peer's abuse defences banned the
//! pod. Ending loudly is the honest answer; the process supervisor's restart
//! backoff is what retries, and the log says what happened.

use std::collections::VecDeque;
use std::fmt;

use brenn_attach_client::TransportConnector;
use brenn_attach_client::conn::{AttachmentFacts, ConnConfig, ConnEvent, ConnInput, DetachReason};
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::publish::{PendingPublishes, PublishRequest};
use brenn_attach_client::subs::{
    DeliverDisposition, ResumePolicy, SubscribeSettlement, SubscriptionDepths, Subscriptions,
};
use brenn_attach_client::transport::native::NativeConnector;
use brenn_attach_proto::{
    AlertSeverity, ClientFrame, GapInfo, PublishOutcome, ServerFrame, VersionRange,
};
use brenn_envelope::MessageEnvelope;
use tokio::sync::{mpsc, oneshot};

use crate::config::{Config, ConfigError, Token};
use crate::exit;

/// Depth of the command channel between the handles and the task.
///
/// Commands are application-rate — a roster reconcile, a wake word, a
/// transcript — not data-plane rate. A full channel makes the caller wait for
/// the task, which is the right backpressure: the task is one step from the
/// socket.
const COMMAND_CAPACITY: usize = 64;

/// Depth of the event channel from the task to the embedder.
///
/// Bounded and awaited rather than dropped: an embedder that falls behind slows
/// the socket read, the peer's bounded queues drop live copies, and the
/// subscription resumes from its held cursor. Dropping events here instead would
/// lose messages the peer believes were delivered, with no cursor left to
/// recover them from.
const EVENT_CAPACITY: usize = 256;

/// One message this attachment was delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub channel: String,
    /// The envelope as the peer stamped it. `sender` is the peer's own
    /// attribution of the publisher, not the publisher's claim about itself.
    pub envelope: MessageEnvelope,
    /// The delivery's span sequence, restarting at 1 with each subscription
    /// span.
    pub seq: u64,
    /// Messages this attachment lost on the channel before this one, because the
    /// channel's window rolled past this attacher's position.
    pub dropped: u64,
}

/// Something the attachment did that a pod application must know about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeEvent {
    /// The attachment is live and every held subscription has been re-sent.
    /// Carries what the peer stated about this connection: the identity it
    /// resolved, the body and frame caps it will enforce, and whether the alert
    /// plane is granted.
    Attached(AttachmentFacts),
    /// The attachment went away; a reconnect is on the backoff schedule.
    /// Subscriptions are held and re-sent at the next attachment.
    Detached { reason: DetachReason },
    /// A dial did not reach a socket; the next attempt is on the backoff
    /// schedule. `timed_out` distinguishes an attempt that outlived
    /// `reconnect.connect_timeout_ms` from one that failed before it.
    ///
    /// All failure modes — refused TCP connect, TLS failure, bad hostname,
    /// rejected upgrade — are collapsed into this single event; no further
    /// detail is available. The event exists so a pod that never attaches does
    /// not look exactly like a pod with nothing to say.
    ConnectFailed { timed_out: bool },
    /// A subscription opened. `replay_count` is how many retained messages are
    /// about to arrive; `gap` is present when the resume claim could not be
    /// covered from the retained window, which is a staleness report and not an
    /// error.
    Subscribed {
        channel: String,
        replay_count: u32,
        gap: Option<GapInfo>,
    },
    /// The channel is inside this bridge's grants but is not there right now.
    ///
    /// Nothing is retried: the hold has been dropped, so a reattach does not
    /// resubscribe it, and asking again is the application's call — whatever
    /// told it the channel exists is what tells it to try again.
    Unavailable { channel: String },
    /// A message arrived on a subscribed channel.
    Delivered(Delivery),
}

/// Why the attachment ended for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeOutcome {
    /// The embedder asked for a shutdown.
    Closed,
    /// Every handle and the event receiver were dropped: nothing is left to
    /// drive or to hear the bridge, so it winds down.
    EmbedderGone,
    /// A server frame could not be reconciled with the protocol, or this bridge
    /// produced a statement the protocol does not admit.
    Fatal { detail: String },
    /// The two ends' version ranges do not overlap. Both are build constants,
    /// so retrying against the same peer build can only reach the same verdict.
    Incompatible {
        ours: VersionRange,
        theirs: VersionRange,
    },
    /// The peer closed with a code this bridge declared terminal.
    PeerClosedTerminal { code: u16, reason: String },
    /// This many consecutive attachments sent something and were answered
    /// nothing before the socket died — the shape a refused frame produces.
    Futile { attachments: u32 },
}

impl BridgeOutcome {
    /// The process exit code this outcome deserves. Zero for the two orderly
    /// endings; see [`crate::exit`] for the rest.
    pub fn exit_code(&self) -> u8 {
        match self {
            BridgeOutcome::Closed | BridgeOutcome::EmbedderGone => 0,
            BridgeOutcome::Incompatible { .. } => exit::VERSION_INCOMPATIBLE,
            BridgeOutcome::Fatal { .. }
            | BridgeOutcome::PeerClosedTerminal { .. }
            | BridgeOutcome::Futile { .. } => exit::HARD_FAILURE,
        }
    }
}

impl fmt::Display for BridgeOutcome {
    /// One line an operator can act on. The incompatible arm spells both ranges
    /// out: which side to update is the whole content of that failure.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeOutcome::Closed => f.write_str("the bridge was asked to shut down"),
            BridgeOutcome::EmbedderGone => {
                f.write_str("every bridge handle and the event receiver were dropped")
            }
            BridgeOutcome::Fatal { detail } => write!(f, "attachment protocol error: {detail}"),
            BridgeOutcome::Incompatible { ours, theirs } => write!(
                f,
                "no wire version in common: this bridge speaks {}..={}, the server speaks {}..={}",
                ours.min, ours.max, theirs.min, theirs.max
            ),
            BridgeOutcome::PeerClosedTerminal { code, reason } => {
                write!(f, "the server closed with code {code}: {reason}")
            }
            BridgeOutcome::Futile { attachments } => write!(
                f,
                "{attachments} consecutive attachments were answered nothing after sending; \
                 the server is refusing what this bridge sends"
            ),
        }
    }
}

/// The bridge task is gone: it ended, or it was never started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the bridge task is no longer running")]
pub struct BridgeGone;

/// Why a publish produced no outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    /// The bridge task is gone.
    #[error("the bridge task is no longer running")]
    Gone,
    /// There was no live attachment to carry the publish, or the one carrying it
    /// went away before the peer answered. Nothing was delivered and nothing was
    /// half-delivered: the caller may retry once reattached.
    #[error("the publish was not carried: the attachment was down or went down")]
    Lost,
}

/// A statement from a pod application to the bridge task.
enum Command {
    Subscribe {
        channel: String,
        depths: SubscriptionDepths,
        resume: ResumePolicy,
    },
    Unsubscribe {
        channel: String,
    },
    Publish {
        request: PublishRequest,
        answer: oneshot::Sender<Result<PublishOutcome, PublishError>>,
    },
    Alert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    Shutdown,
}

/// A pod application's grip on the bridge. Cheap to clone; every clone drives
/// the same attachment.
#[derive(Clone)]
pub struct BridgeHandle {
    commands: mpsc::Sender<Command>,
}

impl BridgeHandle {
    /// Hold `channel` at these depths.
    ///
    /// Returns once the task has the statement, not once the peer has answered
    /// it: the answer is a [`BridgeEvent::Subscribed`] or
    /// [`BridgeEvent::Unavailable`]. Holding is idempotent and refcounted — two
    /// callers holding one channel keep it subscribed until both release it.
    ///
    /// Three preconditions the subscription plane asserts on, so breaking one
    /// panics the bridge task rather than answering an error. They are cheap for
    /// a caller to check and expensive for the bridge to carry as state:
    ///
    /// - `channel` must be transportable — a `local:` address is confined to the
    ///   attacher and never crosses the wire.
    /// - At least one of `depths.push_depth` and `depths.retain_depth` must be
    ///   non-zero; a subscription stating neither asks for nothing.
    /// - While a hold is live, every further acquisition of that channel must
    ///   state the *identical* depths and resume policy. Two components holding
    ///   one channel differently is not a fold the plane can resolve; agree on
    ///   the statement, or give each its own channel.
    pub async fn subscribe(
        &self,
        channel: impl Into<String>,
        depths: SubscriptionDepths,
        resume: ResumePolicy,
    ) -> Result<(), BridgeGone> {
        self.send(Command::Subscribe {
            channel: channel.into(),
            depths,
            resume,
        })
        .await
    }

    /// Drop this caller's hold on `channel`.
    pub async fn unsubscribe(&self, channel: impl Into<String>) -> Result<(), BridgeGone> {
        self.send(Command::Unsubscribe {
            channel: channel.into(),
        })
        .await
    }

    /// Publish one message and wait for the peer's outcome.
    ///
    /// [`PublishError::Lost`] means nothing was carried — there was no live
    /// attachment, or the one carrying it died before the answer. Retrying after
    /// the next [`BridgeEvent::Attached`] is safe with respect to this call;
    /// whether it is safe with respect to the *application* is the application's
    /// question, since a publish lost between the socket and the answer may
    /// still have landed.
    pub async fn publish(&self, request: PublishRequest) -> Result<PublishOutcome, PublishError> {
        let (answer, wait) = oneshot::channel();
        self.send(Command::Publish { request, answer })
            .await
            .map_err(|BridgeGone| PublishError::Gone)?;
        wait.await.unwrap_or(Err(PublishError::Gone))
    }

    /// Raise an operator alert. Fire-and-forget by the plane's own shape: a
    /// granted alert is answered by no frame at all, and an ungranted one by a
    /// closed socket.
    pub async fn alert(
        &self,
        severity: AlertSeverity,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<(), BridgeGone> {
        self.send(Command::Alert {
            severity,
            title: title.into(),
            body: body.into(),
        })
        .await
    }

    /// End the attachment. The task closes the socket and returns
    /// [`BridgeOutcome::Closed`].
    pub async fn shutdown(&self) -> Result<(), BridgeGone> {
        self.send(Command::Shutdown).await
    }

    async fn send(&self, command: Command) -> Result<(), BridgeGone> {
        self.commands.send(command).await.map_err(|_| BridgeGone)
    }
}

/// The supervised attachment. Build one, then `run` it — on its own task, or as
/// one arm of the embedder's own loop.
pub struct Bridge<C: TransportConnector> {
    core: Core<C>,
    commands: mpsc::Receiver<Command>,
}

impl Bridge<NativeConnector> {
    /// Build a bridge that dials the configured URL with the configured bearer
    /// token.
    ///
    /// The config is validated and the token is read here, so both the URL
    /// posture and the token file's posture are structurally impossible to skip:
    /// there is no way to reach a socket without passing through them.
    /// [`Config`]'s fields are public and [`Config::parse`] does not validate,
    /// so an embedder composing a config in memory would otherwise lose the
    /// wss-only refusal and the anti-wedge timing floors that [`Config::load`]
    /// enforces.
    pub fn new(
        config: &Config,
    ) -> Result<(Self, BridgeHandle, mpsc::Receiver<BridgeEvent>), ConfigError> {
        config
            .validate()
            .map_err(|message| ConfigError::Rejected { message })?;
        let token = Token::load(&config.token_file)?;
        Ok(Bridge::with_connector(
            config.conn_config(),
            config.reconnect.max_futile_attachments,
            NativeConnector::with_bearer(token.into_inner()),
        ))
    }
}

impl<C: TransportConnector> Bridge<C> {
    /// Build a bridge over an arbitrary connector — the seam a test drives a
    /// scripted peer through, and the seam a future transport slots into.
    pub fn with_connector(
        conn: ConnConfig,
        max_futile_attachments: u32,
        connector: C,
    ) -> (Self, BridgeHandle, mpsc::Receiver<BridgeEvent>) {
        assert!(
            max_futile_attachments > 0,
            "brenn-bridge: a futile-attachment budget of zero ends the process before it attaches"
        );
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
        let bridge = Bridge {
            core: Core {
                driver: AttachDriver::new(conn, connector),
                subs: Subscriptions::new(),
                pending: PendingPublishes::new(),
                next_correlation: 1,
                events: event_tx,
                max_futile: max_futile_attachments,
                futile: 0,
                spoke: false,
                answered: false,
                outcome: None,
            },
            commands: command_rx,
        };
        (
            bridge,
            BridgeHandle {
                commands: command_tx,
            },
            event_rx,
        )
    }

    /// Drive the attachment until it ends.
    ///
    /// The two arms are the embedder's commands and the driver's I/O, and
    /// commands are biased first: they arrive at application rate while
    /// deliveries can arrive in floods, so the other bias would let a busy
    /// channel starve a shutdown. A connect attempt is run outside the select
    /// entirely — dropping that future abandons the attempt, so racing it
    /// against a command would restart the dial on every command and finish
    /// none.
    pub async fn run(self) -> BridgeOutcome {
        let Bridge {
            mut core,
            mut commands,
        } = self;
        let step = core.driver.start().await;
        core.absorb(step).await;
        while core.outcome.is_none() {
            if let Some(url) = core.driver.take_pending_connect() {
                let input = core.driver.connect(&url).await;
                core.report_dial(&input).await;
                let step = core.driver.on_input(input).await;
                core.absorb(step).await;
                continue;
            }
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    Some(command) => core.on_command(command).await,
                    None => core.finish(BridgeOutcome::EmbedderGone),
                },
                io = core.driver.wait() => core.on_io(io).await,
            }
        }
        core.outcome
            .expect("the loop exits only once an outcome is set")
    }
}

/// Everything the task owns but the command receiver, which the run loop must
/// borrow separately to select on.
struct Core<C: TransportConnector> {
    driver: AttachDriver<C>,
    subs: Subscriptions,
    /// Valued by the oneshot each publish's caller is waiting on.
    pending: PendingPublishes<oneshot::Sender<Result<PublishOutcome, PublishError>>>,
    next_correlation: u64,
    events: mpsc::Sender<BridgeEvent>,
    max_futile: u32,
    /// Consecutive attachments that sent something and were answered nothing.
    futile: u32,
    /// Whether anything has gone out on the current attachment.
    spoke: bool,
    /// Whether the peer has answered anything on the current attachment.
    answered: bool,
    outcome: Option<BridgeOutcome>,
}

impl<C: TransportConnector> Core<C> {
    /// Act on one embedder statement.
    async fn on_command(&mut self, command: Command) {
        match command {
            Command::Subscribe {
                channel,
                depths,
                resume,
            } => {
                let frames = self.subs.acquire(&channel, depths, resume);
                self.write(frames).await;
            }
            Command::Unsubscribe { channel } => {
                // A hold the plane has already dropped is released by nobody: an
                // `Unavailable` answer clears every hold on the channel, and the
                // application whose `unsubscribe` was in flight — or whichever of
                // two holders reaches this first — cannot have known. Releasing
                // an unheld channel panics the plane, and this race is the peer's
                // ordinary answer to a deprovisioned channel, not a bookkeeping
                // bug in the application.
                if self.subs.refcount(&channel) == 0 {
                    return;
                }
                let frames = self.subs.release(&channel);
                self.write(frames).await;
            }
            Command::Publish { request, answer } => {
                // Checked before the publish is registered: off a live
                // attachment the frame would be dropped at the wire and its
                // caller would wait for an answer that only a detach it has
                // already missed could produce.
                if !self.driver.is_active() {
                    let _ = answer.send(Err(PublishError::Lost));
                    return;
                }
                let correlation = self.next_correlation;
                self.next_correlation += 1;
                let frame = self.pending.send(correlation, answer, request);
                self.write(vec![frame]).await;
            }
            Command::Alert {
                severity,
                title,
                body,
            } => {
                self.write(vec![ClientFrame::Alert {
                    severity,
                    title,
                    body,
                }])
                .await;
            }
            Command::Shutdown => {
                let step = self.driver.close().await;
                self.absorb(step).await;
                self.finish(BridgeOutcome::Closed);
            }
        }
    }

    /// Report a dial that reached no socket, so a bridge that never attaches
    /// says so instead of going silent.
    ///
    /// The connect turn is the only place this is knowable: a failed attempt
    /// produces no connection event of its own, and the futile-attachment budget
    /// counts only attachments that spoke, so nothing else in this loop can tell
    /// a dead server from a healthy quiet one.
    ///
    /// TODO(bridge-upgrade-rejection-terminal): a 401 on the upgrade is one of
    /// these, and it should end the process rather than redial into the server's
    /// auth perimeter forever — but the connector collapses the rejection's HTTP
    /// status into a string the driver drops, so this bridge cannot tell it from
    /// a netsplit.
    async fn report_dial(&mut self, input: &ConnInput) {
        // `AttachDriver::connect` answers exactly these three.
        let timed_out = match input {
            ConnInput::Opened => return,
            ConnInput::Tick => true,
            _ => false,
        };
        self.publish(BridgeEvent::ConnectFailed { timed_out }).await;
    }

    /// Act on one thing the driver woke for.
    async fn on_io(&mut self, io: IoEvent) {
        let step = match io {
            IoEvent::Conn(input) => self.driver.on_input(input).await,
            // This bridge registers no outbox and hosts no confined channel, so
            // neither of those deadlines can be armed. A fired one is a bug in
            // this crate, and the fatal path is where a bug in this crate goes.
            other => {
                self.driver
                    .host_fatal(format!("an unarmed deadline fired: {other:?}"))
                    .await
            }
        };
        self.absorb(step).await;
    }

    /// Turn one driver step into events and the frames the planes answered with.
    ///
    /// Iterative rather than recursive: writing frames produces a step of its
    /// own — a failed write is a lost transport — and that step joins this queue
    /// instead of nesting an async call inside itself.
    async fn absorb(&mut self, step: DriverStep) {
        let mut queue = VecDeque::from([step]);
        while let Some(step) = queue.pop_front() {
            for event in step.events {
                if let Some(next) = self.on_conn_event(event).await {
                    queue.push_back(next);
                }
            }
            let Some(frame) = step.routed else { continue };
            self.answered = true;
            match self.route(frame).await {
                Ok(frames) => {
                    if let Some(next) = self.emit(frames).await {
                        queue.push_back(next);
                    }
                }
                // The planes above the connection judge the peer against a
                // contract the connection cannot check. A frame that fails one
                // of those checks is a peer that is not keeping it, which is as
                // terminal as a frame that would not parse.
                Err(detail) => queue.push_back(self.driver.host_fatal(detail).await),
            }
        }
    }

    /// Record one connection event, answering with the step any frames it owed
    /// produced.
    async fn on_conn_event(&mut self, event: ConnEvent) -> Option<DriverStep> {
        match event {
            ConnEvent::Attached(facts) => {
                let frames = self.subs.on_attached();
                self.spoke = false;
                self.answered = false;
                // Before the resubscribes go out: an embedder that reconciles
                // its held set against a fresh attachment must see the
                // attachment first.
                self.publish(BridgeEvent::Attached(facts)).await;
                self.emit(frames).await
            }
            ConnEvent::Detached { reason } => {
                self.subs.on_detached();
                for (_, answer) in self.pending.fail_all() {
                    let _ = answer.send(Err(PublishError::Lost));
                }
                if self.spoke && !self.answered {
                    self.futile += 1;
                } else {
                    self.futile = 0;
                }
                self.spoke = false;
                self.answered = false;
                self.publish(BridgeEvent::Detached { reason }).await;
                if self.futile >= self.max_futile {
                    self.finish(BridgeOutcome::Futile {
                        attachments: self.futile,
                    });
                }
                None
            }
            ConnEvent::Fatal { detail } => {
                self.finish(BridgeOutcome::Fatal { detail });
                None
            }
            ConnEvent::Incompatible { ours, theirs } => {
                self.finish(BridgeOutcome::Incompatible { ours, theirs });
                None
            }
            ConnEvent::PeerClosedTerminal { code, reason } => {
                self.finish(BridgeOutcome::PeerClosedTerminal { code, reason });
                None
            }
        }
    }

    /// Route one server frame into the plane that owns it, answering with the
    /// frames that plane wants sent.
    ///
    /// An `Err` is the peer breaking the protocol, not this bridge's own bug.
    async fn route(&mut self, frame: ServerFrame) -> Result<Vec<ClientFrame>, String> {
        match frame {
            ServerFrame::SubscribeResult {
                channel,
                outcome,
                replay_count,
                gap,
            } => {
                match self
                    .subs
                    .on_subscribe_result(&channel, outcome, replay_count, gap)?
                {
                    SubscribeSettlement::Opened(ack) => {
                        let frames = ack.frames.clone();
                        self.publish(BridgeEvent::Subscribed {
                            channel,
                            replay_count: ack.replay_count,
                            gap: ack.gap,
                        })
                        .await;
                        Ok(frames)
                    }
                    SubscribeSettlement::Unavailable => {
                        self.publish(BridgeEvent::Unavailable { channel }).await;
                        Ok(Vec::new())
                    }
                }
            }
            ServerFrame::Deliver { channel, rows } => {
                match self.subs.on_deliver(&channel, &rows)? {
                    // One pass is one delivery point on the wire and still N
                    // messages an application reads, so each row is its own
                    // event carrying its own wire facts.
                    DeliverDisposition::Accept { .. } => {
                        for row in rows {
                            self.publish(BridgeEvent::Delivered(Delivery {
                                channel: channel.clone(),
                                envelope: row.envelope,
                                seq: row.seq,
                                dropped: row.dropped,
                            }))
                            .await;
                        }
                    }
                    // A pass from a span this bridge has already left, in flight
                    // when its `Unsubscribe` crossed. It advanced nothing.
                    DeliverDisposition::Discard { .. } => {}
                }
                Ok(Vec::new())
            }
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                let answer = self.pending.on_result(correlation)?;
                let _ = answer.send(Ok(outcome));
                Ok(Vec::new())
            }
            // This bridge composes no batch and parks nothing, so the peer owes
            // it neither answer. Either one means the two ends disagree about
            // what was sent.
            ServerFrame::PublishBatchResult { correlation, .. } => Err(format!(
                "a batch result (correlation {correlation}) for a bridge that sends no batches"
            )),
            ServerFrame::DeferredView { channel, .. } => Err(format!(
                "a deferred view on {channel} for a bridge that parks nothing"
            )),
            other => Err(format!(
                "a frame the connection should have consumed reached a plane above it: {other:?}"
            )),
        }
    }

    /// Write frames the planes produced, or drop them when there is no wire.
    ///
    /// Dropping while detached is correct: subscriptions re-send their entire
    /// held set at the next attachment, so a frame dropped here is re-stated
    /// rather than lost.
    async fn emit(&mut self, frames: Vec<ClientFrame>) -> Option<DriverStep> {
        if frames.is_empty() || !self.driver.is_active() {
            return None;
        }
        self.spoke = true;
        Some(self.driver.send(frames).await)
    }

    /// [`emit`](Core::emit) for a caller-initiated write, absorbing what it
    /// produced.
    async fn write(&mut self, frames: Vec<ClientFrame>) {
        if let Some(step) = self.emit(frames).await {
            self.absorb(step).await;
        }
    }

    /// Hand one event to the embedder, ending the attachment if nothing is
    /// listening any more.
    async fn publish(&mut self, event: BridgeEvent) {
        if self.events.send(event).await.is_err() {
            self.finish(BridgeOutcome::EmbedderGone);
        }
    }

    /// Record why the attachment is ending. The first answer wins: a terminal
    /// event and the embedder's own shutdown can land in one step, and what
    /// ended the attachment is whichever got there first.
    fn finish(&mut self, outcome: BridgeOutcome) {
        if self.outcome.is_none() {
            self.outcome = Some(outcome);
        }
    }
}

#[cfg(test)]
mod tests;
