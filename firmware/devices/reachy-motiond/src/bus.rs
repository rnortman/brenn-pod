//! The bus thread: the attachment, the presence subscription, and the fold from
//! a delivery into the lease.
//!
//! Everything here is async and none of it touches a servo. Its whole job is to
//! keep an attachment up, decode what arrives on one channel, and write the
//! result into the lease the motion thread reads between moves. It has one other
//! duty, and it is the reason the fault cell exists: an operator alert leaves
//! over the bus, and the thread that faulted cannot send one — it holds a serial
//! port and by then it is deliberately commanding nothing at all.
//!
//! The loop is a single `select!` with no spawned tasks. The signal streams, the
//! deliveries, the bridge's own ending and the periodic look at the cells are
//! all arms of it. That shape is deliberate: nothing here has to be `'static`,
//! so the sink stays a borrow and the daemon's output has one owner.
//!
//! Decisions that arrive between two ticks of that look — a fault to alert on, a
//! stop to shut down for, a subscription to state again — are decided
//! synchronously by [`Listener::chore`] and awaited by the loop. Which keeps the
//! deciding testable without a socket.

use std::fmt;
use std::future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use brenn_bridge::render::{attached_fields, detached_fields, gap_reason};
use brenn_bridge::{
    AlertSeverity, Bridge, BridgeEvent, BridgeHandle, BridgeOutcome, Delivery, ResumePolicy,
    SubscriptionDepths, TransportConnector,
};
use presence_proto::{DecodeError, PresenceBody, Reduction};
use serde_json::json;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::cells::{Delivered, Shared, Stop};
use crate::report::Sink;

#[cfg(test)]
mod scripted;

/// Subscription statement for the presence channel.
///
/// Live only. A presence intent is a statement about right now, and a retained
/// one is worse than none: an *engaged* published yesterday and replayed at a
/// reattach would pop the head up at three in the morning. The push depth
/// satisfies the plane's "at least one non-zero" precondition with room for a
/// burst; a missed intent is repaired by the publisher's next refresh, and the
/// lease covers the gap either way.
pub const PRESENCE_DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 0,
};

/// See [`PRESENCE_DEPTHS`]: a reattach replays nothing.
pub const PRESENCE_RESUME: ResumePolicy = ResumePolicy::Cursorless;

/// How often the loop looks at the cells the motion thread writes, and at its
/// own timers.
///
/// This is the latency of an operator alert after a fault, and nothing else
/// depends on it: the motion thread has already stopped commanding by the time
/// the cell is set, so nothing is waiting on this tick to become safe.
const WATCH_POLL: Duration = Duration::from_millis(200);

/// How long to wait before asking again for a presence channel the peer said was
/// not there.
///
/// Nothing else retries it — the bridge drops the hold with the refusal — and a
/// daemon with no channel is a daemon that will never hear an intent again. It
/// would still be safe, because a head that hears nothing stays stowed; it would
/// just be useless until somebody restarted it.
const RESUBSCRIBE_DELAY: Duration = Duration::from_secs(30);

/// How long a fault alert with nowhere to go delays the shutdown behind it
/// before the daemon gives up on delivering it.
///
/// An alert handed to a detached bridge is written nowhere and answers `Ok`, so
/// waiting for a wire is the only way it reaches an operator. A daemon that is
/// merely parked waits indefinitely — nothing is pressing and the attachment may
/// come back. This bound exists for the one case where waiting would wedge the
/// exit instead: a fault taken before the first attachment, against a server that
/// is not there to attach to.
const ALERT_GRACE: Duration = Duration::from_secs(5);

/// Something the loop must go and do, decided from the cells and the timers.
///
/// Separated from the doing because the deciding is the part worth testing: each
/// one is awaited by the loop, and each is produced at most once per thing that
/// warrants it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chore {
    /// The machine has faulted. Raise this alert, once.
    Alert { title: String, body: String },
    /// The daemon is stopping for this reason: end the attachment.
    Shutdown(Stop),
    /// State the presence hold again, after a peer that would not serve it.
    Resubscribe,
}

/// The bus thread's state: what it has been told, and what it still owes.
///
/// Holds the shared cells and the sink, and nothing else that survives a
/// delivery — the lease is the state, and it lives where both threads can reach
/// it.
pub struct Listener<'a> {
    shared: Arc<Shared>,
    channel: String,
    ttl: Duration,
    sink: &'a dyn Sink,
    /// Whether the fault alert is settled: delivered to a live attachment, or
    /// abandoned because there was never going to be one. Once either way: a
    /// fault does not improve, and a daemon parked for an hour must not alert for
    /// an hour.
    alerted: bool,
    /// Whether there is an attachment to write over. An alert produced while
    /// this is false is dropped at the wire and still answers `Ok`, so it is not
    /// produced at all until it is true.
    attached: bool,
    /// Whether the wait for an attachment to alert over has been said once.
    alert_waiting: bool,
    /// Whether the shutdown chore has already been produced.
    shutting_down: bool,
    /// When the shutdown first became due, which is what bounds the wait an
    /// undelivered alert may impose on it.
    shutdown_due_at: Option<Instant>,
    /// When to state the presence hold again, or `None` when it is not in doubt.
    resubscribe_at: Option<Instant>,
}

impl<'a> Listener<'a> {
    /// A listener over `shared`, granting each intent a term of `ttl`.
    pub fn new(
        shared: Arc<Shared>,
        channel: impl Into<String>,
        ttl: Duration,
        sink: &'a dyn Sink,
    ) -> Self {
        Self {
            shared,
            channel: channel.into(),
            ttl,
            sink,
            alerted: false,
            attached: false,
            alert_waiting: false,
            shutting_down: false,
            shutdown_due_at: None,
            resubscribe_at: None,
        }
    }

    /// The channel this daemon's intents arrive on.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Render one bridge event, and fold a delivery into the lease.
    ///
    /// Nothing here awaits and nothing here fails: every refusal a delivery can
    /// produce is reported and dropped. A consumer that stopped on a malformed
    /// body would let one bad publisher stow a head for good.
    pub fn on_event(&mut self, event: BridgeEvent) {
        match event {
            BridgeEvent::Attached(facts) => {
                self.attached = true;
                self.sink.line(&format!(
                    "bus: attached as {} (alerts {})",
                    facts.participant_id,
                    if facts.alert_granted {
                        "granted"
                    } else {
                        "not granted — a fault will not reach an operator over the bus"
                    }
                ));
                self.sink.event("bus_attached", &attached_fields(&facts));
            }
            BridgeEvent::Detached { reason } => {
                self.attached = false;
                self.sink.line(&format!(
                    "bus: detached ({reason:?}); the lease keeps running and lapses on its own"
                ));
                self.sink.event("bus_detached", &detached_fields(&reason));
            }
            BridgeEvent::ConnectFailed { timed_out } => {
                self.sink
                    .event("bus_connect_failed", &json!({ "timed_out": timed_out }));
            }
            BridgeEvent::Subscribed {
                channel,
                replay_count,
                gap,
            } => {
                if channel == self.channel {
                    self.resubscribe_at = None;
                    self.sink.line(&format!("bus: subscribed to {channel}"));
                }
                self.sink.event(
                    "bus_subscribed",
                    &json!({
                        "channel": channel,
                        "replay_count": replay_count,
                        "gap": gap.as_ref().map(gap_reason),
                    }),
                );
            }
            BridgeEvent::Unavailable { channel } => self.on_unavailable(&channel),
            BridgeEvent::Delivered(delivery) => self.on_delivery(delivery),
        }
    }

    /// The peer will not serve a channel. For the presence channel that means no
    /// intent will ever arrive, so the ask is scheduled again.
    fn on_unavailable(&mut self, channel: &str) {
        let ours = channel == self.channel;
        if ours {
            self.resubscribe_at = Some(Instant::now() + RESUBSCRIBE_DELAY);
            self.sink.line(&format!(
                "bus: {channel} is not being served; asking again in {}s. \
                 nothing will move until it is.",
                RESUBSCRIBE_DELAY.as_secs()
            ));
        }
        self.sink.event(
            "bus_channel_unavailable",
            &json!({
                "channel": channel,
                "retry_in_ms": ours.then(|| RESUBSCRIBE_DELAY.as_millis() as u64),
            }),
        );
    }

    /// Fold one delivery into the lease.
    fn on_delivery(&mut self, delivery: Delivery) {
        // The lease's provenance, made an invariant rather than a coincidence of
        // this daemon holding exactly one subscription: the channel is the whole
        // of what says a body was authored by something entitled to move this
        // head, and the vocabulary is expected to grow other channels.
        if delivery.channel != self.channel {
            self.ignored(
                "foreign_channel",
                &format!(
                    "delivered on {:?}, and this machine obeys {:?}",
                    delivery.channel, self.channel
                ),
                &delivery,
            );
            return;
        }

        if delivery.dropped > 0 {
            // Not an error: the window rolled past this attachment. The next
            // refresh restates the desired posture, and a lease that lapsed in
            // the meantime stowed the head, which is the safe direction.
            self.sink.event(
                "presence_delivery_dropped",
                &json!({ "count": delivery.dropped }),
            );
        }

        let body = match PresenceBody::decode(&delivery.envelope.body) {
            Ok(body) => body,
            Err(error) => {
                self.ignored(ignored_reason(&error), &error.to_string(), &delivery);
                return;
            }
        };

        match self.shared.apply(&body, Instant::now(), self.ttl) {
            Delivered::Faulted => {
                self.ignored(
                    "faulted",
                    "the machine has faulted and takes no commands",
                    &delivery,
                );
            }
            Delivered::Reduced(Reduction::Foreign) => {
                self.ignored(
                    "foreign",
                    &format!("addressed to {:?}, not to this machine", body.pod),
                    &delivery,
                );
            }
            Delivered::Reduced(reduction) => {
                self.sink.line(&format!(
                    "presence: {} (seq {})",
                    body.state.as_str(),
                    body.seq
                ));
                self.sink.event(
                    "presence_intent",
                    &json!({
                        "pod": body.pod,
                        "state": body.state.as_str(),
                        "seq": body.seq,
                        "sender": delivery.envelope.sender,
                        "held_for_ms": match reduction {
                            Reduction::Engaged { .. } => Some(self.ttl.as_millis() as u64),
                            _ => None,
                        },
                    }),
                );
            }
        }
    }

    /// Report a delivery that changed nothing. Never an error and never a stop:
    /// this channel is expected to grow other tenants, and this daemon is not
    /// one of the machines most of them are about.
    fn ignored(&self, reason: &str, detail: &str, delivery: &Delivery) {
        self.sink.event(
            "presence_ignored",
            &json!({
                "reason": reason,
                "detail": detail,
                "sender": delivery.envelope.sender,
            }),
        );
    }

    /// An operator's signal. The one ending that may take torque off, so it is
    /// the one that says so on the way out — and the one that must not say so
    /// when it will not happen.
    ///
    /// A faulted machine takes no commands at all, this signal included: it is
    /// not stowed, not verified and not released, and torque stays exactly where
    /// the fault left it. Saying "releasing" to the operator at the moment they
    /// act would be a false statement about the one fact that decides whether
    /// they can put a hand on the head.
    pub fn on_signal(&self, name: &str) {
        if !self.shared.request_stop(Stop::Operator) {
            self.sink.line(&format!(
                "{name}: a shutdown is already under way and will not be restarted. \
                 killing the process instead leaves the servos holding their goals."
            ));
            return;
        }

        let faulted = self.shared.faulted();
        if faulted {
            self.sink.line(&format!(
                "{name}: the machine has faulted, so nothing will be commanded and torque \
                 stays on. the head is wherever the fault left it. exiting."
            ));
        } else {
            self.sink.line(&format!(
                "{name}: stowing, verifying and releasing. this is the operator action \
                 that takes torque off."
            ));
        }
        self.sink.event(
            "stop_requested",
            &json!({ "signal": name, "stop": "operator", "will_release": !faulted }),
        );
    }

    /// The next thing the loop owes, or `None` when it owes nothing.
    ///
    /// Called repeatedly until it answers `None`: each answer clears the state
    /// that produced it, so it terminates. The order is the order the answers
    /// matter in — the fault is reported before the attachment it would be
    /// reported over is shut down, and the shutdown waits for the motion thread
    /// to be finished with the machine so a fault taken on the way out is still
    /// one of the answers.
    pub fn chore(&mut self, now: Instant) -> Option<Chore> {
        if !self.alerted
            && let Some(report) = self.shared.fault()
        {
            if self.attached {
                self.alerted = true;
                return Some(Chore::Alert {
                    title: "reachy head motion stopped".to_owned(),
                    body: format!(
                        "{report}. commanding has stopped and torque is untouched: the servos \
                         are holding where they were left. nothing will move again until an \
                         operator restarts the daemon."
                    ),
                });
            }
            if !self.alert_waiting {
                self.alert_waiting = true;
                self.sink.line(
                    "fault: there is no attachment to alert over. the alert waits for one; \
                     the fault is already on this terminal and in the capture.",
                );
            }
        }

        // The ending, not the request for one: a stow, a nine-position verify
        // and a servo-by-servo release take seconds, and any of them can fault.
        // Closing the attachment when the stop was *asked for* would take the
        // alert channel away exactly in the window where the alert is owed.
        if !self.shutting_down
            && self.shared.motion_ended()
            && let Some(stop) = self.shared.stopping()
        {
            let due = *self.shutdown_due_at.get_or_insert(now);
            if !self.alerted && self.shared.faulted() {
                if now.duration_since(due) < ALERT_GRACE {
                    return None;
                }
                self.abandon_alert();
            }
            self.shutting_down = true;
            return Some(Chore::Shutdown(stop));
        }

        if self.resubscribe_at.is_some_and(|due| now >= due) {
            self.resubscribe_at = None;
            return Some(Chore::Resubscribe);
        }

        None
    }

    /// Give up on delivering the fault alert, and say that it was lost.
    ///
    /// The daemon is exiting and no attachment arrived, so the alert has nowhere
    /// left to go. Losing it silently is what this exists to prevent: the push
    /// channel a fault escalates over simply did not fire, and only the capture
    /// and this terminal will ever carry the fault.
    fn abandon_alert(&mut self) {
        self.alerted = true;
        self.sink.line(
            "fault: the alert never reached the bus — no attachment came up before the daemon \
             exited. the fault is on this terminal and in the capture only.",
        );
        self.sink.event(
            "alert_undelivered",
            &json!({
                "detail": self.shared.fault().map(ToString::to_string),
                "waited_ms": ALERT_GRACE.as_millis() as u64,
            }),
        );
    }

    /// Note that the daemon has lost the source of its intents.
    ///
    /// Stowing without releasing is what follows: the head is parked
    /// deliberately, but nobody is present to catch it, so torque stays on.
    fn on_detached_for_good(&self, outcome: &BridgeOutcome) {
        if self.shared.request_stop(Stop::Detached) {
            self.sink.line(
                "bus: the attachment ended for good, so there is no source of intents left. \
                 stowing and leaving torque on — releasing is an operator's action.",
            );
        }
        self.sink.event(
            "bus_exit",
            &json!({ "outcome": outcome.to_string(), "terminal": true }),
        );
    }
}

/// Whether an ended attachment leaves the daemon with nothing to obey.
///
/// The two orderly endings are answers to something this process asked for; the
/// rest are the bridge giving up, and reconnection is already exhausted inside
/// it by the time an outcome exists.
#[must_use]
pub fn stop_for(outcome: &BridgeOutcome) -> Option<Stop> {
    match outcome {
        BridgeOutcome::Closed | BridgeOutcome::EmbedderGone => None,
        BridgeOutcome::Fatal { .. }
        | BridgeOutcome::Incompatible { .. }
        | BridgeOutcome::PeerClosedTerminal { .. }
        | BridgeOutcome::Futile { .. } => Some(Stop::Detached),
    }
}

/// Nothing can ask this daemon to stow and release any more.
///
/// The reason recorded is the whole of it: [`Stop::Detached`], never
/// [`Stop::Operator`]. A daemon that cannot hear a signal has no operator in
/// front of it, and the operator's reason is the only one that takes torque off.
pub fn no_signals(shared: &Shared, sink: &dyn Sink, error: &dyn fmt::Display) {
    sink.line(&format!(
        "signals: cannot listen for SIGTERM/SIGINT ({error}), so nothing could ask this \
         daemon to release. stowing and leaving torque on."
    ));
    sink.event(
        "signals_unavailable",
        &json!({ "detail": error.to_string() }),
    );
    shared.request_stop(Stop::Detached);
}

/// The same for a bus thread that never got a runtime: with no attachment, no
/// intent can ever arrive, so the daemon has nothing left to obey.
///
/// The same [`Stop::Detached`], for the same reason — a bus thread that failed
/// to start is not an operator asking for the head.
pub fn no_runtime(shared: &Shared, sink: &dyn Sink, error: &dyn fmt::Display) {
    sink.line(&format!(
        "bus: no runtime ({error}), so no intent can ever arrive. stowing and leaving \
         torque on."
    ));
    sink.event("bus_unavailable", &json!({ "detail": error.to_string() }));
    shared.request_stop(Stop::Detached);
}

/// Run the attachment until it ends, folding what arrives into the lease.
///
/// Returns the bridge's own outcome, which is what the process's exit status is
/// built from. The signal streams are installed here rather than by the caller
/// because this is the thread with a runtime under it — and they are installed
/// first, before anything is awaited, so the window in which a `SIGINT` still
/// takes the default action is as short as the daemon can make it.
pub async fn serve<C: TransportConnector>(
    bridge: Bridge<C>,
    handle: &BridgeHandle,
    mut events: mpsc::Receiver<BridgeEvent>,
    mut listener: Listener<'_>,
) -> BridgeOutcome {
    let mut signals = match Signals::install() {
        Ok(signals) => Some(signals),
        Err(error) => {
            no_signals(&listener.shared, listener.sink, &error);
            None
        }
    };

    let mut run = std::pin::pin!(bridge.run());
    let mut watch = tokio::time::interval(WATCH_POLL);
    watch.set_missed_tick_behavior(MissedTickBehavior::Delay);
    subscribe(handle, listener.channel()).await;

    let outcome = loop {
        tokio::select! {
            // Biased: a signal outranks a flood of deliveries, and a delivery
            // outranks the periodic look at cells that are not going anywhere.
            biased;
            name = next_signal(&mut signals) => listener.on_signal(name),
            event = events.recv() => match event {
                Some(event) => listener.on_event(event),
                // The event channel closes as the run future returns, so this
                // is the same ending seen a moment earlier.
                None => break run.as_mut().await,
            },
            outcome = run.as_mut() => break outcome,
            _ = watch.tick() => {
                while let Some(chore) = listener.chore(Instant::now()) {
                    do_chore(handle, listener.channel(), chore).await;
                }
            }
        }
    };

    if stop_for(&outcome).is_some() {
        listener.on_detached_for_good(&outcome);
    } else {
        listener.sink.event(
            "bus_exit",
            &json!({ "outcome": outcome.to_string(), "terminal": false }),
        );
    }
    outcome
}

/// Carry out one chore. Every failure here is the bridge already being gone,
/// which the loop learns about from the arm that owns that fact.
async fn do_chore(handle: &BridgeHandle, channel: &str, chore: Chore) {
    match chore {
        Chore::Alert { title, body } => {
            let _ = handle.alert(AlertSeverity::Critical, title, body).await;
        }
        Chore::Shutdown(_) => {
            let _ = handle.shutdown().await;
        }
        Chore::Resubscribe => subscribe(handle, channel).await,
    }
}

/// State the presence hold. A gone bridge is not reported here: the event
/// channel closing says the same thing, and that arm owns the line.
async fn subscribe(handle: &BridgeHandle, channel: &str) {
    let _ = handle
        .subscribe(channel.to_owned(), PRESENCE_DEPTHS, PRESENCE_RESUME)
        .await;
}

/// The two signals an operator ends a foreground run with.
struct Signals {
    term: Signal,
    interrupt: Signal,
}

impl Signals {
    fn install() -> std::io::Result<Self> {
        Ok(Self {
            term: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    async fn next(&mut self) -> &'static str {
        tokio::select! {
            _ = self.term.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
        }
    }
}

/// The signal arm's future: the streams when they are installed, never when they
/// are not.
async fn next_signal(signals: &mut Option<Signals>) -> &'static str {
    match signals {
        Some(signals) => signals.next().await,
        None => future::pending().await,
    }
}

/// The one-word reason a body changed nothing.
fn ignored_reason(error: &DecodeError) -> &'static str {
    match error {
        DecodeError::NotJson { .. } => "not_json",
        DecodeError::WrongType { .. } => "other_tenant",
        DecodeError::Malformed { .. } => "malformed",
    }
}

#[cfg(test)]
mod tests {
    use brenn_bridge::MessageEnvelope;
    use presence_proto::PresenceState;

    use super::*;
    use crate::cells::{FaultReport, FaultStage};
    use crate::report::Collect;

    const POD: &str = "reachy00";
    const CHANNEL: &str = "brenn:reachy.presence";
    const TTL: Duration = Duration::from_secs(15);

    /// A listener over fresh cells, already attached — which is the state
    /// everything but the attachment tests is about. The sink and the cells come
    /// back for the test to look at.
    fn fixture(sink: &Collect) -> (Arc<Shared>, Listener<'_>) {
        let (shared, mut listener) = detached_fixture(sink);
        listener.on_event(BridgeEvent::Attached(facts(true)));
        (shared, listener)
    }

    /// The same before anything has attached: no wire, so nothing written to the
    /// bridge would reach a peer.
    fn detached_fixture(sink: &Collect) -> (Arc<Shared>, Listener<'_>) {
        let shared = Arc::new(Shared::new(POD));
        let listener = Listener::new(Arc::clone(&shared), CHANNEL, TTL, sink);
        (shared, listener)
    }

    /// What an attachment negotiated. Only `alert_granted` varies across the
    /// tests that use it; the rest is a plausible peer.
    fn facts(alert_granted: bool) -> brenn_bridge::AttachmentFacts {
        brenn_bridge::AttachmentFacts {
            participant_id: "remote:reachy-motiond".to_owned(),
            session_id: "0198f0".to_owned(),
            version: 1,
            heartbeat_secs: 20,
            max_body_bytes: 65_536,
            max_frame_bytes: 131_072,
            alert_granted,
        }
    }

    /// A delivery carrying `body` on the presence channel.
    fn delivered(body: &str) -> BridgeEvent {
        BridgeEvent::Delivered(Delivery {
            channel: CHANNEL.to_owned(),
            envelope: envelope(body),
            seq: 1,
            dropped: 0,
        })
    }

    /// An envelope as the peer stamps one. Built from the wire form rather than
    /// field by field: the envelope is the bus's shape, not this daemon's, and a
    /// fixture that names its fields would have to be revisited whenever it
    /// grows one.
    fn envelope(body: &str) -> MessageEnvelope {
        serde_json::from_value(json!({
            "message_id": "11111111-2222-3333-4444-555555555555",
            "source": "bus",
            "channel": CHANNEL,
            "sender": "speech-host",
            "publish_ts": "2026-08-07T00:00:00Z",
            "body": body,
            "urgency": "normal",
            "envelope_type": "brenn",
        }))
        .expect("envelope fixture")
    }

    fn intent(pod: &str, state: PresenceState, seq: u64) -> String {
        PresenceBody::new(pod, state, seq).encode()
    }

    #[test]
    fn an_engaged_intent_for_this_pod_takes_the_lease() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&intent(POD, PresenceState::Engaged, 7)));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Engaged);
        let fields = sink
            .fields("presence_intent")
            .expect("the intent is reported");
        assert_eq!(fields["state"], json!("engaged"));
        assert_eq!(fields["seq"], json!(7));
        assert_eq!(fields["held_for_ms"], json!(TTL.as_millis() as u64));
    }

    #[test]
    fn an_idle_intent_releases_it_at_once() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&intent(POD, PresenceState::Engaged, 1)));
        listener.on_event(delivered(&intent(POD, PresenceState::Idle, 2)));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
    }

    #[test]
    fn another_machines_intent_moves_nothing_and_is_reported() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&intent("reachy01", PresenceState::Engaged, 1)));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
        assert_eq!(
            sink.fields("presence_ignored").expect("reported")["reason"],
            json!("foreign")
        );
        assert!(!sink.saw("presence_intent"));
    }

    #[test]
    fn a_body_that_is_not_ours_is_reported_by_what_kind_of_not_ours_it_is() {
        for (body, reason) in [
            ("not json at all", "not_json"),
            (r#"{"type":"gaze","yaw":10}"#, "other_tenant"),
            (r#"{"type":"presence","pod":"reachy00"}"#, "malformed"),
        ] {
            let sink = Collect::default();
            let (shared, mut listener) = fixture(&sink);

            listener.on_event(delivered(body));

            assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
            assert_eq!(
                sink.fields("presence_ignored").expect("reported")["reason"],
                json!(reason),
                "for {body}"
            );
        }
    }

    #[test]
    fn a_faulted_machine_takes_no_further_intents() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        shared.set_fault(FaultReport::new(
            FaultStage::Presence,
            "a servo stopped answering",
        ));

        listener.on_event(delivered(&intent(POD, PresenceState::Engaged, 1)));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
        assert_eq!(
            sink.fields("presence_ignored").expect("reported")["reason"],
            json!("faulted")
        );
    }

    #[test]
    fn a_fault_becomes_one_alert_and_never_a_second() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        assert_eq!(listener.chore(now), None);

        shared.set_fault(FaultReport::new(
            FaultStage::Arm,
            "the head is not where it says",
        ));

        let Some(Chore::Alert { title, body }) = listener.chore(now) else {
            panic!("a fault owes an alert");
        };
        assert!(title.contains("motion"), "{title}");
        assert!(body.contains("torque is untouched"), "{body}");
        assert_eq!(listener.chore(now), None);
        // The event half of a fault is the motion thread's, on the thread that
        // took it; this side owes only the alert.
        assert!(!sink.saw("motion_fault"));
    }

    #[test]
    fn a_stop_owes_one_shutdown_and_the_fault_is_reported_first() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        shared.set_fault(FaultReport::new(FaultStage::Presence, "read loss"));
        shared.request_stop(Stop::Operator);
        shared.end_motion();

        assert!(matches!(listener.chore(now), Some(Chore::Alert { .. })));
        assert_eq!(listener.chore(now), Some(Chore::Shutdown(Stop::Operator)));
        assert_eq!(listener.chore(now), None);
    }

    /// The window this exists for: an operator signals, the motion thread spends
    /// seconds stowing and verifying, and the release refuses. The attachment is
    /// still up, so the alert that refusal owes has somewhere to go.
    #[test]
    fn the_attachment_outlives_the_stop_and_carries_the_fault_the_ending_took() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();

        shared.request_stop(Stop::Operator);
        assert_eq!(
            listener.chore(now),
            None,
            "the attachment closed while the machine was still being put away"
        );

        shared.set_fault(FaultReport::new(
            FaultStage::Shutdown,
            "the machine is not at stow",
        ));
        let Some(Chore::Alert { body, .. }) = listener.chore(now) else {
            panic!("a fault taken during the ending owes an alert");
        };
        assert!(body.contains("torque is untouched"), "{body}");

        assert_eq!(listener.chore(now), None);
        shared.end_motion();
        assert_eq!(listener.chore(now), Some(Chore::Shutdown(Stop::Operator)));
    }

    #[test]
    fn a_signal_asks_for_the_release_ending_and_a_second_one_does_not_restart_it() {
        let sink = Collect::default();
        let (shared, listener) = fixture(&sink);

        listener.on_signal("SIGINT");
        assert_eq!(shared.stopping(), Some(Stop::Operator));
        listener.on_signal("SIGINT");

        assert_eq!(shared.stopping(), Some(Stop::Operator));
        let said = sink.said();
        assert!(
            said.lines
                .iter()
                .any(|line| line.contains("already under way")),
            "{:?}",
            said.lines
        );
        assert_eq!(
            said.events
                .iter()
                .filter(|(name, _)| name == "stop_requested")
                .count(),
            1
        );
    }

    #[test]
    fn an_unserved_presence_channel_is_asked_for_again_when_its_delay_has_run() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::Unavailable {
            channel: CHANNEL.to_owned(),
        });
        // Read after the event: the delay runs from when the peer refused, not
        // from when the test started.
        let now = Instant::now();
        assert_eq!(listener.chore(now), None);
        assert_eq!(
            listener.chore(now + RESUBSCRIBE_DELAY),
            Some(Chore::Resubscribe)
        );
        assert_eq!(listener.chore(now + RESUBSCRIBE_DELAY), None);
    }

    #[test]
    fn a_subscription_that_lands_clears_the_pending_ask() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);
        let now = Instant::now();

        listener.on_event(BridgeEvent::Unavailable {
            channel: CHANNEL.to_owned(),
        });
        listener.on_event(BridgeEvent::Subscribed {
            channel: CHANNEL.to_owned(),
            replay_count: 0,
            gap: None,
        });

        assert_eq!(listener.chore(now + RESUBSCRIBE_DELAY), None);
    }

    #[test]
    fn another_channel_going_unserved_is_not_this_daemons_problem() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::Unavailable {
            channel: "brenn:something.else".to_owned(),
        });

        assert_eq!(listener.chore(Instant::now() + RESUBSCRIBE_DELAY), None);
    }

    #[test]
    fn only_the_orderly_endings_leave_the_daemon_with_something_to_obey() {
        assert_eq!(stop_for(&BridgeOutcome::Closed), None);
        assert_eq!(stop_for(&BridgeOutcome::EmbedderGone), None);
        assert_eq!(
            stop_for(&BridgeOutcome::Fatal {
                detail: "a frame the protocol does not admit".to_owned()
            }),
            Some(Stop::Detached)
        );
        assert_eq!(
            stop_for(&BridgeOutcome::Futile { attachments: 5 }),
            Some(Stop::Detached)
        );
    }

    #[test]
    fn a_terminal_ending_asks_for_the_torque_held_stow() {
        let sink = Collect::default();
        let (shared, listener) = fixture(&sink);

        listener.on_detached_for_good(&BridgeOutcome::Futile { attachments: 5 });

        assert_eq!(shared.stopping(), Some(Stop::Detached));
        assert_eq!(
            sink.fields("bus_exit").expect("reported")["terminal"],
            json!(true)
        );
    }

    /// The lease's input provenance. This daemon holds one subscription today,
    /// so nothing but presence traffic arrives — but the channel is what says a
    /// body was authored by something entitled to move this head, and a second
    /// subscription would otherwise feed its traffic into the reducer.
    #[test]
    fn a_delivery_on_another_channel_moves_nothing() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::Delivered(Delivery {
            channel: "brenn:reachy.gaze".to_owned(),
            envelope: envelope(&intent(POD, PresenceState::Engaged, 1)),
            seq: 1,
            dropped: 0,
        }));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Idle);
        assert_eq!(
            sink.fields("presence_ignored").expect("reported")["reason"],
            json!("foreign_channel")
        );
        assert!(!sink.saw("presence_intent"));
    }

    /// An attachment says what it negotiated, and says loudly when the peer did
    /// not grant alerts: that grant is the daemon's whole fault-escalation path,
    /// and a run without it reports a motion fault to a terminal and nowhere
    /// else.
    #[test]
    fn an_attachment_says_whether_a_fault_could_reach_an_operator() {
        for granted in [true, false] {
            let sink = Collect::default();
            let (_shared, mut listener) = detached_fixture(&sink);

            listener.on_event(BridgeEvent::Attached(facts(granted)));

            let said = sink.said();
            assert_eq!(
                said.lines[0].contains("a fault will not reach an operator"),
                !granted,
                "{:?}",
                said.lines
            );
            assert_eq!(
                sink.fields("bus_attached").expect("reported")["alert_granted"],
                json!(granted)
            );
        }
    }

    /// A detachment is not a motion event: the lease keeps running on this
    /// machine's own clock and lapses on its own, which is the whole point of a
    /// leased desired state.
    #[test]
    fn a_detachment_says_the_lease_keeps_running() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        shared.apply(
            &PresenceBody::new(POD, PresenceState::Engaged, 1),
            Instant::now(),
            TTL,
        );

        listener.on_event(BridgeEvent::Detached {
            reason: brenn_bridge::DetachReason::LivenessTimeout,
        });

        assert_eq!(shared.desired(Instant::now()), PresenceState::Engaged);
        assert_eq!(shared.stopping(), None);
        assert_eq!(
            sink.fields("bus_detached").expect("reported")["reason"],
            json!("liveness_timeout")
        );
    }

    /// A dial that reached no socket. The one fact on that arm is whether it ran
    /// out of time, which is what separates a server that is down from one that
    /// is refusing.
    #[test]
    fn a_dial_that_reached_nothing_says_whether_it_timed_out() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::ConnectFailed { timed_out: true });

        assert_eq!(
            sink.fields("bus_connect_failed").expect("reported")["timed_out"],
            json!(true)
        );
    }

    /// The window this exists for: a fault taken during a reconnect gap. An
    /// alert handed to a detached bridge is written nowhere and still answers
    /// `Ok`, so producing it then would lose it silently and latch it lost.
    #[test]
    fn a_fault_taken_while_detached_alerts_when_the_attachment_comes_back() {
        let sink = Collect::default();
        let (shared, mut listener) = detached_fixture(&sink);
        let now = Instant::now();
        shared.set_fault(FaultReport::new(FaultStage::Presence, "read loss"));

        assert_eq!(
            listener.chore(now),
            None,
            "an alert produced with no wire under it is dropped at the wire"
        );
        listener.on_event(BridgeEvent::Attached(facts(true)));

        let Some(Chore::Alert { body, .. }) = listener.chore(now) else {
            panic!("the alert is owed as soon as there is somewhere to send it");
        };
        assert!(body.contains("read loss"), "{body}");
        assert_eq!(listener.chore(now), None);
    }

    /// And the case where waiting cannot end well: the arm sequence refuses
    /// while the bridge is still dialling a server that is not there. The
    /// shutdown waits a bounded while for a wire, then goes ahead — and says
    /// that the alert was lost rather than letting it vanish.
    #[test]
    fn an_alert_with_nowhere_to_go_delays_the_exit_once_and_then_says_it_was_lost() {
        let sink = Collect::default();
        let (shared, mut listener) = detached_fixture(&sink);
        let now = Instant::now();
        shared.set_fault(FaultReport::new(
            FaultStage::Arm,
            "servo 21 answered nothing",
        ));
        shared.request_stop(Stop::Detached);
        shared.end_motion();

        assert_eq!(listener.chore(now), None, "the alert is given its grace");
        assert_eq!(
            listener.chore(now + ALERT_GRACE / 2),
            None,
            "and the grace runs from when the shutdown became due"
        );
        assert_eq!(
            listener.chore(now + ALERT_GRACE),
            Some(Chore::Shutdown(Stop::Detached))
        );

        let fields = sink
            .fields("alert_undelivered")
            .expect("a lost alert is a fact of the capture");
        assert!(
            fields["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("servo 21")),
            "{fields}"
        );
        assert_eq!(listener.chore(now + ALERT_GRACE), None);
    }

    /// A faulted machine takes no commands, this signal included: it is not
    /// stowed, not verified and not released. Telling the operator otherwise at
    /// the moment they act is a false statement about whether they may put a
    /// hand on the head.
    #[test]
    fn a_signal_to_a_faulted_daemon_does_not_promise_a_release() {
        let sink = Collect::default();
        let (shared, listener) = fixture(&sink);
        shared.set_fault(FaultReport::new(FaultStage::Presence, "tracking lost"));

        listener.on_signal("SIGTERM");

        let said = sink.said();
        assert!(
            said.lines
                .iter()
                .any(|line| line.contains("torque stays on")),
            "{:?}",
            said.lines
        );
        assert!(
            !said.lines.iter().any(|line| line.contains("releasing")),
            "a release was promised on a machine that takes no commands: {:?}",
            said.lines
        );
        let fields = sink.fields("stop_requested").expect("reported");
        assert_eq!(fields["will_release"], json!(false));
        assert_eq!(shared.stopping(), Some(Stop::Operator));
    }

    /// The same signal on a machine that is still commanding does say what it
    /// is about to do, because it is about to do it.
    #[test]
    fn a_signal_to_a_working_daemon_says_torque_is_coming_off() {
        let sink = Collect::default();
        let (_shared, listener) = fixture(&sink);

        listener.on_signal("SIGINT");

        assert_eq!(
            sink.fields("stop_requested").expect("reported")["will_release"],
            json!(true)
        );
    }

    /// Nothing can ask a daemon with no signal handlers to stow and release, so
    /// it parks itself. `Detached`, never `Operator`: the operator's reason is
    /// the one that takes torque off, and there is no operator this daemon can
    /// hear.
    #[test]
    fn a_daemon_that_cannot_hear_a_signal_parks_rather_than_releasing() {
        let sink = Collect::default();
        let shared = Shared::new(POD);

        no_signals(&shared, &sink, &"too many signal handlers");

        assert_eq!(shared.stopping(), Some(Stop::Detached));
        assert!(
            sink.fields("signals_unavailable").expect("reported")["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("too many")),
        );
    }

    /// And the same for a bus thread that never got a runtime: no attachment
    /// means no intent will ever arrive, which is the same ending by a different
    /// road.
    #[test]
    fn a_bus_thread_with_no_runtime_parks_rather_than_releasing() {
        let sink = Collect::default();
        let shared = Shared::new(POD);

        no_runtime(&shared, &sink, &"cannot spawn a reactor");

        assert_eq!(shared.stopping(), Some(Stop::Detached));
        assert!(sink.saw("bus_unavailable"));
    }

    #[test]
    fn a_delivery_that_rolled_past_this_attachment_is_reported_and_still_applied() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::Delivered(Delivery {
            channel: CHANNEL.to_owned(),
            envelope: envelope(&intent(POD, PresenceState::Engaged, 9)),
            seq: 4,
            dropped: 3,
        }));

        assert_eq!(shared.desired(Instant::now()), PresenceState::Engaged);
        assert_eq!(
            sink.fields("presence_delivery_dropped").expect("reported")["count"],
            json!(3)
        );
    }

    // The loop itself, over a scripted peer. What the tests above pin is what
    // the listener *decides*; these pin that the decisions are carried out —
    // that the hold is stated, that a chore reaches the bridge, and that an
    // ending reaches the cells the motion thread reads.

    /// Wait for `cond`, yielding to the loop under test in between.
    async fn until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + scripted::WAIT;
        while !cond() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A daemon that never states its hold hears no intent for the life of the
    /// process and stays stowed, which looks exactly like an idle system. The
    /// depths are asserted at the wire because they are what keep a retained
    /// *engaged* from popping the head up at a reattach.
    #[tokio::test]
    async fn the_loop_states_its_hold_and_folds_what_arrives_on_it() {
        let sink = Collect::default();
        let (shared, listener) = detached_fixture(&sink);
        let (bridge, handle, events, mut peers) = scripted::scripted(1, 3);
        let mut peer = peers.pop_front().expect("one socket");
        let cells = Arc::clone(&shared);

        let (outcome, ()) = tokio::join!(serve(bridge, &handle, events, listener), async {
            peer.handshake(true).await;
            let frame = peer.answer_subscribe(CHANNEL, "Ok").await;
            assert_eq!(frame["push_depth"], json!(PRESENCE_DEPTHS.push_depth));
            assert_eq!(
                frame["retain_depth"],
                json!(0),
                "a reattach must replay nothing: {frame}"
            );

            peer.deliver(CHANNEL, &intent(POD, PresenceState::Engaged, 1), 1);
            until("the intent to reach the lease", || {
                cells.desired(Instant::now()) == PresenceState::Engaged
            })
            .await;

            cells.request_stop(Stop::Operator);
            cells.end_motion();
        });

        assert!(matches!(outcome, BridgeOutcome::Closed), "{outcome}");
        assert_eq!(
            sink.fields("bus_exit").expect("reported")["terminal"],
            json!(false)
        );
    }

    /// The alert is the only push channel a fault escalates over, and the
    /// attachment it travels on is closed by the shutdown right behind it. Both
    /// halves reach the peer, in that order.
    #[tokio::test]
    async fn a_fault_reaches_the_peer_as_an_alert_before_the_attachment_closes() {
        let sink = Collect::default();
        let (shared, listener) = detached_fixture(&sink);
        let (bridge, handle, events, mut peers) = scripted::scripted(1, 3);
        let mut peer = peers.pop_front().expect("one socket");
        let cells = Arc::clone(&shared);

        let (outcome, ()) = tokio::join!(serve(bridge, &handle, events, listener), async {
            peer.handshake(true).await;
            peer.answer_subscribe(CHANNEL, "Ok").await;

            cells.set_fault(FaultReport::new(
                FaultStage::Presence,
                "servo 13: timed out",
            ));
            cells.request_stop(Stop::Operator);
            cells.end_motion();

            let alert = peer.expect_frame("Alert").await;
            assert_eq!(alert["severity"], json!("critical"));
            assert!(
                alert["body"]
                    .as_str()
                    .is_some_and(|body| body.contains("servo 13")),
                "{alert}"
            );
        });

        assert!(matches!(outcome, BridgeOutcome::Closed), "{outcome}");
    }

    /// A peer that broke the protocol is an attachment nothing reconnects
    /// after, so the daemon has no source of intents left. It parks the head
    /// with torque on — `Detached`, never the operator's reason, because nobody
    /// is present to catch a released head.
    #[tokio::test]
    async fn an_attachment_that_ends_for_good_parks_the_head_with_torque_on() {
        let sink = Collect::default();
        let (shared, listener) = detached_fixture(&sink);
        let (bridge, handle, events, mut peers) = scripted::scripted(1, 3);
        let mut peer = peers.pop_front().expect("one socket");

        let (outcome, ()) = tokio::join!(serve(bridge, &handle, events, listener), async {
            peer.handshake(true).await;
            peer.answer_subscribe(CHANNEL, "Ok").await;
            peer.break_the_protocol();
        });

        assert!(stop_for(&outcome).is_some(), "{outcome}");
        assert_eq!(shared.stopping(), Some(Stop::Detached));
        assert_eq!(
            sink.fields("bus_exit").expect("reported")["terminal"],
            json!(true)
        );
    }
}
