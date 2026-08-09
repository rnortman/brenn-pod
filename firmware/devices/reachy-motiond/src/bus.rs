//! The bus thread: the attachment, the motion subscription, and the path from a
//! delivery into the schedule.
//!
//! Everything here is async and none of it touches a servo. Its whole job is to
//! keep an attachment up, decode what arrives on one channel, and offer the
//! result to the schedule the motion thread reads. It has one
//! other duty, and it is the reason the fault cell exists: an operator alert
//! leaves over the bus, and the thread that faulted cannot send one — it holds a
//! serial port and by then it is deliberately commanding nothing at all.
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
use motion_proto::{Acceptance, DecodeError, MotionScript};
use serde_json::json;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::cells::{Collapsed, Delivered, Shared, Stop};
use crate::report::Sink;

#[cfg(test)]
mod scripted;

/// Subscription statement for the motion channel.
///
/// Live only. A script's offsets run from the moment it arrives, and a retained
/// one is worse than none: a raise published yesterday and replayed at a
/// reattach would pop the head up at three in the morning. The push depth
/// satisfies the plane's "at least one non-zero" precondition with room for a
/// burst; a missed script is repaired by the scripter's next refresh, and the
/// running script's own timeout covers the gap either way.
pub const SCRIPT_DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 0,
};

/// See [`SCRIPT_DEPTHS`]: a reattach replays nothing.
pub const SCRIPT_RESUME: ResumePolicy = ResumePolicy::Cursorless;

/// How often the loop looks at the cells the motion thread writes, and at its
/// own timers.
///
/// This is the latency of an operator alert after a fault, and nothing else
/// depends on it: the motion thread has already stopped commanding by the time
/// the cell is set, so nothing is waiting on this tick to become safe.
const WATCH_POLL: Duration = Duration::from_millis(200);

/// How long to wait before asking again for a motion channel the peer said was
/// not there.
///
/// Nothing else retries it — the bridge drops the hold with the refusal — and a
/// daemon with no channel is a daemon that will never hear a script again. It
/// would still be safe, because a head with no script stays stowed; it would
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

/// How many scripts in a row have to be dropped as stale before somebody is
/// woken up about it.
///
/// The sequence high-water mark only ever rises, so one script carrying a number
/// from a host whose clock read the far future leaves this daemon deaf to every
/// later script for the life of the process — a machine that looks exactly like
/// an idle one. Ordinary staleness is a redelivery or a message overtaken in
/// flight and is one drop, not three: the scripter re-emits at its refresh
/// cadence, so three consecutive drops is already more than a redelivery can
/// explain.
const STALE_ALERT_RUN: u64 = 3;

/// The least time between two alerts about the pre-torque watch.
///
/// One alert per failure run is the policy, and a flapping bus is a run every
/// few seconds — over the channel a motion fault has to arrive on. One a minute
/// is enough to say the wire is unreliable, and the alert that finally goes
/// carries how many runs and recoveries stand behind it. Every edge is in the
/// capture regardless.
const WATCH_ALERT_EVERY: Duration = Duration::from_secs(60);

/// How much of a refusal's own text goes onto the narration stream.
const DETAIL_LIMIT: usize = 200;

/// A decoder's message, made safe to put on a line-oriented stream.
///
/// The message quotes the body that produced it, and the body is whatever some
/// publisher chose to send — this channel is expected to carry traffic that is
/// not ours, so the text is not this daemon's to trust. A newline in it forges a
/// line in the operator's narration and in an alert body, where a forged
/// `fault: …` reads exactly like the daemon's own; a long enough one buries the
/// terminal. Control characters become spaces and the length is bounded. The
/// unabridged text still reaches the capture, in a JSON string field, where it
/// is escaped rather than rendered.
///
/// The state file takes fault details through the same treatment for the same
/// reason: its reader splits on lines, so a newline in a detail would forge a
/// key.
pub(crate) fn one_line(detail: &str) -> String {
    let mut clean: String = detail
        .chars()
        .take(DETAIL_LIMIT)
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if detail.chars().count() > DETAIL_LIMIT {
        clean.push('…');
    }
    clean
}

/// Something the loop must go and do, decided from the cells and the timers.
///
/// Separated from the doing because the deciding is the part worth testing: each
/// one is awaited by the loop, and each is produced at most once per thing that
/// warrants it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chore {
    /// Raise this alert, once.
    ///
    /// The severity is decided where the condition is: a machine that has
    /// stopped taking commands and a machine that is limp, safe and unreadable
    /// are both worth an alert and are not the same news.
    Alert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// The daemon is stopping for this reason: end the attachment.
    Shutdown(Stop),
    /// State the motion hold again, after a peer that would not serve it.
    Resubscribe,
}

/// The bus thread's state: what it has been told, and what it still owes.
///
/// Holds the shared cells and the sink, and nothing else that survives a
/// delivery — the schedule is the state, and it lives where both threads can
/// reach it.
pub struct Listener<'a> {
    shared: Arc<Shared>,
    channel: String,
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
    /// The refused scripts of this run, collapsed. A refused script means the
    /// scripter and this daemon disagree about the schema, which no retry fixes
    /// and nothing else would tell anybody about.
    refusals: Collapsed,
    /// Whether the refusal alert has been raised. Once a run: a scripter
    /// emitting garbage at the refresh cadence would otherwise be an alert every
    /// five seconds for as long as it ran, over the channel a motion fault has
    /// to arrive on.
    alerted_refusal: bool,
    /// Scripts dropped as stale since the last one that was accepted. A run of
    /// them is the shape of a daemon that has gone permanently deaf, which is
    /// indistinguishable from an idle one to everybody but this counter.
    stale_run: u64,
    /// Whether the staleness alert has been raised, latched for the same reason
    /// the refusal alert is.
    alerted_stale: bool,
    /// When the last watch alert went out, which is what bounds how often a
    /// flapping bus may produce another.
    watch_alerted_at: Option<Instant>,
    /// Whether the shutdown chore has already been produced.
    shutting_down: bool,
    /// When the shutdown first became due, which is what bounds the wait an
    /// undelivered alert may impose on it.
    shutdown_due_at: Option<Instant>,
    /// When to state the motion hold again, or `None` when it is not in doubt.
    resubscribe_at: Option<Instant>,
}

impl<'a> Listener<'a> {
    /// A listener over `shared`, obeying the scripts that arrive on `channel`.
    pub fn new(shared: Arc<Shared>, channel: impl Into<String>, sink: &'a dyn Sink) -> Self {
        Self {
            shared,
            channel: channel.into(),
            sink,
            alerted: false,
            attached: false,
            alert_waiting: false,
            refusals: Collapsed::default(),
            alerted_refusal: false,
            stale_run: 0,
            alerted_stale: false,
            watch_alerted_at: None,
            shutting_down: false,
            shutdown_due_at: None,
            resubscribe_at: None,
        }
    }

    /// The channel this daemon's scripts arrive on.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Render one bridge event, and offer a delivery to the schedule.
    ///
    /// Nothing here awaits and nothing here fails: every refusal a delivery can
    /// produce is reported and dropped. A consumer that stopped on a malformed
    /// body would let one bad scripter stow a head for good.
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
                    "bus: detached ({reason:?}); the running script keeps its timeline and \
                     lapses on its own"
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

    /// The peer will not serve a channel. For the motion channel that means no
    /// script will ever arrive, so the ask is scheduled again.
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

    /// Offer one delivery to the schedule.
    fn on_delivery(&mut self, delivery: Delivery) {
        // The schedule's provenance, made an invariant rather than a coincidence
        // of this daemon holding exactly one subscription: the channel is the
        // whole of what says a body was authored by something entitled to move
        // this head, and the vocabulary is expected to grow other channels.
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
            // refresh restates the whole timeline at the same absolute instants,
            // and a script that lapsed in the meantime stowed the head, which is
            // the safe direction.
            self.sink.event(
                "script_delivery_dropped",
                &json!({ "count": delivery.dropped }),
            );
        }

        let script = match MotionScript::decode(&delivery.envelope.body) {
            Ok(script) => script,
            Err(error) => {
                self.on_refusal(&error, &delivery);
                return;
            }
        };

        match self.shared.accept(&script, Instant::now()) {
            Delivered::Faulted => {
                self.ignored(
                    "faulted",
                    "the machine has faulted and takes no commands",
                    &delivery,
                );
            }
            Delivered::Scheduled(Acceptance::Foreign) => {
                self.ignored(
                    "foreign",
                    &format!("addressed to {:?}, not to this machine", script.pod()),
                    &delivery,
                );
            }
            Delivered::Scheduled(Acceptance::Stale { seq, accepted }) => {
                self.stale_run += 1;
                self.ignored(
                    "stale",
                    &format!("seq {seq} is at or below the accepted {accepted}"),
                    &delivery,
                );
            }
            Delivered::Scheduled(Acceptance::Accepted) => {
                self.stale_run = 0;
                self.sink.line(&format!(
                    "script seq {}: {}, timeout {} ms",
                    script.seq(),
                    timeline(&script),
                    script.timeout_ms()
                ));
                self.sink.event(
                    "motion_script",
                    &json!({
                        "pod": script.pod(),
                        "seq": script.seq(),
                        "steps": script.steps().iter().map(|step| json!({
                            "after_ms": step.after_ms,
                            "posture": step.posture.as_str(),
                        })).collect::<Vec<_>>(),
                        "timeout_ms": script.timeout_ms(),
                        "sender": delivery.envelope.sender,
                    }),
                );
            }
        }
    }

    /// A body that claimed to be a script and did not hold an executable one.
    ///
    /// Louder than an ignored delivery, and deliberately: the timeline in force
    /// and its timeout stand, so nothing is unsafe, but the two ends disagree
    /// about the schema and no retry of theirs will fix it. The narration and
    /// the capture carry every one; the bus alert carries the most recent, once
    /// a run — a scripter emitting garbage on its refresh cadence must not
    /// become an alert every five seconds.
    fn on_refusal(&mut self, error: &DecodeError, delivery: &Delivery) {
        let detail = error.to_string();
        self.ignored(ignored_reason(error), &detail, delivery);
        if !matches!(
            error,
            DecodeError::Malformed { .. } | DecodeError::Invalid(_)
        ) {
            // Another tenant's message, or something that is not JSON at all:
            // this channel is expected to carry traffic that is not ours, and
            // neither is a statement about the scripter.
            return;
        }
        // Quoted, not interpolated raw: the message carries the offending body
        // back with it, and the terminal and the alert are both line-oriented.
        let safe = one_line(&detail);
        self.sink
            .line(&format!("script refused, the running one stands: {safe}"));
        self.refusals.note(safe);
    }

    /// Report a delivery that changed nothing. Never an error and never a stop:
    /// this channel is expected to grow other tenants, and this daemon is not
    /// one of the machines most of them are about.
    fn ignored(&self, reason: &str, detail: &str, delivery: &Delivery) {
        self.sink.event(
            "motion_script_ignored",
            &json!({
                "reason": reason,
                "detail": detail,
                "sender": delivery.envelope.sender,
            }),
        );
    }

    /// An operator's signal. The orderly ending — stow, verify, release — so it
    /// is the one that says so on the way out, and the one that must not say so
    /// when it will not happen.
    ///
    /// A faulted machine takes no commands at all, this signal included: it is
    /// not stowed and not verified. Torque is already off — that is what the
    /// fault response did — so what the operator is told is where the head is,
    /// not that anything is about to move it.
    pub fn on_signal(&self, name: &str) {
        if !self.shared.request_stop(Stop::Operator) {
            self.sink.line(&format!(
                "{name}: a shutdown is already under way and will not be restarted. \
                 killing the process instead leaves the servos wherever this ending has \
                 got to, torque and all."
            ));
            return;
        }

        let faulted = self.shared.faulted();
        if faulted {
            self.sink.line(&format!(
                "{name}: the machine has faulted, so nothing will be commanded. torque is \
                 already off and the head has settled into near-stow. exiting."
            ));
        } else {
            self.sink.line(&format!(
                "{name}: stowing, verifying and releasing. torque comes off at the end of it, \
                 as it does on every ending."
            ));
        }
        self.sink.event(
            "stop_requested",
            &json!({ "signal": name, "stop": "operator", "will_stow": !faulted }),
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
                    severity: AlertSeverity::Critical,
                    title: "reachy head motion stopped".to_owned(),
                    body: format!(
                        "{report}. commanding has stopped and torque is off: the head has \
                         settled into near-stow and the machine is at the minimum risk \
                         condition. nothing will move again until an operator restarts the \
                         daemon."
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

        // After the fault and before the shutdown: a run that refuses scripts and
        // then faults has the fault as its diagnosis, and a run that only refuses
        // scripts still owes somebody the news before the attachment closes.
        if self.attached
            && !self.alerted_refusal
            && let Some((detail, count)) = self.refusals.take()
        {
            self.alerted_refusal = true;
            return Some(Chore::Alert {
                severity: AlertSeverity::Critical,
                title: "reachy motion scripts refused".to_owned(),
                body: format!(
                    "{count} script(s) refused so far; the most recent was: {detail}. the \
                     timeline already running stands, and its timeout still bounds the head. \
                     the scripter and this daemon disagree about the schema. this is the only \
                     refusal alert of the run; the terminal and the capture carry every one."
                ),
            });
        }

        // A daemon that is dropping every script it is sent. The high-water mark
        // only rises, so one message carrying a number from a clock that read the
        // far future silences this process for good — and it goes on looking
        // exactly like an idle machine, which is why this is an alert and not
        // another capture line nobody reads.
        if self.attached && !self.alerted_stale && self.stale_run >= STALE_ALERT_RUN {
            self.alerted_stale = true;
            let run = self.stale_run;
            return Some(Chore::Alert {
                severity: AlertSeverity::Critical,
                title: "reachy head is dropping every script".to_owned(),
                body: format!(
                    "{run} script(s) in a row dropped as stale: their sequence numbers are at or \
                     below the highest this daemon has accepted. the head will not move again \
                     until a script arrives above that mark, or until the daemon is restarted — \
                     a restart forgets the mark. the likeliest cause is a scripter that emitted \
                     one script while its clock read the wrong time."
                ),
            });
        }

        // A release that left the head somewhere other than its fold. Torque is
        // off — nothing gates that — but this is the state a hand might go near,
        // and it is the one thing the capture alone would not get in front of
        // anybody.
        if self.attached
            && let Some((detail, count)) = self.shared.take_stow_miss()
        {
            return Some(Chore::Alert {
                severity: AlertSeverity::Critical,
                title: "reachy head released away from stow".to_owned(),
                body: format!(
                    "{count} release(s) did not find the head folded; the most recent: {detail} \
                     the machine is limp, so nothing is holding it there and nothing will move \
                     it; the next engage measures it and re-stows it."
                ),
            });
        }

        // A machine that would not take torque. Not a fault — nothing was
        // written and the daemon is still resting — but a head that cannot come
        // up will not answer the next wake word either, so somebody is owed the
        // news while the rail is still sagging.
        if self.attached
            && let Some((detail, count)) = self.shared.take_engage_refusal()
        {
            return Some(Chore::Alert {
                severity: AlertSeverity::Critical,
                title: "reachy head could not take torque".to_owned(),
                body: format!(
                    "{count} engage(s) refused; the most recent was: {detail}. torque was not \
                     written, so the machine is limp where it stands and the next script tries \
                     again."
                ),
            });
        }

        // A machine nobody can read while it lies limp. A warning and not a
        // fault, deliberately: torque is off, the head is where it was, and the
        // daemon goes on sweeping and recovers by itself the moment the wire
        // does. What is lost meanwhile is presence — an engage plans from a
        // measurement, so the head stays down until a sweep answers.
        if self.attached
            && self
                .watch_alerted_at
                .is_none_or(|last| now.duration_since(last) >= WATCH_ALERT_EVERY)
            && let Some(alarm) = self.shared.take_watch_alarm()
        {
            self.watch_alerted_at = Some(now);
            let state = if alarm.failing {
                "sweeps are still failing".to_owned()
            } else {
                format!("reads have come back {} time(s) since", alarm.restores)
            };
            return Some(Chore::Alert {
                severity: AlertSeverity::Warning,
                title: "reachy head cannot read its machine".to_owned(),
                body: format!(
                    "{} run(s) of failing position sweeps; the most recent began: {}. {state}. \
                     torque is off and the head is limp where it was left, so nothing is at risk \
                     and nothing has to be restarted — but no script will raise the head until \
                     the sweeps answer again. at most one of these a minute; the capture carries \
                     every one.",
                    alarm.runs, alarm.detail
                ),
            });
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

    /// Note that the daemon has lost the source of its scripts.
    ///
    /// The orderly ending follows, exactly as it does on an operator's signal:
    /// nothing about a lost script source is a reason to leave a machine
    /// torqued. What the reason changes is the exit status, not the posture.
    fn on_detached_for_good(&self, outcome: &BridgeOutcome) {
        if self.shared.request_stop(Stop::Detached) {
            self.sink.line(
                "bus: the attachment ended for good, so there is no source of scripts left. \
                 stowing and releasing.",
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
/// front of it, and the exit status is what says so.
pub fn no_signals(shared: &Shared, sink: &dyn Sink, error: &dyn fmt::Display) {
    sink.line(&format!(
        "signals: cannot listen for SIGTERM/SIGINT ({error}), so nothing could ask this \
         daemon to stop later. stowing and releasing now."
    ));
    sink.event(
        "signals_unavailable",
        &json!({ "detail": error.to_string() }),
    );
    shared.request_stop(Stop::Detached);
}

/// The same for a bus thread that never got a runtime: with no attachment, no
/// script can ever arrive, so the daemon has nothing left to obey.
///
/// The same [`Stop::Detached`], for the same reason — a bus thread that failed
/// to start is not an operator asking for the head.
pub fn no_runtime(shared: &Shared, sink: &dyn Sink, error: &dyn fmt::Display) {
    sink.line(&format!(
        "bus: no runtime ({error}), so no script can ever arrive. stowing and releasing."
    ));
    sink.event("bus_unavailable", &json!({ "detail": error.to_string() }));
    shared.request_stop(Stop::Detached);
}

/// Run the attachment until it ends, offering what arrives to the schedule.
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
        Chore::Alert {
            severity,
            title,
            body,
        } => {
            let _ = handle.alert(severity, title, body).await;
        }
        Chore::Shutdown(_) => {
            let _ = handle.shutdown().await;
        }
        Chore::Resubscribe => subscribe(handle, channel).await,
    }
}

/// State the motion hold. A gone bridge is not reported here: the event
/// channel closing says the same thing, and that arm owns the line.
async fn subscribe(handle: &BridgeHandle, channel: &str) {
    let _ = handle
        .subscribe(channel.to_owned(), SCRIPT_DEPTHS, SCRIPT_RESUME)
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
        DecodeError::Invalid(_) => "not_executable",
    }
}

/// A script's timeline as one narration phrase.
///
/// The steps are what an operator watching the head wants against the clock;
/// the empty timeline is lawful and reads as itself rather than as an empty
/// list.
fn timeline(script: &MotionScript) -> String {
    if script.steps().is_empty() {
        return "no steps".to_owned();
    }
    script
        .steps()
        .iter()
        .map(|step| format!("{} at {} ms", step.posture, step.after_ms))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use brenn_bridge::MessageEnvelope;
    use motion_proto::{Desired, Posture, Step};

    use super::*;
    use crate::cells::{FaultReport, FaultStage};
    use crate::report::Collect;

    const POD: &str = "reachy00";
    const CHANNEL: &str = "brenn:reachy.motion";

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
        let listener = Listener::new(Arc::clone(&shared), CHANNEL, sink);
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

    /// A delivery carrying `body` on the motion channel.
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

    /// A script as the scripter puts it on the wire.
    fn script(pod: &str, seq: u64, steps: Vec<Step>) -> String {
        MotionScript::new(pod, seq, steps, 30_000)
            .expect("a lawful script")
            .encode()
    }

    /// The nominal conversation script: up as it lands, stow when the audio it
    /// was scheduled against ends.
    fn nominal(seq: u64) -> String {
        script(
            POD,
            seq,
            vec![Step::new(0, Posture::Up), Step::new(6_740, Posture::Stow)],
        )
    }

    /// The whole timeline arrives in one message, and the daemon can say what it
    /// is going to do before it does any of it.
    #[test]
    fn a_script_for_this_pod_takes_the_schedule_whole() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&nominal(7)));

        let now = Instant::now();
        assert_eq!(shared.desired(now), Desired::Posture(Posture::Up));
        let fields = sink
            .fields("motion_script")
            .expect("the script is reported");
        assert_eq!(fields["seq"], json!(7));
        assert_eq!(fields["timeout_ms"], json!(30_000));
        assert_eq!(
            fields["steps"],
            json!([
                { "after_ms": 0, "posture": "up" },
                { "after_ms": 6_740, "posture": "stow" },
            ])
        );
        assert!(
            sink.said()
                .lines
                .iter()
                .any(|line| line.contains("stow at 6740 ms")),
            "the whole timeline is on the terminal: {:?}",
            sink.said().lines
        );
    }

    /// The latest script replaces the one running; a redelivery of an overtaken
    /// one is dropped by number and says so.
    #[test]
    fn a_later_script_replaces_the_running_one_and_a_stale_one_is_reported() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&nominal(7)));
        listener.on_event(delivered(&script(
            POD,
            9,
            vec![Step::new(0, Posture::Stow)],
        )));
        assert_eq!(
            shared.desired(Instant::now()),
            Desired::Posture(Posture::Stow)
        );

        listener.on_event(delivered(&nominal(7)));

        assert_eq!(
            shared.desired(Instant::now()),
            Desired::Posture(Posture::Stow)
        );
        let fields = sink
            .fields("motion_script_ignored")
            .expect("a stale script is reported");
        assert_eq!(fields["reason"], json!("stale"));
        assert!(
            fields["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("accepted 9")),
            "{fields}"
        );
    }

    #[test]
    fn another_machines_script_moves_nothing_and_is_reported() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(&script(
            "reachy01",
            1,
            vec![Step::new(0, Posture::Up)],
        )));

        assert_eq!(shared.desired(Instant::now()), Desired::Unchanged);
        assert_eq!(
            sink.fields("motion_script_ignored").expect("reported")["reason"],
            json!("foreign")
        );
        assert!(!sink.saw("motion_script"));
    }

    #[test]
    fn a_body_that_is_not_ours_is_reported_by_what_kind_of_not_ours_it_is() {
        for (body, reason) in [
            ("not json at all", "not_json"),
            (r#"{"type":"gaze","yaw":10}"#, "other_tenant"),
            (r#"{"type":"motion-script","pod":"reachy00"}"#, "malformed"),
            (
                r#"{"type":"motion-script","pod":"reachy00","seq":1,"steps":[],"timeout_ms":0}"#,
                "not_executable",
            ),
        ] {
            let sink = Collect::default();
            let (shared, mut listener) = fixture(&sink);

            listener.on_event(delivered(body));

            assert_eq!(shared.desired(Instant::now()), Desired::Unchanged);
            assert_eq!(
                sink.fields("motion_script_ignored").expect("reported")["reason"],
                json!(reason),
                "for {body}"
            );
        }
    }

    /// A refused script is the two ends disagreeing about the schema, so it is
    /// said out loud and alerted on — once a run, however many arrive and
    /// whenever they arrive, because a scripter emitting garbage on its refresh
    /// cadence must not become an alert every five seconds over the channel a
    /// motion fault has to travel on. The timeline already running is untouched.
    #[test]
    fn a_refused_script_leaves_the_running_one_standing_and_alerts_once_a_run() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        listener.on_event(delivered(&nominal(1)));

        listener.on_event(delivered(r#"{"type":"motion-script","pod":"reachy00"}"#));
        listener.on_event(delivered(r#"{"type":"motion-script","seq":"soon"}"#));

        assert_eq!(shared.desired(now), Desired::Posture(Posture::Up));
        let Some(Chore::Alert { title, body, .. }) = listener.chore(now) else {
            panic!("a refused script owes one alert");
        };
        assert!(title.contains("refused"), "{title}");
        assert!(body.contains("2 script(s) refused"), "{body}");
        assert_eq!(listener.chore(now), None);

        // The case the once-a-run bound is about: the disagreement persists, so
        // more refusals arrive after the alert has been drained. They narrate
        // and they are captured; they do not re-arm the alert.
        listener.on_event(delivered(r#"{"type":"motion-script","pod":"reachy00"}"#));
        assert_eq!(
            listener.chore(now),
            None,
            "a refusal after the alert must not raise a second one"
        );

        assert_eq!(
            sink.said()
                .lines
                .iter()
                .filter(|line| line.contains("script refused"))
                .count(),
            3,
            "every refusal reaches the terminal: {:?}",
            sink.said().lines
        );
    }

    /// Another tenant's message is not a refusal: this channel is expected to
    /// carry traffic that is not ours, and alerting on it would make the
    /// vocabulary growing an incident.
    #[test]
    fn another_tenants_message_is_not_alerted_on() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(r#"{"type":"gaze","yaw":10}"#));
        listener.on_event(delivered("not json at all"));

        assert_eq!(listener.chore(Instant::now()), None);
    }

    #[test]
    fn a_faulted_machine_takes_no_further_scripts() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        shared.set_fault(FaultReport::new(
            FaultStage::Motion,
            "a servo stopped answering",
        ));

        listener.on_event(delivered(&nominal(1)));

        assert_eq!(shared.desired(Instant::now()), Desired::Unchanged);
        assert_eq!(
            sink.fields("motion_script_ignored").expect("reported")["reason"],
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
            FaultStage::Commission,
            "the head is not where it says",
        ));

        let Some(Chore::Alert {
            severity,
            title,
            body,
        }) = listener.chore(now)
        else {
            panic!("a fault owes an alert");
        };
        assert_eq!(
            severity,
            AlertSeverity::Critical,
            "a machine that stopped taking commands is not a warning"
        );
        assert!(title.contains("motion"), "{title}");
        assert!(body.contains("torque is off"), "{body}");
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
        shared.set_fault(FaultReport::new(FaultStage::Motion, "read loss"));
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
        assert!(body.contains("torque is off"), "{body}");

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

    /// A torque-on gate refusing is not a fault — nothing was written and the
    /// daemon is still resting — but a head that cannot come up will not answer
    /// the next wake word either. The alert carries the count, because with no
    /// attachment a rail that has been sagging for an hour collapses into one.
    #[test]
    fn refused_engages_become_one_alert_carrying_their_count() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        assert_eq!(listener.chore(now), None);

        shared.refuse_engage("the supply is below the floor: 5.5 V against 6.0 V");
        shared.refuse_engage("the supply is below the floor: 5.4 V against 6.0 V");

        let Some(Chore::Alert { title, body, .. }) = listener.chore(now) else {
            panic!("a refused engage owes an alert");
        };
        assert!(title.contains("could not take torque"), "{title}");
        assert!(body.contains("2 engage(s) refused"), "{body}");
        assert!(
            body.contains("5.4 V"),
            "the latest refusal is the one named: {body}"
        );
        assert_eq!(
            listener.chore(now),
            None,
            "the refusals were alerted on twice"
        );
        assert!(!shared.faulted(), "a gate refusal parked the daemon");
    }

    /// A machine that cannot be read while it lies limp is degraded presence,
    /// not a hazard: the alert says so at warning, where a fault says so at
    /// critical, and nothing about it parks the daemon.
    #[test]
    fn a_watch_that_stopped_reading_is_a_warning_and_not_a_fault() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        assert_eq!(listener.chore(now), None);

        shared.note_watch_lost("servo 11: timed out waiting for a reply");

        let Some(Chore::Alert {
            severity,
            title,
            body,
        }) = listener.chore(now)
        else {
            panic!("a watch that stopped reading owes an alert");
        };
        assert_eq!(severity, AlertSeverity::Warning);
        assert!(title.contains("cannot read"), "{title}");
        assert!(body.contains("1 run(s)"), "{body}");
        assert!(body.contains("servo 11"), "{body}");
        assert!(body.contains("still failing"), "{body}");
        assert_eq!(listener.chore(now), None, "alerted on twice");
        assert!(!shared.faulted(), "a failing watch parked the daemon");
    }

    /// A bus that flaps would otherwise be an alert every few seconds, over the
    /// channel a motion fault has to arrive on. The runs behind the suppressed
    /// ones are not lost — the next alert that goes out carries them.
    #[test]
    fn a_flapping_watch_alerts_at_most_once_a_minute_and_carries_what_it_missed() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();

        shared.note_watch_lost("servo 11: timed out");
        assert!(matches!(listener.chore(now), Some(Chore::Alert { .. })));

        // Two more runs, inside the window the first alert opened.
        shared.note_watch_restored();
        shared.note_watch_lost("servo 12: timed out");
        shared.note_watch_restored();
        shared.note_watch_lost("servo 13: timed out");
        assert_eq!(
            listener.chore(now + WATCH_ALERT_EVERY / 2),
            None,
            "a run inside the window raised its own alert"
        );

        shared.note_watch_restored();
        let Some(Chore::Alert { body, .. }) = listener.chore(now + WATCH_ALERT_EVERY) else {
            panic!("the window passed with runs still owed");
        };
        assert!(body.contains("2 run(s)"), "{body}");
        assert!(body.contains("servo 13"), "{body}");
        assert!(body.contains("come back 3 time(s)"), "{body}");
    }

    /// A release that did not find the head folded is the one thing a capture
    /// alone would not get in front of anybody, and it is the state a hand might
    /// go near. Not a fault — torque is off, which is the whole doctrine — and
    /// not a refusal either.
    #[test]
    fn a_release_away_from_stow_becomes_an_alert() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        let now = Instant::now();
        assert_eq!(listener.chore(now), None);

        shared.note_stow_miss("released away from stow: 14.5° off at the worst joint");

        let Some(Chore::Alert { title, body, .. }) = listener.chore(now) else {
            panic!("a release away from stow owes an alert");
        };
        assert!(title.contains("away from stow"), "{title}");
        assert!(body.contains("14.5"), "{body}");
        assert_eq!(listener.chore(now), None, "alerted on twice");
        assert!(!shared.faulted(), "a stow miss parked the daemon");
    }

    /// The high-water mark only rises, so one script carrying a number from a
    /// clock that read the far future silences this daemon for the life of the
    /// process — and a silent daemon looks exactly like an idle one. A run of
    /// stale drops is the only evidence there is, so it escalates the way the
    /// two lesser conditions beside it do.
    #[test]
    fn a_run_of_stale_scripts_is_alerted_on_once() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);
        let now = Instant::now();

        listener.on_event(delivered(&script(POD, u64::MAX, vec![])));
        for seq in 1..=STALE_ALERT_RUN {
            listener.on_event(delivered(&nominal(seq)));
            assert_eq!(
                sink.fields("motion_script_ignored").expect("reported")["reason"],
                json!("stale")
            );
        }

        let Some(Chore::Alert { title, body, .. }) = listener.chore(now) else {
            panic!("a daemon dropping every script owes an alert");
        };
        assert!(title.contains("dropping every script"), "{title}");
        assert!(body.contains("3 script(s) in a row"), "{body}");
        listener.on_event(delivered(&nominal(4)));
        assert_eq!(
            listener.chore(now),
            None,
            "the staleness was alerted on twice"
        );
    }

    /// Ordinary staleness — a redelivery, a message overtaken in flight — is not
    /// an incident, and one accepted script clears the run.
    #[test]
    fn an_occasional_stale_script_alerts_nobody() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);
        let now = Instant::now();

        for seq in [7, 5, 9, 8, 11] {
            listener.on_event(delivered(&nominal(seq)));
        }

        assert_eq!(listener.chore(now), None);
    }

    /// A refusal's own message quotes the body that produced it, and the body is
    /// whatever a publisher on this channel chose to send. It reaches a
    /// line-oriented terminal and an alert body, so a newline in it would forge
    /// a line that reads exactly like the daemon's own.
    #[test]
    fn a_foreign_body_cannot_forge_a_line_in_the_narration() {
        let sink = Collect::default();
        let (_shared, mut listener) = fixture(&sink);

        listener.on_event(delivered(
            r#"{"type":"motion-script","pod":"reachy00","seq":1,
                "steps":[{"after_ms":0,"posture":"x\nfault: the machine is at the minimum risk condition"}],
                "timeout_ms":30000}"#,
        ));

        let said = sink.said();
        let refusals: Vec<&String> = said
            .lines
            .iter()
            .filter(|line| line.contains("script refused"))
            .collect();
        assert_eq!(refusals.len(), 1, "{:?}", said.lines);
        assert!(
            !refusals[0].contains('\n'),
            "a body wrote its own line into the narration: {:?}",
            refusals[0]
        );
        // The unabridged text is still in the capture, where it is a JSON string
        // and is escaped rather than rendered.
        assert!(
            sink.fields("motion_script_ignored").expect("reported")["detail"]
                .as_str()
                .is_some_and(|detail| detail.contains("fault:")),
            "the refusal's own text is lost"
        );
    }

    /// A body long enough to bury the terminal is cut, and says it was cut.
    #[test]
    fn an_overlong_refusal_is_bounded_before_it_is_narrated() {
        let long = "x".repeat(DETAIL_LIMIT * 4);
        let bounded = one_line(&long);

        assert_eq!(bounded.chars().count(), DETAIL_LIMIT + 1);
        assert!(bounded.ends_with('…'));
        assert_eq!(one_line("short"), "short", "nothing else is touched");
        assert_eq!(one_line("two\nlines"), "two lines");
    }

    /// With no wire an alert is written nowhere and still answers `Ok`, so the
    /// refusals stay owed until there is somewhere to send them.
    #[test]
    fn refused_engages_wait_for_an_attachment() {
        let sink = Collect::default();
        let (shared, mut listener) = detached_fixture(&sink);
        let now = Instant::now();

        shared.refuse_engage("a latched hardware error on servo 12");
        assert_eq!(listener.chore(now), None);

        listener.on_event(BridgeEvent::Attached(facts(true)));
        let Some(Chore::Alert { body, .. }) = listener.chore(now) else {
            panic!("the refusal is owed once there is a wire");
        };
        assert!(body.contains("servo 12"), "{body}");
    }

    #[test]
    fn an_unserved_motion_channel_is_asked_for_again_when_its_delay_has_run() {
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
    fn a_terminal_ending_asks_for_the_orderly_stow_and_release() {
        let sink = Collect::default();
        let (shared, listener) = fixture(&sink);

        listener.on_detached_for_good(&BridgeOutcome::Futile { attachments: 5 });

        assert_eq!(shared.stopping(), Some(Stop::Detached));
        assert_eq!(
            sink.fields("bus_exit").expect("reported")["terminal"],
            json!(true)
        );
    }

    /// The schedule's input provenance. This daemon holds one subscription
    /// today, so nothing but script traffic arrives — but the channel is what
    /// says a body was authored by something entitled to move this head, and a
    /// second subscription would otherwise feed its traffic into the executor.
    #[test]
    fn a_delivery_on_another_channel_moves_nothing() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);

        listener.on_event(BridgeEvent::Delivered(Delivery {
            channel: "brenn:reachy.gaze".to_owned(),
            envelope: envelope(&nominal(1)),
            seq: 1,
            dropped: 0,
        }));

        assert_eq!(shared.desired(Instant::now()), Desired::Unchanged);
        assert_eq!(
            sink.fields("motion_script_ignored").expect("reported")["reason"],
            json!("foreign_channel")
        );
        assert!(!sink.saw("motion_script"));
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

    /// A detachment is not a motion event: the running script keeps its
    /// timeline on this machine's own clock and lapses on its own, which is the
    /// whole point of a script that carries its own timeout.
    #[test]
    fn a_detachment_says_the_running_script_keeps_its_timeline() {
        let sink = Collect::default();
        let (shared, mut listener) = fixture(&sink);
        listener.on_event(delivered(&nominal(1)));

        listener.on_event(BridgeEvent::Detached {
            reason: brenn_bridge::DetachReason::LivenessTimeout,
        });

        assert_eq!(
            shared.desired(Instant::now()),
            Desired::Posture(Posture::Up)
        );
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
        shared.set_fault(FaultReport::new(FaultStage::Motion, "read loss"));

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

    /// And the case where waiting cannot end well: commissioning refuses while
    /// the bridge is still dialling a server that is not there. The
    /// shutdown waits a bounded while for a wire, then goes ahead — and says
    /// that the alert was lost rather than letting it vanish.
    #[test]
    fn an_alert_with_nowhere_to_go_delays_the_exit_once_and_then_says_it_was_lost() {
        let sink = Collect::default();
        let (shared, mut listener) = detached_fixture(&sink);
        let now = Instant::now();
        shared.set_fault(FaultReport::new(
            FaultStage::Commission,
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
    /// stowed and not verified. Torque is already off, so what the operator is
    /// told is where the head is — promising a stow that is not coming is a
    /// false statement about the machine at the moment they act.
    #[test]
    fn a_signal_to_a_faulted_daemon_does_not_promise_a_stow() {
        let sink = Collect::default();
        let (shared, listener) = fixture(&sink);
        shared.set_fault(FaultReport::new(FaultStage::Motion, "tracking lost"));

        listener.on_signal("SIGTERM");

        let said = sink.said();
        assert!(
            said.lines
                .iter()
                .any(|line| line.contains("torque is already off")),
            "{:?}",
            said.lines
        );
        assert!(
            !said.lines.iter().any(|line| line.contains("stowing")),
            "a stow was promised on a machine that takes no commands: {:?}",
            said.lines
        );
        let fields = sink.fields("stop_requested").expect("reported");
        assert_eq!(fields["will_stow"], json!(false));
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
            sink.fields("stop_requested").expect("reported")["will_stow"],
            json!(true)
        );
    }

    /// Nothing could ask a daemon with no signal handlers to stop later, so it
    /// stops itself now. `Detached`, never `Operator`: there is no operator this
    /// daemon can hear, and the exit status is what says so.
    #[test]
    fn a_daemon_that_cannot_hear_a_signal_stops_itself_detached() {
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
    fn a_bus_thread_with_no_runtime_stops_itself_detached() {
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
            envelope: envelope(&nominal(9)),
            seq: 4,
            dropped: 3,
        }));

        assert_eq!(
            shared.desired(Instant::now()),
            Desired::Posture(Posture::Up)
        );
        assert_eq!(
            sink.fields("script_delivery_dropped").expect("reported")["count"],
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

    /// A daemon that never states its hold hears no script for the life of the
    /// process and stays stowed, which looks exactly like an idle system. The
    /// depths are asserted at the wire because they are what keep a retained
    /// raise from popping the head up at a reattach.
    #[tokio::test]
    async fn the_loop_states_its_hold_and_runs_what_arrives_on_it() {
        let sink = Collect::default();
        let (shared, listener) = detached_fixture(&sink);
        let (bridge, handle, events, mut peers) = scripted::scripted(1, 3);
        let mut peer = peers.pop_front().expect("one socket");
        let cells = Arc::clone(&shared);

        let (outcome, ()) = tokio::join!(serve(bridge, &handle, events, listener), async {
            peer.handshake(true).await;
            let frame = peer.answer_subscribe(CHANNEL, "Ok").await;
            assert_eq!(frame["push_depth"], json!(SCRIPT_DEPTHS.push_depth));
            assert_eq!(
                frame["retain_depth"],
                json!(0),
                "a reattach must replay nothing: {frame}"
            );

            peer.deliver(CHANNEL, &nominal(1), 1);
            until("the script to reach the schedule", || {
                cells.desired(Instant::now()) == Desired::Posture(Posture::Up)
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

            cells.set_fault(FaultReport::new(FaultStage::Motion, "servo 13: timed out"));
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
    /// after, so the daemon has no source of scripts left. It stows and
    /// releases, under `Detached` rather than the operator's reason: the
    /// posture is the same and the exit status is what differs.
    #[tokio::test]
    async fn an_attachment_that_ends_for_good_stows_and_releases_the_head() {
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
