//! Head presence: the per-pod state machine that decides whether an interaction
//! is live, and the task that publishes that decision on the bus.
//!
//! The pipeline knows a dozen ways an interaction starts and stops; a motion
//! consumer wants one bit. This module is the reduction between them. Every
//! lifecycle point the pipeline already reaches sends one [`PresenceInput`]
//! here — a one-line, non-blocking `try_send` beside the code that was going to
//! run anyway — and this module turns the stream into `engaged`/`idle` intents
//! on the presence channel.
//!
//! Two layers, split so the decisions are testable without a socket:
//!
//! - [`Tracker`] is the state machine. Inputs and a clock go in, publishes come
//!   out; it owns no I/O and no timers, only deadlines it reports through
//!   [`Tracker::deadline`].
//! - [`PresenceTracker`] is the task: it selects on the input channel and the
//!   nearest deadline, and publishes what the tracker decides.
//!
//! Three timers shape an engagement, and they answer different questions:
//!
//! - **linger** — armed by every settle candidate, so a lull between the turns
//!   of one exchange never stows the head. Idle is published when it fires with
//!   no turn in flight and nothing playing.
//! - **refresh** — the consumer holds `engaged` as a lease, so silence stows the
//!   head. Staying engaged means saying so repeatedly.
//! - **ceiling** — the backstop for the adverse case where *nothing* settles:
//!   a wake with no utterance whose transport segment never closes emits no
//!   host event at all, so the engagement carries its own bound. It is re-armed
//!   every time it fires, so an engagement it could not settle still has a
//!   bound on it.
//!
//! Nothing here fails a turn. A publish that the bus refuses is a JSONL line;
//! an input that finds a full queue is a counted drop. A refused publish is
//! repaired by the next refresh. A dropped input is not, because `turns` and
//! `playing` count paired events and a lost end stays wrong: so the loss is
//! reported to the tracker, and the next ceiling to fire on a pod that has lost
//! an input forfeits those two counts rather than letting them hold a head up
//! for good.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use brenn_bridge::{BridgeHandle, PublishRequest, Urgency};
use presence_proto::{PresenceBody, PresenceState};
use serde_json::json;
use speech_pipeline::PodId;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::brenn::publish_once;
use crate::config::BrennConfig;
use crate::jsonl::JsonlHandle;

/// Depth of the queue between the pipeline's taps and the tracker task.
///
/// Deeper than the notice queue because the taps are more numerous and a burst
/// is ordinary (a barge fans out playback events while a turn starts), but still
/// small: the tracker's work per input is a map lookup and some arithmetic, so a
/// backlog this deep means the task is not running at all, and the consumer's
/// lease already covers that.
pub const PRESENCE_QUEUE_DEPTH: usize = 32;

/// One thing that happened to an interaction, as the tracker cares about it.
///
/// Deliberately not the pipeline's own vocabulary: the tap sites are spread
/// across three modules and several event enums, and naming the *effect on
/// presence* rather than the cause is what keeps the state machine from growing
/// a branch per call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceInput {
    /// A confirmed wake word. The head goes up.
    Wake(PodId),
    /// Speech over live playback: interaction with no wake word in front of it.
    Barge(PodId),
    /// An utterance went to the brain. A raise, and a turn is now in flight.
    TurnStarted(PodId),
    /// That turn came back. The turn is no longer in flight, and its end is a
    /// settle candidate.
    TurnEnded(PodId),
    /// A raise produced no turn: the wake arm expired with no command, or the
    /// confidence gate declined what was said. A settle candidate.
    Unanswered(PodId),
    /// A playback job started or ended. A job in progress holds the engagement
    /// past a linger; the last one ending is a settle candidate.
    Playback {
        /// Whose floor.
        pod: PodId,
        /// True when a job started, false when one ended.
        speaking: bool,
    },
}

impl PresenceInput {
    /// Whose interaction this is about.
    fn pod(&self) -> &PodId {
        match self {
            PresenceInput::Wake(pod)
            | PresenceInput::Barge(pod)
            | PresenceInput::TurnStarted(pod)
            | PresenceInput::TurnEnded(pod)
            | PresenceInput::Unanswered(pod)
            | PresenceInput::Playback { pod, .. } => pod,
        }
    }
}

/// The tap end of the input queue. Cloned into every module that reports a
/// lifecycle point; sending never blocks and never fails a caller.
#[derive(Clone)]
pub struct PresenceHandle {
    tx: mpsc::Sender<PresenceInput>,
    dropped: Arc<AtomicU64>,
    jsonl: JsonlHandle,
}

impl PresenceHandle {
    /// Report one lifecycle point. A full or closed queue drops the input and
    /// says so: the pipeline's own progress is never held up by the head.
    pub fn send(&self, input: PresenceInput) {
        if let Err(err) = self.tx.try_send(input) {
            let (reason, input) = match err {
                mpsc::error::TrySendError::Full(input) => ("queue_full", input),
                mpsc::error::TrySendError::Closed(input) => ("tracker_gone", input),
            };
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            self.jsonl.emit(
                "presence_input_dropped",
                &json!({ "pod": input.pod().0, "reason": reason, "dropped": dropped }),
            );
        }
    }

    /// How many inputs this handle's queue has lost, process-wide.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// The tracker's end of the input queue: the receiver, and the loss counter the
/// sending end bumps.
///
/// The counter travels with the receiver because losing an input is not the
/// same to the tracker as never getting one. Most inputs are edges a later one
/// repairs, but `turns` and `playing` are running counts of paired events, and
/// a lost end leaves them wrong until the pod stows — which is the one thing
/// they can prevent.
pub struct PresenceInbox {
    rx: mpsc::Receiver<PresenceInput>,
    dropped: Arc<AtomicU64>,
}

impl PresenceInbox {
    /// The next input, or `None` once every sending end is gone. For the tests
    /// that assert on what the taps sent, with no tracker in between.
    #[cfg(test)]
    pub(crate) async fn recv(&mut self) -> Option<PresenceInput> {
        self.rx.recv().await
    }
}

/// Build the input queue. The two ends are minted together so their depth has
/// one owner and they cannot be paired wrongly.
pub fn channel(jsonl: JsonlHandle) -> (PresenceHandle, PresenceInbox) {
    let (tx, rx) = mpsc::channel(PRESENCE_QUEUE_DEPTH);
    let dropped = Arc::new(AtomicU64::new(0));
    (
        PresenceHandle {
            tx,
            dropped: dropped.clone(),
            jsonl,
        },
        PresenceInbox { rx, dropped },
    )
}

/// The three intervals an engagement is measured in.
#[derive(Debug, Clone, Copy)]
pub struct PresenceTiming {
    /// How often `engaged` is republished while the interaction is live.
    pub refresh: Duration,
    /// How long after the last settle candidate `idle` is published.
    pub linger: Duration,
    /// The longest one engagement lasts with no turn in flight.
    pub max_engaged: Duration,
}

/// What moved the tracker. Carried onto the JSONL line so a transition reads as
/// a cause and not just a state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A confirmed wake word.
    Wake,
    /// Speech over live playback.
    Barge,
    /// An utterance dispatched to the brain.
    Turn,
    /// The lease being kept alive; not a transition.
    Refresh,
    /// The linger ran out with nothing left in flight.
    Linger,
}

impl Cause {
    fn as_str(self) -> &'static str {
        match self {
            Cause::Wake => "wake",
            Cause::Barge => "barge",
            Cause::Turn => "turn",
            Cause::Refresh => "refresh",
            Cause::Linger => "linger",
        }
    }
}

/// One intent the tracker decided to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresencePublish {
    /// Whose head.
    pub pod: PodId,
    /// The body to encode onto the channel.
    pub body: PresenceBody,
    /// What moved.
    pub cause: Cause,
    /// True when this publish is a state change rather than a lease refresh.
    /// Only a change is worth a console line; a refresh every few seconds is
    /// not.
    pub transition: bool,
}

/// One pod's engagement.
#[derive(Debug)]
struct PodPresence {
    state: PresenceState,
    /// The publisher's counter, bumped on every publish so a reader of the
    /// consumer's log can see a gap.
    seq: u64,
    /// Turns dispatched to the brain and not yet returned. A count rather than
    /// a flag: nothing in the pipeline forbids two pods' turns overlapping in
    /// this map's lifetime, and a saturating count cannot go negative on an
    /// unpaired end.
    turns: u32,
    /// Playback jobs started and not yet ended. A count and not the fan-out's
    /// last word, for the reason `turns` is one: a pod that reconnects mid-
    /// answer has two live writer tasks for a moment, and the superseded one's
    /// terminal event can land after the replacement's `Started`. Last-event-
    /// wins would read that as silence and stow the head mid-sentence; a count
    /// only reaches zero when every job that opened has closed.
    playing: u32,
    /// An input was lost while this engagement was live, so `turns` and
    /// `playing` may be missing an end. Cleared when the ceiling forfeits them
    /// and when the pod stows, both of which restore a known state.
    lossy: bool,
    linger: Option<Instant>,
    refresh: Option<Instant>,
    ceiling: Option<Instant>,
}

impl Default for PodPresence {
    /// A pod nobody has heard from is idle, with no timer armed.
    fn default() -> Self {
        Self {
            state: PresenceState::Idle,
            seq: 0,
            turns: 0,
            playing: 0,
            lossy: false,
            linger: None,
            refresh: None,
            ceiling: None,
        }
    }
}

impl PodPresence {
    /// Mint the next body for this pod, bumping the counter. Every publish goes
    /// through here, so the sequence numbers the consumer sees have no holes
    /// this side put in them.
    fn publish(&mut self, pod: &PodId, cause: Cause, transition: bool) -> PresencePublish {
        self.seq += 1;
        PresencePublish {
            pod: pod.clone(),
            body: PresenceBody::new(pod.0.clone(), self.state, self.seq),
            cause,
            transition,
        }
    }
}

/// The presence state machine: inputs and a clock in, intents out.
///
/// Pure and synchronous. The task around it supplies `now` and carries what
/// comes back to the bus; nothing here reads a clock, spawns anything, or can
/// fail.
pub struct Tracker {
    timing: PresenceTiming,
    pods: HashMap<PodId, PodPresence>,
    /// A loss has been reported and the inputs queued ahead of the report have
    /// not all been applied yet, so a pod that is not marked may still be about
    /// to be. Cleared by the next [`Tracker::tick`].
    pending_loss: bool,
    /// The input queue's loss counter, and this tracker's last reading of it.
    /// A reading that moved is the only notice it gets that its paired counts
    /// may be short an end.
    losses: Arc<AtomicU64>,
    seen_losses: u64,
}

impl Tracker {
    /// A tracker with no pod engaged, watching `losses` — the counter the input
    /// queue's sending end bumps on every drop.
    pub fn new(timing: PresenceTiming, losses: Arc<AtomicU64>) -> Self {
        Self {
            timing,
            pods: HashMap::new(),
            pending_loss: false,
            losses,
            seen_losses: 0,
        }
    }

    /// Read the loss counter and taint what a drop since the last reading could
    /// have broken.
    ///
    /// Belongs here rather than in the task around it so the reading and its
    /// consequence are one step something can test: the two halves are silent
    /// in both directions of failure — never taking the reading leaves a stale
    /// count holding a head up for good, and taking it every pass forfeits
    /// counts that were never lost.
    pub fn absorb_losses(&mut self) {
        let losses = self.losses.load(Ordering::Relaxed);
        if losses != self.seen_losses {
            self.seen_losses = losses;
            self.lost_input();
        }
    }

    /// Apply one input. At most one publish comes back: raising an idle pod
    /// says so at once, and everything else only moves a deadline.
    pub fn apply(&mut self, input: PresenceInput, now: Instant) -> Option<PresencePublish> {
        if self.pending_loss {
            self.pods.entry(input.pod().clone()).or_default().lossy = true;
        }
        match input {
            PresenceInput::Wake(pod) => self.raise(&pod, now, Cause::Wake),
            PresenceInput::Barge(pod) => self.raise(&pod, now, Cause::Barge),
            PresenceInput::TurnStarted(pod) => {
                self.pods.entry(pod.clone()).or_default().turns += 1;
                self.raise(&pod, now, Cause::Turn)
            }
            PresenceInput::TurnEnded(pod) => {
                let p = self.pods.entry(pod).or_default();
                p.turns = p.turns.saturating_sub(1);
                Self::settle(p, now, self.timing.linger);
                None
            }
            PresenceInput::Unanswered(pod) => {
                let p = self.pods.entry(pod).or_default();
                Self::settle(p, now, self.timing.linger);
                None
            }
            PresenceInput::Playback { pod, speaking } => {
                let p = self.pods.entry(pod).or_default();
                if speaking {
                    p.playing = p.playing.saturating_add(1);
                } else {
                    // Saturating, so an unpaired end — a dropped `Started`, a
                    // job reported ended twice — settles rather than wrapping
                    // into a count that never comes down.
                    p.playing = p.playing.saturating_sub(1);
                    if p.playing == 0 {
                        Self::settle(p, now, self.timing.linger);
                    }
                }
                None
            }
        }
    }

    /// Raise `pod`: cancel any pending settle, restart the ceiling, and publish
    /// on the way up. Already engaged means the ceiling moves and nothing is
    /// said — the refresh cadence is what carries an engagement, and a burst of
    /// raises must not become a burst of publishes.
    fn raise(&mut self, pod: &PodId, now: Instant, cause: Cause) -> Option<PresencePublish> {
        let timing = self.timing;
        let p = self.pods.entry(pod.clone()).or_default();
        p.linger = None;
        p.ceiling = Some(now + timing.max_engaged);
        match p.state {
            PresenceState::Engaged => None,
            PresenceState::Idle => {
                p.state = PresenceState::Engaged;
                p.refresh = Some(now + timing.refresh);
                Some(p.publish(pod, cause, true))
            }
        }
    }

    /// Arm the linger. Whether the engagement actually ends is decided when it
    /// fires, not here: a turn may still be in flight and the pod may still be
    /// talking, and both of those are ordinary at every settle candidate.
    fn settle(p: &mut PodPresence, now: Instant, linger: Duration) {
        if p.state == PresenceState::Engaged {
            p.linger = Some(now + linger);
        }
    }

    /// Report that the input queue lost something.
    ///
    /// Which input, and whose, is exactly what the sending end could not say —
    /// so every pod is marked, and so is every pod touched between here and the
    /// next tick, since the report can overtake inputs still queued ahead of it.
    /// The mark costs nothing until a ceiling fires on a pod that has not
    /// settled, and then it is the difference between a head that comes down
    /// and one that does not.
    fn lost_input(&mut self) {
        self.pending_loss = true;
        for p in self.pods.values_mut() {
            p.lossy = true;
        }
    }

    /// Fire every deadline due at `now`.
    ///
    /// Order within a pod is ceiling, then linger, then refresh. The ceiling
    /// settles rather than stows — it re-arms the linger, so a ceiling and the
    /// linger it arms can never both fire in one pass, and the "no turn in
    /// flight, nothing playing" check still gets its full linger to be wrong in.
    pub fn tick(&mut self, now: Instant) -> Vec<PresencePublish> {
        let timing = self.timing;
        // A tick means the input queue came up empty, so anything a reported
        // loss could have tainted has been applied and marked by now.
        self.pending_loss = false;
        let mut out = Vec::new();
        for (pod, p) in &mut self.pods {
            if p.state != PresenceState::Engaged {
                continue;
            }
            if p.ceiling.is_some_and(|at| at <= now) {
                // Re-armed rather than spent: the bound belongs to the
                // engagement, and an engagement the ceiling declined to settle
                // is precisely the one that must not be left unbounded.
                p.ceiling = Some(now + timing.max_engaged);
                if p.lossy {
                    // A whole ceiling has passed with no fresh raise, on a pod
                    // whose paired counts are known to have lost an input.
                    // They read as busy; they are stale, and a stale flag has
                    // forfeited its say.
                    p.turns = 0;
                    p.playing = 0;
                    p.lossy = false;
                }
                if p.turns == 0 {
                    p.linger = Some(now + timing.linger);
                }
            }
            if p.linger.is_some_and(|at| at <= now) {
                p.linger = None;
                if p.turns == 0 && p.playing == 0 {
                    p.state = PresenceState::Idle;
                    p.refresh = None;
                    p.ceiling = None;
                    p.lossy = false;
                    out.push(p.publish(pod, Cause::Linger, true));
                    continue;
                }
            }
            if p.refresh.is_some_and(|at| at <= now) {
                p.refresh = Some(now + timing.refresh);
                out.push(p.publish(pod, Cause::Refresh, false));
            }
        }
        out
    }

    /// The earliest instant [`Tracker::tick`] has anything to do, or `None` when
    /// no pod is engaged.
    pub fn deadline(&self) -> Option<Instant> {
        self.pods
            .values()
            .filter(|p| p.state == PresenceState::Engaged)
            .flat_map(|p| [p.ceiling, p.linger, p.refresh])
            .flatten()
            .min()
    }

    /// What this pod's head should be doing, for tests and assertions.
    #[cfg(test)]
    fn state(&self, pod: &PodId) -> PresenceState {
        self.pods.get(pod).map_or(PresenceState::Idle, |p| p.state)
    }

    /// Whether a reported loss is still waiting for the queue to drain.
    #[cfg(test)]
    fn pending_loss(&self) -> bool {
        self.pending_loss
    }
}

/// The presence task: the tracker, its input queue, and the bridge it publishes
/// on.
pub struct PresenceTracker {
    core: Tracker,
    rx: mpsc::Receiver<PresenceInput>,
    handle: BridgeHandle,
    channel: String,
    attribution: Option<String>,
    jsonl: JsonlHandle,
}

/// Depth of the queue between the tracker loop and its publisher.
///
/// Shallow on purpose: a backlog means the bus is not answering, and the
/// intents behind a stalled one are already stale. Dropping the newest with a
/// line beats holding a queue of postures nobody will act on — the consumer's
/// lease lapses to stow while the bus is silent either way.
const PUBLISH_QUEUE_DEPTH: usize = 8;

/// One intent on its way to the bus, with the state it carries kept beside it
/// for the failure line (the request's body is encoded JSON text, not something
/// a log line should be re-parsing).
struct Outbound {
    request: PublishRequest,
    state: &'static str,
}

impl PresenceTracker {
    /// Assemble the task. `channel` is the caller's copy of
    /// `brenn.presence_channel`, which is also what decides whether presence
    /// runs at all — so it is passed rather than re-read here.
    pub fn new(
        config: &BrennConfig,
        channel: String,
        handle: BridgeHandle,
        inbox: PresenceInbox,
        jsonl: JsonlHandle,
    ) -> Self {
        Self {
            core: Tracker::new(config.presence_timing(), inbox.dropped),
            rx: inbox.rx,
            handle,
            channel,
            attribution: config.attribution.clone(),
            jsonl,
        }
    }

    /// Reduce inputs and fire timers until the queue closes or `teardown` fires.
    ///
    /// No final `idle` on the way out. The consumer's lease lapses on its own
    /// when the refreshes stop, which is the same outcome by the same mechanism
    /// that covers this host dying outright — and unlike a parting publish, it
    /// needs no bridge that is already being torn down.
    pub async fn run(mut self, teardown: CancellationToken) {
        let (outbound, queue) = mpsc::channel(PUBLISH_QUEUE_DEPTH);
        let publisher = tokio::spawn(publish_loop(self.handle.clone(), queue, self.jsonl.clone()));
        loop {
            // Before anything is decided on the state: a drop since the last
            // pass means what the state says about turns and playback is a
            // claim, not a fact.
            self.core.absorb_losses();
            let deadline = self.core.deadline();
            tokio::select! {
                // Teardown first: a burst of inputs must not starve a stop.
                // Inputs ahead of timers: a timer only fires on a drained
                // queue, which is when a reported loss has finished tainting
                // what it can.
                biased;
                () = teardown.cancelled() => break,
                input = self.rx.recv() => match input {
                    Some(input) => {
                        if let Some(publish) = self.core.apply(input, Instant::now()) {
                            self.publish(&outbound, publish);
                        }
                    }
                    None => break,
                },
                () = due(deadline) => {
                    for publish in self.core.tick(Instant::now()) {
                        self.publish(&outbound, publish);
                    }
                }
            }
        }
        // The publisher drains what is already queued and ends with the sender.
        // Not joined: a peer that never answers must not hold up a teardown
        // that the driver behind it is waiting on.
        drop(outbound);
        drop(publisher);
    }

    /// Hand one intent to the publisher, in the order this loop decided it.
    ///
    /// Never awaited here: `BridgeHandle::publish` waits for the peer's answer,
    /// and this loop owes the pipeline's taps a queue it keeps draining. It
    /// goes onto an ordered queue rather than a task of its own because two
    /// detached tasks reach the bridge in scheduler order, not spawn order —
    /// and a `stow` overtaking the `engage` that a re-wake published a moment
    /// later leaves the head down through a live interaction, the one direction
    /// the consumer's lease does not fail safe on.
    ///
    /// `Urgency::Normal`: a presence intent moves a head, so it outranks the
    /// advisory wake nudge, but it is not conversation content and a lost one
    /// is repaired by the next refresh.
    fn publish(&self, outbound: &mpsc::Sender<Outbound>, publish: PresencePublish) {
        if publish.transition {
            self.jsonl.emit(
                "presence",
                &json!({
                    "pod": publish.pod.0,
                    "state": publish.body.state.as_str(),
                    "seq": publish.body.seq,
                    "cause": publish.cause.as_str(),
                }),
            );
        }
        let state = publish.body.state.as_str();
        let request = PublishRequest {
            channel: self.channel.clone(),
            attribution: self.attribution.clone(),
            body: publish.body.encode(),
            urgency: Urgency::Normal,
        };
        if outbound.try_send(Outbound { request, state }).is_err() {
            self.jsonl.emit(
                "brenn_presence_publish_failed",
                &json!({
                    "channel": self.channel,
                    "state": state,
                    "detail": "the publish queue is full; the bus is not answering",
                }),
            );
        }
    }
}

/// Publish what the tracker decided, one at a time and in order.
///
/// One task rather than one per intent: the order these reach the bridge is the
/// order the consumer applies them in, and the consumer treats `seq` as
/// observable rather than authoritative. Serializing costs a stalled bus one
/// queue depth of intents, which the loop reports and the next refresh repairs.
async fn publish_loop(
    handle: BridgeHandle,
    mut queue: mpsc::Receiver<Outbound>,
    jsonl: JsonlHandle,
) {
    while let Some(Outbound { request, state }) = queue.recv().await {
        let channel = request.channel.clone();
        if let Err(detail) = publish_once(&handle, request).await {
            jsonl.emit(
                "brenn_presence_publish_failed",
                &json!({ "channel": channel, "state": state, "detail": detail }),
            );
        }
    }
}

/// The timer arm's future: the deadline when there is one, never when there is
/// not. Takes it by value so the arm holds no borrow of the task.
async fn due(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use brenn_bridge::Bridge;

    use super::*;
    use crate::brenn::scripted::{Attempt, WAIT, scripted};
    use crate::config::JsonlSink;
    use crate::jsonl::probe::{expect_line, lines};

    const REFRESH: Duration = Duration::from_secs(5);
    const LINGER: Duration = Duration::from_secs(8);
    const CEILING: Duration = Duration::from_secs(30);

    fn timing() -> PresenceTiming {
        PresenceTiming {
            refresh: REFRESH,
            linger: LINGER,
            max_engaged: CEILING,
        }
    }

    fn pod() -> PodId {
        PodId("pod-kitchen".into())
    }

    /// A tracker, the pod every test drives, the instant its clock starts from,
    /// and the queue-loss counter it watches. `now` is supplied by the test
    /// rather than read, so every assertion below is about the state machine
    /// and never about a timer racing one.
    struct Fx {
        tracker: Tracker,
        pod: PodId,
        t0: Instant,
        losses: Arc<AtomicU64>,
    }

    fn fixture() -> Fx {
        let losses = Arc::new(AtomicU64::new(0));
        Fx {
            tracker: Tracker::new(timing(), losses.clone()),
            pod: pod(),
            t0: Instant::now(),
            losses,
        }
    }

    impl Fx {
        fn at(&self, offset: Duration) -> Instant {
            self.t0 + offset
        }

        /// The queue drops an input and the tracker takes the reading, which is
        /// the join the task's loop makes on every pass.
        fn lose(&mut self) {
            self.losses.fetch_add(1, Ordering::Relaxed);
            self.tracker.absorb_losses();
        }

        fn apply(&mut self, input: PresenceInput, offset: Duration) -> Option<PresencePublish> {
            let now = self.at(offset);
            self.tracker.apply(input, now)
        }

        fn tick(&mut self, offset: Duration) -> Vec<PresencePublish> {
            let now = self.at(offset);
            self.tracker.tick(now)
        }

        /// Only the transitions in a tick. A refresh riding along with one is
        /// the lease talking, not the head moving, and every test below is
        /// about the head.
        fn moved(&mut self, offset: Duration) -> Vec<PresencePublish> {
            self.tick(offset)
                .into_iter()
                .filter(|publish| publish.transition)
                .collect()
        }

        fn state(&self) -> PresenceState {
            self.tracker.state(&self.pod)
        }
    }

    const ZERO: Duration = Duration::ZERO;

    #[test]
    fn a_wake_raises_the_head_and_says_so() {
        let mut fx = fixture();
        let publish = fx
            .apply(PresenceInput::Wake(pod()), ZERO)
            .expect("a wake on an idle pod publishes");
        assert_eq!(publish.body.state, PresenceState::Engaged);
        assert_eq!(publish.body.pod, "pod-kitchen");
        assert_eq!(publish.body.seq, 1);
        assert_eq!(publish.cause, Cause::Wake);
        assert!(publish.transition, "the head moved");
        assert_eq!(fx.state(), PresenceState::Engaged);
    }

    /// A barge is a live interaction with no wake word in front of it, so it
    /// raises on its own account.
    #[test]
    fn a_barge_raises_the_head() {
        let mut fx = fixture();
        let publish = fx
            .apply(PresenceInput::Barge(pod()), ZERO)
            .expect("a barge on an idle pod publishes");
        assert_eq!(publish.body.state, PresenceState::Engaged);
        assert_eq!(publish.cause, Cause::Barge);
    }

    /// Wake, barge and dispatch all raise, and a multi-turn exchange trips
    /// several of them per turn. Only the first says anything: the refresh
    /// cadence carries the engagement from there.
    #[test]
    fn further_raises_while_engaged_publish_nothing() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        assert_eq!(
            fx.apply(PresenceInput::Wake(pod()), Duration::from_secs(1)),
            None
        );
        assert_eq!(
            fx.apply(PresenceInput::TurnStarted(pod()), Duration::from_secs(2)),
            None
        );
        assert_eq!(
            fx.apply(PresenceInput::Barge(pod()), Duration::from_secs(3)),
            None
        );
    }

    /// The consumer holds `engaged` as a lease, so staying engaged means saying
    /// so on a cadence. A refresh is not a transition and carries the next
    /// sequence number.
    #[test]
    fn the_lease_is_refreshed_on_its_own_cadence() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        assert!(fx.tick(Duration::from_secs(4)).is_empty(), "not due yet");

        let publish = fx.tick(REFRESH).pop().expect("the refresh is due");
        assert_eq!(publish.body.state, PresenceState::Engaged);
        assert_eq!(publish.body.seq, 2);
        assert_eq!(publish.cause, Cause::Refresh);
        assert!(!publish.transition, "a refresh is not a state change");

        let publish = fx
            .tick(REFRESH + REFRESH)
            .pop()
            .expect("and again one interval later");
        assert_eq!(publish.body.seq, 3);
    }

    /// The whole false-positive-wake path: a wake, no command, the arm expires,
    /// and one `idle` goes out a linger later. Once — the pod is stowed, and a
    /// stowed pod has no timers left to fire.
    #[test]
    fn a_settled_engagement_stows_once_after_the_linger() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        assert_eq!(fx.apply(PresenceInput::Unanswered(pod()), ZERO), None);

        assert!(
            fx.moved(Duration::from_secs(7)).is_empty(),
            "the linger has not run out"
        );
        let publish = fx.moved(LINGER).pop().expect("the linger ran out");
        assert_eq!(publish.body.state, PresenceState::Idle);
        assert_eq!(publish.cause, Cause::Linger);
        assert!(publish.transition);
        assert_eq!(fx.state(), PresenceState::Idle);

        assert!(
            fx.tick(LINGER + LINGER).is_empty(),
            "a stowed pod has nothing left to fire"
        );
        assert_eq!(fx.tracker.deadline(), None);
    }

    /// The multi-turn case: the next wake lands inside the linger the last turn
    /// armed, and the head never bounces.
    #[test]
    fn a_wake_inside_the_linger_keeps_the_head_up() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        fx.apply(PresenceInput::Unanswered(pod()), ZERO);
        assert_eq!(
            fx.apply(PresenceInput::Wake(pod()), Duration::from_secs(4)),
            None
        );

        assert!(
            fx.moved(LINGER).is_empty(),
            "the second wake cancelled the settle"
        );
        assert_eq!(fx.state(), PresenceState::Engaged);
    }

    /// A turn in flight is the pod thinking, which is interaction. The linger
    /// fires and decides not to stow; the turn's own end re-arms it.
    #[test]
    fn a_turn_in_flight_holds_the_engagement_past_the_linger() {
        let mut fx = fixture();
        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");
        fx.apply(PresenceInput::Unanswered(pod()), ZERO);

        assert!(fx.moved(LINGER).is_empty(), "a turn is still in flight");
        assert_eq!(fx.state(), PresenceState::Engaged);

        fx.apply(PresenceInput::TurnEnded(pod()), LINGER);
        let publish = fx
            .moved(LINGER + LINGER)
            .pop()
            .expect("the turn's end armed a fresh linger");
        assert_eq!(publish.body.state, PresenceState::Idle);
    }

    /// The pod is speaking its answer: the floor is open, so the linger that
    /// fires under it decides nothing. The floor closing is itself a settle
    /// candidate, which is what eventually stows the head.
    #[test]
    fn playback_holds_the_engagement_until_the_floor_closes() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        fx.apply(
            PresenceInput::Playback {
                pod: pod(),
                speaking: true,
            },
            ZERO,
        );
        fx.apply(PresenceInput::Unanswered(pod()), ZERO);
        assert!(fx.moved(LINGER).is_empty(), "the pod is still talking");

        fx.apply(
            PresenceInput::Playback {
                pod: pod(),
                speaking: false,
            },
            LINGER,
        );
        let publish = fx
            .moved(LINGER + LINGER)
            .pop()
            .expect("the floor closed a linger ago");
        assert_eq!(publish.body.state, PresenceState::Idle);
    }

    /// A pod that reconnects mid-answer has two writer tasks alive for a
    /// moment, and the superseded one's terminal event can land after the
    /// replacement's `Started`. Read as a level, that stale close says the pod
    /// fell silent and stows the head mid-sentence; counted, the engagement
    /// stands until every job that opened has closed.
    #[test]
    fn a_superseded_writers_close_does_not_stow_a_pod_still_speaking() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        let open = PresenceInput::Playback {
            pod: pod(),
            speaking: true,
        };
        let close = PresenceInput::Playback {
            pod: pod(),
            speaking: false,
        };
        // The old writer's job, then the replacement's, then the old writer's
        // terminal event arriving late.
        fx.apply(open.clone(), ZERO);
        fx.apply(open, Duration::from_secs(1));
        fx.apply(close.clone(), Duration::from_secs(2));
        fx.apply(PresenceInput::Unanswered(pod()), Duration::from_secs(2));

        assert!(
            fx.moved(LINGER + Duration::from_secs(2)).is_empty(),
            "the replacement's job is still playing"
        );
        assert_eq!(fx.state(), PresenceState::Engaged);

        fx.apply(close, LINGER + Duration::from_secs(3));
        assert_eq!(
            fx.moved(LINGER + LINGER + Duration::from_secs(3))
                .pop()
                .expect("the last job ended a linger ago")
                .body
                .state,
            PresenceState::Idle
        );
    }

    /// The adverse case the ceiling exists for: a wake whose transport segment
    /// never closes and never carves, so no host event ever settles it. The
    /// ceiling settles it exactly as an expired arm would — through the linger,
    /// not straight to stowed.
    #[test]
    fn the_ceiling_settles_an_engagement_nothing_else_ends() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");

        // Refreshes go out the whole time; the head is up and nothing has said
        // otherwise.
        let publishes = fx.tick(CEILING);
        assert!(
            publishes.iter().all(|p| !p.transition),
            "the ceiling arms the linger, it does not stow: {publishes:?}"
        );
        assert_eq!(fx.state(), PresenceState::Engaged);

        let publish = fx
            .moved(CEILING + LINGER)
            .pop()
            .expect("the linger the ceiling armed");
        assert_eq!(publish.body.state, PresenceState::Idle);
        assert_eq!(publish.cause, Cause::Linger);
    }

    /// The ceiling measures the engagement, not the publishing. Refresh ticks
    /// are the tracker talking to itself and must not push it out; a raise is
    /// real activity and does.
    #[test]
    fn the_ceiling_is_restarted_by_a_raise_and_not_by_a_refresh() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        for i in 1..6 {
            fx.tick(REFRESH * i);
        }
        // Five refreshes have gone out since the wake, and the ceiling is due
        // on schedule anyway.
        fx.tick(CEILING);
        assert_eq!(
            fx.moved(CEILING + LINGER)
                .pop()
                .expect("stowed on the ceiling's schedule")
                .body
                .state,
            PresenceState::Idle
        );

        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        fx.apply(PresenceInput::Wake(pod()), Duration::from_secs(20));
        fx.tick(CEILING);
        assert!(
            fx.tick(CEILING + LINGER).iter().all(|p| !p.transition),
            "the wake at 20 s moved the ceiling to 50 s"
        );
        assert_eq!(fx.state(), PresenceState::Engaged);
    }

    /// A ceiling that declines to settle is not a ceiling that is spent. The
    /// turn is genuinely in flight, so the head stays up — but the bound has to
    /// still be there afterwards, or a long turn buys an unbounded engagement.
    #[test]
    fn the_ceiling_re_arms_when_it_cannot_settle() {
        let mut fx = fixture();
        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");

        assert!(
            fx.moved(CEILING).is_empty(),
            "a turn is in flight, so the ceiling arms no linger"
        );
        let armed = fx
            .tracker
            .deadline()
            .expect("something is still armed after the ceiling fired");
        assert_eq!(
            armed,
            fx.at(CEILING + REFRESH),
            "the refresh is nearer than the re-armed ceiling at 60 s"
        );

        assert!(fx.moved(CEILING * 2).is_empty(), "still in flight");
        fx.apply(PresenceInput::TurnEnded(pod()), CEILING * 2);
        assert_eq!(
            fx.moved(CEILING * 2 + LINGER)
                .pop()
                .expect("the turn's end armed a linger")
                .body
                .state,
            PresenceState::Idle
        );
    }

    /// The drop path the taps are allowed to take: a lost `TurnEnded` leaves
    /// `turns` counting a turn that finished. Nothing else will ever decrement
    /// it, and the tracker keeps refreshing the consumer's lease meanwhile — so
    /// the ceiling forfeits the count rather than the head staying up for good.
    #[test]
    fn a_lost_input_lets_the_ceiling_forfeit_a_stale_count() {
        let mut fx = fixture();
        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");
        // The queue drops the matching TurnEnded; all the tracker learns is
        // that something was lost.
        fx.lose();

        assert!(
            fx.moved(CEILING).is_empty(),
            "the ceiling settles through the linger, it does not stow"
        );
        let publish = fx
            .moved(CEILING + LINGER)
            .pop()
            .expect("the linger the ceiling armed once the count was forfeit");
        assert_eq!(publish.body.state, PresenceState::Idle);
        assert_eq!(publish.cause, Cause::Linger);
    }

    /// The report of a loss can overtake the inputs that were queued ahead of
    /// it, so a pod that is idle when the loss lands may be the very pod whose
    /// count the loss broke. The taint holds until the queue drains.
    #[test]
    fn a_loss_taints_a_pod_that_engages_after_it() {
        let mut fx = fixture();
        fx.lose();
        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");

        assert!(fx.moved(CEILING).is_empty(), "settled through the linger");
        assert_eq!(
            fx.moved(CEILING + LINGER)
                .pop()
                .expect("the ceiling forfeited the count the loss broke")
                .body
                .state,
            PresenceState::Idle
        );
    }

    /// The same for the other paired input: a lost floor close would otherwise
    /// leave the pod speaking forever.
    #[test]
    fn a_lost_floor_close_still_stows_by_the_ceiling() {
        let mut fx = fixture();
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        fx.apply(
            PresenceInput::Playback {
                pod: pod(),
                speaking: true,
            },
            ZERO,
        );
        fx.apply(PresenceInput::Unanswered(pod()), ZERO);
        fx.lose();

        assert!(fx.moved(LINGER).is_empty(), "the floor is still open");
        fx.tick(CEILING);
        assert_eq!(
            fx.moved(CEILING + LINGER)
                .pop()
                .expect("the ceiling forfeited the open floor")
                .body
                .state,
            PresenceState::Idle
        );
    }

    /// The join between the queue's counter and the taint, over the queue the
    /// taps really use. Overflowing it is what a wedged tracker looks like from
    /// the pipeline; the tracker learns of it only by reading the counter, and
    /// nothing else ever tells it.
    #[tokio::test]
    async fn a_real_queue_overflow_reaches_the_tracker() {
        let (jsonl, _writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        let (handle, inbox) = channel(jsonl);
        let mut tracker = Tracker::new(timing(), inbox.dropped.clone());
        let t0 = Instant::now();
        tracker
            .apply(PresenceInput::TurnStarted(pod()), t0)
            .expect("dispatch raises");

        // The tracker is not draining, so the queue fills and then loses the
        // `TurnEnded` that would have decremented the count.
        for _ in 0..=PRESENCE_QUEUE_DEPTH {
            handle.send(PresenceInput::TurnEnded(pod()));
        }
        assert_eq!(handle.dropped(), 1, "one input over the depth was lost");
        tracker.absorb_losses();

        tracker.tick(t0 + CEILING);
        let stowed: Vec<_> = tracker
            .tick(t0 + CEILING + LINGER)
            .into_iter()
            .filter(|p| p.transition)
            .collect();
        assert_eq!(
            stowed.len(),
            1,
            "the ceiling forfeited the count the overflow broke: {stowed:?}"
        );
        assert_eq!(stowed[0].body.state, PresenceState::Idle);
    }

    /// The other direction of the same join: a counter that has not moved
    /// taints nothing. Reading it every pass and marking every time would
    /// forfeit live turn counts at the first ceiling — a head stowing 30 s into
    /// a long answer.
    #[test]
    fn an_unmoved_loss_counter_taints_nothing() {
        let mut fx = fixture();
        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");
        for _ in 0..5 {
            fx.tracker.absorb_losses();
        }

        assert!(fx.moved(CEILING).is_empty(), "the turn is genuinely live");
        assert!(
            fx.moved(CEILING + LINGER).is_empty(),
            "nothing was forfeit, so nothing settled"
        );
        assert_eq!(fx.state(), PresenceState::Engaged);
    }

    /// The taint that outlives its report is bounded by the next tick, which
    /// only happens on a drained queue. Without the clear, a loss reported while
    /// nothing was engaged would taint the next engagement — and every one after
    /// it — for the life of the process.
    #[test]
    fn a_tick_clears_a_pending_loss() {
        let mut fx = fixture();
        fx.lose();
        assert!(fx.tracker.pending_loss(), "the report is still pending");

        assert!(fx.tick(ZERO).is_empty(), "no pod is engaged");
        assert!(!fx.tracker.pending_loss(), "the queue drained");

        fx.apply(PresenceInput::TurnStarted(pod()), ZERO)
            .expect("dispatch raises");
        assert!(fx.moved(CEILING).is_empty(), "a turn is in flight");
        assert!(
            fx.moved(CEILING + LINGER).is_empty(),
            "the later engagement was not tainted by the earlier loss"
        );
    }

    /// A loss on one pod says nothing about another's counts, but the sending
    /// end cannot say whose it was — so both are marked, and a pod whose turn
    /// really is in flight is only settled after a full ceiling of no raise.
    #[test]
    fn a_loss_marks_every_live_engagement() {
        let (a, b) = (PodId("pod-a".into()), PodId("pod-b".into()));
        let losses = Arc::new(AtomicU64::new(0));
        let mut tracker = Tracker::new(timing(), losses.clone());
        let t0 = Instant::now();
        tracker
            .apply(PresenceInput::TurnStarted(a.clone()), t0)
            .expect("a raised");
        tracker
            .apply(PresenceInput::TurnStarted(b.clone()), t0)
            .expect("b raised");
        losses.fetch_add(1, Ordering::Relaxed);
        tracker.absorb_losses();

        tracker.tick(t0 + CEILING);
        let stowed: Vec<_> = tracker
            .tick(t0 + CEILING + LINGER)
            .into_iter()
            .filter(|p| p.transition)
            .collect();
        assert_eq!(stowed.len(), 2, "{stowed:?}");
        assert_eq!(tracker.state(&a), PresenceState::Idle);
        assert_eq!(tracker.state(&b), PresenceState::Idle);
    }

    /// Two pods on one host are two interactions. The tracker keys everything
    /// on the pod, so one stowing says nothing about the other.
    #[test]
    fn pods_engage_and_stow_independently() {
        let (a, b) = (PodId("pod-a".into()), PodId("pod-b".into()));
        let mut tracker = Tracker::new(timing(), Arc::new(AtomicU64::new(0)));
        let t0 = Instant::now();
        tracker
            .apply(PresenceInput::Wake(a.clone()), t0)
            .expect("a raised");
        tracker
            .apply(PresenceInput::Wake(b.clone()), t0)
            .expect("b raised");
        tracker.apply(PresenceInput::Unanswered(a.clone()), t0);

        let stowed: Vec<_> = tracker
            .tick(t0 + LINGER)
            .into_iter()
            .filter(|p| p.transition)
            .collect();
        assert_eq!(stowed.len(), 1, "{stowed:?}");
        assert_eq!(stowed[0].pod, a);
        assert_eq!(tracker.state(&a), PresenceState::Idle);
        assert_eq!(tracker.state(&b), PresenceState::Engaged);
    }

    /// The task sleeps until the nearest timer of any pod, so a deadline that is
    /// not the nearest one is still never missed.
    #[test]
    fn the_deadline_is_the_nearest_armed_timer() {
        let mut fx = fixture();
        assert_eq!(fx.tracker.deadline(), None, "nothing is engaged");
        fx.apply(PresenceInput::Wake(pod()), ZERO).expect("raised");
        assert_eq!(
            fx.tracker.deadline(),
            Some(fx.at(REFRESH)),
            "the refresh is nearer than the ceiling"
        );
        fx.apply(PresenceInput::Unanswered(pod()), Duration::from_secs(1));
        assert_eq!(
            fx.tracker.deadline(),
            Some(fx.at(REFRESH)),
            "the linger sits behind the refresh"
        );
    }

    /// A `[brenn]` table with presence configured, parsed the way an operator
    /// writes one. The intervals are the caller's: a task test that waits out a
    /// timer waits it out in real time, so the tests below wind them down to
    /// milliseconds rather than pausing a clock the scripted socket also
    /// drives.
    fn brenn_config_with(refresh_ms: u64, linger_ms: u64, ceiling_ms: u64) -> BrennConfig {
        toml::from_str(&format!(
            "publish_channel = \"brenn:pod.utterance\"\n\
             response_channel = \"brenn:pod.speak\"\n\
             presence_channel = \"brenn:reachy.presence\"\n\
             attribution = \"voice\"\n\
             presence_refresh_ms = {refresh_ms}\n\
             presence_linger_ms = {linger_ms}\n\
             presence_max_engaged_ms = {ceiling_ms}\n\
             [bridge]\n\
             server_url = \"wss://peer.example.net/remote/pod-kitchen/ws\"\n\
             token_file = \"/nonexistent/pod.token\"\n\
             ident = \"speech-surface/test\"\n",
        ))
        .expect("the test table parses")
    }

    /// The shipped defaults, long enough that no timer fires during a test that
    /// is not about one.
    fn brenn_config() -> BrennConfig {
        brenn_config_with(5_000, 8_000, 30_000)
    }

    /// A tracker task over a scripted bus peer, plus the JSONL file its
    /// judgement lands in.
    struct TaskFx {
        handle: PresenceHandle,
        peers: std::collections::VecDeque<crate::brenn::scripted::Peer>,
        task: tokio::task::JoinHandle<()>,
        teardown: CancellationToken,
        path: std::path::PathBuf,
        _dir: tempfile::TempDir,
        _jsonl: JsonlHandle,
        _writer: tokio::task::JoinHandle<()>,
    }

    async fn task_fixture() -> TaskFx {
        task_fixture_with(brenn_config()).await
    }

    async fn task_fixture_with(config: BrennConfig) -> TaskFx {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let (bridge, bus, mut events, peers) = scripted(&[Attempt::Open], 1);
        let bridge: Bridge<_> = bridge;
        tokio::spawn(bridge.run());
        // The bridge awaits its event channel, so an embedder that stops
        // reading back-pressures the socket. The tracker subscribes to nothing
        // and has no use for the events; something still has to drain them.
        tokio::spawn(async move { while events.recv().await.is_some() {} });
        let (handle, rx) = channel(jsonl.clone());
        let tracker = PresenceTracker::new(
            &config,
            config.presence_channel.clone().unwrap(),
            bus,
            rx,
            jsonl.clone(),
        );
        let teardown = CancellationToken::new();
        let task = tokio::spawn(tracker.run(teardown.clone()));
        TaskFx {
            handle,
            peers,
            task,
            teardown,
            path,
            _dir: dir,
            _jsonl: jsonl,
            _writer: writer,
        }
    }

    impl TaskFx {
        async fn stop(&mut self) {
            self.teardown.cancel();
            tokio::time::timeout(WAIT, &mut self.task)
                .await
                .expect("the tracker stops when told to")
                .expect("the tracker task does not panic");
        }
    }

    /// The wiring end to end: a tap fires, and what reaches the bus is a
    /// presence body on the configured channel, addressed to the pod, with the
    /// transition on the console's stream beside it.
    #[tokio::test]
    async fn an_engaged_intent_reaches_the_presence_channel() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.handle.send(PresenceInput::Wake(pod()));
        let published = peer.answer_publish("Ok").await;
        assert_eq!(published["channel"], "brenn:reachy.presence");
        assert_eq!(published["urgency"], "normal");
        assert_eq!(published["attribution"], "voice");
        let body: serde_json::Value =
            serde_json::from_str(published["body"].as_str().expect("a string body")).unwrap();
        assert_eq!(body["type"], "presence");
        assert_eq!(body["pod"], "pod-kitchen");
        assert_eq!(body["state"], "engaged");

        let line = expect_line(&fx.path, "presence").await;
        assert_eq!(line["state"], "engaged");
        assert_eq!(line["cause"], "wake");
        fx.stop().await;
    }

    /// The body of a `Publish` frame, decoded from the JSON text it carries.
    fn body_of(published: &serde_json::Value) -> serde_json::Value {
        serde_json::from_str(published["body"].as_str().expect("a string body"))
            .expect("the body is JSON")
    }

    /// The first `presence` line reporting `state`, waited for.
    async fn expect_presence_line(path: &std::path::Path, state: &str) -> serde_json::Value {
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            if let Some(line) = lines(path)
                .into_iter()
                .find(|line| line["event"] == "presence" && line["state"] == state)
            {
                return line;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no presence line reporting {state}; got {:?}",
                lines(path)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The task's timer arm, end to end: nothing in the input queue, and the
    /// lease is republished anyway. Without it the head goes up and the
    /// consumer's lease lapses under it — the backstop doing the mechanism's
    /// job.
    #[tokio::test]
    async fn the_task_refreshes_the_lease_on_its_own_timer() {
        let mut fx = task_fixture_with(brenn_config_with(40, 60_000, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.handle.send(PresenceInput::Wake(pod()));
        let first = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(first["state"], "engaged");
        assert_eq!(first["seq"], 1);

        let refreshed = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(refreshed["state"], "engaged", "the refresh timer fired");
        assert_eq!(refreshed["seq"], 2);

        let transitions: Vec<_> = lines(&fx.path)
            .into_iter()
            .filter(|line| line["event"] == "presence")
            .collect();
        assert_eq!(
            transitions.len(),
            1,
            "a refresh is the lease talking, not the head moving: {transitions:?}"
        );
        fx.stop().await;
    }

    /// The other half of the same arm, and the one no other task test reaches:
    /// a deadline that fires with nothing left in flight puts an `idle` on the
    /// bus. This is the head actually coming down.
    #[tokio::test]
    async fn the_task_publishes_idle_when_the_linger_runs_out() {
        let mut fx = task_fixture_with(brenn_config_with(60_000, 40, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.handle.send(PresenceInput::Wake(pod()));
        assert_eq!(
            body_of(&peer.answer_publish("Ok").await)["state"],
            "engaged"
        );

        fx.handle.send(PresenceInput::Unanswered(pod()));
        let stowed = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(stowed["state"], "idle", "the linger ran out");
        assert_eq!(stowed["pod"], "pod-kitchen");
        assert_eq!(stowed["seq"], 2);

        let line = expect_presence_line(&fx.path, "idle").await;
        assert_eq!(line["cause"], "linger");
        fx.stop().await;
    }

    /// The consumer applies intents in arrival order and treats `seq` as
    /// observable rather than authoritative, so the order these reach the bus
    /// is the posture it ends on. A stow overtaking the engage behind it leaves
    /// the head down through a live interaction — the one direction the lease
    /// does not fail safe on — so at most one intent is ever in flight.
    #[tokio::test]
    async fn intents_reach_the_bus_in_the_order_they_were_decided() {
        let mut fx = task_fixture_with(brenn_config_with(60_000, 30, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.handle.send(PresenceInput::Wake(pod()));
        let engaged = body_of(&peer.answer_publish("Ok").await);
        fx.handle.send(PresenceInput::Unanswered(pod()));
        let stowed = body_of(&peer.answer_publish("Ok").await);
        fx.handle.send(PresenceInput::Wake(pod()));
        let re_engaged = body_of(&peer.answer_publish("Ok").await);

        let seen: Vec<_> = [&engaged, &stowed, &re_engaged]
            .iter()
            .map(|body| (body["state"].clone(), body["seq"].clone()))
            .collect();
        assert_eq!(
            seen,
            vec![
                (json!("engaged"), json!(1)),
                (json!("idle"), json!(2)),
                (json!("engaged"), json!(3)),
            ]
        );
        fx.stop().await;
    }

    /// A refused publish is a line and nothing more: the tracker keeps reducing,
    /// and the next refresh repairs the consumer's view on its own.
    #[tokio::test]
    async fn a_refused_publish_is_reported_and_does_not_wedge_the_tracker() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.handle.send(PresenceInput::Wake(pod()));
        peer.answer_publish("RateLimited").await;
        let line = expect_line(&fx.path, "brenn_presence_publish_failed").await;
        assert_eq!(line["channel"], "brenn:reachy.presence");
        assert_eq!(line["state"], "engaged");
        assert_eq!(line["detail"], "the peer rate-limited the publish");

        // Still reducing: a fresh pod raises and publishes as if nothing had
        // happened.
        fx.handle
            .send(PresenceInput::Wake(PodId("pod-hall".into())));
        let published = peer.answer_publish("Ok").await;
        let body: serde_json::Value =
            serde_json::from_str(published["body"].as_str().expect("a string body")).unwrap();
        assert_eq!(body["pod"], "pod-hall");
        fx.stop().await;
    }

    /// The taps never block the pipeline, so a wedged tracker loses inputs
    /// rather than the other way round — loudly, and counted.
    #[tokio::test]
    async fn a_full_queue_drops_the_newest_input_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let (handle, inbox) = channel(jsonl.clone());

        for _ in 0..PRESENCE_QUEUE_DEPTH {
            handle.send(PresenceInput::Wake(pod()));
        }
        assert_eq!(handle.dropped(), 0, "the queue holds its stated depth");
        handle.send(PresenceInput::Wake(pod()));
        assert_eq!(handle.dropped(), 1);

        drop(inbox);
        handle.send(PresenceInput::Wake(pod()));
        assert_eq!(handle.dropped(), 2, "a departed tracker is not silence");

        drop(handle);
        drop(jsonl);
        writer.await.unwrap();
        let reasons: Vec<String> = lines(&path)
            .iter()
            .filter(|line| line["event"] == "presence_input_dropped")
            .map(|line| line["reason"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(reasons, vec!["queue_full", "tracker_gone"]);
    }
}
