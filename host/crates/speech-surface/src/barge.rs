//! The turn ledger: the surface's per-pod barge-in bookkeeping.
//!
//! Three things live here, all keyed by the pod and the turn (`UtteranceId`)
//! that produced the response:
//!
//! - **The interrupted mark.** The router evicts every `SpeakCmd` replying to a
//!   marked turn — from its queue, from mid-synthesis, and once more after
//!   synthesis returns. One id per pod suffices: pipeline dispatch awaits
//!   `brain.handle()` inline, so at most one turn per pod streams responses at a
//!   time. That holds for a brain that streams several clips within one `handle`
//!   call, which `BrennBrain`'s multi-part responses exercise: the mark is per turn,
//!   and settlement counts each of the turn's commands.
//! - **The response and transcript capture.** The sink tap records each turn's
//!   outgoing response text and the pipeline records its transcript, so an
//!   interrupt can mint a [`ContextSegment`] describing the turn it cut.
//! - **The context chain.** Every interrupt pushes a segment; a response that
//!   completes cleanly clears the whole chain. Bounded at
//!   [`MAX_CONTEXT_SEGMENTS`], drop-oldest.
//!
//! Clean completion is decided here, from settlement accounting — never inferred
//! from a single `PlaybackEvent::Finished`. That event is per *job*: a turn may
//! emit several `SpeakCmd`s, and the pacer can drain the queue between a turn's
//! clips while the next is still in synthesis. A turn completes cleanly only when
//! its dispatch has returned, every cmd the tap saw has settled, every settlement
//! was clean, and nothing interrupted it.
//!
//! The same records answer a second, different question — [`TurnAudio`], the
//! turn's *cmd accounting*: is every cmd accounted for (started playing, or
//! resolved without ever starting), and when is the speech queued so far
//! estimated to finish. That answer schedules motion from when playback *starts*
//! and how long the audio is, which is why it is a separate reading of the same
//! bookkeeping rather than another use of settlement: settlement is about audio
//! that has already finished.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use speech_pipeline::{
    BargeInContext, ContextSegment, InterruptProgress, MAX_CONTEXT_SEGMENTS, PodId, UtteranceId,
    audio_ms,
};
use tokio::time::{Duration, Instant};

/// Per-turn accounting: what the tap has sent, what playback has started and
/// resolved, and whether the brain is done producing.
#[derive(Debug, Default)]
struct TurnSettlement {
    /// `SpeakCmd`s the sink tap saw for this turn.
    cmds_sent: u64,
    /// Cmds that reached a terminal outcome (played out, aborted, flushed, or
    /// dropped by the router).
    cmds_settled: u64,
    /// Every settlement so far was clean. Starts true; one unclean settle latches
    /// it false for the turn's life.
    all_clean: bool,
    /// `brain.handle()` has returned, so no further cmds are coming.
    dispatch_done: bool,
    /// Cmds whose playback began (a `PlaybackEvent::Started`).
    cmds_started: u64,
    /// Started and not yet settled — the cmds whose audio is in the speaker.
    playing: u64,
    /// Settles that no started-but-unsettled cmd could account for, so they belong
    /// to cmds that resolved without ever playing (refused by the queue, dropped by
    /// the router, or dead in synthesis).
    settled_unstarted: u64,
    /// The latest instant this turn's started audio is estimated to finish.
    horizon: Option<Instant>,
}

impl TurnSettlement {
    fn new() -> Self {
        Self {
            all_clean: true,
            ..Self::default()
        }
    }

    /// Every cmd accounted for, all of them clean, and the brain finished. A turn
    /// that produced no cmd at all never completes here: nothing reached the user,
    /// so a silent turn leaves the chain standing for the next real response rather
    /// than clearing it on a vacuous truth.
    fn completed_clean(&self) -> bool {
        self.dispatch_done
            && self.all_clean
            && self.cmds_sent > 0
            && self.cmds_settled >= self.cmds_sent
    }

    /// The turn's cmd accounting as it stands.
    fn audio(&self) -> TurnAudio {
        TurnAudio {
            dispatch_done: self.dispatch_done,
            cmds_sent: self.cmds_sent,
            awaiting_start: self
                .cmds_sent
                .saturating_sub(self.cmds_started + self.settled_unstarted),
            horizon: self.horizon,
        }
    }
}

/// What a turn's cmds have done, at one instant.
///
/// The reading a motion scripter schedules from: it says whether anything is still
/// to come, and when what has already begun will be over. Deliberately a snapshot
/// rather than a handle — it is answered by the call that changed the accounting,
/// so a turn whose records are retired by that same call still reports the state
/// that retired them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnAudio {
    /// `brain.handle()` has returned: no further cmds are coming.
    pub dispatch_done: bool,
    /// `SpeakCmd`s the tap saw for the turn. Zero at `dispatch_done` is a silent
    /// turn — the brain answered with nothing to say.
    pub cmds_sent: u64,
    /// Cmds that have neither started playing nor resolved without starting.
    ///
    /// Conservative in one shape, deliberately: a cmd that resolves without ever
    /// starting while another is still playing cannot be told apart from that
    /// playing cmd's own settlement, since nothing downstream carries a per-cmd
    /// identity — playback events name the turn. Such a turn keeps one cmd awaited
    /// until the playing one settles, which is the end of its audio. The error is
    /// always in the direction of waiting.
    pub awaiting_start: u64,
    /// When the audio started so far is estimated to finish. `None` when nothing
    /// has started — including a turn that never will.
    pub horizon: Option<Instant>,
}

/// One pod's barge-in state.
#[derive(Debug, Default)]
struct PodTurns {
    /// The most recently interrupted turn; `SpeakCmd`s replying to it are dropped.
    interrupted: Option<UtteranceId>,
    /// Response text seen for each in-flight turn (from the sink tap). Pruned when
    /// the turn completes or is interrupted.
    responses: HashMap<UtteranceId, String>,
    /// The dispatched transcript per in-flight turn (from the pipeline), pruned
    /// alongside `responses`.
    transcripts: HashMap<UtteranceId, Option<String>>,
    /// Settlement accounting per in-flight turn, pruned alongside `responses`.
    settlement: HashMap<UtteranceId, TurnSettlement>,
    /// The interrupted-turn chain, oldest first.
    chain: VecDeque<ContextSegment>,
}

impl PodTurns {
    /// Drop every per-turn record for `id`. The `interrupted` mark is deliberately
    /// not touched: the router still needs it to evict cmds that are already in
    /// flight for the turn.
    fn prune_turn(&mut self, id: UtteranceId) {
        self.responses.remove(&id);
        self.transcripts.remove(&id);
        self.settlement.remove(&id);
    }
}

/// Per-pod barge-in bookkeeping, shared by the pipeline, the router, and the
/// playback-event adapter. Every method takes `&self` and locks internally.
#[derive(Debug, Default)]
pub(crate) struct TurnLedger {
    inner: Mutex<HashMap<PodId, PodTurns>>,
    /// Woken whenever any turn is interrupted, so the router's in-flight synthesis
    /// await can drop out promptly. Process-wide rather than per-pod: waiters
    /// re-check [`TurnLedger::is_interrupted`] for their own turn, so a wake for
    /// another pod costs one map lookup.
    interrupted: tokio::sync::Notify,
}

impl TurnLedger {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The notify a router await parks on to learn that *some* turn was
    /// interrupted. The waiter must re-check `is_interrupted` for its own turn.
    pub(crate) fn interrupted_notify(&self) -> &tokio::sync::Notify {
        &self.interrupted
    }

    fn with_pod<T>(&self, pod: &PodId, f: impl FnOnce(&mut PodTurns) -> T) -> T {
        let mut inner = self.inner.lock().expect("turn ledger poisoned");
        f(inner.entry(pod.clone()).or_default())
    }

    /// Read-only pod access that never inserts. A pod the ledger has not seen yields
    /// `T::default()` — the empty answer for every read — without cloning the id or
    /// leaving a permanent empty entry behind, so the router's per-command
    /// `is_interrupted` probes stay allocation-free.
    fn read_pod<T: Default>(&self, pod: &PodId, f: impl FnOnce(&PodTurns) -> T) -> T {
        let inner = self.inner.lock().expect("turn ledger poisoned");
        inner.get(pod).map(f).unwrap_or_default()
    }

    /// Record the transcript a turn was dispatched with, so a later interrupt can
    /// name what the user had said. Called at every brain dispatch, barge or not.
    ///
    /// Dispatching a turn also retires every older one on the pod: dispatch awaits
    /// the brain inline, so a new turn starting proves the previous one will never
    /// produce another command. That is what bounds the per-turn maps — a turn
    /// whose reply was refused by a full queue, or whose clip's terminal event
    /// never arrived, leaves records that nothing else would ever settle.
    pub(crate) fn record_dispatch(&self, pod: &PodId, id: UtteranceId, transcript: Option<String>) {
        self.with_pod(pod, |p| {
            p.responses.retain(|turn, _| *turn == id);
            p.transcripts.retain(|turn, _| *turn == id);
            p.settlement.retain(|turn, _| *turn == id);
            p.transcripts.insert(id, transcript);
            p.settlement.entry(id).or_insert_with(TurnSettlement::new);
        });
    }

    /// Record one `SpeakCmd` the turn produced, from the sink tap. Every cmd counts
    /// toward settlement, and is awaited by the turn's cmd accounting until it
    /// starts playing or resolves without starting; `text` is `Some` only for a
    /// `SpeakBody::Text` body, whose
    /// words are what a readback can quote. A turn's later text replaces an earlier
    /// one — the last thing said is what was cut.
    pub(crate) fn record_cmd(&self, pod: &PodId, id: UtteranceId, text: Option<String>) {
        self.with_pod(pod, |p| {
            p.settlement
                .entry(id)
                .or_insert_with(TurnSettlement::new)
                .cmds_sent += 1;
            if let Some(text) = text {
                p.responses.insert(id, text);
            }
        });
    }

    /// Mark that `brain.handle()` has returned for `id`: no more cmds are coming,
    /// so settlement can complete. Dispatch awaits the brain inline, which is what
    /// makes this a sound "that's all of them" signal. Answers the turn's cmd
    /// accounting as it stands with that fact in.
    pub(crate) fn dispatch_done(&self, pod: &PodId, id: UtteranceId) -> TurnAudio {
        self.with_pod(pod, |p| {
            let s = p.settlement.entry(id).or_insert_with(TurnSettlement::new);
            s.dispatch_done = true;
            let audio = s.audio();
            // Playback can outrun the brain's return, leaving this the last piece.
            settle_check(p, id);
            audio
        })
    }

    /// Record that one of the turn's cmds began playing at `at`, carrying `samples`
    /// of audio. Extends the turn's horizon to the later of what it already knew
    /// and this clip's own end.
    ///
    /// A turn with no records — interrupted, or completed and retired — is not
    /// resurrected: its cmds are being evicted, and a horizon for a turn nothing is
    /// waiting on would schedule against speech that is about to stop.
    pub(crate) fn record_started(
        &self,
        pod: &PodId,
        id: Option<UtteranceId>,
        samples: u64,
        at: Instant,
    ) -> Option<TurnAudio> {
        let id = id?;
        self.with_pod(pod, |p| {
            let s = p.settlement.get_mut(&id)?;
            s.cmds_started += 1;
            s.playing += 1;
            let ends = at + Duration::from_millis(audio_ms(samples));
            s.horizon = Some(s.horizon.map_or(ends, |h| h.max(ends)));
            Some(s.audio())
        })
    }

    /// Cut `id` at `progress`: push its context segment, mark it interrupted so the
    /// router evicts its pending responses, and wake any in-flight synthesis await.
    /// Returns the pod's chain as it stands after the push — never empty, since this
    /// call just pushed a link.
    pub(crate) fn interrupt(
        &self,
        pod: &PodId,
        id: UtteranceId,
        progress: InterruptProgress,
    ) -> BargeInContext {
        let ctx = self.with_pod(pod, |p| {
            let segment = ContextSegment {
                utterance: id,
                transcript: p.transcripts.get(&id).cloned().flatten(),
                response_text: p.responses.get(&id).cloned(),
                interrupted: progress,
            };
            if p.chain.len() == MAX_CONTEXT_SEGMENTS {
                p.chain.pop_front();
            }
            p.chain.push_back(segment);
            p.interrupted = Some(id);
            // The turn is over; only the mark outlives it.
            p.prune_turn(id);
            BargeInContext {
                chain: p.chain.iter().cloned().collect(),
            }
        });
        // Woken after the mark is visible, so every waiter's re-check sees it.
        self.interrupted.notify_waiters();
        ctx
    }

    /// The pod's chain as it stands, or `None` when nothing is pending — the state
    /// left by a response that completed without a barge-in.
    pub(crate) fn chain(&self, pod: &PodId) -> Option<BargeInContext> {
        self.read_pod(pod, |p| {
            (!p.chain.is_empty()).then(|| BargeInContext {
                chain: p.chain.iter().cloned().collect(),
            })
        })
    }

    /// Whether responses for `id` should be dropped. `None` (a job with no
    /// originating utterance) is never interrupted — there is no turn to name.
    pub(crate) fn is_interrupted(&self, pod: &PodId, id: Option<UtteranceId>) -> bool {
        let Some(id) = id else {
            return false;
        };
        self.read_pod(pod, |p| p.interrupted == Some(id))
    }

    /// Settle one of the turn's cmds. Called from the playback adapter on every
    /// terminal job event and from the router on every cmd it drops, so the count
    /// converges whatever became of the cmd. `clean` is false for anything but a
    /// job that played out and wrote its end-of-audio.
    ///
    /// This is where clean completion fires: the turn's chain — the whole pod's
    /// chain — is cleared once the turn finishes with nothing having cut it.
    ///
    /// Answers the turn's cmd accounting with this settle in, or `None` for a turn
    /// the ledger no longer holds.
    pub(crate) fn settle_job(
        &self,
        pod: &PodId,
        id: Option<UtteranceId>,
        clean: bool,
    ) -> Option<TurnAudio> {
        let id = id?;
        self.with_pod(pod, |p| {
            let s = p.settlement.get_mut(&id)?;
            s.cmds_settled += 1;
            s.all_clean &= clean;
            // A settle with something playing is that clip's own ending; one with
            // nothing playing belongs to a cmd that never played.
            if s.playing > 0 {
                s.playing -= 1;
            } else {
                s.settled_unstarted += 1;
            }
            let audio = s.audio();
            settle_check(p, id);
            Some(audio)
        })
    }
}

/// Complete `id` if its settlement is done and clean and nothing interrupted it:
/// drop the pod's chain and the turn's records.
fn settle_check(p: &mut PodTurns, id: UtteranceId) {
    let completed = p.interrupted != Some(id)
        && p.settlement
            .get(&id)
            .is_some_and(TurnSettlement::completed_clean);
    if completed {
        // A response reached the user unbroken: whatever was interrupted before it
        // is no longer context for anything.
        p.chain.clear();
        p.prune_turn(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn pod(name: &str) -> PodId {
        PodId(name.into())
    }

    fn progress(heard_ms: u64) -> InterruptProgress {
        InterruptProgress {
            heard_ms,
            total_ms: 1_000,
        }
    }

    /// Drive one turn from dispatch to a single clean clip — the shape every
    /// brain today produces.
    fn clean_turn(ledger: &TurnLedger, p: &PodId, id: u64, transcript: &str, response: &str) {
        let id = UtteranceId(id);
        ledger.record_dispatch(p, id, Some(transcript.into()));
        ledger.record_cmd(p, id, Some(response.into()));
        ledger.dispatch_done(p, id);
        ledger.settle_job(p, Some(id), true);
    }

    #[test]
    fn an_interrupt_chains_the_turns_transcript_and_response() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.record_dispatch(&p, UtteranceId(1), Some("what time is it".into()));
        ledger.record_cmd(&p, UtteranceId(1), Some("it is half past three".into()));

        let ctx = ledger.interrupt(&p, UtteranceId(1), progress(400));

        assert_eq!(ctx.chain.len(), 1);
        let seg = &ctx.chain[0];
        assert_eq!(seg.utterance, UtteranceId(1));
        assert_eq!(seg.transcript.as_deref(), Some("what time is it"));
        assert_eq!(seg.response_text.as_deref(), Some("it is half past three"));
        assert_eq!(seg.interrupted.heard_ms, 400);
        assert!(ledger.is_interrupted(&p, Some(UtteranceId(1))));
    }

    #[test]
    fn a_turn_with_no_captured_text_chains_a_segment_with_none_fields() {
        // A Pcm-bodied response and a transcript-less dispatch: the segment still
        // records where the cut landed, which is what the readback degrades to.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.record_dispatch(&p, UtteranceId(1), None);
        ledger.record_cmd(&p, UtteranceId(1), None);

        let ctx = ledger.interrupt(&p, UtteranceId(1), progress(50));

        assert_eq!(ctx.chain[0].transcript, None);
        assert_eq!(ctx.chain[0].response_text, None);
        assert_eq!(ctx.chain[0].interrupted.heard_ms, 50);
    }

    #[test]
    fn interrupting_a_turn_that_was_never_recorded_still_chains_it() {
        // The mark and the cut position are the load-bearing part; a turn the tap
        // never saw (interrupt racing the first cmd) must not lose its link.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");

        let ctx = ledger.interrupt(&p, UtteranceId(9), progress(10));

        assert_eq!(ctx.chain.len(), 1);
        assert_eq!(ctx.chain[0].utterance, UtteranceId(9));
        assert!(ledger.is_interrupted(&p, Some(UtteranceId(9))));
    }

    #[test]
    fn the_chain_builds_oldest_first_across_cycles() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        for id in 1..=3u64 {
            ledger.record_dispatch(&p, UtteranceId(id), Some(format!("said {id}")));
            ledger.record_cmd(&p, UtteranceId(id), Some(format!("replied {id}")));
            ledger.interrupt(&p, UtteranceId(id), progress(id * 100));
        }

        let chain = ledger.chain(&p).expect("three interrupts left a chain");
        let ids: Vec<u64> = chain.chain.iter().map(|s| s.utterance.0).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(chain.chain[2].response_text.as_deref(), Some("replied 3"));
    }

    #[test]
    fn the_chain_bound_drops_oldest() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        // One past the bound: the first segment must be the one that fell off.
        for id in 0..(MAX_CONTEXT_SEGMENTS as u64 + 1) {
            ledger.interrupt(&p, UtteranceId(id), progress(1));
        }

        let chain = ledger.chain(&p).unwrap();
        assert_eq!(chain.chain.len(), MAX_CONTEXT_SEGMENTS);
        assert_eq!(chain.chain[0].utterance, UtteranceId(1));
        assert_eq!(
            chain.chain[MAX_CONTEXT_SEGMENTS - 1].utterance,
            UtteranceId(MAX_CONTEXT_SEGMENTS as u64)
        );
    }

    #[test]
    fn a_clean_completion_clears_the_chain() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(200));
        assert!(ledger.chain(&p).is_some());

        clean_turn(&ledger, &p, 2, "sorry, the weather", "it is raining");

        assert!(
            ledger.chain(&p).is_none(),
            "an output completed without barge-in drops every segment"
        );
    }

    #[test]
    fn a_zero_cmd_turn_does_not_clear_the_chain() {
        // A dispatched turn whose brain produced no `SpeakCmd` delivered nothing the
        // user heard; the interrupted context it followed must survive for the next
        // real response, not be dropped on a turn that said nothing.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(200));

        ledger.record_dispatch(&p, UtteranceId(2), Some("hmm".into()));
        ledger.dispatch_done(&p, UtteranceId(2));

        assert!(
            ledger.chain(&p).is_some(),
            "a turn with no output does not count as a completed output"
        );
    }

    #[test]
    fn an_unclean_settle_never_completes_the_turn() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(200));

        // Turn 2's only clip aborts (a dead writer): the chain must survive, since
        // the user never heard a response through.
        ledger.record_dispatch(&p, UtteranceId(2), Some("again".into()));
        ledger.record_cmd(&p, UtteranceId(2), Some("raining".into()));
        ledger.dispatch_done(&p, UtteranceId(2));
        ledger.settle_job(&p, Some(UtteranceId(2)), false);

        assert!(ledger.chain(&p).is_some());
    }

    #[test]
    fn an_interrupted_turn_never_completes_even_if_a_clip_settles_clean() {
        // The flush cuts clip 2 of the turn, but clip 1 already played out clean.
        // Settling clip 1's event afterwards must not clear the chain the flush
        // just pushed.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        ledger.record_dispatch(&p, id, Some("hi".into()));
        ledger.record_cmd(&p, id, Some("one".into()));
        ledger.record_cmd(&p, id, Some("two".into()));
        ledger.dispatch_done(&p, id);
        ledger.settle_job(&p, Some(id), true);

        ledger.interrupt(&p, id, progress(30));
        ledger.settle_job(&p, Some(id), false);

        assert!(ledger.chain(&p).is_some());
    }

    #[test]
    fn a_multi_cmd_turn_does_not_complete_on_its_first_clip() {
        // The pacer can drain clip 1 while clip 2 is still in synthesis; a single
        // `Finished` is not the turn ending, which is the whole reason settlement
        // is counted rather than inferred.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(2);
        ledger.interrupt(&p, UtteranceId(1), progress(200));

        ledger.record_dispatch(&p, id, Some("tell me a story".into()));
        ledger.record_cmd(&p, id, Some("once upon a time".into()));
        ledger.settle_job(&p, Some(id), true);
        assert!(
            ledger.chain(&p).is_some(),
            "clip 1 settling is not the turn completing"
        );

        ledger.record_cmd(&p, id, Some("the end".into()));
        ledger.dispatch_done(&p, id);
        assert!(
            ledger.chain(&p).is_some(),
            "clip 2 is dispatched but has not settled"
        );

        ledger.settle_job(&p, Some(id), true);
        assert!(
            ledger.chain(&p).is_none(),
            "every clip settled clean and the brain is done"
        );
    }

    #[test]
    fn a_turn_whose_cmds_all_settle_before_dispatch_returns_completes_at_dispatch_done() {
        // Playback can outrun the brain's own return; `dispatch_done` is then the
        // last piece of the completion and must fire it.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(2);
        ledger.interrupt(&p, UtteranceId(1), progress(200));

        ledger.record_cmd(&p, id, Some("done".into()));
        ledger.settle_job(&p, Some(id), true);
        assert!(ledger.chain(&p).is_some());

        ledger.dispatch_done(&p, id);
        assert!(ledger.chain(&p).is_none());
    }

    #[test]
    fn a_settle_for_a_pruned_turn_is_inert() {
        // The router settles the cmds it evicts *after* the interrupt pruned the
        // turn; those late settles must not resurrect accounting or clear a chain.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.record_dispatch(&p, UtteranceId(1), Some("hi".into()));
        ledger.record_cmd(&p, UtteranceId(1), Some("hello".into()));
        ledger.interrupt(&p, UtteranceId(1), progress(20));

        ledger.settle_job(&p, Some(UtteranceId(1)), false);
        ledger.dispatch_done(&p, UtteranceId(1));
        ledger.settle_job(&p, Some(UtteranceId(1)), true);

        assert!(ledger.chain(&p).is_some());
    }

    #[test]
    fn a_job_with_no_turn_settles_nothing() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(200));

        ledger.settle_job(&p, None, true);

        assert!(!ledger.is_interrupted(&p, None));
        assert!(ledger.chain(&p).is_some());
    }

    #[test]
    fn only_the_named_turn_is_interrupted() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(100));

        assert!(ledger.is_interrupted(&p, Some(UtteranceId(1))));
        assert!(!ledger.is_interrupted(&p, Some(UtteranceId(2))));

        // A later interrupt names the newer turn; the older mark is spent, and its
        // cmds are long gone.
        ledger.interrupt(&p, UtteranceId(2), progress(100));
        assert!(!ledger.is_interrupted(&p, Some(UtteranceId(1))));
        assert!(ledger.is_interrupted(&p, Some(UtteranceId(2))));
    }

    #[test]
    fn pods_are_isolated() {
        let ledger = TurnLedger::new();
        let (kitchen, office) = (pod("kitchen"), pod("office"));
        ledger.record_dispatch(&kitchen, UtteranceId(1), Some("kitchen said".into()));
        ledger.record_dispatch(&office, UtteranceId(1), Some("office said".into()));

        let ctx = ledger.interrupt(&kitchen, UtteranceId(1), progress(100));

        assert_eq!(ctx.chain[0].transcript.as_deref(), Some("kitchen said"));
        assert!(!ledger.is_interrupted(&office, Some(UtteranceId(1))));
        assert!(
            ledger.chain(&office).is_none(),
            "a barge in the kitchen leaves the office chain untouched"
        );

        // And the office's own turn completes on its own accounting.
        clean_turn(&ledger, &office, 1, "office said", "office reply");
        assert!(ledger.chain(&kitchen).is_some());
    }

    #[test]
    fn a_new_dispatch_retires_the_previous_turns_records() {
        // Turn 1's reply was refused by a full queue, so its cmd never settles and
        // its records would sit there forever. Turn 2's dispatch retires them; the
        // chain, which belongs to the pod rather than the turn, is untouched.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        ledger.interrupt(&p, UtteranceId(1), progress(200));
        ledger.record_dispatch(&p, UtteranceId(2), Some("hi".into()));
        ledger.record_cmd(&p, UtteranceId(2), Some("hello".into()));
        ledger.dispatch_done(&p, UtteranceId(2));

        ledger.record_dispatch(&p, UtteranceId(3), Some("again".into()));

        {
            let inner = ledger.inner.lock().unwrap();
            let turns = &inner[&p];
            assert_eq!(turns.responses.len(), 0);
            assert_eq!(
                turns.transcripts.keys().collect::<Vec<_>>(),
                [&UtteranceId(3)]
            );
            assert_eq!(
                turns.settlement.keys().collect::<Vec<_>>(),
                [&UtteranceId(3)]
            );
        }
        assert!(
            ledger.chain(&p).is_some(),
            "the abandoned turn never completed cleanly, so the chain stands"
        );
    }

    /// One second of spine-format audio, in samples.
    const ONE_SECOND: u64 = 16_000;

    /// A turn with `cmds` commands queued and its dispatch not yet returned.
    fn dispatched_turn(ledger: &TurnLedger, p: &PodId, id: UtteranceId, cmds: usize) {
        ledger.record_dispatch(p, id, Some("say something".into()));
        for _ in 0..cmds {
            ledger.record_cmd(p, id, Some("a clip".into()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_started_clip_dates_the_end_of_the_turns_audio() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 1);
        let t0 = Instant::now();

        let audio = ledger
            .record_started(&p, Some(id), 2 * ONE_SECOND, t0)
            .expect("the turn is on the books");

        assert_eq!(audio.horizon, Some(t0 + Duration::from_secs(2)));
        assert_eq!(audio.awaiting_start, 0, "the turn's one cmd is playing");
        assert!(
            !audio.dispatch_done,
            "the brain has not returned, so more cmds may still come"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_horizon_is_the_latest_ending_over_the_turns_clips() {
        // A second clip starting later extends the turn's audio; a short one
        // starting inside the first clip's span does not shorten it.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 3);
        let t0 = Instant::now();

        ledger.record_started(&p, Some(id), 6 * ONE_SECOND, t0);
        let extended = ledger
            .record_started(&p, Some(id), 4 * ONE_SECOND, t0 + Duration::from_secs(5))
            .unwrap();
        assert_eq!(extended.horizon, Some(t0 + Duration::from_secs(9)));

        let unchanged = ledger
            .record_started(&p, Some(id), ONE_SECOND, t0 + Duration::from_secs(6))
            .unwrap();
        assert_eq!(unchanged.horizon, Some(t0 + Duration::from_secs(9)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_turn_is_accounted_when_its_last_cmd_starts_playing() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 2);
        let t0 = Instant::now();

        let audio = ledger.dispatch_done(&p, id);
        assert_eq!(audio.awaiting_start, 2, "neither clip has started");
        assert!(audio.dispatch_done);
        assert_eq!(audio.cmds_sent, 2);

        ledger.record_started(&p, Some(id), ONE_SECOND, t0);
        let audio = ledger
            .record_started(&p, Some(id), ONE_SECOND, t0 + Duration::from_secs(1))
            .unwrap();

        assert_eq!(audio.awaiting_start, 0);
        assert_eq!(audio.horizon, Some(t0 + Duration::from_secs(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cmd_that_never_played_stops_being_awaited_when_it_resolves() {
        // Clip 1 plays out and settles; clip 2 dies in synthesis and is settled by
        // the router without ever starting. Nothing is left to wait for.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 2);
        let t0 = Instant::now();

        ledger.record_started(&p, Some(id), ONE_SECOND, t0);
        ledger.dispatch_done(&p, id);
        ledger.settle_job(&p, Some(id), true);
        let audio = ledger
            .settle_job(&p, Some(id), false)
            .expect("the turn is still on the books");

        assert_eq!(audio.awaiting_start, 0);
        assert_eq!(
            audio.horizon,
            Some(t0 + Duration::from_secs(1)),
            "the clip that never played moves no horizon"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_cmd_dying_while_another_plays_is_awaited_until_that_one_ends() {
        // The conservative shape, asserted rather than left to be discovered: no
        // per-cmd identity reaches the ledger, so a settle arriving while a clip is
        // in the speaker is read as that clip's own — and the turn keeps waiting
        // until the player really does settle.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 2);
        let t0 = Instant::now();

        ledger.record_started(&p, Some(id), 6 * ONE_SECOND, t0);
        ledger.dispatch_done(&p, id);
        let audio = ledger.settle_job(&p, Some(id), false).unwrap();
        assert_eq!(audio.awaiting_start, 1, "read as the playing clip's ending");

        let audio = ledger.settle_job(&p, Some(id), true).unwrap();
        assert_eq!(audio.awaiting_start, 0, "and repaired when it settles");
        assert_eq!(audio.horizon, Some(t0 + Duration::from_secs(6)));
    }

    #[tokio::test]
    async fn a_silent_turn_is_accounted_the_moment_its_dispatch_returns() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        ledger.record_dispatch(&p, id, Some("never mind".into()));

        let audio = ledger.dispatch_done(&p, id);

        assert_eq!(audio.cmds_sent, 0);
        assert_eq!(audio.awaiting_start, 0);
        assert_eq!(audio.horizon, None, "there is no audio to wait out");
    }

    #[tokio::test(start_paused = true)]
    async fn a_clip_starting_for_a_turn_the_ledger_retired_is_ignored() {
        // The barge pruned the turn and its clips are being evicted; a horizon for
        // it would schedule against speech that is about to stop.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 1);
        ledger.interrupt(&p, id, progress(10));

        assert_eq!(
            ledger.record_started(&p, Some(id), ONE_SECOND, Instant::now()),
            None
        );
        assert_eq!(ledger.settle_job(&p, Some(id), false), None);
    }

    #[tokio::test(start_paused = true)]
    async fn the_settle_that_retires_a_turn_still_reports_its_accounting() {
        // The clean completion prunes the turn's records inside this very call, so
        // an answer read afterwards would find nothing — which is why the call
        // answers.
        let ledger = TurnLedger::new();
        let p = pod("pod-x");
        let id = UtteranceId(1);
        dispatched_turn(&ledger, &p, id, 1);
        let t0 = Instant::now();
        ledger.record_started(&p, Some(id), ONE_SECOND, t0);
        ledger.dispatch_done(&p, id);

        let audio = ledger
            .settle_job(&p, Some(id), true)
            .expect("the settle that completed the turn answers for it");

        assert_eq!(audio.awaiting_start, 0);
        assert_eq!(audio.horizon, Some(t0 + Duration::from_secs(1)));
        assert!(
            ledger
                .record_started(&p, Some(id), ONE_SECOND, t0)
                .is_none(),
            "and the records are gone by the time it returns"
        );
    }

    #[tokio::test]
    async fn a_job_with_no_turn_records_no_audio() {
        let ledger = TurnLedger::new();
        let p = pod("pod-x");

        assert_eq!(
            ledger.record_started(&p, None, ONE_SECOND, Instant::now()),
            None
        );
    }

    #[tokio::test]
    async fn an_interrupt_wakes_a_parked_waiter() {
        let ledger = Arc::new(TurnLedger::new());
        let p = pod("pod-x");

        let waiter = {
            let ledger = Arc::clone(&ledger);
            let p = p.clone();
            tokio::spawn(async move {
                let notified = ledger.interrupted_notify().notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                notified.await;
                ledger.is_interrupted(&p, Some(UtteranceId(1)))
            })
        };
        // Let the waiter register before the notify fires.
        tokio::task::yield_now().await;
        ledger.interrupt(&p, UtteranceId(1), progress(100));

        let saw_mark = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("the waiter wakes on an interrupt")
            .unwrap();
        assert!(saw_mark, "the mark is visible to every woken waiter");
    }
}
