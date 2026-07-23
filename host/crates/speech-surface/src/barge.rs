//! The turn ledger: the surface's per-pod barge-in bookkeeping.
//!
//! Three things live here, all keyed by the pod and the turn (`UtteranceId`)
//! that produced the response:
//!
//! - **The interrupted mark.** The router evicts every `SpeakCmd` replying to a
//!   marked turn — from its queue, from mid-synthesis, and once more after
//!   synthesis returns. One id per pod suffices: pipeline dispatch awaits
//!   `brain.handle()` inline, so at most one turn per pod streams responses at a
//!   time (revisit with streaming brains).
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

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use speech_pipeline::{
    BargeInContext, ContextSegment, InterruptProgress, MAX_CONTEXT_SEGMENTS, PodId, UtteranceId,
};

/// Per-turn settlement accounting: what the tap has sent, what playback has
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
    /// toward settlement; `text` is `Some` only for a `SpeakBody::Text` body, whose
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
    /// makes this a sound "that's all of them" signal.
    pub(crate) fn dispatch_done(&self, pod: &PodId, id: UtteranceId) {
        self.with_pod(pod, |p| {
            let s = p.settlement.entry(id).or_insert_with(TurnSettlement::new);
            s.dispatch_done = true;
            // Playback can outrun the brain's return, leaving this the last piece.
            settle_check(p, id);
        });
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
    pub(crate) fn settle_job(&self, pod: &PodId, id: Option<UtteranceId>, clean: bool) {
        let Some(id) = id else {
            return;
        };
        self.with_pod(pod, |p| {
            let Some(s) = p.settlement.get_mut(&id) else {
                // An interrupted or already-completed turn; its accounting is gone
                // and nothing it does now can complete it cleanly.
                return;
            };
            s.cmds_settled += 1;
            s.all_clean &= clean;
            settle_check(p, id);
        });
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
