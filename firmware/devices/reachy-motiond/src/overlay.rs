//! The overlays the motion thread is playing, period by period.
//!
//! One player per open `play` window, keyed by the wire step that opened it,
//! kept in step order because that order is the composition order. What the
//! timeline says is open comes from [`crate::cells::Shared::playing`]; what a
//! player does with its own clock, its ramps and its frames belongs to
//! `reachy-clips`. This is only the join between them: start the players the
//! timeline has opened, drop the ones it has closed, and hand the loop one
//! sample per player per period.
//!
//! Two pieces of memory make the join stable, and both are per script:
//!
//! - A window whose player has played out is **spent**, and is never started
//!   again. The timeline's window is the motion's clock plus its blend-out, and
//!   a player that faded a period before the arithmetic closes the window would
//!   otherwise be started afresh over its own tail.
//! - A script whose composed setpoint the machine refused is **refused**, whole.
//!   Not the one overlay and not the one window: the refusal says the
//!   composition left the envelope or outran a step bound, and re-entering
//!   overlay mode for the same script would refuse again every period until it
//!   lapsed.
//!
//! Nothing here survives a fault, an expiry or a replacement — the loop drops
//! the whole set, which is why a set is all this is.

use std::sync::Arc;
use std::time::Duration;

use reachy_clips::{ClipPlayer, OverlaySample};
use reachy_motion::{CommandRejection, JointId};
use serde_json::json;

use crate::cells::{Overlaid, Playing};
use crate::report::Sink;

/// The players the daemon is running, and the script they belong to.
#[derive(Debug, Default)]
pub struct Overlays {
    /// The script whose timeline opened these windows. A different one means a
    /// different timeline, and everything below is forgotten.
    seq: Option<u64>,
    /// One player per open window, keyed by the step that opened it and held
    /// in step order.
    players: Vec<(usize, ClipPlayer)>,
    /// The steps whose players have played out and faded.
    spent: Vec<usize>,
    /// This period's contributions, kept across periods so the one real-time
    /// loop in the daemon allocates nothing to compose a setpoint.
    samples: Vec<OverlaySample>,
    /// Whether a composed setpoint this script produced was refused.
    refused: bool,
}

impl Overlays {
    /// Nothing playing.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether anything is playing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.players.is_empty()
    }

    /// How many overlays are playing.
    #[must_use]
    pub fn len(&self) -> usize {
        self.players.len()
    }

    /// The script these players belong to, if any do.
    #[must_use]
    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    /// Whether any of the windows `overlaid` says are open is one this daemon
    /// would play.
    ///
    /// The loop's own question — whether to run this pass at tick cadence — and
    /// the answer accounts for what has already happened to this script: a
    /// window whose player is spent is not played twice, and a script whose
    /// composition the machine refused is not played at all.
    #[must_use]
    pub fn wants(&self, overlaid: &Overlaid) -> bool {
        if self.seq != overlaid.seq {
            return !overlaid.plays.is_empty();
        }
        if self.refused {
            return false;
        }
        overlaid
            .plays
            .iter()
            .any(|play| !self.spent.contains(&play.index))
    }

    /// Drop the players of a script that is no longer the running one, saying
    /// what that cost.
    ///
    /// Asked at the top of every pass as well as from [`Self::sync`], because
    /// `sync` is only reached by a script with a window open now. A
    /// replacement that opens none — a posture-only republish, or one whose
    /// first `play` step is not due yet — would otherwise leave the motion it
    /// truncated unsaid until some later script happened to play something,
    /// and then said against that script instead of the one that did it.
    ///
    /// Nothing else survives the change either: a spent window belongs to the
    /// timeline that closed it, and a refusal to the composition that produced
    /// it.
    pub fn forget(&mut self, seq: Option<u64>, sink: &dyn Sink) {
        if self.seq == seq {
            return;
        }
        if !self.players.is_empty() {
            ended(self.seq, seq, &self.players, sink);
        }
        self.seq = seq;
        self.players.clear();
        self.spent.clear();
        self.refused = false;
    }

    /// Take up what `overlaid` says is open now, saying what changed.
    ///
    /// A window the timeline has opened since the last call starts a player at
    /// the offset the timeline gives it — zero for one that opens on this
    /// period, and the elapsed offset for a window this daemon joined in
    /// progress, whose weights ramp from zero either way. A window that has
    /// closed drops its player and a script that is gone drops all of them,
    /// each saying which motions it cut short.
    ///
    /// Every open window arrives with the motion it plays already resolved —
    /// the schedule and the library are read together, under one lock — so
    /// nothing here asks the library anything.
    pub fn sync(&mut self, overlaid: &Overlaid, sink: &dyn Sink) {
        self.forget(overlaid.seq, sink);
        // A script whose composition the machine refused is not played again,
        // here any more than at the pass boundary [`Self::wants`] guards: the
        // same clip over the same base composes the same setpoint, and a run
        // that took its windows up again would offer the tick what it has just
        // refused, once a period, for as long as the windows stayed open.
        if self.refused {
            return;
        }
        let seq = self.seq;
        self.players.retain(|(index, player)| {
            if overlaid.plays.iter().any(|play| play.index == *index) {
                return true;
            }
            cut(seq, *index, player, sink);
            false
        });
        for play in &overlaid.plays {
            if self.spent.contains(&play.index)
                || self.players.iter().any(|(index, _)| *index == play.index)
            {
                continue;
            }
            let player = ClipPlayer::joining_at(
                Arc::clone(&play.motion),
                play.speed,
                Duration::from_millis(play.elapsed_ms),
            );
            self.players.push((play.index, player));
            started(play, sink);
        }
        // Step order, whatever order the windows opened in: the compositor
        // right-multiplies head deltas, so which overlay came first is what
        // decides the pose.
        self.players.sort_by_key(|(index, _)| *index);
    }

    /// Advance every player one period and answer this period's contributions,
    /// in composition order.
    ///
    /// A player whose motion has ended and whose channels have all faded is
    /// dropped here and its window marked spent: it contributes nothing, and
    /// the window it belongs to stays open on the timeline for as long as its
    /// blend-out was budgeted for.
    pub fn advance(&mut self, period: Duration) -> &[OverlaySample] {
        let Self {
            players,
            spent,
            samples,
            ..
        } = self;
        samples.clear();
        players.retain_mut(|(index, player)| match player.advance(period) {
            Some(sample) => {
                samples.push(sample);
                true
            }
            None => {
                spent.push(*index);
                false
            }
        });
        samples
    }

    /// Drop every player of this script and play none of it again, saying why.
    ///
    /// The composed-target refusal path. Naming the motions rather than
    /// counting them: the next question an operator asks is which clip did it.
    ///
    /// The refusal is reported by class and by joint as well as in words. This
    /// is the failure mode the design accepts as normal on a live machine, so
    /// "how often are we leaving the envelope against outrunning a step bound,
    /// and on which joint" has to be a question a session's events answer
    /// without anybody matching English.
    pub fn refuse(&mut self, why: &CommandRejection, sink: &dyn Sink) {
        self.refused = true;
        let names: Vec<String> = self
            .players
            .iter()
            .map(|(_, player)| player.motion().name().to_owned())
            .collect();
        self.players.clear();
        sink.line(&format!(
            "play: dropped {} — {why}. the base is re-acquired and the script runs on.",
            if names.is_empty() {
                "the overlay".to_owned()
            } else {
                names.join(", ")
            }
        ));
        sink.event(
            "motion_overlays_dropped",
            &json!({
                "seq": self.seq,
                "motions": names,
                "reason": reason(why),
                "joint": joint(why).map(|joint| joint.to_string()),
                "detail": why.to_string(),
            }),
        );
    }
}

/// The slug a composed-target refusal is aggregated under.
///
/// Shared with the loop's bare-base refusal report: which class of refusal a
/// session saw is one question, and deriving its answer twice is two
/// vocabularies for one aggregation.
pub fn reason(why: &CommandRejection) -> &'static str {
    match why {
        CommandRejection::Envelope(_) => "envelope",
        CommandRejection::Trajectory(_) => "unshapeable",
        CommandRejection::AntennaUnreachable { .. } => "antenna_unreachable",
        CommandRejection::StepTooLarge { .. } => "step_too_large",
    }
}

/// The joint a refusal names, for the refusals that name one.
pub fn joint(why: &CommandRejection) -> Option<JointId> {
    match why {
        CommandRejection::AntennaUnreachable { joint, .. }
        | CommandRejection::StepTooLarge { joint, .. } => Some(*joint),
        CommandRejection::Envelope(_) | CommandRejection::Trajectory(_) => None,
    }
}

/// Say that the script an overlay belonged to is gone, naming what that cut
/// short.
///
/// The same question a drop answers, asked of the other disposition that ends a
/// motion early: a clip a republish cut a third of the way through is otherwise
/// indistinguishable in a capture from one that played out, and a scripter
/// publishing too often truncates every motion it starts while the trace reads
/// as if they all played. Said only when players were actually cut, so an
/// ordinary posture-script republish stays silent.
///
/// Classified by which end it was, because they are different things for an
/// operator to answer: a script another script replaced is a publisher's doing
/// and a rate to look at, where a script that simply stopped running lapsed on
/// its own timeout. Counting the second as the first would read as a republish
/// storm that never happened.
fn ended(from: Option<u64>, to: Option<u64>, players: &[(usize, ClipPlayer)], sink: &dyn Sink) {
    let names: Vec<String> = players
        .iter()
        .map(|(_, player)| player.motion().name().to_owned())
        .collect();
    let dropped = names.join(", ");
    match to {
        Some(seq) => {
            sink.line(&format!(
                "play: dropped {dropped} — the script that opened those windows was replaced."
            ));
            sink.event(
                "motion_overlays_replaced",
                &json!({
                    "seq": from,
                    "replaced_by": seq,
                    "motions": names,
                }),
            );
        }
        None => {
            sink.line(&format!(
                "play: dropped {dropped} — the script that opened those windows is no longer \
                 running."
            ));
            sink.event(
                "motion_overlays_lapsed",
                &json!({
                    "seq": from,
                    "motions": names,
                }),
            );
        }
    }
}

/// Say that the timeline closed a window with its player still playing.
///
/// The third way a motion ends before its clip does, and the quietest: the
/// window is wall-clock arithmetic while the player advances one period per
/// call however late the loop wakes, so a stalled daemon closes windows on
/// motions that are still mid-track. Unsaid, that reads in a capture exactly
/// like a motion that played out.
fn cut(seq: Option<u64>, index: usize, player: &ClipPlayer, sink: &dyn Sink) {
    let name = player.motion().name();
    sink.line(&format!(
        "play: dropped {name} — its window closed while it was still playing."
    ));
    sink.event(
        "motion_overlay_cut",
        &json!({
            "seq": seq,
            "name": name,
            "step": index,
        }),
    );
}

/// Say that an overlay has started, once, where it starts.
fn started(play: &Playing, sink: &dyn Sink) {
    sink.line(&format!(
        "play: {} at {:.2}x{}",
        play.name(),
        play.speed,
        if play.elapsed_ms == 0 {
            String::new()
        } else {
            format!(", joined {} ms in", play.elapsed_ms)
        }
    ));
    sink.event(
        "motion_play",
        &json!({
            "name": play.name(),
            "speed": play.speed,
            "step": play.index,
            "joined_ms": play.elapsed_ms,
        }),
    );
}

#[cfg(test)]
mod tests {
    use reachy_clips::Motion;
    use reachy_motion::TrajectoryError;

    use super::*;
    use crate::library::Motions;
    use crate::library::fixtures::{clip, loaded};
    use crate::report::Collect;

    const WIGGLE: &str = "test/wiggle";
    /// A head-only motion, for what tells one player's samples from another's.
    const NOD: &str = "test/nod";
    /// One control period at the rate everything is floored at.
    const PERIOD: Duration = Duration::from_millis(20);

    /// The library these tests play out of, and a directory that outlives it.
    fn motions() -> (tempfile::TempDir, Motions) {
        loaded(
            &[
                ("wiggle.json", clip(WIGGLE, 10, 1.0)),
                (
                    "nod.json",
                    crate::library::fixtures::head_clip(NOD, 10, 0.002),
                ),
            ],
            &Collect::default(),
        )
    }

    /// The motion every window in these tests plays.
    fn wiggle(motions: &Motions) -> Arc<Motion> {
        held(motions, WIGGLE)
    }

    /// A motion the library holds, by name.
    fn held(motions: &Motions, name: &str) -> Arc<Motion> {
        Arc::clone(motions.motion(name).expect("the fixture is held"))
    }

    /// One window open on the antennas motion, at `elapsed` into it.
    fn open(motions: &Motions, seq: u64, index: usize, elapsed_ms: u64) -> Overlaid {
        playing(wiggle(motions), seq, index, elapsed_ms)
    }

    /// One window open on `motion`, at `elapsed` into it.
    fn playing(motion: Arc<Motion>, seq: u64, index: usize, elapsed_ms: u64) -> Overlaid {
        Overlaid {
            seq: Some(seq),
            plays: vec![Playing {
                index,
                motion,
                speed: 1.0,
                elapsed_ms,
            }],
        }
    }

    /// A window the timeline opens starts a player, and one it has not opened
    /// starts nothing.
    #[test]
    fn a_player_is_started_for_each_open_window() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        players.sync(&Overlaid::default(), &sink);
        assert!(players.is_empty(), "a closed timeline started a player");

        players.sync(&open(&motions, 1, 1, 0), &sink);
        assert_eq!(players.len(), 1);
        assert_eq!(players.seq(), Some(1));
        assert_eq!(
            sink.fields("motion_play").expect("the start is announced")["name"],
            json!(WIGGLE)
        );

        // The same window on the next period is the same player, not a second
        // one and not a second announcement.
        players.sync(&open(&motions, 1, 1, 20), &sink);
        assert_eq!(players.len(), 1);
        assert_eq!(sink.all_fields("motion_play").len(), 1);
    }

    /// A player that has played out is not started again while its window is
    /// still open.
    ///
    /// The window on the timeline is the motion's clock plus its blend-out, and
    /// a player that faded a period before that arithmetic closes it would
    /// otherwise be started afresh over its own tail — forever, since the same
    /// thing would happen to its replacement.
    #[test]
    fn a_spent_window_is_not_played_again() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        let mut periods = 0;
        loop {
            players.sync(&open(&motions, 1, 1, periods * 20), &sink);
            if players.is_empty() && periods > 0 {
                break;
            }
            players.advance(PERIOD);
            periods += 1;
            assert!(periods < 200, "the player never ended");
        }
        assert!(!players.wants(&open(&motions, 1, 1, periods * 20)));
        assert_eq!(
            sink.all_fields("motion_play").len(),
            1,
            "the spent window was played a second time"
        );
    }

    /// A refusal takes the whole script's overlays and does not give them back.
    ///
    /// Not the one player and not the one window: what the machine refused was
    /// the composition, and re-entering for the same script would refuse again
    /// every period until the script lapsed.
    #[test]
    fn a_refusal_ends_the_scripts_overlays_and_a_new_script_starts_afresh() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();
        players.sync(&open(&motions, 1, 1, 0), &sink);

        players.refuse(
            &CommandRejection::StepTooLarge {
                joint: JointId::AntennaRight,
                delta: 0.9,
            },
            &sink,
        );

        assert!(players.is_empty());
        assert!(
            !players.wants(&open(&motions, 1, 1, 20)),
            "the refused script was played again"
        );
        assert!(
            !players.wants(&Overlaid {
                seq: Some(1),
                plays: vec![Playing {
                    index: 7,
                    motion: wiggle(&motions),
                    speed: 1.0,
                    elapsed_ms: 0,
                }],
            }),
            "another window of the refused script was played"
        );
        players.sync(&open(&motions, 1, 1, 20), &sink);
        assert!(
            players.is_empty(),
            "a run inside the refused script took its windows up again"
        );
        assert_eq!(
            sink.all_fields("motion_play").len(),
            1,
            "the refused window was started a second time"
        );
        assert_eq!(
            sink.fields("motion_overlays_dropped")
                .expect("the drop is reported")["motions"],
            json!([WIGGLE])
        );
        assert!(
            players.wants(&open(&motions, 2, 1, 0)),
            "a replacement script inherited the refusal"
        );
    }

    /// A window the timeline has closed drops its player and says which motion
    /// that cut short; the windows still open keep theirs, clock and all.
    ///
    /// The drop is selective — it is one script's own timeline closing one of
    /// its windows, not a replacement — so an all-or-nothing assertion would
    /// pass a predicate that dropped everything. And the cut is said because a
    /// window closes on the wall clock while a player advances one period per
    /// call however late the loop wakes: after a stall the timeline closes
    /// windows on motions that are still mid-track, which unsaid reads exactly
    /// like a motion that played out.
    #[test]
    fn a_closed_window_drops_its_player_alone_and_says_what_it_cut() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        let mut both = open(&motions, 1, 1, 0);
        both.plays.push(Playing {
            index: 4,
            motion: held(&motions, NOD),
            speed: 1.0,
            elapsed_ms: 0,
        });
        players.sync(&both, &sink);
        let opening = players.advance(PERIOD)[0].frame;

        // The step-4 head motion's window closes; the step-1 antennas one is
        // still open, and the script is the same script throughout.
        players.sync(&open(&motions, 1, 1, 20), &sink);

        assert_eq!(players.len(), 1, "the open window lost its player too");
        assert_eq!(players.seq(), Some(1));
        let fields = sink
            .fields("motion_overlay_cut")
            .expect("the closed window says what it cut short");
        assert_eq!(fields["name"], json!(NOD));
        assert_eq!(fields["step"], json!(4));
        assert_eq!(fields["seq"], json!(1));
        assert!(
            !sink.saw("motion_overlays_replaced") && !sink.saw("motion_overlays_lapsed"),
            "one script closing its own window was reported as the script ending"
        );

        let carried = players.advance(PERIOD)[0].frame;
        assert_ne!(
            carried, opening,
            "the surviving player was restarted rather than carried"
        );
    }

    /// A replacement that cuts a motion short names the motion it cut and not
    /// the one that replaced it, and a replacement that cuts nothing short says
    /// nothing.
    ///
    /// Otherwise a capture cannot tell a clip a republish truncated from one
    /// that played out — the trace is two starts and an acceptance, and the
    /// reader has to know the clip's duration to notice. Naming the incoming
    /// motion instead would be worse than silence: it says the motion that is
    /// playing is the one that got cut. The silence on the empty case keeps the
    /// ordinary posture-script republish quiet.
    #[test]
    fn a_replacement_names_the_motions_it_cut_short() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        players.sync(&open(&motions, 1, 1, 0), &sink);
        players.sync(&playing(held(&motions, NOD), 2, 3, 0), &sink);

        assert_eq!(
            sink.all_fields("motion_overlays_replaced").len(),
            1,
            "the replacement was reported more than once"
        );
        let fields = sink
            .fields("motion_overlays_replaced")
            .expect("the replacement is reported");
        assert_eq!(fields["motions"], json!([WIGGLE]));
        assert_eq!(fields["seq"], json!(1));
        assert_eq!(fields["replaced_by"], json!(2));
        assert_eq!(players.len(), 1, "a replacement kept the old player");
        assert_eq!(players.seq(), Some(2));

        // Two posture-only scripts in a row: the seq changes with no player
        // under it, so there is nothing to have cut short.
        let sink = Collect::default();
        let mut idle = Overlays::none();
        idle.sync(
            &Overlaid {
                seq: Some(1),
                plays: Vec::new(),
            },
            &sink,
        );
        idle.sync(
            &Overlaid {
                seq: Some(2),
                plays: Vec::new(),
            },
            &sink,
        );
        assert!(
            !sink.saw("motion_overlays_replaced"),
            "a republish with no motion under it was reported as cutting one short"
        );
    }

    /// A script that stops running while a motion is playing says so as a lapse
    /// and not as a replacement.
    ///
    /// The two ends are different questions for an operator: a republish rate
    /// is a publisher's doing, where a script that simply ran out of timeout
    /// truncated its own motion. One event for both would read as a republish
    /// storm that never happened.
    #[test]
    fn a_script_that_stops_running_says_its_motions_lapsed() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        players.sync(&open(&motions, 1, 1, 0), &sink);
        players.forget(None, &sink);

        assert!(players.is_empty(), "the lapsed script kept its players");
        assert_eq!(players.seq(), None);
        let fields = sink
            .fields("motion_overlays_lapsed")
            .expect("the lapse says what it cut short");
        assert_eq!(fields["motions"], json!([WIGGLE]));
        assert_eq!(fields["seq"], json!(1));
        assert!(
            !sink.saw("motion_overlays_replaced"),
            "a script that ran out was reported as replaced"
        );
    }

    /// The players compose in wire step order, whatever order their windows
    /// opened in.
    ///
    /// The compositor right-multiplies head deltas, so which overlay came first
    /// is what decides the pose — and a set ordered by when a daemon happened
    /// to notice a window would make that a matter of scheduling.
    #[test]
    fn the_players_are_held_in_step_order() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();

        // Two motions with different masks, so the samples say which player
        // produced them: what the order has to hold for is `compose`, which
        // reads the samples and never the set they came out of.
        players.sync(&open(&motions, 1, 5, 0), &sink);
        let mut both = open(&motions, 1, 5, 20);
        both.plays.push(Playing {
            index: 2,
            motion: held(&motions, NOD),
            speed: 1.0,
            elapsed_ms: 0,
        });
        players.sync(&both, &sink);

        let samples = players.advance(PERIOD);
        assert_eq!(samples.len(), 2);
        assert!(
            samples[0].frame.head.is_some() && samples[0].frame.antennas.is_none(),
            "the step-2 head motion did not come out first: {samples:?}"
        );
        assert!(
            samples[1].frame.antennas.is_some() && samples[1].frame.head.is_none(),
            "the step-5 antennas motion did not come out second: {samples:?}"
        );
    }

    /// A window this daemon joins in progress starts its player at the offset
    /// the timeline gives it.
    ///
    /// The timeline is authoritative in absolute time: a daemon that read a
    /// script whose overlay started before it looked has to pick the motion up
    /// where it should be by now. Joining at zero instead would replay it from
    /// its first frame, late — out of step with its own window, which then cuts
    /// it off mid-frame, and with the sound sidecar the recording came with.
    #[test]
    fn a_joined_window_starts_at_the_offset_the_timeline_gives_it() {
        let (_dir, motions) = motions();
        let sink = Collect::default();

        let mut from_the_top = Overlays::none();
        from_the_top.sync(&open(&motions, 1, 1, 0), &sink);
        let opening = from_the_top.advance(PERIOD)[0];

        // An odd number of the clip's own frames in: its track alternates, so
        // an even offset would carry the same delta as its first frame and
        // prove nothing.
        let joined_ms = 100;
        let mut joining = Overlays::none();
        joining.sync(&open(&motions, 1, 1, joined_ms), &sink);
        let midway = joining.advance(PERIOD)[0];

        assert_ne!(
            opening.frame, midway.frame,
            "the join offset never reached the player"
        );
        let joins = sink
            .all_fields("motion_play")
            .into_iter()
            .map(|fields| fields["joined_ms"].clone())
            .collect::<Vec<_>>();
        assert_eq!(joins, vec![json!(0), json!(joined_ms)]);
    }

    /// A drop says which class of refusal it was and which joint it named.
    #[test]
    fn a_drop_reports_the_refusal_by_class_and_joint() {
        let (_dir, motions) = motions();
        let sink = Collect::default();
        let mut players = Overlays::none();
        players.sync(&open(&motions, 1, 1, 0), &sink);

        players.refuse(
            &CommandRejection::StepTooLarge {
                joint: JointId::AntennaLeft,
                delta: 0.7,
            },
            &sink,
        );

        let fields = sink
            .fields("motion_overlays_dropped")
            .expect("the drop is reported");
        assert_eq!(fields["reason"], json!("step_too_large"));
        assert_eq!(fields["joint"], json!(JointId::AntennaLeft.to_string()));
        assert!(
            fields["detail"]
                .as_str()
                .expect("the words as well")
                .contains("step"),
            "{fields:?}"
        );

        // A refusal about the whole plan rather than a joint names no joint.
        let sink = Collect::default();
        let mut players = Overlays::none();
        players.sync(&open(&motions, 2, 1, 0), &sink);
        players.refuse(
            &CommandRejection::Trajectory(TrajectoryError::NonFinite),
            &sink,
        );
        let fields = sink
            .fields("motion_overlays_dropped")
            .expect("the drop is reported");
        assert_eq!(fields["reason"], json!("unshapeable"));
        assert_eq!(fields["joint"], json!(null));
    }
}
