//! The motion scripter: what a pod's head should be doing, as a timed script.
//!
//! The pipeline knows a dozen ways an interaction starts and stops, and it also
//! knows — from the moment playback begins — how long the speech it queued is
//! going to take. This module turns both into [`MotionScript`]s: a timeline of
//! poses the motion daemon executes on its own clock, rather than a stream of
//! states it has to reduce.
//!
//! That is the whole reason the ordinary conversation is *one* message. The
//! audio starts streaming and the same instant carries the entire motion
//! timeline including the stow, so the head starts down when the speech ends
//! instead of waiting out a linger nobody can shorten.
//!
//! Per pod, the scripter is a pure function from (turn state, audio horizon) to
//! the script it wants standing at the daemon, emitted when that answer changes
//! and re-emitted on a refresh cadence. Two answers exist:
//!
//! - **hold** — `<pose>@0`. The head is at a pose and the timeline is not known
//!   yet: a wake with no utterance yet, an utterance with the brain still
//!   thinking, a barge over someone else's answer. Its timeout is the bound that
//!   matters, and the refresh is what keeps a long think from crossing it.
//! - **closing** — `<pose>@0, stow@t`. The whole turn in one message: the head
//!   is at that pose now and comes down at `t`, which is where the turn's speech
//!   is estimated to end plus a margin. Re-emitting it recomputes the offset
//!   from that same absolute instant, so a re-emission never moves the stow.
//!
//! Which pose is the event's: a wake and a barge are *listening* and take
//! [`ScriptRaises::wake`], a dispatched utterance is *answering* and takes
//! [`ScriptRaises::turn`]. A hold whose pose changed is a new answer and is
//! published; the same event always produces the same pose, whatever the head
//! was doing. A closing script never retargets — it is built from the pose the
//! hold it closes was carrying — so a re-emission moves nobody.
//!
//! A step may state a pace of its own, which is what each [`Raise`]'s own
//! `move_ms` puts on the wire; absent, the library's default for that pose
//! applies.
//!
//! There is no third answer for "down": a script that has run its stow leaves
//! the daemon resting, which is its default state, so the scripter goes quiet —
//! after saying the stow one last time. That confirming `stow@0` is what makes
//! the refresh cadence's repair promise true for a turn whose stow falls due
//! inside one refresh period; two lost messages in a row fall to the standing
//! script's own timeout, which is what that timeout is for.
//!
//! Every emitted script's timeout covers its own timeline, because the wire
//! contract makes the timeout an unconditional ceiling: the configured bound, or
//! the stow offset plus a refresh period where the speech reaches further. A
//! plan reaching past the protocol's own ceiling is cut back to it and said out
//! loud rather than emitted and refused — the scripter bounding its own plan,
//! which is what keeps script construction infallible here.
//!
//! The closing answer waits on three facts, in any arrival order — the brain has
//! said how the turn ended, dispatch has returned, and no cmd is still waiting to
//! start playing. None of them is a state machine and none of them has to arrive
//! before another; that is why this module has no ordering invariant to protect.
//!
//! Every fact carries the turn it belongs to and a fact for some other turn is
//! ignored. A barged turn's dispatch still returns and still reports, and without
//! that check it would close a conversation the barge had just reopened.
//!
//! Two layers, split so the decisions are testable without a socket:
//!
//! - [`Scripter`] is the decision. Inputs and a clock go in, scripts come out;
//!   it owns no I/O and no timers, only the re-emission deadline it reports
//!   through [`Scripter::deadline`]. Nothing in it reads a clock, spawns
//!   anything, or can fail.
//! - [`ScriptTask`] is the task: it selects on the input channel and that
//!   deadline, and publishes what the scripter decides.
//!
//! Every lifecycle point the pipeline already reaches sends one [`ScriptInput`]
//! here — a one-line, non-blocking `try_send` beside the code that was going to
//! run anyway — so a wedged task loses inputs rather than holding up a turn. A
//! lost input is bounded by the same mechanism a lost message is: the standing
//! script carries a timeout, and the daemon stows when it lapses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use brenn_bridge::{BridgeHandle, PublishRequest, Urgency};
use motion_proto::{MAX_TIMEOUT_MS, MotionScript, Play, STOW_POSE, SeqSource, Step, unix_millis};
use serde_json::json;
use speech_pipeline::{PodId, TurnEnd, UtteranceId};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Duration, Instant, sleep};
use tokio_util::sync::CancellationToken;

use crate::barge::TurnAudio;
use crate::brenn::publish_once;
use crate::config::BrennConfig;
use crate::jsonl::JsonlHandle;
use crate::time::due;

/// The pose a config that names none takes for both of its presence events.
///
/// Every deployment's library holds it under this name, and a configuration
/// that names no pose gets it for both events: one place the head goes for
/// every raise, which is a deployment that has not decided to tell listening
/// and answering apart.
pub const DEFAULT_PRESENCE_POSE: &str = "neutral";

/// The move one presence event asks for: a pose, and the pace of the move to it.
///
/// One value, because the two are one decision — choosing where the head goes
/// is choosing how fast it gets there — and because a want carries what it is
/// standing at, so a re-paced raise is a different answer and is published.
/// Where the pose *is* stays the daemon's library's; a name this side cannot
/// resolve is not an error it can detect.
///
/// The name is held as [`Arc<str>`] because every hold and every closing want
/// carries it, and those are cloned per input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Raise {
    /// The pose's name in the daemon's library.
    pub pose: Arc<str>,
    /// The pace stated for the move, or `None` to leave it to the library.
    pub move_ms: Option<u64>,
}

impl Raise {
    /// A raise to `pose` at the library's own pace.
    #[must_use]
    pub fn to(pose: &str) -> Self {
        Self {
            pose: pose.into(),
            move_ms: None,
        }
    }

    /// This raise as a base step due `after_ms` past the script's arrival.
    pub(crate) fn step(&self, after_ms: u64) -> Step {
        match self.move_ms {
            Some(move_ms) => Step::timed(after_ms, self.pose.as_ref(), move_ms),
            None => Step::new(after_ms, self.pose.as_ref()),
        }
    }
}

/// The three moves this scripter ever asks for.
///
/// Pose and pace together per event, rather than the poses here and the paces
/// among the intervals a script is measured in: the scripter reads one of these
/// per cause, so a split would be one fact held twice — in the value it was
/// configured as and in the raise published from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptRaises {
    /// Where the head goes on a wake word and on a barge: *listening*.
    pub wake: Raise,
    /// Where the head goes when an utterance is dispatched: *answering*.
    pub turn: Raise,
    /// The stow every step this scripter emits names: the reserved pose, at
    /// whatever pace the configuration gives the scripter's own endings.
    pub stow: Raise,
}

/// The raises a config that names none of the presence keys produces.
///
/// [`BrennConfig::script_raises`] must agree with this, and a test asserts it
/// does.
///
/// [`BrennConfig::script_raises`]: crate::config::BrennConfig::script_raises
impl Default for ScriptRaises {
    fn default() -> Self {
        Self {
            wake: Raise::to(DEFAULT_PRESENCE_POSE),
            turn: Raise::to(DEFAULT_PRESENCE_POSE),
            stow: Raise::to(STOW_POSE),
        }
    }
}

/// One movement a response asked for, resolved against the deployed library.
///
/// Resolved, not named: the name has been checked against the library and the
/// pace it asked for has become the absolute number the wire carries, because
/// an unresolvable name in a script makes the daemon refuse the *whole* script
/// it rides in — every other movement in it with it.
#[derive(Debug, Clone, PartialEq)]
pub enum MotionCue {
    /// Put the head at this pose and leave it there. The same value a presence
    /// event's raise carries, because a cued pose is a raise the reply chose.
    Pose(Raise),
    /// Play this motion once over whatever pose is standing.
    Motion {
        /// The overlay to start, at the speed the reply asked for.
        play: Play,
        /// How long the overlay occupies the timeline at that speed, blend-out
        /// included: the library's own duration, not a guess. What tells the
        /// scripter when the motion is over and the head's ending may proceed.
        span_ms: u64,
    },
}

/// The four intervals a script is measured in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptTiming {
    /// How often the standing script is re-emitted while it still says
    /// something. A lost message is repaired within one of these, and a hold
    /// script is kept clear of its own timeout by them. Also the headroom every
    /// emitted timeout carries past its own last step, so a re-emission always
    /// has room to land; on a timeline ending at the stow, a stow paced longer
    /// than this sets that headroom instead, because that last step's own move
    /// has to fit inside the timeout too. Must not exceed [`MAX_TIMEOUT_MS`].
    pub refresh: Duration,
    /// How long the head stays up after a turn that asked to keep listening, and
    /// after a raise that produced no turn at all. The `<listen/>` window.
    pub linger: Duration,
    /// The *floor* under the timeout every emitted script carries: the daemon
    /// stows this long after receipt whatever else happens. On a hold script,
    /// whose timeline is not known yet, it is the whole bound; on a closing
    /// script whose stow falls inside it, it is a backstop past the stow. A
    /// closing script reaching further than this carries a timeout sized from
    /// its own timeline instead, because the timeout is a ceiling on that
    /// timeline and may not be shorter than it. Must not exceed
    /// [`MAX_TIMEOUT_MS`].
    pub max_engaged: Duration,
    /// How long after the estimated end of a closed turn's speech the head
    /// starts down. Absorbs the jitter between an estimate made when playback
    /// started and what the speaker actually did.
    pub stow_margin: Duration,
}

/// The timings a config that names none of the presence keys produces.
///
/// The same four numbers `[brenn]`'s defaults are built from, so a caller with
/// no config in hand — a test, a fixture — measures the head the way the
/// shipped deployment does.
/// [`BrennConfig::script_timing`] must agree with this, and a test asserts
/// it does.
///
/// [`BrennConfig::script_timing`]: crate::config::BrennConfig::script_timing
impl Default for ScriptTiming {
    fn default() -> Self {
        Self {
            refresh: Duration::from_millis(crate::config::default_presence_refresh_ms()),
            linger: Duration::from_millis(crate::config::default_presence_linger_ms()),
            max_engaged: Duration::from_millis(crate::config::default_presence_max_engaged_ms()),
            stow_margin: Duration::from_millis(crate::config::default_presence_stow_margin_ms()),
        }
    }
}

/// The clock reading one decision is made against.
///
/// Two clocks, because a script needs both: the timeline is measured on the
/// monotonic clock, and the sequence number is wall-clock milliseconds so that a
/// restarted scripter resumes above its own high-water mark with nothing
/// persisted. Passed in rather than read here, so every decision below is
/// testable without a timer racing it.
#[derive(Debug, Clone, Copy)]
pub struct Now {
    /// The instant offsets are measured from.
    pub at: Instant,
    /// Unix milliseconds, for the sequence number.
    pub unix_ms: u64,
}

impl Now {
    /// Read both clocks.
    #[must_use]
    pub fn read() -> Self {
        Self {
            at: Instant::now(),
            unix_ms: unix_millis(SystemTime::now()),
        }
    }
}

/// One thing that happened to an interaction, as the scripter cares about it.
///
/// Deliberately not the pipeline's own vocabulary: the tap sites are spread
/// across several modules and event enums, and naming the *effect on the head*
/// rather than the cause is what keeps this from growing a branch per call site.
// No `Eq`: a cue carries a speed, which is a float.
#[derive(Debug, Clone, PartialEq)]
pub enum ScriptInput {
    /// A confirmed wake word. The head goes up.
    Wake(PodId),
    /// Speech over live playback: interaction with no wake word in front of it.
    /// The head goes up and the turn being cut stops counting.
    Barge(PodId),
    /// An utterance went to the brain. The head goes up, and this is the turn
    /// every later fact must name to be heard.
    TurnStarted {
        /// Whose interaction.
        pod: PodId,
        /// The utterance the brain is answering.
        turn: UtteranceId,
    },
    /// That turn came back, with the brain's word on how it ended.
    TurnEnded {
        /// Whose interaction.
        pod: PodId,
        /// Which turn ended.
        turn: UtteranceId,
        /// `Open` when the response asked to keep listening, so the head waits
        /// out a linger rather than a margin.
        end: TurnEnd,
    },
    /// A raise produced no turn: the wake arm expired with no command, or the
    /// confidence gate declined what was said.
    Unanswered(PodId),
    /// Speech was heard inside an open `<listen/>` window. The head keeps
    /// waiting where it stands, a linger from now.
    Heard(PodId),
    /// A reply asked the head to move. The movements of one response message,
    /// in the order the reply named them; the last pose and the last motion in
    /// the batch are the ones that take effect.
    ///
    /// Content, not a person: a cue never raises a head that is at rest.
    Cues {
        /// Whose head.
        pod: PodId,
        /// The turn whose reply asked. Reported, not matched — the cue arrives
        /// through the brain's own sink for the turn in flight.
        turn: UtteranceId,
        /// What to do, already resolved against the library.
        cues: Vec<MotionCue>,
    },
    /// The turn's cmd accounting moved: dispatch returned, a clip started
    /// playing, or a cmd resolved.
    Audio {
        /// Whose interaction.
        pod: PodId,
        /// Which turn the accounting is about.
        turn: UtteranceId,
        /// The reading as it stands.
        audio: TurnAudio,
    },
}

impl ScriptInput {
    /// Whose interaction this is about.
    #[must_use]
    pub fn pod(&self) -> &PodId {
        match self {
            ScriptInput::Wake(pod)
            | ScriptInput::Barge(pod)
            | ScriptInput::TurnStarted { pod, .. }
            | ScriptInput::TurnEnded { pod, .. }
            | ScriptInput::Unanswered(pod)
            | ScriptInput::Heard(pod)
            | ScriptInput::Cues { pod, .. }
            | ScriptInput::Audio { pod, .. } => pod,
        }
    }
}

/// Why a script went out. Carried onto the JSONL line so an emission reads as a
/// cause and not just a timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A confirmed wake word.
    Wake,
    /// Speech over live playback.
    Barge,
    /// An utterance dispatched to the brain.
    Turn,
    /// A raise that produced no turn.
    Unanswered,
    /// Speech inside an open capture window.
    Heard,
    /// A reply asked the head to move.
    Cue,
    /// A cued motion's own span ran out, so the standing want is said plainly
    /// again — with its ending back in it.
    MotionEnded,
    /// The turn's speech is accounted for, so its ending can be scheduled.
    Closing,
    /// The standing script said again; not a change.
    Refresh,
}

impl Cause {
    /// The cause as a log line spells it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Cause::Wake => "wake",
            Cause::Barge => "barge",
            Cause::Turn => "turn",
            Cause::Unanswered => "unanswered",
            Cause::Heard => "heard",
            Cause::Cue => "cue",
            Cause::MotionEnded => "motion_ended",
            Cause::Closing => "closing",
            Cause::Refresh => "refresh",
        }
    }
}

/// One script the scripter decided to publish.
#[derive(Debug, Clone, PartialEq)]
pub struct ScriptPublish {
    /// Whose head.
    pub pod: PodId,
    /// The timeline to put on the wire.
    pub script: MotionScript,
    /// What moved.
    pub cause: Cause,
    /// True when this is a change in what the head is being asked to do rather
    /// than a re-emission of the standing answer. Only a change is worth a
    /// console line; saying the same thing every few seconds is not.
    pub change: bool,
    /// The stow offset this script would have named had the ceiling not cut it
    /// back, in milliseconds from the decision instant. `None` on every script
    /// the ceiling did not touch, which is all of them under any horizon a real
    /// turn produces.
    ///
    /// Carried out rather than logged here because the decision half of this
    /// module owns no I/O: the task publishing the script is what narrates it.
    pub clamped_from_ms: Option<u64>,
}

/// What the scripter wants standing at the daemon for one pod.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum Want {
    /// Nothing. The daemon's default state is rest, so silence is the ask.
    #[default]
    Quiet,
    /// Head at a pose, timeline unknown.
    Hold {
        /// The pose the head is held at, and the pace the move to it states.
        /// Part of the want, so a hold whose pose or pace changed is a
        /// different answer and is published.
        at: Raise,
    },
    /// Head at a pose now, down at this instant.
    Closing {
        /// The pose the head stays at until it starts down: whatever the hold
        /// this closes was carrying, so closing retargets nobody.
        at: Raise,
        /// When the head starts down, as an absolute instant, so that
        /// re-emissions do not move it.
        stow_at: Instant,
    },
    /// Head down now, and said once already.
    ///
    /// The state between a closing want whose stow instant was already past
    /// when the want was born and the silence that follows it. The head is on
    /// its way down and nothing is scheduled; what is left is to say so a second
    /// time, because a stow due at the moment it was decided never got the
    /// re-emission the refresh cadence gives every other closing instruction.
    Stowing,
}

impl Want {
    /// This want as a timeline measured from `now`, or `None` when there is
    /// nothing left to schedule — nothing wanted at all, or a stow whose
    /// instant has already arrived. What to do about the second case is the
    /// caller's: it depends on what is standing at the daemon.
    fn steps(&self, now: Instant, stow: &Raise) -> Option<Vec<Step>> {
        match self {
            Want::Quiet | Want::Stowing => None,
            Want::Hold { at } => Some(vec![at.step(0)]),
            // A raise and a stow at the same offset is not a timeline, so a
            // stow that is already due has no future to describe.
            Want::Closing { at, stow_at } => {
                let after_ms = millis_between(now, *stow_at);
                (after_ms > 0).then(|| vec![at.step(0), stow.step(after_ms)])
            }
        }
    }

    /// The raise this want is standing at, or `None` for a want that holds the
    /// head nowhere.
    fn at(&self) -> Option<&Raise> {
        match self {
            Want::Hold { at } | Want::Closing { at, .. } => Some(at),
            Want::Quiet | Want::Stowing => None,
        }
    }
}

/// A cued motion the head is in the middle of.
///
/// The span is the library's own, so this is not a guess about when the motion
/// is over: it is when the daemon's player goes inert. Every script emitted
/// while this stands restates the play, because a script wholly replaces its
/// predecessor at the daemon and an overlay the replacement does not name is
/// cut on the next tick. One at a time — a second cue replaces this one.
#[derive(Debug, Clone, PartialEq)]
struct Running {
    /// The overlay to restate, at the speed it was cued with. A row the daemon
    /// picks up keeps the speed it joined at, so restating it is inert.
    play: Play,
    /// How long the window is, measured from each script's own arrival. The
    /// edge refuses a play window reaching past its script's horizon, so this
    /// is also what the emitted timeout has to cover.
    span_ms: u64,
    /// When the motion is over and the standing want may be said plainly again.
    ends_at: Instant,
}

/// One pod's script state.
#[derive(Debug, Default)]
struct PodScript {
    /// The turn in flight. Every fact naming another turn is stale — the turn
    /// was barged, or a new interaction started over it — and is ignored.
    turn: Option<UtteranceId>,
    /// How the brain said the turn ended, once it has.
    end: Option<TurnEnd>,
    /// The turn's cmd accounting as last reported.
    audio: Option<TurnAudio>,
    /// What this pod's head is being asked to do.
    want: Want,
    /// A cued motion still playing over that want, if any. Not part of the
    /// want: it does not decide where the head ends up, only what every script
    /// says while it lasts.
    running: Option<Running>,
    /// When the standing script is said again.
    refresh: Option<Instant>,
}

impl PodScript {
    /// Forget the turn in flight and everything said about it. A new
    /// interaction, or one abandoned, starts the facts over — and a fact left
    /// behind by the previous turn would close the new one early.
    fn clear_turn(&mut self) {
        self.turn = None;
        self.end = None;
        self.audio = None;
    }
}

/// The scripter: inputs and a clock in, scripts out.
pub struct Scripter {
    timing: ScriptTiming,
    /// Where each presence event puts the head, paired with the pace its own
    /// timing states, so a raise is one lookup by cause.
    wake: Raise,
    turn: Raise,
    /// The stow every step this scripter emits names: the reserved pose, at
    /// whatever pace the configuration gives the scripter's own endings.
    stow: Raise,
    pods: HashMap<PodId, PodScript>,
    /// One source across every pod. Numbers only have to climb per pod, and a
    /// single strictly-increasing stream satisfies that for all of them.
    seq: SeqSource,
}

impl Scripter {
    /// A scripter with nothing to say about any pod.
    #[must_use]
    pub fn new(timing: ScriptTiming, raises: ScriptRaises) -> Self {
        Self {
            wake: raises.wake,
            turn: raises.turn,
            stow: raises.stow,
            timing,
            pods: HashMap::new(),
            seq: SeqSource::new(),
        }
    }

    /// Apply one input. At most one script comes back: most inputs only move a
    /// fact, and a fact that leaves the answer where it was is not published.
    pub fn apply(&mut self, input: ScriptInput, now: Now) -> Option<ScriptPublish> {
        let pod = input.pod().clone();
        let publish = match input {
            ScriptInput::Wake(_) => self.raise(&pod, now, Cause::Wake),
            ScriptInput::Barge(_) => self.raise(&pod, now, Cause::Barge),
            ScriptInput::TurnStarted { turn, .. } => {
                let publish = self.raise(&pod, now, Cause::Turn);
                // After the raise, which cleared the previous turn's facts.
                self.pods.entry(pod.clone()).or_default().turn = Some(turn);
                publish
            }
            ScriptInput::TurnEnded { turn, end, .. } => {
                let p = self.pods.entry(pod.clone()).or_default();
                if p.turn != Some(turn) {
                    None
                } else {
                    p.end = Some(end);
                    self.reconsider(&pod, now)
                }
            }
            ScriptInput::Unanswered(_) | ScriptInput::Heard(_) | ScriptInput::Cues { .. }
                if !self.engaged(&pod) =>
            {
                // The head is down or on its way: this pod's script has run, or
                // it never had one. Both tap sites can fire twice about the same
                // raise — the confidence gate declines and the arm then expires
                // — and raising the head to lower it again is not what either
                // means. A `Heard` and a `Cues` are refused for the same reason
                // and one more: content never raises a head that is at rest.
                None
            }
            ScriptInput::Unanswered(_) => self.wait_out_linger(&pod, now, Cause::Unanswered),
            ScriptInput::Heard(_) => self.wait_out_linger(&pod, now, Cause::Heard),
            ScriptInput::Cues { cues, .. } => self.cue(&pod, now, cues),
            ScriptInput::Audio { turn, audio, .. } => {
                let p = self.pods.entry(pod.clone()).or_default();
                if p.turn != Some(turn) {
                    None
                } else {
                    p.audio = Some(audio);
                    self.reconsider(&pod, now)
                }
            }
        };
        self.tidy(&pod);
        publish
    }

    /// Whether this pod's head is up: a want that holds it somewhere, which is
    /// the precondition for every input that is content rather than a person.
    #[must_use]
    pub fn engaged(&self, pod: &PodId) -> bool {
        !matches!(self.want(pod), Want::Quiet | Want::Stowing)
    }

    /// Fire every re-emission due at `now`, and lift every cued motion whose
    /// span has run out.
    ///
    /// A closing script whose stow instant has arrived gets one last emission —
    /// the stow, immediately — and then the scripter goes quiet. See
    /// [`Scripter::emit`] for why that confirming re-send is what makes the
    /// refresh cadence's repair promise true for short turns.
    ///
    /// A motion's lift is its own cause: what goes out is the standing want
    /// said plainly, which is the first script since the cue to carry the
    /// head's ending again. The stow a cued motion held back is taken here
    /// rather than cutting the motion for it.
    pub fn tick(&mut self, now: Now) -> Vec<ScriptPublish> {
        let due: Vec<(PodId, bool)> = self
            .pods
            .iter()
            .filter_map(|(pod, p)| {
                let lift = p.running.as_ref().is_some_and(|r| r.ends_at <= now.at);
                let refresh = p.refresh.is_some_and(|at| at <= now.at);
                (lift || refresh).then(|| (pod.clone(), lift))
            })
            .collect();
        let mut out = Vec::new();
        for (pod, lift) in due {
            let cause = if lift {
                if let Some(p) = self.pods.get_mut(&pod) {
                    p.running = None;
                }
                Cause::MotionEnded
            } else {
                Cause::Refresh
            };
            if let Some(publish) = self.emit(&pod, now, cause, lift, None) {
                out.push(publish);
            }
            self.tidy(&pod);
        }
        out
    }

    /// The earliest instant [`Scripter::tick`] has anything to do, or `None`
    /// when no pod has a script standing.
    ///
    /// A running motion's end is one of those instants: the standing want has
    /// an ending in it that is not being said while the motion plays, so the
    /// lift cannot wait for the next refresh.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.pods
            .values()
            .flat_map(|p| [p.refresh, p.running.as_ref().map(|r| r.ends_at)])
            .flatten()
            .min()
    }

    /// Apply one response message's movements and say so at once.
    ///
    /// Last of each kind wins: a reply naming two poses means the second, and a
    /// second motion replaces one still playing rather than queueing behind it.
    /// A pose does not disturb a running motion — the motion plays over
    /// whatever base is standing — and a motion does not move the pose the head
    /// returns to when it ends.
    ///
    /// Callers must have refused every want that holds the head nowhere: a cue
    /// never raises a head that is at rest.
    fn cue(&mut self, pod: &PodId, now: Now, cues: Vec<MotionCue>) -> Option<ScriptPublish> {
        {
            let p = self.pods.get_mut(pod)?;
            for cue in cues {
                match cue {
                    MotionCue::Pose(raise) => match &mut p.want {
                        Want::Hold { at } | Want::Closing { at, .. } => *at = raise,
                        // Refused by the caller's guard; a want holding the head
                        // nowhere has no base for a cued pose to replace.
                        Want::Quiet | Want::Stowing => {}
                    },
                    MotionCue::Motion { play, span_ms } => {
                        p.running = Some(Running {
                            play,
                            span_ms,
                            ends_at: now.at + Duration::from_millis(span_ms),
                        });
                    }
                }
            }
        }
        // Not `set`: the want may be unchanged — a motion over the standing
        // pose — and the script still has to go out, because the play is only
        // in front of the daemon once a script names it.
        self.emit(pod, now, Cause::Cue, true, None)
    }

    /// Put the head at the pose this cause means and start the turn's facts
    /// over. Already held at that pose means the refresh cadence carries it and
    /// nothing is said: a wake, its utterance and a barge over the answer are
    /// three raises in a few seconds, and where two of them agree they are one
    /// hold script. Where they do not, the base retargets mid-hold — which is
    /// the whole point of naming a pose per event.
    fn raise(&mut self, pod: &PodId, now: Now, cause: Cause) -> Option<ScriptPublish> {
        let at = match cause {
            Cause::Turn => self.turn.clone(),
            _ => self.wake.clone(),
        };
        // A raise is a person. Whatever the last reply asked the head to do,
        // this outranks it: the cued motion is dropped and the script that goes
        // out names the raise alone.
        let cut = {
            let p = self.pods.entry(pod.clone()).or_default();
            p.clear_turn();
            p.running.take().is_some()
        };
        if cut {
            // The want may be exactly what it was — a raise to the pose already
            // held — and the script still has to go out: it is the one that
            // stops naming the play, and an overlay a replacement does not
            // restate is what ends it.
            let p = self.pods.get_mut(pod)?;
            p.want = Want::Hold { at };
            return self.emit(pod, now, cause, true, None);
        }
        self.set(pod, now, Want::Hold { at }, cause, None)
    }

    /// The room a timeout keeps past its own last step when that step is the
    /// stow.
    ///
    /// The refresh period is the floor: a re-emission that lands late still
    /// finds the previous script standing. A stow paced longer than that sets
    /// the room instead, because the daemon measures a stow-terminal timeline
    /// as its last step plus the pace that step states — a timeout shorter than
    /// that sum is a script it refuses whole, so the head would come down on
    /// nothing the configuration asked for.
    ///
    /// A script whose last step is not the stow keeps the refresh period alone:
    /// a move it does not carry has no room to buy, and charging it the stow's
    /// pace would lift the daemon's backstop on a raised head past the
    /// configured engagement ceiling.
    fn stow_headroom_ms(&self) -> u64 {
        millis(self.timing.refresh).max(self.stow.move_ms.unwrap_or(0))
    }

    /// A stow at `stow_at` as a want, cut back to the last instant a script
    /// emitted at `now` may name — and the offset it asked for when the cut
    /// engaged.
    ///
    /// Every script carries a timeout that covers its own timeline, and no
    /// timeout may exceed [`MAX_TIMEOUT_MS`], so the furthest stow this scripter
    /// can express is the ceiling less the room a stow-terminal timeout keeps
    /// past its last step — and the script this bounds is stow-terminal, so the
    /// rule cut from here is the rule that sizes it. Bounding the plan here
    /// rather than letting the emission fail is what keeps script construction
    /// infallible, and it is the host-side
    /// half of the slip protection: a stow instant computed from seconds where
    /// milliseconds were meant lands at the ceiling, minutes out and narrated,
    /// rather than hours out.
    ///
    /// This is not the daemon clamping somebody's timeline. The scripter is
    /// bounding its own plan from its own facts, and saying so; the instruction
    /// that goes out is whole.
    ///
    /// An ending already scheduled stands. The cut instant is dated from `now`
    /// where an uncut one is dated from the audio, so re-deriving it on every
    /// further fact about the same slipped turn would walk the stow forward one
    /// inter-fact gap at a time and emit a script for each step — the head's
    /// exposure measured from the last fact instead of from the decision. This
    /// is the rule the no-horizon branch of [`Scripter::reconsider`] already
    /// keeps, for the same reason.
    fn closing(&self, pod: &PodId, now: Now, at: Raise, stow_at: Instant) -> (Want, Option<u64>) {
        let headroom =
            Duration::from_millis(MAX_TIMEOUT_MS.saturating_sub(self.stow_headroom_ms()));
        let ceiling = now.at + headroom;
        if stow_at <= ceiling {
            return (Want::Closing { at, stow_at }, None);
        }
        let scheduled = match self.pods.get(pod).map(|p| &p.want) {
            Some(Want::Closing { stow_at, .. }) => (*stow_at).min(ceiling),
            _ => ceiling,
        };
        (
            Want::Closing {
                at,
                stow_at: scheduled,
            },
            Some(millis_between(now.at, stow_at)),
        )
    }

    /// Keep the head where it stands and re-date its ending to a linger from
    /// now. The answer to both facts that say "the interaction is not over, and
    /// nothing new is coming through the brain": a raise that produced no turn,
    /// and speech heard inside an open capture window.
    ///
    /// The turn's facts are dropped first. A `TurnEnded` or `Audio` about the
    /// turn that just finished, arriving after this, would otherwise reconsider
    /// the ending back to that turn's own horizon; with no turn standing it is
    /// refused.
    ///
    /// Callers must have refused every want that holds the head nowhere — the
    /// closing carries the standing raise, so there has to be one.
    fn wait_out_linger(&mut self, pod: &PodId, now: Now, cause: Cause) -> Option<ScriptPublish> {
        let p = self.pods.entry(pod.clone()).or_default();
        p.clear_turn();
        // An unanswered raise ends what the last reply was doing; speech heard
        // inside the window does not — a cued nod plays on while the person
        // talks, and cutting it would be the head reacting to being listened
        // to.
        if cause == Cause::Unanswered {
            p.running = None;
        }
        let at = p
            .want
            .at()
            .cloned()
            .expect("the caller's guard refused every want that holds the head nowhere");
        let (want, clamped) = self.closing(pod, now, at, now.at + self.timing.linger);
        self.set(pod, now, want, cause, clamped)
    }

    /// Decide whether the turn in flight can be scheduled to its end yet, and
    /// with what stow instant.
    ///
    /// The three facts are ANDed in whatever order they arrived. A turn whose
    /// speech never started — silent, or every cmd dead before playback — has no
    /// horizon and stows off the margin alone.
    ///
    /// A fact about a turn whose script has already run never arrives here: a
    /// pod with nothing standing is dropped from the map, so the caller's turn
    /// check finds no turn to match and refuses it before this is called.
    fn reconsider(&mut self, pod: &PodId, now: Now) -> Option<ScriptPublish> {
        // The pose the closing holds is the one the standing want is already at,
        // so an ending retargets nothing: `reconsider` runs on every further
        // fact about the turn, and a closing built from the event's own pose
        // would move the head each time one arrived.
        //
        // This is also the one refusal of a want that holds the head nowhere,
        // `Want::Stowing` included: the stow has been said and is being
        // confirmed, and a further fact about a turn that is over must not
        // reopen an ending already in front of the daemon.
        let at = self.pods.get(pod)?.want.at()?.clone();
        let stow_at = {
            let p = self.pods.get(pod)?;
            let end = p.end?;
            let audio = p.audio?;
            if !audio.dispatch_done || audio.awaiting_start > 0 {
                return None;
            }
            let tail = match end {
                TurnEnd::Closed => self.timing.stow_margin,
                TurnEnd::Open => self.timing.linger,
            };
            match audio.horizon {
                // The stow is dated from the audio, not from now, so every
                // recomputation over the same horizon yields the same instant.
                // A horizon past the ceiling is the one that would be dated from
                // now instead; `closing` keeps the instant it already scheduled
                // so that this holds there too.
                Some(horizon) => horizon + tail,
                // Nothing has played. Keep an ending already scheduled rather
                // than sliding it forward on each further fact.
                None => match &p.want {
                    Want::Closing { .. } => return None,
                    _ => now.at + tail,
                },
            }
        };
        let (want, clamped) = self.closing(pod, now, at, stow_at);
        self.set(pod, now, want, Cause::Closing, clamped)
    }

    /// Adopt `want` and say so, unless it is what this pod is already being
    /// asked for.
    fn set(
        &mut self,
        pod: &PodId,
        now: Now,
        want: Want,
        cause: Cause,
        clamped_from_ms: Option<u64>,
    ) -> Option<ScriptPublish> {
        {
            let p = self.pods.get_mut(pod)?;
            if p.want == want {
                return None;
            }
            p.want = want;
        }
        self.emit(pod, now, cause, true, clamped_from_ms)
    }

    /// Render the pod's standing want as a script and arm the next re-emission.
    /// A want with nothing left to say publishes the stow once and then goes
    /// quiet.
    ///
    /// The overdue branch is the confirming re-send. Past the stow instant the
    /// want has no future to describe, and saying nothing would be the one turn
    /// shape whose closing instruction went out exactly once: the refresh
    /// cadence repairs a lost message only while the stow it carries is still
    /// ahead, and on a short answer or a silent turn it never is. One immediate
    /// `stow@0` here means every closing instruction is sent at least twice, so
    /// a single lost message is repaired within one refresh period — and it is
    /// safe when nothing was lost, because a stow-resolving script at a resting
    /// daemon commands nothing and this script cannot raise a head: its only
    /// step is the stow, and a wake arriving first replaces the want outright.
    ///
    /// A stow that was *already* due when the want was decided — a short answer
    /// whose facts settle after its own audio finished — reaches that branch on
    /// its very first publish, where one immediate stow would again be a single
    /// send. That one is armed for one more refresh instead, as
    /// [`Want::Stowing`], and confirmed on the tick after it. Either way the
    /// instruction goes out twice and the pod is then forgotten.
    fn emit(
        &mut self,
        pod: &PodId,
        now: Now,
        cause: Cause,
        change: bool,
        clamped_from_ms: Option<u64>,
    ) -> Option<ScriptPublish> {
        let refresh = self.timing.refresh;
        let floor_ms = millis(self.timing.max_engaged);
        let refresh_ms = millis(refresh);
        let stow_headroom_ms = self.stow_headroom_ms();
        let stow = self.stow.clone();
        let p = self.pods.get_mut(pod)?;
        // A repair publish is a change whatever the caller thought: it puts a
        // stow in front of a daemon holding a script that has none. Without
        // this the refresh path would publish it and never narrate it.
        let mut change = change;
        // Whether the timeline this publish carries ends at the stow, which is
        // what decides how much room the timeout keeps past it.
        let mut stow_terminal = matches!(p.want, Want::Closing { .. });
        // While a cued motion plays, every script says the same two things: the
        // pose the want stands at, and the play. Not the want's stow — each
        // restatement's window runs the motion's full span from *that* script's
        // arrival, because the edge cannot know the player is part-way through,
        // so a stow dated inside it would have the window reaching past the
        // script's own horizon and the script would be refused whole. The head
        // is covered by the timeout below until the lift puts the ending back.
        //
        // The base is the pose restated rather than a keep: a replacement
        // re-dispatches it from the composed setpoint, which is a no-op when
        // the head is already there and a re-plan from where it is when it is
        // not, and it is lawful from any phase the daemon might be in.
        if let (Some(running), Some(at)) = (p.running.clone(), p.want.at().cloned()) {
            p.refresh = Some(now.at + refresh);
            let steps = vec![at.step(0), Step::play(1, running.play)];
            // The window's own end is the last thing this timeline reaches, so
            // that is what the timeout has to cover — otherwise the edge
            // refuses a play running past the horizon.
            let timeout_ms = floor_ms.max((1 + running.span_ms).saturating_add(refresh_ms));
            return Some(self.rendered(
                pod,
                now,
                cause,
                change,
                clamped_from_ms,
                steps,
                timeout_ms,
            ));
        }
        let steps = match p.want.steps(now.at, &stow) {
            Some(steps) => {
                p.refresh = Some(now.at + refresh);
                steps
            }
            None => {
                match &p.want {
                    // A stow already due when the want was born: this publish is
                    // its first, so it is owed the second that every other
                    // closing instruction gets from the refresh cadence. One
                    // more refresh, and then quiet.
                    Want::Closing { .. } if change => {
                        p.want = Want::Stowing;
                        p.refresh = Some(now.at + refresh);
                    }
                    // A stow that has been standing at the daemon since before
                    // it came due, or the second half of the pair above: said
                    // once more and done with.
                    Want::Closing { .. } | Want::Stowing => {
                        p.want = Want::Quiet;
                        p.refresh = None;
                    }
                    // Nothing was wanted, so there is nothing to confirm.
                    Want::Quiet | Want::Hold { .. } => {
                        p.want = Want::Quiet;
                        p.refresh = None;
                        return None;
                    }
                }
                change = true;
                stow_terminal = true;
                vec![stow.step(0)]
            }
        };
        // The timeout is a ceiling on this script's own timeline, so it is the
        // configured bound or the timeline plus its headroom, whichever is
        // larger — a stow that outruns the bound takes the timeout with it
        // rather than being executed past a number the message contradicts. The
        // headroom is a refresh period, so a re-emission that lands late still
        // finds the previous script standing; on a timeline ending at the stow
        // it is the stow's own stated pace when that is longer, so the closing
        // move fits inside the timeout the daemon measures it against. A hold
        // script carries no stow, so it keeps the configured bound.
        let headroom_ms = if stow_terminal {
            stow_headroom_ms
        } else {
            refresh_ms
        };
        let last_step_ms = steps.last().map_or(0, |step| step.after_ms);
        let timeout_ms = floor_ms.max(last_step_ms.saturating_add(headroom_ms));
        Some(self.rendered(pod, now, cause, change, clamped_from_ms, steps, timeout_ms))
    }

    /// Number one decided timeline and wrap it as the publish it goes out as.
    #[allow(clippy::too_many_arguments)]
    fn rendered(
        &mut self,
        pod: &PodId,
        now: Now,
        cause: Cause,
        change: bool,
        clamped_from_ms: Option<u64>,
        steps: Vec<Step>,
        timeout_ms: u64,
    ) -> ScriptPublish {
        let seq = self.seq.next(now.unix_ms);
        // Every refusal is unreachable by construction: the steps ascend from
        // zero; the timeout exceeds the last step by the headroom, which is a
        // refresh period config validation keeps positive, or on a stow-terminal
        // timeline the stow's own stated pace when that is longer; and
        // `Scripter::closing` bounds the stow by that same stow-terminal
        // headroom so the sum stays inside `MAX_TIMEOUT_MS`, which config
        // validation also holds `max_engaged` under. The headroom covering the
        // stow pace is also what keeps the daemon's own room check for a
        // stow-terminal timeline — its last step plus the pace that step
        // states, against this timeout — satisfied for every script emitted
        // here. The two refusals a step
        // carries are screened at the same door: every pose name here is either
        // `STOW_POSE` or one config validation has already offered to this same
        // constructor, and every stated pace is one it has bounded. A cued pose
        // or play is screened at the same door one step earlier — its name
        // against the library and its speed against the wire's own range —
        // before it ever reaches a want, and a play always follows the base
        // step the render puts in front of it. The expect
        // states that rather than pushing an impossible error onto every caller.
        let script = MotionScript::new(pod.0.clone(), seq, steps, timeout_ms)
            .expect("the scripter's timelines ascend inside a timeout sized to cover them");
        ScriptPublish {
            pod: pod.clone(),
            script,
            cause,
            change,
            clamped_from_ms,
        }
    }

    /// Forget a pod with nothing standing. Its next raise starts from the same
    /// state a pod nobody has heard from is in, so the map holds live
    /// interactions only.
    fn tidy(&mut self, pod: &PodId) {
        if self.pods.get(pod).is_some_and(|p| p.want == Want::Quiet) {
            self.pods.remove(pod);
        }
    }

    /// What this pod is being asked for. A pod with no entry is being asked for
    /// nothing, which is exactly [`Want::Quiet`]: `tidy` turns the one state
    /// into the other, and no caller may tell them apart.
    fn want(&self, pod: &PodId) -> Want {
        self.pods.get(pod).map_or(Want::Quiet, |p| p.want.clone())
    }
}

/// Whole milliseconds from `from` to `to`, saturating at zero and at the
/// integer's own ceiling.
fn millis_between(from: Instant, to: Instant) -> u64 {
    millis(to.saturating_duration_since(from))
}

/// A duration as whole milliseconds, saturating rather than wrapping.
fn millis(span: Duration) -> u64 {
    u64::try_from(span.as_millis()).unwrap_or(u64::MAX)
}

/// Depth of the queue between the pipeline's taps and the script task.
///
/// Deeper than the notice queue because the taps are more numerous and a burst
/// is ordinary (a barge fans out playback events while a turn starts), but still
/// small: the scripter's work per input is a map lookup and some arithmetic, so
/// a backlog this deep means the task is not running at all, and the standing
/// script's own timeout already covers that.
pub const SCRIPT_QUEUE_DEPTH: usize = 32;

/// The tap end of the input queue. Cloned into every module that reports a
/// lifecycle point; sending never blocks and never fails a caller.
#[derive(Clone)]
pub struct ScriptHandle {
    tx: mpsc::Sender<ScriptInput>,
    dropped: Arc<AtomicU64>,
    jsonl: JsonlHandle,
}

impl ScriptHandle {
    /// Report one lifecycle point. A full or closed queue drops the input and
    /// says so: the pipeline's own progress is never held up by the head.
    pub fn send(&self, input: ScriptInput) {
        if let Err(err) = self.tx.try_send(input) {
            let (reason, input) = match err {
                mpsc::error::TrySendError::Full(input) => ("queue_full", input),
                mpsc::error::TrySendError::Closed(input) => ("scripter_gone", input),
            };
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            self.jsonl.emit(
                "script_input_dropped",
                &json!({ "pod": input.pod().0, "reason": reason, "dropped": dropped }),
            );
        }
    }

    /// How many inputs this handle's queue has lost, process-wide.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// The scripter's end of the input queue.
///
/// Minted with the sending end so the depth has one owner and the two cannot be
/// paired wrongly.
pub struct ScriptInbox {
    rx: mpsc::Receiver<ScriptInput>,
}

impl ScriptInbox {
    /// The next input, or `None` once every sending end is gone. For the tests
    /// that assert on what the taps sent, with no scripter in between.
    #[cfg(test)]
    pub(crate) async fn recv(&mut self) -> Option<ScriptInput> {
        self.rx.recv().await
    }

    /// What is queued right now, without waiting for more. For a test reading the
    /// taps partway through a run, where waiting would mean waiting for the input
    /// the case says has not been sent.
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Option<ScriptInput> {
        self.rx.try_recv().ok()
    }
}

/// Build the input queue.
#[must_use]
pub fn channel(jsonl: JsonlHandle) -> (ScriptHandle, ScriptInbox) {
    let (tx, rx) = mpsc::channel(SCRIPT_QUEUE_DEPTH);
    (
        ScriptHandle {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            jsonl,
        },
        ScriptInbox { rx },
    )
}

/// One decided script on its way out of the task.
///
/// The body is the encoded `MotionScript` — the same bytes the bus publish
/// carries — so a sink that hands it to a consumer in-process and a sink that
/// puts it on the wire are looking at the same thing. `pod` and `seq` are beside
/// it because every sink orders and reports by them, and re-parsing the body to
/// recover two numbers it already decided is what a sink should not have to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptOut {
    /// Which pod the timeline addresses.
    pub pod: String,
    /// The script's sequence number: rising per pod, and the number a consumer's
    /// own gate compares against.
    pub seq: u64,
    /// The encoded `MotionScript` JSON.
    pub body: String,
}

/// Where a decided script goes.
///
/// The default is [`BusSink`] — a publish on the presence channel — which is
/// what every pod deployment does. A deployment that consumes its own scripter's
/// emissions in-process injects its own sink instead and the decision half above
/// is untouched: nothing in `Scripter` knows which of the two it is feeding.
///
/// Nothing here may block or await. The task calling it owes the pipeline's taps
/// a queue it keeps draining, so a sink that needs to wait keeps its own queue
/// and its own task, the way [`BusSink`] does.
pub trait ScriptSink: Send + Sync + 'static {
    /// Start whatever carries scripts onward. Called once, inside the task's
    /// runtime, before the first [`ScriptSink::offer`]. A sink needing nothing
    /// started leaves it be.
    fn start(&self) {}

    /// Take one script, in the order the decision loop decided it.
    fn offer(&self, out: ScriptOut);

    /// No more scripts are coming. A sink holding anything sends it and ends.
    fn close(&self) {}
}

/// The script task: the scripter, its input queue, and the sink its decisions
/// leave through.
pub struct ScriptTask {
    core: Scripter,
    rx: mpsc::Receiver<ScriptInput>,
    sink: Arc<dyn ScriptSink>,
    jsonl: JsonlHandle,
}

/// How many times the publisher offers one script to the bus.
///
/// A refused publish is not repaired by the decision loop: a closing script is
/// re-emitted only while its stow instant is still ahead, and on the short-fuse
/// shapes — a silent turn, a stow a few hundred milliseconds out, a short answer
/// — that instant is behind the first refresh. Without a retry here one
/// transient refusal leaves the daemon holding the hold script and the head up
/// until that script's timeout. Bounded, because a bus that is down stays down
/// for longer than a retry: past these attempts the standing script's timeout is
/// the backstop, which is the case it exists for.
const PUBLISH_ATTEMPTS: u32 = 3;

/// How long the publisher waits before offering a refused script again. Long
/// enough for a rate limit to lapse, short enough that the stow it carries still
/// lands inside the tolerance the timeline is designed around.
const PUBLISH_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// One script on its way to the bus, with the numbers the log lines need kept
/// beside it (the request's body is encoded JSON text, not something a log line
/// should be re-parsing).
struct Outbound {
    request: PublishRequest,
    pod: String,
    seq: u64,
    /// How many times the bus has refused this script.
    attempts: u32,
}

/// What is waiting to go out: the newest script per pod, and whether the
/// decision loop has finished.
#[derive(Default)]
struct Pending {
    latest: HashMap<String, Outbound>,
    closed: bool,
}

/// The handover between the decision loop and the publisher: one slot per pod,
/// overwritten on arrival.
///
/// Not a queue, because a script wholly replaces its predecessor at the daemon.
/// A script still waiting behind a stalled bus has no value once a newer one for
/// the same pod is decided: publishing it spends a bus round trip on a timeline
/// that is replaced on arrival, and delays the one that matters. Keeping the
/// newest per pod means the freshest intent is never the one discarded, a
/// recovered bus carries one message per pod rather than a backlog of superseded
/// ones, and the cadence the design's growth path raises (streaming-TTS chunk
/// re-emissions, emote steps) costs the same one slot.
///
/// Per-pod ordering is preserved by construction — one entry per pod, and the
/// entry only ever moves forward in seq. A batch spanning pods is published in
/// the order the loop decided it, which is seq order.
struct Publisher {
    pending: Mutex<Pending>,
    wake: Notify,
}

impl Publisher {
    fn new() -> Self {
        Self {
            pending: Mutex::new(Pending::default()),
            wake: Notify::new(),
        }
    }

    /// Take the lock, or carry on with the poisoned state: `Pending` is a map
    /// and a flag, so a panic elsewhere cannot have left it half-updated.
    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Hand over one script, returning the script it superseded, if any.
    fn offer(&self, outgoing: Outbound) -> Option<Outbound> {
        let replaced = self.pending().latest.insert(outgoing.pod.clone(), outgoing);
        self.wake.notify_one();
        replaced
    }

    /// Offer a refused script again, unless the decision loop has left a newer
    /// one for that pod in its place: the newer timeline wholly replaces this one
    /// at the daemon, so spending the retry on the older one buys nothing and
    /// delays the one that matters.
    fn retry(&self, outgoing: Outbound) {
        {
            let mut pending = self.pending();
            if pending
                .latest
                .get(&outgoing.pod)
                .is_some_and(|newer| newer.seq > outgoing.seq)
            {
                return;
            }
            pending.latest.insert(outgoing.pod.clone(), outgoing);
        }
        self.wake.notify_one();
    }

    /// No more scripts are coming. The publisher ends once what it holds is out.
    fn close(&self) {
        self.pending().closed = true;
        self.wake.notify_one();
    }

    /// Everything waiting, oldest decision first, and whether the loop may stop
    /// once it is published.
    fn take(&self) -> (Vec<Outbound>, bool) {
        let mut pending = self.pending();
        let mut batch: Vec<Outbound> = pending.latest.drain().map(|(_, out)| out).collect();
        batch.sort_unstable_by_key(|out| out.seq);
        (batch, pending.closed)
    }
}

/// The default sink: one publish per script on the presence channel, ordered,
/// superseded per pod, and retried when the bus refuses.
///
/// Everything bus-shaped lives here rather than in the task — the ordering, the
/// per-pod slot, the retry, and the lines that report them — so a task feeding
/// some other sink carries none of it.
pub struct BusSink {
    handle: BridgeHandle,
    channel: String,
    attribution: Option<String>,
    jsonl: JsonlHandle,
    pending: Arc<Publisher>,
}

impl BusSink {
    /// Build the sink. `channel` is the caller's copy of
    /// `brenn.presence_channel`, which is also what decides whether the head is
    /// scripted at all — so it is passed rather than re-read here.
    #[must_use]
    pub fn new(
        channel: String,
        attribution: Option<String>,
        handle: BridgeHandle,
        jsonl: JsonlHandle,
    ) -> Self {
        Self {
            handle,
            channel,
            attribution,
            jsonl,
            pending: Arc::new(Publisher::new()),
        }
    }
}

impl ScriptSink for BusSink {
    /// Spawn the publisher.
    ///
    /// Not joined by anything: a peer that never answers must not hold up a
    /// teardown that the driver behind it is waiting on. It ends on its own once
    /// [`ScriptSink::close`] has been called and what it holds is out.
    fn start(&self) {
        drop(tokio::spawn(publish_loop(
            self.handle.clone(),
            Arc::clone(&self.pending),
            self.jsonl.clone(),
        )));
    }

    /// `Urgency::Normal`: a script moves a head, so it outranks the advisory
    /// wake nudge, but it is not conversation content and a lost one is repaired
    /// by the next re-emission.
    fn offer(&self, out: ScriptOut) {
        let request = PublishRequest {
            channel: self.channel.clone(),
            attribution: self.attribution.clone(),
            body: out.body,
            urgency: Urgency::Normal,
        };
        let seq = out.seq;
        let outgoing = Outbound {
            request,
            pod: out.pod,
            seq,
            attempts: 0,
        };
        // A script this one replaced before the bus took it. Not a failure: the
        // timeline that goes out is the newer one, which is what the daemon
        // would have been left with anyway. Said in the file so a capture can
        // tell a decided script that never reached the wire from one that did.
        if let Some(superseded) = self.pending.offer(outgoing) {
            self.jsonl.emit(
                "script_publish_superseded",
                &json!({
                    "channel": self.channel,
                    "pod": superseded.pod,
                    "seq": superseded.seq,
                    "by_seq": seq,
                }),
            );
        }
    }

    fn close(&self) {
        self.pending.close();
    }
}

impl ScriptTask {
    /// Assemble the task over the default bus sink.
    #[must_use]
    pub fn new(
        config: &BrennConfig,
        channel: String,
        handle: BridgeHandle,
        inbox: ScriptInbox,
        jsonl: JsonlHandle,
    ) -> Self {
        let sink = BusSink::new(channel, config.attribution.clone(), handle, jsonl.clone());
        Self::with_sink(config, Arc::new(sink), inbox, jsonl)
    }

    /// Assemble the task over a caller-supplied sink, for a deployment that
    /// consumes its own scripter's emissions instead of publishing them.
    #[must_use]
    pub fn with_sink(
        config: &BrennConfig,
        sink: Arc<dyn ScriptSink>,
        inbox: ScriptInbox,
        jsonl: JsonlHandle,
    ) -> Self {
        Self {
            core: Scripter::new(config.script_timing(), config.script_raises()),
            rx: inbox.rx,
            sink,
            jsonl,
        }
    }

    /// Script the head until the queue closes or `teardown` fires.
    ///
    /// No parting script on the way out. The standing one carries its own
    /// timeout, so the head comes down on the same mechanism that covers this
    /// host dying outright — and unlike a parting publish, it needs no bridge
    /// that is already being torn down.
    pub async fn run(mut self, teardown: CancellationToken) {
        self.sink.start();
        loop {
            let deadline = self.core.deadline();
            tokio::select! {
                // Teardown first: a burst of inputs must not starve a stop.
                // Inputs ahead of the timer, so a re-emission never gets in
                // front of a fact that would have changed what it says.
                biased;
                () = teardown.cancelled() => break,
                input = self.rx.recv() => match input {
                    Some(input) => {
                        // A cue that arrives with the head already at rest is
                        // dropped, and that is worth a line: the reply asked
                        // for a movement nobody saw. Every other input's
                        // outcome is legible from the scripts that follow it,
                        // so only this one is narrated here.
                        if let ScriptInput::Cues { pod, turn, cues } = &input
                            && !self.core.engaged(pod)
                        {
                            self.jsonl.emit(
                                "cue_ignored",
                                &json!({
                                    "pod": pod.0,
                                    "utterance": turn,
                                    "cues": cues.len(),
                                    "reason": "head_at_rest",
                                }),
                            );
                        }
                        if let Some(publish) = self.core.apply(input, Now::read()) {
                            self.publish(publish);
                        }
                    }
                    None => break,
                },
                () = due(deadline) => {
                    for publish in self.core.tick(Now::read()) {
                        self.publish(publish);
                    }
                }
            }
        }
        self.sink.close();
    }

    /// Hand one script to the sink, in the order this loop decided it.
    ///
    /// Never awaited: this loop owes the pipeline's taps a queue it keeps
    /// draining, and the sink's contract is that it does not block. Order is the
    /// loop's to keep, because a consumer drops a script numbered below the last
    /// it accepted — a re-emission overtaking the change behind it would silence
    /// that change for good rather than for a moment.
    fn publish(&self, publish: ScriptPublish) {
        let script = &publish.script;
        // Ahead of the script's own line, because it explains that line's
        // numbers: the stow it names is the ceiling's, not the horizon's.
        if let Some(requested_stow_ms) = publish.clamped_from_ms {
            self.jsonl.emit(
                "script_horizon_clamped",
                &json!({
                    "pod": script.pod(),
                    "seq": script.seq(),
                    "requested_stow_ms": requested_stow_ms,
                    "stow_ms": script.steps().last().map(|step| step.after_ms),
                    "ceiling_ms": MAX_TIMEOUT_MS,
                }),
            );
        }
        if publish.change {
            self.jsonl.emit(
                "motion_script",
                &json!({
                    "pod": script.pod(),
                    "seq": script.seq(),
                    "steps": steps_json(script),
                    "timeout_ms": script.timeout_ms(),
                    "cause": publish.cause.as_str(),
                }),
            );
        }
        self.sink.offer(ScriptOut {
            pod: script.pod().to_owned(),
            seq: script.seq(),
            body: script.encode(),
        });
    }
}

/// A script's timeline as JSONL fields: the wire's own shape, from the protocol
/// crate, so a capture on this side joins against the daemon's and neither has
/// a copy of the shape to keep current.
fn steps_json(script: &MotionScript) -> Vec<serde_json::Value> {
    script.steps().iter().map(Step::capture).collect()
}

/// Publish what the scripter decided, one at a time and in order, retrying what
/// the bus refuses.
///
/// One task rather than one per script: the order these reach the bridge is the
/// order the daemon sees them in, and a script numbered below the last accepted
/// one is dropped. Whatever a stalled bus superseded meanwhile is said by the
/// decision loop; the only failure reported from here is the peer's own.
///
/// The retry lives here because this is the only place that learns a script
/// never reached the daemon. The decision loop cannot repair it: the answer it
/// would re-emit goes quiet once the stow it carries is due.
async fn publish_loop(handle: BridgeHandle, outbound: Arc<Publisher>, jsonl: JsonlHandle) {
    loop {
        let (batch, closed) = outbound.take();
        if batch.is_empty() {
            if closed {
                break;
            }
            outbound.wake.notified().await;
            continue;
        }
        for mut outgoing in batch {
            let Err(detail) = publish_once(&handle, outgoing.request.clone()).await else {
                continue;
            };
            outgoing.attempts += 1;
            let again = outgoing.attempts < PUBLISH_ATTEMPTS;
            jsonl.emit(
                "brenn_script_publish_failed",
                &json!({
                    "channel": outgoing.request.channel,
                    "pod": outgoing.pod,
                    "seq": outgoing.seq,
                    "detail": detail,
                    "attempt": outgoing.attempts,
                    "retrying": again,
                }),
            );
            if again {
                sleep(PUBLISH_RETRY_BACKOFF).await;
                outbound.retry(outgoing);
            }
        }
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
    const MARGIN: Duration = Duration::from_millis(500);
    const ZERO: Duration = Duration::ZERO;

    /// The pose both presence events take when a configuration names neither,
    /// which is what every case below that is not about poses runs on: one
    /// place the head goes for every raise.
    const NEUTRAL: &str = DEFAULT_PRESENCE_POSE;
    /// The poses of a deployment that tells its two presence events apart.
    const PEEK: &str = "peek";

    fn timing() -> ScriptTiming {
        ScriptTiming {
            refresh: REFRESH,
            linger: LINGER,
            max_engaged: CEILING,
            stow_margin: MARGIN,
        }
    }

    /// The hold want a raise to `pose` at the library's own pace produces, for
    /// the cases that assert what a pod is being asked for.
    fn holding(pose: &str) -> Want {
        Want::Hold {
            at: Raise {
                pose: pose.into(),
                move_ms: None,
            },
        }
    }

    /// The same, at a pace the raise states rather than the library's.
    fn holding_at(pose: &str, move_ms: Option<u64>) -> Want {
        Want::Hold {
            at: Raise {
                pose: pose.into(),
                move_ms,
            },
        }
    }

    fn pod() -> PodId {
        PodId("pod-kitchen".into())
    }

    const TURN: UtteranceId = UtteranceId(7);

    /// A cmd accounting reading, spelled the way the tests below want to talk
    /// about it: how many cmds the turn sent, how many are still to start, and
    /// when the audio that did start ends.
    ///
    /// `cmds_sent` is the ledger's own field and is carried so a reading here
    /// reads like one from the ledger; no branch below turns on it — a silent
    /// turn is one with no horizon, not one with no cmds.
    fn audio(
        dispatch_done: bool,
        cmds_sent: u64,
        awaiting_start: u64,
        horizon: Option<Instant>,
    ) -> TurnAudio {
        TurnAudio {
            dispatch_done,
            // The scripter's ending does not turn on the capture window; the
            // `<listen/>` linger it already keeps for an open turn is the
            // `TurnEnded` disposition's business.
            listen_open: false,
            cmds_sent,
            awaiting_start,
            horizon,
        }
    }

    /// A scripter, the pod every test drives, and the instant its clock starts
    /// from. `now` is supplied rather than read, so every assertion is about the
    /// decision and never about a timer racing one.
    struct Fx {
        scripter: Scripter,
        t0: Instant,
    }

    fn fixture() -> Fx {
        fixture_with(timing())
    }

    /// A scripter whose wake and turn poses differ, as the acceptance
    /// deployment's do: peek to listen, neutral to answer.
    fn two_pose_fixture() -> Fx {
        Fx {
            scripter: Scripter::new(
                timing(),
                ScriptRaises {
                    wake: Raise::to(PEEK),
                    turn: Raise::to(NEUTRAL),
                    ..ScriptRaises::default()
                },
            ),
            t0: Instant::now(),
        }
    }

    /// The same, on timings a test chooses — for the ones about what the
    /// configuration's own admitted extremes do to the arithmetic.
    fn fixture_with(timing: ScriptTiming) -> Fx {
        Fx {
            scripter: Scripter::new(timing, ScriptRaises::default()),
            t0: Instant::now(),
        }
    }

    impl Fx {
        fn now(&self, offset: Duration) -> Now {
            Now {
                at: self.t0 + offset,
                // Wall-clock milliseconds move with the monotonic clock here;
                // the two only diverge under a clock step, which the seq
                // source's own tests cover.
                unix_ms: 1_786_543_210_123 + millis(offset),
            }
        }

        fn apply(&mut self, input: ScriptInput, offset: Duration) -> Option<ScriptPublish> {
            let now = self.now(offset);
            self.scripter.apply(input, now)
        }

        /// Apply an input that is expected to publish.
        fn publish(&mut self, input: ScriptInput, offset: Duration) -> ScriptPublish {
            self.apply(input, offset).expect("the input publishes")
        }

        fn tick(&mut self, offset: Duration) -> Vec<ScriptPublish> {
            let now = self.now(offset);
            self.scripter.tick(now)
        }

        /// Raise the head and start a turn, the opening every conversation has.
        fn wake_and_dispatch(&mut self, offset: Duration) {
            self.publish(ScriptInput::Wake(pod()), offset);
            self.apply(
                ScriptInput::TurnStarted {
                    pod: pod(),
                    turn: TURN,
                },
                offset,
            );
        }

        fn want(&self) -> Want {
            self.scripter.want(&pod())
        }
    }

    /// The pose a scripter-built step names. The scripter emits base steps and
    /// nothing else, so anything else here is a bug in the test rather than a
    /// case to handle.
    fn pose_of(step: &Step) -> &str {
        step.action
            .base()
            .and_then(motion_proto::Base::pose)
            .expect("the scripter emits base steps naming a pose")
    }

    /// The steps of a script, as (offset, pose) pairs.
    fn steps(publish: &ScriptPublish) -> Vec<(u64, &str)> {
        publish
            .script
            .steps()
            .iter()
            .map(|step| (step.after_ms, pose_of(step)))
            .collect()
    }

    /// The steps of a script, as (offset, pose, stated pace) triples, for the
    /// cases that are about the pace a step carries.
    fn paced(publish: &ScriptPublish) -> Vec<(u64, &str, Option<u64>)> {
        publish
            .script
            .steps()
            .iter()
            .map(|step| {
                let base = step.action.base().expect("a base step");
                (step.after_ms, pose_of(step), base.move_ms())
            })
            .collect()
    }

    /// The room the daemon measures a stow-terminal script by: its last step
    /// plus the pace that step states, against the timeout the script carries.
    ///
    /// Restated here because the compiler that applies it is in the other
    /// repository, behind a published pin: what this side can hold is the
    /// arithmetic, on the scripts it actually builds.
    fn stow_fits(publish: &ScriptPublish) -> bool {
        let last = publish.script.steps().last().expect("a step");
        let pace = last
            .action
            .base()
            .expect("a base step")
            .move_ms()
            .unwrap_or(0);
        last.after_ms + pace <= publish.script.timeout_ms()
    }

    /// The offset of a script's stow step.
    fn stow_ms(publish: &ScriptPublish) -> u64 {
        publish
            .script
            .steps()
            .iter()
            .find(|step| pose_of(step) == STOW_POSE)
            .expect("a closing script stows")
            .after_ms
    }

    /// A wake is a hold script — the head up now, under the engagement ceiling,
    /// with no ending scheduled because none is known.
    #[test]
    fn a_wake_holds_the_head_up_under_the_ceiling() {
        let mut fx = fixture();
        let publish = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(steps(&publish), vec![(0, NEUTRAL)]);
        assert_eq!(publish.script.pod(), "pod-kitchen");
        assert_eq!(publish.script.timeout_ms(), 30_000);
        assert_eq!(publish.cause, Cause::Wake);
        assert!(publish.change, "the head moved");
    }

    /// A barge and a dispatch raise on their own account, and the raise a pod
    /// already up does not publish: three raises inside one exchange are one
    /// hold script, and the refresh cadence is what carries it.
    #[test]
    fn a_barge_and_a_dispatch_raise_and_a_second_raise_says_nothing() {
        let mut fx = fixture();
        let barge = fx.publish(ScriptInput::Barge(pod()), ZERO);
        assert_eq!(barge.cause, Cause::Barge);
        assert_eq!(steps(&barge), vec![(0, NEUTRAL)]);
        assert!(
            fx.apply(
                ScriptInput::TurnStarted {
                    pod: pod(),
                    turn: TURN
                },
                ZERO
            )
            .is_none(),
            "the head is already up"
        );

        let mut fresh = fixture();
        let turn = fresh.publish(
            ScriptInput::TurnStarted {
                pod: pod(),
                turn: TURN,
            },
            ZERO,
        );
        assert_eq!(turn.cause, Cause::Turn);
        assert_eq!(steps(&turn), vec![(0, NEUTRAL)]);
    }

    /// The sequence a person sees on a deployment whose two presence events
    /// name different poses: peek to listen, neutral to answer, and the base
    /// retargeting mid-hold when the utterance is dispatched.
    #[test]
    fn a_wake_peeks_and_the_dispatched_utterance_answers() {
        let mut fx = two_pose_fixture();
        let wake = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(steps(&wake), vec![(0, PEEK)]);
        let turn = fx.publish(
            ScriptInput::TurnStarted {
                pod: pod(),
                turn: TURN,
            },
            ZERO,
        );
        assert_eq!(steps(&turn), vec![(0, NEUTRAL)], "the base retargets");
        assert_eq!(turn.cause, Cause::Turn);
        assert!(turn.change, "a hold whose pose changed is a new script");
    }

    /// Every listening event takes the wake pose, whatever the head was doing:
    /// a barge over live playback, and a second wake mid-conversation. Each is
    /// a change when the head was answering, and each is followed by the turn
    /// pose when an utterance is dispatched.
    #[test]
    fn a_barge_and_a_second_wake_both_peek_again() {
        for listening in [ScriptInput::Barge(pod()), ScriptInput::Wake(pod())] {
            let mut fx = two_pose_fixture();
            fx.wake_and_dispatch(ZERO);
            assert_eq!(fx.want(), holding(NEUTRAL), "answering");

            let again = fx.publish(listening.clone(), ZERO);
            assert_eq!(steps(&again), vec![(0, PEEK)], "{listening:?}");
            let turn = fx.publish(
                ScriptInput::TurnStarted {
                    pod: pod(),
                    turn: UtteranceId(8),
                },
                ZERO,
            );
            assert_eq!(steps(&turn), vec![(0, NEUTRAL)], "{listening:?}");
        }
    }

    /// A raise to the pose the head is already at says nothing: the same event
    /// always produces the same pose, so a wake over a peek is the standing
    /// script and the refresh cadence carries it.
    #[test]
    fn a_raise_to_the_standing_pose_publishes_nothing() {
        let mut fx = two_pose_fixture();
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert!(
            fx.apply(ScriptInput::Barge(pod()), ZERO).is_none(),
            "already peeking"
        );
        assert!(
            fx.apply(ScriptInput::Wake(pod()), ZERO).is_none(),
            "still peeking"
        );
    }

    /// A closing script holds the pose the head is at, and never retargets: an
    /// unanswered peek folds from the peek, and a closed turn from the pose the
    /// dispatch put the head at.
    #[test]
    fn a_closing_script_keeps_the_pose_the_hold_carried() {
        let mut fx = two_pose_fixture();
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        let unanswered = fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        assert_eq!(steps(&unanswered), vec![(0, PEEK), (8_000, STOW_POSE)]);

        let mut answered = two_pose_fixture();
        answered.wake_and_dispatch(ZERO);
        answered.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let closing = answered.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(answered.t0 + Duration::from_secs(6))),
            },
            ZERO,
        );
        assert_eq!(steps(&closing), vec![(0, NEUTRAL), (6_500, STOW_POSE)]);
    }

    /// A configured pace is stated on the wire per step, and each of the three
    /// reaches the step it paces. A pace nobody configured states nothing and
    /// leaves the move at the library's own.
    #[test]
    fn a_configured_pace_is_stated_on_the_step_it_paces() {
        let mut fx = Fx {
            scripter: Scripter::new(
                timing(),
                ScriptRaises {
                    wake: Raise {
                        pose: PEEK.into(),
                        move_ms: Some(600),
                    },
                    turn: Raise {
                        pose: NEUTRAL.into(),
                        move_ms: Some(900),
                    },
                    stow: Raise {
                        pose: STOW_POSE.into(),
                        move_ms: Some(1_500),
                    },
                },
            ),
            t0: Instant::now(),
        };
        let wake = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(paced(&wake), vec![(0, PEEK, Some(600))]);
        let turn = fx.publish(
            ScriptInput::TurnStarted {
                pod: pod(),
                turn: TURN,
            },
            ZERO,
        );
        assert_eq!(paced(&turn), vec![(0, NEUTRAL, Some(900))]);
        let closing = fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        assert_eq!(
            paced(&closing),
            vec![(0, NEUTRAL, Some(900)), (8_000, STOW_POSE, Some(1_500))],
            "the closing keeps the hold's pace and states the stow's"
        );

        let mut unpaced = fixture();
        let wake = unpaced.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(paced(&wake), vec![(0, NEUTRAL, None)], "the library's pace");
    }

    /// The confirming stow — the one the overdue branch emits — is the
    /// scripter's own stow step and carries the configured stow pace too.
    #[test]
    fn the_confirming_stow_states_the_configured_stow_pace() {
        let mut fx = Fx {
            scripter: Scripter::new(
                timing(),
                ScriptRaises {
                    stow: Raise {
                        pose: STOW_POSE.into(),
                        move_ms: Some(1_200),
                    },
                    ..ScriptRaises::default()
                },
            ),
            t0: Instant::now(),
        };
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        let confirming = fx.tick(LINGER);
        assert_eq!(
            confirming.iter().map(paced).collect::<Vec<_>>(),
            vec![vec![(0, STOW_POSE, Some(1_200))]]
        );
    }

    /// A raise that produced no turn comes down at the linger.
    #[test]
    fn an_unanswered_raise_stows_at_the_linger() {
        let mut fx = fixture();
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        let publish = fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        assert_eq!(steps(&publish), vec![(0, NEUTRAL), (8_000, STOW_POSE)]);
        assert_eq!(publish.cause, Cause::Unanswered);
        assert!(publish.change);
    }

    /// Speech heard inside the capture window keeps the head where the reply
    /// left it and moves its ending out to a linger from the moment it was
    /// heard. Nothing is raised and nothing retargets: the person is still
    /// talking to the same head.
    #[test]
    fn speech_inside_the_window_re_dates_the_stow_from_now() {
        let mut fx = two_pose_fixture();
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        fx.apply(
            ScriptInput::TurnStarted {
                pod: pod(),
                turn: TURN,
            },
            ZERO,
        );
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Open,
            },
            ZERO,
        );
        let closing = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
            },
            ZERO,
        );
        assert_eq!(
            steps(&closing),
            vec![(0, NEUTRAL), (10_000, STOW_POSE)],
            "the open turn's own linger, past its audio",
        );

        let heard = fx.publish(ScriptInput::Heard(pod()), Duration::from_secs(3));
        assert_eq!(
            steps(&heard),
            vec![(0, NEUTRAL), (8_000, STOW_POSE)],
            "a linger from the instant the speech was heard, off the same pose",
        );
        assert_eq!(heard.cause, Cause::Heard);
        assert!(heard.change);
    }

    /// The window's speech clears the finished turn, so a fact about that turn
    /// still in flight behind it cannot pull the ending back to the turn's own
    /// horizon.
    #[test]
    fn a_late_fact_about_the_cleared_turn_does_not_move_the_heard_stow() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Open,
            },
            ZERO,
        );
        fx.publish(ScriptInput::Heard(pod()), Duration::from_secs(1));
        let want = fx.want();
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_millis(1_500))),
                },
                Duration::from_secs(1),
            )
            .is_none(),
            "the turn was cleared with the speech that was heard",
        );
        assert_eq!(
            fx.want(),
            want,
            "and the ending stands where `Heard` put it"
        );
    }

    /// Two facts that mean the same thing about the same instant say it once.
    /// A declined follow-up is both — heard, then unanswered — and the head is
    /// not re-instructed for the second.
    #[test]
    fn heard_then_unanswered_at_one_instant_publishes_once() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(ScriptInput::Heard(pod()), Duration::from_secs(2));
        assert!(
            fx.apply(ScriptInput::Unanswered(pod()), Duration::from_secs(2))
                .is_none(),
            "the same closing, already standing",
        );
        let later = fx.publish(ScriptInput::Unanswered(pod()), Duration::from_secs(4));
        assert_eq!(
            steps(&later),
            vec![(0, NEUTRAL), (8_000, STOW_POSE)],
            "a decline that lands later does move the ending out",
        );
    }

    /// Content never raises a head that is down or on its way down: a window's
    /// speech arriving after the stow was decided is refused, exactly as an
    /// unanswered raise is.
    #[test]
    fn heard_for_a_head_at_rest_is_refused() {
        let mut fx = fixture();
        assert!(
            fx.apply(ScriptInput::Heard(pod()), ZERO).is_none(),
            "no head is up",
        );

        let mut stowing = fixture();
        stowing.wake_and_dispatch(ZERO);
        stowing.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        // A turn whose stow was already due when its facts settled: the want is
        // armed for one confirming re-send and nothing else.
        stowing.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(stowing.t0)),
            },
            Duration::from_secs(2),
        );
        assert_eq!(stowing.want(), Want::Stowing);
        assert!(
            stowing
                .apply(ScriptInput::Heard(pod()), Duration::from_secs(2))
                .is_none(),
            "a stow in front of the daemon is not reopened by content",
        );
    }

    /// The nominal turn: 6.24 s of speech starts, and one message carries the
    /// whole timeline — up now, down at 6.74 s.
    #[test]
    fn a_closed_turn_schedules_its_stow_a_margin_past_the_audio() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        let speech = Duration::from_millis(6_240);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + speech)),
            },
            ZERO,
        );
        assert_eq!(steps(&publish), vec![(0, NEUTRAL), (6_740, STOW_POSE)]);
        assert_eq!(publish.cause, Cause::Closing);
        assert_eq!(publish.script.timeout_ms(), 30_000);
    }

    /// The invariant behind the closing path, on the default the product runs:
    /// a stow is never scheduled earlier than the far-end's estimated end plus
    /// the margin, and a re-emission does not walk it forward.
    ///
    /// A stow that lands inside the audio moves the head while the reply is
    /// still leaving the speaker, which is the one motion-under-far-end shape
    /// production could produce on its own.
    #[test]
    fn no_closing_script_stows_before_the_estimated_end_plus_the_margin() {
        let default = ScriptTiming::default();
        assert_eq!(
            default.stow_margin,
            Duration::from_millis(500),
            "the measured margin is what ships"
        );
        let mut fx = fixture_with(default);
        fx.wake_and_dispatch(ZERO);

        // The reply is playing and its audio is estimated to end three seconds
        // in; the brain's turn closes while it still plays.
        let speech = Duration::from_secs(3);
        let horizon = fx.t0 + speech;
        fx.apply(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(horizon)),
            },
            Duration::from_millis(200),
        );
        let closed = Duration::from_millis(400);
        let publish = fx.publish(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            closed,
        );
        assert_eq!(
            stow_ms(&publish),
            millis(speech + default.stow_margin) - millis(closed),
            "the stow is dated from the audio's end, not from the decision"
        );

        // The confirming re-emission says `stow@0`, and it falls after the
        // audio's end plus the margin rather than inside the reply.
        let refresh_at = closed + default.refresh;
        assert!(refresh_at >= speech + default.stow_margin);
        let refreshed = fx.tick(refresh_at);
        assert_eq!(refreshed.len(), 1);
        assert_eq!(stow_ms(&refreshed[0]), 0);
    }

    /// The shipped presence settings and the compiled-in ones agree: a
    /// `[brenn]` table that names none of the presence keys measures the head
    /// exactly as [`ScriptTiming::default`] does and puts it exactly where
    /// [`ScriptRaises::default`] does.
    #[test]
    fn the_default_timing_is_the_configured_default() {
        let brenn: BrennConfig = toml::from_str(
            "publish_channel = \"brenn:pod.utterance\"\n\
             response_channel = \"brenn:pod.speak\"\n\
             [bridge]\n\
             server_url = \"wss://peer.example.net/remote/pod-kitchen/ws\"\n\
             token_file = \"/nonexistent/pod.token\"\n",
        )
        .expect("a [brenn] table naming none of the presence keys");
        assert_eq!(brenn.script_timing(), ScriptTiming::default());
        assert_eq!(brenn.script_raises(), ScriptRaises::default());
        assert_eq!(
            ScriptRaises::default().wake,
            ScriptRaises::default().turn,
            "a deployment that names no pose does not tell its two events apart"
        );
    }

    /// A turn that asked to keep listening holds the `<listen/>` window open
    /// instead of the margin.
    #[test]
    fn an_open_turn_stows_a_linger_past_the_audio() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
            },
            ZERO,
        );
        let publish = fx.publish(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Open,
            },
            ZERO,
        );
        assert_eq!(stow_ms(&publish), 10_000, "2 s of audio plus an 8 s linger");
    }

    /// The trigger ANDs three facts and does not care which order they land in.
    /// Both orders reach the same script.
    #[test]
    fn the_closing_script_waits_for_all_three_facts_in_either_order() {
        let horizon = Duration::from_secs(3);
        for end_first in [true, false] {
            let mut fx = fixture();
            fx.wake_and_dispatch(ZERO);
            let ended = ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            };
            // Dispatch has returned and one cmd is still waiting to start: two
            // of the three facts, and no ending yet.
            let waiting = ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 2, 1, Some(fx.t0 + horizon)),
            };
            let started = ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 2, 0, Some(fx.t0 + horizon)),
            };
            if end_first {
                assert!(fx.apply(ended.clone(), ZERO).is_none());
                assert!(fx.apply(waiting, ZERO).is_none(), "a cmd has not started");
                let publish = fx.publish(started, ZERO);
                assert_eq!(stow_ms(&publish), 3_500);
            } else {
                assert!(fx.apply(waiting, ZERO).is_none());
                assert!(fx.apply(started, ZERO).is_none(), "the brain has not said");
                let publish = fx.publish(ended, ZERO);
                assert_eq!(stow_ms(&publish), 3_500);
            }
            assert_eq!(
                fx.want(),
                Want::Closing {
                    at: Raise {
                        pose: NEUTRAL.into(),
                        move_ms: None,
                    },
                    stow_at: fx.t0 + horizon + MARGIN
                }
            );
        }
    }

    /// Dispatch that has not returned is the fact still missing, whatever the
    /// playback side says.
    #[test]
    fn a_turn_whose_dispatch_has_not_returned_does_not_close() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(false, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
                },
                ZERO,
            )
            .is_none()
        );
        assert_eq!(fx.want(), holding(NEUTRAL));
    }

    /// A cmd that dies in synthesis resolves without ever starting, and that
    /// resolution is what stops the turn waiting on it.
    #[test]
    fn a_cmd_that_never_played_stops_being_awaited() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 1, 1, None),
                },
                ZERO,
            )
            .is_none(),
            "the cmd is still awaited"
        );
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, None),
            },
            ZERO,
        );
        assert_eq!(
            stow_ms(&publish),
            500,
            "nothing played, so the margin alone"
        );
    }

    /// The brain answered with nothing to say. The head comes down promptly
    /// rather than waiting out a window for speech that is not coming.
    #[test]
    fn a_silent_turn_stows_at_the_margin() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 0, 0, None),
            },
            ZERO,
        );
        assert_eq!(stow_ms(&publish), 500);
        // Further facts about a turn with no audio do not slide the ending
        // forward; the instant it was scheduled at stands.
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 0, 0, None),
                },
                Duration::from_millis(100),
            )
            .is_none()
        );
    }

    /// A continuation clip starts after the closing script went out: the horizon
    /// moves, and the replacement script — higher seq, whole replacement — moves
    /// the stow with it.
    #[test]
    fn a_continuation_moves_the_stow_later_under_a_higher_seq() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let first = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 2, 0, Some(fx.t0 + Duration::from_secs(3))),
            },
            ZERO,
        );
        let second = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 2, 0, Some(fx.t0 + Duration::from_secs(9))),
            },
            Duration::from_secs(3),
        );
        assert_eq!(stow_ms(&first), 3_500);
        assert_eq!(stow_ms(&second), 6_500, "9 s of audio, 3 s in, plus margin");
        assert!(second.script.seq() > first.script.seq());
        assert!(second.change);
    }

    /// The re-emission is idempotent in effect: the same absolute stow instant,
    /// a smaller offset because time has passed, and a higher seq each time.
    #[test]
    fn a_closing_script_is_re_emitted_at_the_same_absolute_stow_instant() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let first = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(20))),
            },
            ZERO,
        );
        assert_eq!(stow_ms(&first), 20_500);
        assert!(fx.tick(Duration::from_secs(4)).is_empty(), "not due yet");
        let again = fx.tick(REFRESH);
        assert_eq!(again.len(), 1);
        assert_eq!(stow_ms(&again[0]), 15_500, "the same instant, 5 s later");
        assert_eq!(again[0].cause, Cause::Refresh);
        assert!(!again[0].change, "the head is not being asked anything new");
        assert!(again[0].script.seq() > first.script.seq());
    }

    /// Past the stow instant the scripter says the stow once more and then goes
    /// quiet.
    ///
    /// The confirming re-send is the whole repair for a short turn: this stow
    /// was due at 2.5 s, inside the first refresh, so the closing script that
    /// carried it was sent exactly once and a bus that dropped it would have
    /// left the head up until the standing script timed out. The re-send is
    /// narrated, because it changes what stands at the daemon.
    #[test]
    fn the_scripter_confirms_the_stow_once_and_then_goes_quiet() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
            },
            ZERO,
        );
        // 2.5 s is the stow instant; the refresh at 5 s is the first tick past
        // it.
        let repair = fx.tick(REFRESH);
        assert_eq!(repair.len(), 1, "the stow is confirmed: {repair:?}");
        assert_eq!(steps(&repair[0]), vec![(0, STOW_POSE)]);
        assert_eq!(repair[0].cause, Cause::Refresh);
        assert!(
            repair[0].change,
            "it puts a stow in front of a daemon that may never have got one"
        );
        assert_eq!(repair[0].script.timeout_ms(), 30_000);
        assert!(fx.tick(REFRESH * 2).is_empty(), "and then nothing further");
        assert_eq!(fx.want(), Want::Quiet);
        assert_eq!(fx.scripter.deadline(), None);
        // And a late fact about that turn does not raise a head that is down.
        // What refuses it is the turn check: a pod with nothing standing is no
        // longer in the map, so there is no turn for the fact to match.
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(9))),
                },
                Duration::from_secs(6),
            )
            .is_none()
        );
    }

    /// A hold script is re-emitted for as long as the timeline is unknown, so a
    /// long think never crosses the timeout it is holding under.
    #[test]
    fn a_hold_script_is_re_emitted_while_the_timeline_is_unknown() {
        let mut fx = fixture();
        let first = fx.publish(ScriptInput::Wake(pod()), ZERO);
        let mut last = first.script.seq();
        for round in 1..=4 {
            let out = fx.tick(REFRESH * round);
            assert_eq!(out.len(), 1, "round {round}");
            assert_eq!(steps(&out[0]), vec![(0, NEUTRAL)]);
            assert_eq!(out[0].cause, Cause::Refresh);
            assert!(out[0].script.seq() > last);
            last = out[0].script.seq();
        }
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + REFRESH * 5),
            "always one refresh ahead"
        );
    }

    /// The barged turn's dispatch still returns and still reports, after the
    /// barge has already put the head back up. Those facts name a turn the
    /// scripter has stopped tracking, so they close nothing.
    #[test]
    fn a_barged_turns_facts_do_not_close_the_interaction_that_replaced_it() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(ScriptInput::Barge(pod()), Duration::from_secs(1));
        for input in [
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, None),
            },
        ] {
            assert!(
                fx.apply(input, Duration::from_secs(1)).is_none(),
                "the turn was cut"
            );
        }
        assert_eq!(
            fx.want(),
            holding(NEUTRAL),
            "the barge's hold script governs"
        );
    }

    /// A wake starting a fresh interaction does the same for the previous turn's
    /// facts: a stale ending must not schedule a stow over a live raise.
    #[test]
    fn a_new_interaction_forgets_the_previous_turns_facts() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
            },
            ZERO,
        );
        let raise = fx.publish(ScriptInput::Wake(pod()), Duration::from_secs(1));
        assert_eq!(steps(&raise), vec![(0, NEUTRAL)], "back to a hold");
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
                },
                Duration::from_secs(1),
            )
            .is_none()
        );
        assert_eq!(fx.want(), holding(NEUTRAL));
    }

    /// A second utterance in the same exchange — the `<listen/>` case, with no
    /// wake word in front of it — starts its own turn. The previous turn's
    /// disposition is not evidence about this one, and pairing it with this
    /// turn's accounting would stow the head over a live answer.
    #[test]
    fn a_second_turn_does_not_inherit_the_first_turns_disposition() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Open,
            },
            ZERO,
        );
        let next = UtteranceId(TURN.0 + 1);
        assert!(
            fx.apply(
                ScriptInput::TurnStarted {
                    pod: pod(),
                    turn: next
                },
                Duration::from_secs(1),
            )
            .is_none(),
            "the head is already up"
        );
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: next,
                    audio: audio(true, 0, 0, None),
                },
                Duration::from_secs(1),
            )
            .is_none(),
            "this turn's brain has not said how it ends"
        );
        assert_eq!(fx.want(), holding(NEUTRAL));
    }

    /// Two pods are two interactions. Each carries its own timeline, and the
    /// seq numbers still climb across both because one source mints them.
    ///
    /// Their refresh instants are their own too, and the deadline is the
    /// earliest of them: a pod whose hold script is due in a second must not
    /// wait behind a pod due in twenty-five, or its script lapses at the daemon
    /// and the head comes down mid-think.
    #[test]
    fn each_pod_carries_its_own_script() {
        let other = PodId("pod-study".into());
        let mut fx = fixture();
        let first = fx.publish(ScriptInput::Wake(pod()), ZERO);
        let second = fx.publish(ScriptInput::Wake(other.clone()), Duration::from_secs(1));
        assert_eq!(first.script.pod(), "pod-kitchen");
        assert_eq!(second.script.pod(), "pod-study");
        assert!(second.script.seq() > first.script.seq());

        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + REFRESH),
            "the earlier of the two, which is the kitchen's"
        );
        let due = fx.tick(REFRESH);
        assert_eq!(due.len(), 1, "one pod is due: {due:?}");
        assert_eq!(due[0].pod, pod());
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + Duration::from_secs(1) + REFRESH),
            "and the study's is now the earliest"
        );
        let then = fx.tick(Duration::from_secs(1) + REFRESH);
        assert_eq!(then.len(), 1, "and it is served in its turn: {then:?}");
        assert_eq!(then[0].pod, other);

        fx.publish(ScriptInput::Unanswered(pod()), Duration::from_secs(6));
        assert_eq!(fx.scripter.want(&other), holding(NEUTRAL), "untouched");
    }

    /// The three closing facts can complete after the stow they schedule was
    /// already due — a cmd that dies in synthesis and settles ten seconds after
    /// the one second of audio the turn actually played. The ending goes out at
    /// once.
    ///
    /// What is standing at the daemon at that moment is the *hold* script, which
    /// has no stow in it and whose timeout the refresh keeps re-arming. Saying
    /// nothing here is the head waiting out that timeout — the very defect the
    /// scheduled stow exists to end, in the cases where the tail is slow.
    ///
    /// And it goes out twice. This is the shape whose stow was already due when
    /// it was decided, so the refresh cadence never carried it: without the
    /// confirming re-send below, one lost message would leave this head up until
    /// the hold script's own timeout, which is the case the re-send exists for.
    #[test]
    fn facts_completing_after_the_stow_was_due_publish_the_stow_at_once() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        // One second of audio played; a second cmd is still to start.
        let horizon = fx.t0 + Duration::from_secs(1);
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 2, 1, Some(horizon)),
                },
                ZERO,
            )
            .is_none(),
            "a cmd is still awaited"
        );

        // It dies in synthesis and only settles at ten seconds, long past the
        // 1.5 s stow the horizon and the margin name.
        let late = Duration::from_secs(10);
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 2, 0, Some(horizon)),
            },
            late,
        );
        assert_eq!(steps(&publish), vec![(0, STOW_POSE)]);
        assert_eq!(publish.cause, Cause::Closing);
        assert!(publish.change);

        // One confirmation is owed, because that publish was this instruction's
        // first and only send.
        assert_eq!(fx.want(), Want::Stowing);
        assert_eq!(fx.scripter.deadline(), Some(fx.t0 + late + REFRESH));
        let again = fx.tick(late + REFRESH);
        assert_eq!(again.len(), 1, "the stow was said once only: {again:?}");
        assert_eq!(steps(&again[0]), vec![(0, STOW_POSE)]);
        assert!(
            again[0].change,
            "a stow the daemon may not have is a change"
        );
        assert!(
            again[0].script.seq() > publish.script.seq(),
            "the confirmation must replace the first at the daemon"
        );

        // And then there is nothing further to say: the stow is on the wire.
        assert_eq!(fx.want(), Want::Quiet);
        assert_eq!(fx.scripter.deadline(), None);
        assert!(fx.tick(late + REFRESH + REFRESH).is_empty());
    }

    /// A wake between the stow and its confirmation cancels the confirmation:
    /// the pod is being asked for the head up, and a stow behind that would put
    /// it straight back down.
    #[test]
    fn a_wake_before_the_confirming_stow_replaces_it() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let horizon = fx.t0 + Duration::from_secs(1);
        let late = Duration::from_secs(10);
        fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(horizon)),
            },
            late,
        );
        assert_eq!(fx.want(), Want::Stowing);

        let raised = fx.publish(ScriptInput::Wake(pod()), late + Duration::from_millis(1));

        assert_eq!(steps(&raised), vec![(0, NEUTRAL)]);
        assert_eq!(fx.want(), holding(NEUTRAL));
        assert!(
            fx.tick(late + REFRESH)
                .iter()
                .all(|out| steps(out) == vec![(0, NEUTRAL)]),
            "the head was put back down under a live interaction"
        );
    }

    /// An `Unanswered` for a pod whose head is already down schedules nothing.
    ///
    /// Its two tap sites can both fire about one raise — the confidence gate
    /// declines what was said, and the wake arm later expires — so a second one
    /// arrives after the first one's stow has run. Adopting it would raise the
    /// head for a full linger with no interaction behind it.
    #[test]
    fn an_unanswered_raise_for_a_head_that_is_down_says_nothing() {
        let mut fx = fixture();
        assert!(
            fx.apply(ScriptInput::Unanswered(pod()), ZERO).is_none(),
            "nobody raised this pod"
        );
        assert!(fx.scripter.pods.is_empty());

        fx.publish(ScriptInput::Wake(pod()), ZERO);
        fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        fx.tick(LINGER + REFRESH);
        assert_eq!(fx.want(), Want::Quiet, "the linger has run");
        assert!(
            fx.apply(ScriptInput::Unanswered(pod()), LINGER + REFRESH)
                .is_none(),
            "and the second one does not put it back up"
        );
        assert!(fx.scripter.pods.is_empty());
    }

    /// A turn whose speech outlasts `max_engaged` carries a timeout sized from
    /// its own timeline, because the timeout is a ceiling on that timeline.
    ///
    /// A 40 s read-aloud stows at 40.5 s under a stated timeout of 45.5 s — the
    /// stow plus one refresh period of headroom — rather than under a stated
    /// 30 s the head then sails past. What a reader of the message gets is the
    /// truth: this head is up for at most 45.5 s.
    #[test]
    fn a_turn_whose_speech_outlasts_the_ceiling_carries_a_timeout_that_covers_it() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(40))),
            },
            ZERO,
        );
        assert_eq!(steps(&publish), vec![(0, NEUTRAL), (40_500, STOW_POSE)]);
        assert_eq!(
            publish.script.timeout_ms(),
            45_500,
            "the timeline plus a refresh period, not the configured floor"
        );
        assert!(publish.clamped_from_ms.is_none(), "well inside the ceiling");
    }

    /// A stow paced past both the refresh period and the engagement ceiling
    /// still fits inside the timeout of every script that carries it.
    ///
    /// The daemon measures a stow-terminal timeline as its last step plus the
    /// pace that step states, and refuses a script whose timeout is shorter.
    /// `presence_stow_move_ms` is admitted up to the protocol's own ceiling, so
    /// the headroom the timeout keeps is the refresh period or that pace,
    /// whichever is longer — otherwise a configuration in the admitted range
    /// produces closings the daemon throws away and a head that only ever comes
    /// down on the timeout's compiled-in stow.
    ///
    /// The hold script in the same run carries no stow, so it keeps the
    /// configured engagement ceiling: the room a stow needs is not charged to a
    /// timeline without one, where it would only lengthen how long a head stays
    /// up after the host goes quiet.
    #[test]
    fn a_stow_paced_past_the_ceiling_still_fits_inside_its_own_timeout() {
        const PACE_MS: u64 = 45_000;
        let mut fx = Fx {
            scripter: Scripter::new(
                timing(),
                ScriptRaises {
                    stow: Raise {
                        pose: STOW_POSE.into(),
                        move_ms: Some(PACE_MS),
                    },
                    ..ScriptRaises::default()
                },
            ),
            t0: Instant::now(),
        };
        let hold = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(paced(&hold), vec![(0, NEUTRAL, None)]);
        assert_eq!(
            hold.script.timeout_ms(),
            millis(CEILING),
            "a hold script carries no stow, so the stow's pace is not its room",
        );
        fx.apply(
            ScriptInput::TurnStarted {
                pod: pod(),
                turn: TURN,
            },
            ZERO,
        );
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let closing = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(40))),
            },
            ZERO,
        );
        assert_eq!(
            paced(&closing),
            vec![(0, NEUTRAL, None), (40_500, STOW_POSE, Some(PACE_MS))],
        );
        assert_eq!(
            closing.script.timeout_ms(),
            40_500 + PACE_MS,
            "the timeline plus the pace its own last step states",
        );
        assert!(stow_fits(&closing), "{closing:?}");

        // The confirming stow, whose whole timeline is the paced move.
        let confirming = fx.tick(Duration::from_secs(41));
        let [confirming] = confirming.as_slice() else {
            panic!("the stow is confirmed once: {confirming:?}");
        };
        assert_eq!(paced(confirming), vec![(0, STOW_POSE, Some(PACE_MS))]);
        assert!(stow_fits(confirming), "{confirming:?}");
    }

    /// The ordinary turn is untouched by the sizing rule: its stow is well
    /// inside the configured ceiling, so the ceiling stands as the timeout and
    /// as the backstop past the stow.
    #[test]
    fn an_ordinary_turns_timeout_is_the_configured_ceiling() {
        let mut fx = fixture();
        let hold = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(hold.script.timeout_ms(), 30_000);

        let unanswered = fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        assert_eq!(stow_ms(&unanswered), 8_000);
        assert_eq!(unanswered.script.timeout_ms(), 30_000);
    }

    /// The seconds-for-milliseconds slip: a horizon eleven hours out, which is
    /// what a duration read on the wrong scale produces. The stow lands at the
    /// protocol's ceiling instead — minutes out, not hours — and the script
    /// says what it wanted.
    #[test]
    fn a_horizon_past_the_protocols_ceiling_stows_at_the_ceiling_and_says_so() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let slipped = Duration::from_secs(40_000);
        let publish = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + slipped)),
            },
            ZERO,
        );

        let headroom = MAX_TIMEOUT_MS - millis(REFRESH);
        assert_eq!(stow_ms(&publish), headroom);
        assert_eq!(publish.script.timeout_ms(), MAX_TIMEOUT_MS);
        assert_eq!(
            publish.clamped_from_ms,
            Some(millis(slipped) + millis(MARGIN)),
            "the instant it asked for, so the trace names the slip"
        );
    }

    /// A second fact about the same slipped turn does not walk the cut stow
    /// forward.
    ///
    /// The cut instant is the only one dated from the decision rather than from
    /// the audio, so re-deriving it on every further fact would push the head's
    /// exposure out by the inter-fact gap each time and emit a script — and a
    /// clamp trace — for every step of it. One decision, one instant, one line.
    #[test]
    fn a_further_fact_over_a_slipped_horizon_leaves_the_cut_stow_where_it_was() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let slipped = audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(40_000)));
        fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: slipped,
            },
            ZERO,
        );
        let cut = fx.want();

        // The same horizon, a second later — another clip of the same turn
        // reporting in.
        let again = fx.apply(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: slipped,
            },
            Duration::from_secs(1),
        );

        assert!(
            again.is_none(),
            "the ending was already scheduled, so nothing new is asked for: {again:?}"
        );
        assert_eq!(fx.want(), cut, "and the instant it was scheduled at stands");
    }

    /// Whatever the inputs, the script that comes out is one the daemon
    /// accepts. Construction already refuses an unlawful timeline, so a
    /// violation is a panic here; these assertions name which rule broke when
    /// it is the numbers rather than the construction that drifted.
    ///
    /// Over the configurations as well as the turns. The two knobs that feed
    /// the arithmetic — the refresh period and the engaged bound — are
    /// admitted by configuration validation anywhere up to the protocol
    /// ceiling, and both ends of that range are load-bearing: the ceiling cut is
    /// `MAX_TIMEOUT_MS − refresh`, which a refresh at the ceiling takes to
    /// nothing, and the timeout is the engaged bound or the timeline plus a
    /// refresh, whichever is larger. A pairing that broke the invariant would
    /// not be refused — it would panic the host's script task, taking presence
    /// down for every pod — so the sweep has to reach the corners validation
    /// tells an operator are fine.
    #[test]
    fn every_script_the_scripter_can_be_driven_to_emit_is_lawful() {
        let horizons = [
            None,
            Some(Duration::from_millis(1)),
            Some(Duration::from_secs(6)),
            Some(Duration::from_secs(45)),
            Some(Duration::from_secs(3_600)),
            Some(Duration::from_secs(40_000)),
        ];
        let ceiling = Duration::from_millis(MAX_TIMEOUT_MS);
        let tick = Duration::from_millis(1);
        let configs = [
            timing(),
            ScriptTiming {
                refresh: tick,
                max_engaged: ceiling,
                ..timing()
            },
            ScriptTiming {
                refresh: ceiling,
                max_engaged: ceiling,
                ..timing()
            },
            ScriptTiming {
                refresh: ceiling,
                max_engaged: tick,
                ..timing()
            },
        ];
        for timing in configs {
            for horizon in horizons {
                for end in [TurnEnd::Closed, TurnEnd::Open] {
                    let named = format!(
                        "refresh {:?}/engaged {:?}/{horizon:?}/{end:?}",
                        timing.refresh, timing.max_engaged
                    );
                    let mut fx = fixture_with(timing);
                    let mut seen = vec![fx.publish(ScriptInput::Wake(pod()), ZERO)];
                    fx.apply(
                        ScriptInput::TurnStarted {
                            pod: pod(),
                            turn: TURN,
                        },
                        ZERO,
                    );
                    fx.apply(
                        ScriptInput::TurnEnded {
                            pod: pod(),
                            turn: TURN,
                            end,
                        },
                        ZERO,
                    );
                    seen.extend(fx.apply(
                        ScriptInput::Audio {
                            pod: pod(),
                            turn: TURN,
                            audio: audio(true, 1, 0, horizon.map(|h| fx.t0 + h)),
                        },
                        ZERO,
                    ));
                    // Far enough past any stow this table can produce that the
                    // repair emit fires, and then its confirmation.
                    seen.extend(fx.tick(Duration::from_secs(1_000_000)));
                    seen.extend(fx.tick(Duration::from_secs(2_000_000)));

                    assert!(seen.len() >= 2, "{named}: only {} published", seen.len());
                    for publish in &seen {
                        let script = &publish.script;
                        let last = script.steps().last().map_or(0, |step| step.after_ms);
                        assert!(
                            last < script.timeout_ms(),
                            "{named}: {last} against {}",
                            script.timeout_ms()
                        );
                        assert!(
                            script.timeout_ms() <= MAX_TIMEOUT_MS,
                            "{named}: {}",
                            script.timeout_ms()
                        );
                    }
                }
            }
        }
    }

    /// A wake between the stow instant and the refresh takes the pod back to a
    /// hold, so the repair never fires under a live interaction: the want it
    /// would have confirmed is gone.
    #[test]
    fn a_wake_before_the_repair_suppresses_it() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(2))),
            },
            ZERO,
        );
        // The stow is due at 2.5 s; the wake lands at 3 s, before the refresh.
        let raise = fx.publish(ScriptInput::Wake(pod()), Duration::from_secs(3));
        assert_eq!(steps(&raise), vec![(0, NEUTRAL)]);

        let due = fx.tick(REFRESH + Duration::from_secs(3));
        assert_eq!(due.len(), 1, "the hold's own re-emission: {due:?}");
        assert_eq!(steps(&due[0]), vec![(0, NEUTRAL)], "no stow went out");
        assert_eq!(fx.want(), holding(NEUTRAL));
    }

    /// Sequence numbers are the wall clock, so a restarted scripter resumes
    /// above its own high-water mark with nothing persisted — and two emissions
    /// inside one millisecond still climb.
    #[test]
    fn sequence_numbers_are_wall_clock_and_strictly_increasing() {
        let mut fx = fixture();
        let first = fx.publish(ScriptInput::Wake(pod()), ZERO);
        assert_eq!(first.script.seq(), 1_786_543_210_123);
        let second = fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        assert_eq!(second.script.seq(), 1_786_543_210_124, "same millisecond");

        let mut restarted = Scripter::new(timing(), ScriptRaises::default());
        let after = restarted
            .apply(
                ScriptInput::Wake(pod()),
                Now {
                    at: fx.t0,
                    unix_ms: 1_786_543_215_000,
                },
            )
            .expect("a wake publishes");
        assert!(after.script.seq() > second.script.seq());
    }

    /// A pod with nothing standing is not held in the map. The next raise starts
    /// from the same state a pod nobody has heard from is in.
    #[test]
    fn a_quiet_pod_is_forgotten() {
        let mut fx = fixture();
        fx.publish(ScriptInput::Wake(pod()), ZERO);
        fx.publish(ScriptInput::Unanswered(pod()), ZERO);
        fx.tick(LINGER + REFRESH);
        assert!(fx.scripter.pods.is_empty(), "nothing standing anywhere");
        // A stray fact about a pod nobody has raised leaves no entry behind.
        assert!(
            fx.apply(
                ScriptInput::Audio {
                    pod: pod(),
                    turn: TURN,
                    audio: audio(true, 1, 0, None),
                },
                LINGER + REFRESH,
            )
            .is_none()
        );
        assert!(fx.scripter.pods.is_empty());
    }
    /// A `[brenn]` table with the head's channel configured, parsed the way an
    /// operator writes one. The intervals are the caller's: a task test that
    /// waits out a timer waits it out in real time, so the tests below wind
    /// them down to milliseconds rather than pausing a clock the scripted
    /// socket also drives.
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

    /// A script task over a scripted bus peer, plus the JSONL file its
    /// narration lands in.
    ///
    /// The taps and the log handle are held as options so [`TaskFx::stop_and_flush`]
    /// can close them: every clone has to be gone before the log's writer ends,
    /// and the file has to outlive that.
    struct TaskFx {
        handle: Option<ScriptHandle>,
        peers: std::collections::VecDeque<crate::brenn::scripted::Peer>,
        task: Option<tokio::task::JoinHandle<()>>,
        teardown: CancellationToken,
        path: std::path::PathBuf,
        _dir: tempfile::TempDir,
        jsonl: Option<JsonlHandle>,
        writer: Option<tokio::task::JoinHandle<()>>,
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
        // The bridge awaits its event channel, so an embedder that stops reading
        // back-pressures the socket. This task subscribes to nothing and has no
        // use for the events; something still has to drain them.
        tokio::spawn(async move { while events.recv().await.is_some() {} });
        let (handle, inbox) = channel(jsonl.clone());
        let task = ScriptTask::new(
            &config,
            config.presence_channel.clone().unwrap(),
            bus,
            inbox,
            jsonl.clone(),
        );
        let teardown = CancellationToken::new();
        let task = tokio::spawn(task.run(teardown.clone()));
        TaskFx {
            handle: Some(handle),
            peers,
            task: Some(task),
            teardown,
            path,
            _dir: dir,
            jsonl: Some(jsonl),
            writer: Some(writer),
        }
    }

    impl TaskFx {
        /// Report one input, the way a pipeline tap does.
        fn send(&self, input: ScriptInput) {
            self.handle.as_ref().expect("the taps are open").send(input);
        }

        /// Tell the task to stop and wait for it. Saying so twice is saying it
        /// once: a test that stops mid-way and flushes at the end does both.
        async fn stop(&mut self) {
            self.teardown.cancel();
            let Some(task) = self.task.take() else {
                return;
            };
            tokio::time::timeout(WAIT, task)
                .await
                .expect("the task stops when told to")
                .expect("the task does not panic");
        }

        /// Stop, then close the log and wait for its writer, so an assertion that
        /// some line is *absent* is reading everything that was ever written
        /// rather than whatever had reached the disk by then.
        ///
        /// Every publish must have been answered first: the publisher outlives
        /// the decision loop by design and holds a log handle until the bus lets
        /// it finish.
        async fn stop_and_flush(&mut self) {
            self.stop().await;
            self.handle.take();
            self.jsonl.take();
            let writer = self.writer.take().expect("the log is flushed once");
            tokio::time::timeout(WAIT, writer)
                .await
                .expect("the log's writer ends once every handle is dropped")
                .expect("the writer does not panic");
        }
    }

    /// The body of a `Publish` frame, decoded from the JSON text it carries.
    fn body_of(published: &serde_json::Value) -> serde_json::Value {
        serde_json::from_str(published["body"].as_str().expect("a string body"))
            .expect("the body is JSON")
    }

    /// The wiring end to end: a tap fires, and what reaches the bus is a motion
    /// script on the configured channel, addressed to the pod, with its timeline
    /// on the console's stream beside it.
    #[tokio::test]
    async fn a_hold_script_reaches_the_configured_channel() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::Wake(pod()));
        let published = peer.answer_publish("Ok").await;
        assert_eq!(published["channel"], "brenn:reachy.presence");
        assert_eq!(published["urgency"], "normal");
        assert_eq!(published["attribution"], "voice");
        let body = body_of(&published);
        assert_eq!(body["type"], "motion-script");
        assert_eq!(body["pod"], "pod-kitchen");
        assert_eq!(body["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));
        assert_eq!(body["timeout_ms"], 30_000);

        let line = expect_line(&fx.path, "motion_script").await;
        assert_eq!(line["pod"], "pod-kitchen");
        assert_eq!(line["cause"], "wake");
        assert_eq!(line["timeout_ms"], 30_000);
        assert_eq!(line["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));
        assert_eq!(line["seq"], body["seq"]);
        fx.stop().await;
    }

    /// The task's timer arm, end to end: nothing in the input queue, and the
    /// standing script goes out again anyway. Without it a hold script crosses
    /// its own timeout during a long think, and the head comes down mid-answer.
    #[tokio::test]
    async fn the_task_re_emits_the_standing_script_on_its_own_timer() {
        let mut fx = task_fixture_with(brenn_config_with(40, 60_000, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::Wake(pod()));
        let first = body_of(&peer.answer_publish("Ok").await);
        let again = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(again["steps"], first["steps"], "the same ask, said again");
        assert!(
            again["seq"].as_u64().unwrap() > first["seq"].as_u64().unwrap(),
            "{again} is not later than {first}"
        );

        // Only a change is narrated, so anything beyond that line is the
        // regression this test catches; without the wait the count can fail
        // on the writer's latency instead.
        expect_line(&fx.path, "motion_script").await;
        let changes: Vec<_> = lines(&fx.path)
            .into_iter()
            .filter(|line| line["event"] == "motion_script")
            .collect();
        assert_eq!(
            changes.len(),
            1,
            "a re-emission is the cadence talking, not the head moving: {changes:?}"
        );
        fx.stop().await;
    }

    /// The whole turn in one message, which is the point of the design: the
    /// facts land, and what goes out carries the raise and the stow together.
    #[tokio::test]
    async fn a_closed_turn_publishes_its_whole_timeline_at_once() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::TurnStarted {
            pod: pod(),
            turn: TURN,
        });
        let hold = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(hold["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));

        fx.send(ScriptInput::TurnEnded {
            pod: pod(),
            turn: TURN,
            end: TurnEnd::Closed,
        });
        fx.send(ScriptInput::Audio {
            pod: pod(),
            turn: TURN,
            audio: audio(true, 1, 0, Some(Instant::now() + Duration::from_secs(6))),
        });
        let closing = body_of(&peer.answer_publish("Ok").await);
        let steps = closing["steps"].as_array().expect("a timeline").clone();
        assert_eq!(steps.len(), 2, "{closing}");
        assert_eq!(steps[0], json!({ "after_ms": 0, "pose": NEUTRAL }));
        assert_eq!(steps[1]["pose"], "stow");
        let stow_ms = steps[1]["after_ms"].as_u64().expect("an offset");
        // Six seconds of audio plus the shipped 500 ms margin, less however long
        // the two facts took to cross the queue.
        assert!(
            (6_000..=6_500).contains(&stow_ms),
            "the stow is dated from the audio, not from the linger: {stow_ms}"
        );
        assert!(closing["seq"].as_u64().unwrap() > hold["seq"].as_u64().unwrap());

        let line = expect_line(&fx.path, "motion_script").await;
        assert_eq!(line["cause"], "turn", "the hold script's own line");
        // And the ending's own line beside it: the supervised console reads the
        // head's timeline off these, so a closing script that publishes without
        // narrating leaves an operator watching a head come down for no stated
        // reason.
        let narrated = expect_narrated(&fx.path, closing["seq"].as_u64().unwrap()).await;
        assert_eq!(narrated["cause"], "closing");
        assert_eq!(narrated["pod"], "pod-kitchen");
        assert_eq!(narrated["steps"], closing["steps"]);
        assert_eq!(narrated["timeout_ms"], closing["timeout_ms"]);
        fx.stop().await;
    }

    /// The confirming re-send, end to end: the stow instant passes, one
    /// `stow@0` reaches the bus, and it is narrated — a refresh re-emission is
    /// not, so an operator reading the file sees the head's closing instruction
    /// go out rather than a silent extra message.
    #[tokio::test]
    async fn the_confirming_stow_reaches_the_bus_and_is_narrated() {
        let mut fx = task_fixture_with(brenn_config_with(40, 60_000, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::TurnStarted {
            pod: pod(),
            turn: TURN,
        });
        let hold = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(hold["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));

        // A silent turn: the stow is the 500 ms margin alone, a dozen refresh
        // periods out, so the re-emissions walk it down to zero.
        fx.send(ScriptInput::TurnEnded {
            pod: pod(),
            turn: TURN,
            end: TurnEnd::Closed,
        });
        fx.send(ScriptInput::Audio {
            pod: pod(),
            turn: TURN,
            audio: audio(true, 0, 0, None),
        });

        let confirming = json!([{ "after_ms": 0, "pose": STOW_POSE }]);
        let mut walked = 0;
        let stow = loop {
            let body = body_of(&peer.answer_publish("Ok").await);
            if body["steps"] == confirming {
                break body;
            }
            walked += 1;
            assert!(walked < 60, "the stow never came down to zero: {body}");
        };

        let narrated = expect_narrated(&fx.path, stow["seq"].as_u64().unwrap()).await;
        assert_eq!(narrated["steps"], confirming);
        assert_eq!(narrated["cause"], "refresh", "the timer is what fired");
        fx.stop().await;
    }

    /// A horizon past the protocol's ceiling: the stow lands at the ceiling and
    /// the file carries the instant it asked for, which is the only trace of a
    /// duration read on the wrong scale.
    #[tokio::test]
    async fn a_clamped_horizon_is_narrated_with_what_it_asked_for() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::TurnStarted {
            pod: pod(),
            turn: TURN,
        });
        peer.answer_publish("Ok").await;

        let slipped = Duration::from_secs(40_000);
        fx.send(ScriptInput::TurnEnded {
            pod: pod(),
            turn: TURN,
            end: TurnEnd::Closed,
        });
        fx.send(ScriptInput::Audio {
            pod: pod(),
            turn: TURN,
            audio: audio(true, 1, 0, Some(Instant::now() + slipped)),
        });

        let closing = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(closing["timeout_ms"], MAX_TIMEOUT_MS);
        assert_eq!(closing["steps"][1]["after_ms"], MAX_TIMEOUT_MS - 5_000);

        let line = expect_line(&fx.path, "script_horizon_clamped").await;
        assert_eq!(line["pod"], "pod-kitchen");
        assert_eq!(line["seq"], closing["seq"]);
        assert_eq!(line["stow_ms"], MAX_TIMEOUT_MS - 5_000);
        assert_eq!(line["ceiling_ms"], MAX_TIMEOUT_MS);
        let requested = line["requested_stow_ms"].as_u64().expect("an offset");
        assert!(
            (40_000_000..=40_000_500).contains(&requested),
            "the horizon it was handed, plus the margin: {requested}"
        );
        fx.stop().await;
    }

    /// Wait until `count` lines naming `event` have been written. A count is the
    /// only way to wait for the *n*th of several identical-looking lines.
    async fn expect_lines(path: &std::path::Path, event: &str, count: usize) {
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            let seen = lines(path)
                .iter()
                .filter(|line| line["event"] == event)
                .count();
            if seen >= count {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{seen} {event} lines, wanted {count}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The `motion_script` line for one published script, waiting for it to be
    /// written. Keyed on the sequence number, which is what joins a line to the
    /// body that went out.
    async fn expect_narrated(path: &std::path::Path, seq: u64) -> serde_json::Value {
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            if let Some(line) = lines(path)
                .into_iter()
                .find(|line| line["event"] == "motion_script" && line["seq"] == seq)
            {
                return line;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no motion_script line for seq {seq}; got {:?}",
                lines(path)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The daemon drops a script numbered at or below the last it accepted, so
    /// the order these reach the bus is the order it applies them in. A
    /// re-emission overtaking the change behind it would silence that change for
    /// the life of the process, not for a moment.
    ///
    /// Driven through a batch: the bus stalls on the first publish while four
    /// pods are decided behind it, so what leaves the publisher afterwards is a
    /// drained map and not a chain of already-serialized publishes.
    #[tokio::test]
    async fn scripts_reach_the_bus_in_the_order_they_were_decided() {
        let mut fx = task_fixture_with(brenn_config_with(60_000, 30, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        // The peer sits on this one, so every decision behind it waits in the
        // publisher's map at once.
        fx.send(ScriptInput::Wake(pod()));
        let stalled = peer.expect_frame("Publish").await;
        let held = body_of(&stalled)["seq"].as_u64().expect("a seq");

        let waiting = ["pod-hall", "pod-attic", "pod-study", "pod-porch"];
        for name in waiting {
            fx.send(ScriptInput::Wake(PodId(name.into())));
        }
        // Every one of them is decided — and so offered, which the line the
        // decision precedes proves — before the bus is let go, so what the
        // publisher drains next is one batch of four and not four batches of one.
        expect_lines(&fx.path, "motion_script", waiting.len() + 1).await;
        peer.say(json!({
            "type": "PublishResult",
            "correlation": stalled["correlation"].clone(),
            "outcome": { "kind": "Ok" },
        }));

        let mut seqs = vec![held];
        let mut pods = Vec::new();
        for _ in waiting {
            let body = body_of(&peer.answer_publish("Ok").await);
            seqs.push(body["seq"].as_u64().expect("a seq"));
            pods.push(body["pod"].as_str().expect("a pod").to_owned());
        }
        assert_eq!(pods, waiting, "the batch went out in decision order");
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(seqs, sorted, "decision order is sequence order: {seqs:?}");
        fx.stop().await;
    }

    /// A bus that stops answering must not leave the head executing a timeline
    /// three decisions old. What waits behind the stalled publish is one slot
    /// per pod, so the script that goes out when the bus comes back is the
    /// newest — and the one it replaced is said in the file, not as a failure.
    ///
    /// The teardown comes first, before the bus is let go, because that is the
    /// contract the publisher outliving the decision loop exists for: the script
    /// most likely to be sitting in the slot at a shutdown is the last turn's
    /// closing one, and dropping it there would leave the head up until the
    /// daemon timed the standing script out.
    #[tokio::test]
    async fn a_stalled_bus_holds_the_newest_script_and_not_a_backlog() {
        let mut fx = task_fixture_with(brenn_config_with(60_000, 30, 60_000)).await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        // The first script reaches the socket and the peer sits on it: every
        // decision from here waits in the slot.
        fx.send(ScriptInput::Wake(pod()));
        let stalled = peer.expect_frame("Publish").await;
        let held = body_of(&stalled)["seq"].as_u64().expect("a seq");

        fx.send(ScriptInput::Unanswered(pod()));
        fx.send(ScriptInput::Wake(pod()));
        let superseded = expect_line(&fx.path, "script_publish_superseded").await;
        assert_eq!(superseded["pod"], "pod-kitchen");
        let dropped = superseded["seq"].as_u64().expect("a seq");
        let newest = superseded["by_seq"].as_u64().expect("a seq");
        assert!(held < dropped && dropped < newest, "{superseded}");

        // Told to stop with the newest decision still in the slot and the bus
        // still holding the publish in front of it.
        fx.stop().await;

        // What comes next is the newest decision — the raise — and not the
        // closing script it replaced, and it arrives after the decision loop
        // is already gone.
        peer.say(json!({
            "type": "PublishResult",
            "correlation": stalled["correlation"].clone(),
            "outcome": { "kind": "Ok" },
        }));
        let next = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(next["seq"].as_u64(), Some(newest));
        assert_eq!(next["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));

        fx.stop_and_flush().await;
        assert!(
            !lines(&fx.path)
                .iter()
                .any(|line| line["event"] == "brenn_script_publish_failed"),
            "a supersede is not a publish failure",
        );
    }

    /// A refused publish is reported and tried again: the peer's refusal is the
    /// one thing that keeps a decided script off the wire, and nothing else
    /// repairs it.
    #[tokio::test]
    async fn a_refused_publish_is_reported_and_tried_again() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::Wake(pod()));
        let refused = body_of(&peer.answer_publish("RateLimited").await);
        let line = expect_line(&fx.path, "brenn_script_publish_failed").await;
        assert_eq!(line["channel"], "brenn:reachy.presence");
        assert_eq!(line["pod"], "pod-kitchen");
        assert_eq!(line["detail"], "the peer rate-limited the publish");
        assert_eq!(line["attempt"], 1);
        assert_eq!(line["retrying"], true);

        // The same script, offered again, rather than a decision the head never
        // hears about.
        let again = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(again["seq"], refused["seq"]);
        assert_eq!(again["steps"], refused["steps"]);

        // Still deciding: a fresh pod raises and publishes as if nothing had
        // happened.
        fx.send(ScriptInput::Wake(PodId("pod-hall".into())));
        assert_eq!(body_of(&peer.answer_publish("Ok").await)["pod"], "pod-hall");
        fx.stop().await;
    }

    /// The shape the retry exists for: a closing script whose stow is due sooner
    /// than the refresh cadence. Nothing re-emits it — the standing answer goes
    /// quiet once the stow instant passes — so a refusal the publisher did not
    /// try again would leave the daemon holding the hold script and the head up
    /// for its whole timeout.
    #[tokio::test]
    async fn a_refused_closing_script_still_reaches_the_bus() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::TurnStarted {
            pod: pod(),
            turn: TURN,
        });
        let hold = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(hold["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));

        // A silent turn: nothing played, so the ending is the margin alone —
        // 500 ms, a tenth of the 5 s refresh.
        fx.send(ScriptInput::TurnEnded {
            pod: pod(),
            turn: TURN,
            end: TurnEnd::Closed,
        });
        fx.send(ScriptInput::Audio {
            pod: pod(),
            turn: TURN,
            audio: audio(true, 0, 0, None),
        });
        let closing = body_of(&peer.answer_publish("Failed").await);
        let stow = closing["steps"][1].clone();
        assert_eq!(stow["pose"], "stow");
        assert!(
            stow["after_ms"].as_u64().expect("an offset") <= 500,
            "the stow is due long before a refresh: {closing}"
        );

        let again = body_of(&peer.answer_publish("Ok").await);
        assert_eq!(
            again["seq"], closing["seq"],
            "the ending the peer refused is the one tried again"
        );
        assert_eq!(again["steps"], closing["steps"]);
        fx.stop().await;
    }

    /// The retry gives up rather than grinding: a peer refusing everything is a
    /// bus that is down, and what covers that is the standing script's own
    /// timeout at the daemon.
    #[tokio::test]
    async fn a_bus_that_refuses_everything_is_left_to_the_scripts_timeout() {
        let mut fx = task_fixture().await;
        let mut peer = fx.peers.pop_front().expect("the script opens a socket");
        peer.handshake().await;

        fx.send(ScriptInput::Wake(pod()));
        for _ in 0..PUBLISH_ATTEMPTS {
            peer.answer_publish("Failed").await;
        }
        let failures = expect_failures(&fx.path, PUBLISH_ATTEMPTS as usize).await;
        assert_eq!(failures.last().expect("a line")["retrying"], false);

        // And it stops there: a fresh decision is what next reaches the socket.
        fx.send(ScriptInput::Wake(PodId("pod-hall".into())));
        assert_eq!(body_of(&peer.answer_publish("Ok").await)["pod"], "pod-hall");
        fx.stop_and_flush().await;
        assert_eq!(
            lines(&fx.path)
                .iter()
                .filter(|line| line["event"] == "brenn_script_publish_failed")
                .count(),
            PUBLISH_ATTEMPTS as usize,
            "the attempts are bounded",
        );
    }

    /// Every publish-failure line so far, once `count` of them have been
    /// written.
    async fn expect_failures(path: &std::path::Path, count: usize) -> Vec<serde_json::Value> {
        expect_lines(path, "brenn_script_publish_failed", count).await;
        lines(path)
            .into_iter()
            .filter(|line| line["event"] == "brenn_script_publish_failed")
            .collect()
    }

    /// The taps never block the pipeline, so a wedged task loses inputs rather
    /// than the other way round — loudly, and counted. What bounds the damage is
    /// the standing script's own timeout, not anything recovered here.
    #[tokio::test]
    async fn a_full_queue_drops_the_newest_input_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(path.clone()))
            .await
            .unwrap();
        let (handle, inbox) = channel(jsonl.clone());

        for _ in 0..SCRIPT_QUEUE_DEPTH {
            handle.send(ScriptInput::Wake(pod()));
        }
        assert_eq!(handle.dropped(), 0, "the queue holds its stated depth");
        handle.send(ScriptInput::Wake(pod()));
        assert_eq!(handle.dropped(), 1);

        drop(inbox);
        handle.send(ScriptInput::Wake(pod()));
        assert_eq!(handle.dropped(), 2, "a departed scripter is not silence");

        drop(handle);
        drop(jsonl);
        writer.await.unwrap();
        let reasons: Vec<String> = lines(&path)
            .iter()
            .filter(|line| line["event"] == "script_input_dropped")
            .map(|line| line["reason"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(reasons, vec!["queue_full", "scripter_gone"]);
    }

    /// A sink that keeps what it was handed, so a test can read the emissions
    /// without a bus behind them.
    #[derive(Default)]
    struct Recorder {
        taken: Mutex<Vec<ScriptOut>>,
        started: AtomicU64,
        /// What `started` read when the first script arrived. Taken there rather
        /// than after the fact: a sink started only after it had already been
        /// handed work would still read 1 by the time a test looked.
        started_at_first_offer: Mutex<Option<u64>>,
        closed: AtomicU64,
    }

    impl Recorder {
        fn taken(&self) -> Vec<ScriptOut> {
            self.taken.lock().unwrap().clone()
        }
    }

    impl ScriptSink for Recorder {
        fn start(&self) {
            self.started.fetch_add(1, Ordering::Relaxed);
        }

        fn offer(&self, out: ScriptOut) {
            let mut taken = self.taken.lock().unwrap();
            if taken.is_empty() {
                *self.started_at_first_offer.lock().unwrap() =
                    Some(self.started.load(Ordering::Relaxed));
            }
            taken.push(out);
        }

        fn close(&self) {
            self.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A task over an injected sink and no bus at all: the seam a deployment
    /// consuming its own emissions in-process stands on.
    async fn sink_fixture(
        sink: Arc<Recorder>,
    ) -> (ScriptHandle, CancellationToken, tokio::task::JoinHandle<()>) {
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::None).await.unwrap();
        drop(writer);
        let config = brenn_config();
        let (handle, inbox) = channel(jsonl.clone());
        let task = ScriptTask::with_sink(&config, sink, inbox, jsonl);
        let teardown = CancellationToken::new();
        let join = tokio::spawn(task.run(teardown.clone()));
        (handle, teardown, join)
    }

    /// Everything the sink has taken, once it has taken `count` of them.
    async fn taken_at_least(sink: &Recorder, count: usize) -> Vec<ScriptOut> {
        tokio::time::timeout(WAIT, async {
            loop {
                let taken = sink.taken();
                if taken.len() >= count {
                    return taken;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sink is offered the decided scripts")
    }

    /// The injected sink is handed exactly what the bus publish would have
    /// carried: the encoded script, and the two numbers a consumer orders and
    /// screens by, agreeing with the body's own fields.
    #[tokio::test]
    async fn an_injected_sink_receives_what_the_publisher_would_have_sent() {
        let sink = Arc::new(Recorder::default());
        let (handle, teardown, join) = sink_fixture(sink.clone()).await;

        handle.send(ScriptInput::Wake(pod()));
        let out = taken_at_least(&sink, 1).await.remove(0);

        let body: serde_json::Value = serde_json::from_str(&out.body).expect("the body is JSON");
        assert_eq!(body["type"], "motion-script");
        assert_eq!(body["pod"], "pod-kitchen");
        assert_eq!(body["steps"], json!([{ "after_ms": 0, "pose": NEUTRAL }]));
        assert_eq!(body["timeout_ms"], 30_000);
        assert_eq!(out.pod, "pod-kitchen");
        assert_eq!(body["seq"].as_u64(), Some(out.seq));
        // The same text the wire would carry, not a re-render of it.
        assert_eq!(
            MotionScript::decode(&out.body)
                .expect("the sink's body decodes as the wire contract")
                .seq(),
            out.seq
        );

        assert_eq!(sink.started.load(Ordering::Relaxed), 1);
        assert_eq!(
            *sink.started_at_first_offer.lock().unwrap(),
            Some(1),
            "the sink is started before it is handed anything"
        );

        // The second decision of the same engagement: the raise that produced no
        // turn comes down at the linger. Two scripts is what makes the seam's
        // ordering observable — a consumer drops a script numbered below the last
        // it accepted, so a sink wiring that reordered would be silent damage.
        handle.send(ScriptInput::Unanswered(pod()));
        let taken = taken_at_least(&sink, 2).await;
        assert!(
            taken[0].seq < taken[1].seq,
            "the sink sees the decisions in the order they were decided: {taken:?}"
        );
        let closing: serde_json::Value =
            serde_json::from_str(&taken[1].body).expect("the body is JSON");
        assert_eq!(
            closing["steps"],
            json!([
                { "after_ms": 0, "pose": NEUTRAL },
                { "after_ms": 8_000, "pose": STOW_POSE },
            ]),
            "the second script is the close, not a re-render of the raise"
        );
        assert_eq!(closing["seq"].as_u64(), Some(taken[1].seq));

        teardown.cancel();
        tokio::time::timeout(WAIT, join)
            .await
            .expect("the task stops when told to")
            .expect("the task does not panic");
        assert_eq!(
            sink.closed.load(Ordering::Relaxed),
            1,
            "a stopped task tells its sink no more are coming"
        );
    }

    // --- cued poses and motions -------------------------------------------

    /// A pose a reply asked for, at the pace the tap computed for it.
    fn pose_cue(pose: &str, move_ms: Option<u64>) -> MotionCue {
        MotionCue::Pose(Raise {
            pose: pose.into(),
            move_ms,
        })
    }

    /// A motion a reply asked for, with the span the library gave it.
    fn motion_cue(name: &str, span_ms: u64) -> MotionCue {
        MotionCue::Motion {
            play: Play::new(name),
            span_ms,
        }
    }

    fn cued(cues: Vec<MotionCue>) -> ScriptInput {
        ScriptInput::Cues {
            pod: pod(),
            turn: TURN,
            cues,
        }
    }

    /// The steps of a script including its plays, which `steps` cannot render:
    /// a base step reads as its pose, a play as `play <name>`.
    fn timeline(publish: &ScriptPublish) -> Vec<(u64, String)> {
        publish
            .script
            .steps()
            .iter()
            .map(|step| {
                let label = match &step.action {
                    motion_proto::Action::Base(base) => base.pose().unwrap_or("keep").to_string(),
                    motion_proto::Action::Play(play) => format!("play {}", play.name),
                };
                (step.after_ms, label)
            })
            .collect()
    }

    /// A pose the reply named is the pose the hold is standing at, at the pace
    /// the cue states — and it goes out the moment it arrives, because a reply's
    /// movement is not something to hold until the next refresh.
    #[test]
    fn a_cued_pose_retargets_the_hold_and_is_said_at_once() {
        let mut fx = two_pose_fixture();
        fx.wake_and_dispatch(ZERO);
        let publish = fx.publish(
            cued(vec![pose_cue(PEEK, Some(400))]),
            Duration::from_secs(1),
        );
        assert_eq!(publish.cause, Cause::Cue);
        assert!(publish.change, "a movement the head makes is a change");
        assert_eq!(
            paced(&publish),
            vec![(0, PEEK, Some(400))],
            "the cued pose, at the pace the cue asked for"
        );
        assert_eq!(fx.want(), holding_at(PEEK, Some(400)));
    }

    /// A motion plays over the standing pose, and the script that carries it
    /// restates that pose as the base a play needs in front of it.
    #[test]
    fn a_cued_motion_plays_over_the_standing_pose() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        let publish = fx.publish(cued(vec![motion_cue("nod", 2_000)]), ZERO);
        assert_eq!(publish.cause, Cause::Cue);
        assert_eq!(
            timeline(&publish),
            vec![(0, NEUTRAL.to_string()), (1, "play nod".to_string())],
            "the pose restated as the base, then the play"
        );
        assert_eq!(
            fx.want(),
            holding(NEUTRAL),
            "a motion does not change where the head ends up"
        );
    }

    /// The timeout has to cover the play window, which is measured from the
    /// script's own arrival: a window reaching past the script's horizon is
    /// refused at the edge, and the whole script with it.
    #[test]
    fn a_long_motions_timeout_covers_its_whole_window() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        let short = fx.publish(cued(vec![motion_cue("nod", 2_000)]), ZERO);
        assert_eq!(
            short.script.timeout_ms(),
            millis(CEILING),
            "a window inside the engagement ceiling leaves the bound alone"
        );

        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        let long = fx.publish(cued(vec![motion_cue("long/one", 40_000)]), ZERO);
        assert_eq!(
            long.script.timeout_ms(),
            1 + 40_000 + millis(REFRESH),
            "the window's end plus a refresh, when that outruns the ceiling"
        );
    }

    /// Both kinds in one message: the pose the reply named becomes the base the
    /// motion plays over.
    #[test]
    fn a_pose_cued_while_a_motion_runs_becomes_the_plays_base() {
        let mut fx = two_pose_fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 4_000)]), ZERO);
        let publish = fx.publish(cued(vec![pose_cue(PEEK, None)]), Duration::from_millis(500));
        assert_eq!(
            timeline(&publish),
            vec![(0, PEEK.to_string()), (1, "play nod".to_string())],
            "the new base under the motion that is still running"
        );
    }

    /// The last of each kind wins: the user does not want a queue, and a reply
    /// that names two motions means the second.
    #[test]
    fn a_second_motion_replaces_the_first() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 10_000)]), ZERO);
        let publish = fx.publish(
            cued(vec![motion_cue("shake", 2_000)]),
            Duration::from_secs(1),
        );
        assert_eq!(
            timeline(&publish),
            vec![(0, NEUTRAL.to_string()), (1, "play shake".to_string())],
            "the replacement, alone: the daemon cuts a row nothing restates"
        );
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + Duration::from_secs(3)),
            "the second motion's own end, not the first's"
        );
    }

    /// Every script while a motion runs restates it, because a script wholly
    /// replaces its predecessor and an overlay the replacement does not name is
    /// cut on the daemon's next tick.
    #[test]
    fn a_refresh_while_a_motion_runs_restates_the_same_play() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 20_000)]), ZERO);
        let ticked = fx.tick(REFRESH);
        assert_eq!(
            ticked.len(),
            1,
            "the refresh says the standing script again"
        );
        assert_eq!(
            timeline(&ticked[0]),
            vec![(0, NEUTRAL.to_string()), (1, "play nod".to_string())],
            "the same play, so the daemon picks the row up where it stands"
        );
        assert_eq!(ticked[0].cause, Cause::Refresh);
        assert!(!ticked[0].change, "a re-emission is not a change");
    }

    /// The turn ends while the motion is still going: the ending is *not* in the
    /// script, because the window is measured from each script's arrival and a
    /// stow dated inside it would have the edge refuse the whole thing. The stow
    /// arrives at the lift instead — the motion is never cut for it.
    #[test]
    fn a_turn_ending_under_a_motion_waits_for_the_lift() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 3_000)]), ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        let closing = fx.publish(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(10))),
            },
            ZERO,
        );
        assert_eq!(
            timeline(&closing),
            vec![(0, NEUTRAL.to_string()), (1, "play nod".to_string())],
            "the ending is decided but not said while the motion runs"
        );

        let lifted = fx.tick(Duration::from_secs(3));
        assert_eq!(lifted.len(), 1, "the lift says the standing want plainly");
        assert_eq!(lifted[0].cause, Cause::MotionEnded);
        assert_eq!(
            steps(&lifted[0]),
            vec![(0, NEUTRAL), (7_500, STOW_POSE)],
            "the stow the motion held back, still dated from the audio"
        );
    }

    /// A stow that came due while the motion was playing is taken at the lift:
    /// the head waits for the motion and then comes down, rather than the stow
    /// cutting the motion short.
    #[test]
    fn a_stow_that_fell_due_under_a_motion_is_taken_at_the_lift() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 3_000)]), ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Closed,
            },
            ZERO,
        );
        fx.apply(
            ScriptInput::Audio {
                pod: pod(),
                turn: TURN,
                audio: audio(true, 1, 0, Some(fx.t0 + Duration::from_secs(1))),
            },
            ZERO,
        );
        let lifted = fx.tick(Duration::from_secs(3));
        assert_eq!(lifted.len(), 1);
        assert_eq!(
            steps(&lifted[0]),
            vec![(0, STOW_POSE)],
            "the confirming stow, immediately: its instant is behind the lift"
        );
        assert_eq!(fx.want(), Want::Stowing);
    }

    /// A raise is a person, and cutting a nod for a person is right.
    #[test]
    fn a_wake_cuts_a_running_motion() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 20_000)]), ZERO);
        let publish = fx.publish(ScriptInput::Wake(pod()), Duration::from_secs(1));
        assert_eq!(
            timeline(&publish),
            vec![(0, NEUTRAL.to_string())],
            "the raise alone: nothing restates the play, so the daemon cuts it"
        );
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + Duration::from_secs(1) + REFRESH),
            "no motion is left to lift"
        );
    }

    /// Speech inside the capture window re-dates the ending and leaves the nod
    /// alone: the head does not react to being listened to.
    #[test]
    fn heard_keeps_a_running_motion() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 20_000)]), ZERO);
        fx.apply(
            ScriptInput::TurnEnded {
                pod: pod(),
                turn: TURN,
                end: TurnEnd::Open,
            },
            ZERO,
        );
        let publish = fx.publish(ScriptInput::Heard(pod()), Duration::from_secs(1));
        assert_eq!(publish.cause, Cause::Heard);
        assert_eq!(
            timeline(&publish),
            vec![(0, NEUTRAL.to_string()), (1, "play nod".to_string())],
            "the play stands; the re-dated ending is not said until the lift"
        );
    }

    /// An unanswered raise ends what the reply was doing: nothing is coming of
    /// it, so the head settles rather than finishing a gesture at nobody.
    #[test]
    fn an_unanswered_raise_cuts_a_running_motion() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 20_000)]), ZERO);
        let publish = fx.publish(ScriptInput::Unanswered(pod()), Duration::from_secs(1));
        assert_eq!(
            steps(&publish),
            vec![(0, NEUTRAL), (millis(LINGER), STOW_POSE)],
            "a linger from now, with no play left in the script"
        );
    }

    /// Content never raises a head that is at rest: a cue that lost the race
    /// with the ending moves nothing.
    #[test]
    fn a_cue_with_no_head_up_moves_nothing() {
        let mut fx = fixture();
        assert!(
            fx.apply(cued(vec![motion_cue("nod", 2_000)]), ZERO)
                .is_none(),
            "a cue for a pod with nothing standing"
        );
        assert_eq!(fx.want(), Want::Quiet);
        assert!(
            !fx.scripter.engaged(&pod()),
            "and the pod is still one nobody has heard from"
        );
    }

    /// The lift cannot wait for the next refresh: the standing want has an
    /// ending in it that is not being said while the motion plays.
    #[test]
    fn a_running_motion_owns_the_deadline_when_it_ends_first() {
        let mut fx = fixture();
        fx.wake_and_dispatch(ZERO);
        fx.publish(cued(vec![motion_cue("nod", 2_000)]), ZERO);
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + Duration::from_secs(2)),
            "the motion's end, which is sooner than the refresh"
        );
        let lifted = fx.tick(Duration::from_secs(2));
        assert_eq!(lifted.len(), 1);
        assert_eq!(
            timeline(&lifted[0]),
            vec![(0, NEUTRAL.to_string())],
            "the hold, plainly, with the motion gone"
        );
        assert_eq!(
            fx.scripter.deadline(),
            Some(fx.t0 + Duration::from_secs(2) + REFRESH),
            "and the refresh cadence carries it from there"
        );
    }
}
