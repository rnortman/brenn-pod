//! The message body: a timed motion script, and how it survives the wire.
//!
//! JSON, because that is what a bus body is here, and with a `"type"` field
//! because this channel carries more than one kind of intent over time. A
//! consumer that filters on the discriminator keeps working when a second kind
//! arrives; one that assumed every body on the channel was a script would start
//! misreading them.
//!
//! A script is three things: whose head it is about, an ordering number, and a
//! timeline — zero or more steps at offsets from the moment the script
//! arrives, plus the timeout after which the head goes back down whether or not
//! anything else arrives.
//!
//! The timeline carries two kinds of step, and they are two timelines in one
//! list. A **base** step ([`Base`]) says where the head is going — a posture, or
//! `keep`, which is "hold the base where it is commanded now". A **play** step
//! ([`Play`]) starts an overlay: a named motion the daemon looks up in its own
//! library and layers on top of whatever the base is doing, at a speed the
//! caller picks. The base collapses to the last due step ([`base_at`]); overlays
//! resolve as windows ([`overlays_at`]), because two of them can run at once and
//! the second one starting does not end the first.
//!
//! This crate holds no library, so every question that needs one — does this
//! name resolve, is this speed inside the motion's own ceiling — is the daemon's
//! to answer. What is here is the arithmetic that does not need one: the window
//! a play step occupies given a duration the caller supplies, and how many
//! overlays that timeline would ever run at once.
//!
//! [`base_at`]: MotionScript::base_at
//! [`overlays_at`]: MotionScript::overlays_at
//!
//! The timeout is an **unconditional ceiling on the
//! script's own timeline**: every step falls strictly inside it, and a script
//! that says otherwise is refused whole rather than executed past the bound it
//! stated. So the number in the message is the number the head is exposed for,
//! and [`MotionScript::expiry_ms`] is that number with no arithmetic on top.
//! A second bound applies to the timeout itself — [`MAX_TIMEOUT_MS`] — so no
//! single message can name an exposure nobody would mean. There is no
//! vocabulary here for a conversation, a lease, or a turn: the daemon executes
//! timed posture intents and knows nothing else.
//!
//! Both bounds are refusals rather than clamps. A publisher whose timeline
//! outruns its timeout has miscomputed one of them, and executing the part that
//! fits would silently drop instructions it asked for; the daemon's rule is
//! that a script runs entirely or not at all, and the script already standing —
//! with its own timeout — stays in force.
//!
//! Tolerance runs in one direction only. Unknown *fields* are ignored, so a
//! newer scripter may add one without a lockstep deploy. An unknown *posture* is
//! a refusal: the postures are the whole meaning of the message, and guessing at
//! one nobody has defined would move a head on a guess.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The discriminator every motion script carries.
pub const MOTION_SCRIPT_TYPE: &str = "motion-script";

/// The largest timeout any script may name: ten minutes.
///
/// The per-script ceiling bounds a timeline against the timeout beside it, and
/// both numbers come from the same publisher — so a slip that inflates one
/// inflates the other, and the pair stays self-consistent while naming an
/// exposure of hours. This is the bound the pair is checked against, and it is
/// deliberately far above any turn a speech interaction produces and far below
/// the accident: a scripter dating its stow from a horizon in seconds where
/// milliseconds were meant reaches it, and a real answer does not.
///
/// Ten minutes rather than something tighter because the horizon a closing
/// script carries is one clip's remaining playback plus a tail — a clip that
/// has not started playing moves no horizon — so reaching this ceiling honestly
/// takes a single synthesized clip over ten minutes long.
pub const MAX_TIMEOUT_MS: u64 = 600_000;

/// A posture the head can be asked to take.
///
/// A closed vocabulary, and small on purpose: everything richer — a thinking
/// tilt, a gaze direction, an emote — is a new value with its own parameters,
/// added here and executed by the same script executor. None of them is a new
/// state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Posture {
    /// Head up and attending — the neutral pose.
    Up,
    /// Head stowed. Reaching it is also what lets the daemon rest.
    Stow,
}

impl Posture {
    /// The posture as the wire spells it.
    ///
    /// Defined beside the serde rename that decides the spelling, because every
    /// consumer that logs a posture would otherwise hand-copy it: a JSONL line
    /// whose spelling drifts from the wire's stops joining against the
    /// scripter's capture, and the drift is silent.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Stow => "stow",
        }
    }
}

impl std::fmt::Display for Posture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The slowest an overlay may be asked to play. Below this a motion degenerates
/// into a creep that squats on the session without reading as motion.
pub const MIN_SPEED: f64 = 0.25;

/// The fastest an overlay may be asked to play. Above this even a gentle motion
/// approaches the machine's per-tick step bounds and the content reads as
/// glitch. A motion may name a tighter ceiling of its own, which the daemon
/// checks against its library; this is the bound both ends can check without
/// one.
///
/// These two are the authoritative pair. `reachy-clips`, on the far side of the
/// repo seam, carries a mirror of them and of [`MAX_MOTION_NAME_LEN`]; the
/// daemon depends on both crates and asserts the copies agree, so a change that
/// crosses the seam without its mirror fails there.
pub const MAX_SPEED: f64 = 2.0;

/// The most overlays a script may ever have running at one instant.
///
/// Each one costs a sampled frame and a layer of composition every tick, and
/// four independently weighted motions on one head is already past what anyone
/// can perceive as separate. A timeline that would exceed it is refused whole
/// rather than run with the excess dropped.
pub const MAX_CONCURRENT_OVERLAYS: usize = 4;

/// The longest motion name the wire carries, matching the library's own name
/// bound under the same spelling.
///
/// The length is all this side checks. The library also holds a charset, which
/// stays there: it is the library's alphabet, and duplicating it here would be
/// a second copy across the repo seam of a rule whose owner is the asset
/// format. A name of legal length in a wrong alphabet therefore decodes and is
/// refused at the daemon as a name no library holds.
///
/// The third of the three constants `reachy-clips` mirrors, held to this one by
/// the daemon's drift guard.
pub const MAX_MOTION_NAME_LEN: usize = 128;

/// What the base layer — the reference an overlay rides on — is asked to do.
///
/// [`Posture`] stays the two-value vocabulary it is, because it is also the
/// daemon's posture *state* and every target set is written against it.
/// `Keep` has no target set and no holdable meaning there: it is an instruction
/// to the timeline, not a place to be, so it lives here instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Base {
    /// Go to, or stay at, a named posture.
    Posture(Posture),
    /// Do not move the base: hold it wherever it is commanded now.
    ///
    /// What a publisher opens a replacement script with when it wants to change
    /// overlays mid-motion. Restating a named posture there would *retarget* the
    /// base — mid-transition, toward somewhere it is already leaving — where the
    /// intent was to leave it alone.
    ///
    /// It never wakes a resting machine: a machine at rest has no commanded
    /// pose to keep, and a script that wants motion from rest says `up`.
    Keep,
}

impl Base {
    /// The base command as the wire spells it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Posture(posture) => posture.as_str(),
            Self::Keep => "keep",
        }
    }

    /// The posture this names, or `None` for `keep`.
    #[must_use]
    pub const fn posture(self) -> Option<Posture> {
        match self {
            Self::Posture(posture) => Some(posture),
            Self::Keep => None,
        }
    }
}

impl std::fmt::Display for Base {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The base command as the wire spells it, which is also how an older daemon
/// reads it: straight into two-value [`Posture`], refusing `keep` as it refuses
/// any posture outside its vocabulary.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BaseWire {
    Up,
    Stow,
    Keep,
}

impl From<Base> for BaseWire {
    fn from(base: Base) -> Self {
        match base {
            Base::Posture(Posture::Up) => Self::Up,
            Base::Posture(Posture::Stow) => Self::Stow,
            Base::Keep => Self::Keep,
        }
    }
}

impl From<BaseWire> for Base {
    fn from(wire: BaseWire) -> Self {
        match wire {
            BaseWire::Up => Self::Posture(Posture::Up),
            BaseWire::Stow => Self::Posture(Posture::Stow),
            BaseWire::Keep => Self::Keep,
        }
    }
}

/// An overlay to start: a motion the daemon holds in its library, played at a
/// speed this invocation picks.
///
/// The name is the join key between this wire and the library, and nothing here
/// can check it — a publisher may have no library at all. Resolution, and the
/// motion's own speed ceiling, are checked by the daemon at acceptance.
#[derive(Debug, Clone, PartialEq)]
pub struct Play {
    /// The motion's name in the daemon's library.
    pub name: String,
    /// The multiplier on the motion's clock, within [`MIN_SPEED`]..=[`MAX_SPEED`].
    pub speed: f64,
}

impl Play {
    /// `name` at its recorded speed.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            speed: UNIT_SPEED,
        }
    }

    /// `name`, played at `speed`.
    #[must_use]
    pub fn at_speed(name: impl Into<String>, speed: f64) -> Self {
        Self {
            name: name.into(),
            speed,
        }
    }
}

impl std::fmt::Display for Play {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if (self.speed - UNIT_SPEED).abs() < f64::EPSILON {
            write!(f, "play {}", self.name)
        } else {
            write!(f, "play {} at {:.2}x", self.name, self.speed)
        }
    }
}

/// The speed an invocation that does not name one asks for.
const UNIT_SPEED: f64 = 1.0;

const fn unit_speed() -> f64 {
    UNIT_SPEED
}

/// What one step does: move the base, or start an overlay.
///
/// Exactly one of the two, which is why the wire spells them as two mutually
/// exclusive fields rather than as a tagged union — a step naming both, or
/// neither, is a refusal rather than a precedence rule nobody would remember.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Drive the base layer.
    Base(Base),
    /// Start an overlay.
    Play(Play),
}

impl Action {
    /// The base command this step gives, or `None` for a play step.
    #[must_use]
    pub const fn base(&self) -> Option<Base> {
        match self {
            Self::Base(base) => Some(*base),
            Self::Play(_) => None,
        }
    }

    /// The overlay this step starts, or `None` for a base step.
    #[must_use]
    pub const fn play(&self) -> Option<&Play> {
        match self {
            Self::Base(_) => None,
            Self::Play(play) => Some(play),
        }
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(base) => base.fmt(f),
            Self::Play(play) => play.fmt(f),
        }
    }
}

/// One action, due at an offset from the script's arrival.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    /// Milliseconds after receipt at which this step comes due.
    pub after_ms: u64,
    /// What happens then.
    pub action: Action,
}

impl Step {
    /// A base step naming `posture` at `after_ms` past receipt.
    #[must_use]
    pub const fn new(after_ms: u64, posture: Posture) -> Self {
        Self {
            after_ms,
            action: Action::Base(Base::Posture(posture)),
        }
    }

    /// A base step at `after_ms` that holds the base where it is.
    #[must_use]
    pub const fn keep(after_ms: u64) -> Self {
        Self {
            after_ms,
            action: Action::Base(Base::Keep),
        }
    }

    /// A step starting `play` as an overlay at `after_ms`.
    #[must_use]
    pub fn play(after_ms: u64, play: Play) -> Self {
        Self {
            after_ms,
            action: Action::Play(play),
        }
    }

    /// The step as a capture field set.
    ///
    /// This *is* the wire shape rather than a rendering of it, so a capture on
    /// either end joins against the script that produced it with no translation
    /// table in between, and a field added to a step reaches every capture at
    /// once. Surfaces that record steps call this rather than matching on the
    /// action itself: two hand-written copies of one shape diverge on the first
    /// field only one of them cares about, and nothing fails when they do.
    ///
    /// # Panics
    ///
    /// If a step cannot be rendered as JSON, which requires a step whose speed
    /// is not a number — a value no constructor and no decode admits.
    #[must_use]
    pub fn capture(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("a step renders as the JSON it is sent as")
    }
}

/// The step's JSON shape: the two actions as two optional fields, so a base
/// step encodes exactly as it always has and an older daemon reading a play
/// step sees a step with no `posture` and refuses the script whole.
///
/// No `deny_unknown_fields`, matching the envelope around it: a scripter may
/// add a field before its daemon knows it.
#[derive(Serialize, Deserialize)]
struct StepWire {
    after_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    posture: Option<BaseWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    play: Option<PlayWire>,
}

#[derive(Serialize, Deserialize)]
struct PlayWire {
    name: String,
    #[serde(default = "unit_speed")]
    speed: f64,
}

impl Serialize for Step {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (posture, play) = match &self.action {
            Action::Base(base) => (Some(BaseWire::from(*base)), None),
            Action::Play(play) => (
                None,
                Some(PlayWire {
                    name: play.name.clone(),
                    speed: play.speed,
                }),
            ),
        };
        StepWire {
            after_ms: self.after_ms,
            posture,
            play,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Step {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = StepWire::deserialize(deserializer)?;
        let action = match (wire.posture, wire.play) {
            (Some(base), None) => Action::Base(base.into()),
            (None, Some(play)) => Action::Play(Play {
                name: play.name,
                speed: play.speed,
            }),
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "a step names both `posture` and `play`; a step does exactly one of them",
                ));
            }
            (None, None) => {
                return Err(serde::de::Error::custom(
                    "a step names neither `posture` nor `play`",
                ));
            }
        };
        Ok(Self {
            after_ms: wire.after_ms,
            action,
        })
    }
}

/// What the caller's library says about how long a motion occupies the
/// timeline.
///
/// Two numbers rather than one because they scale differently: the motion's own
/// clock is what a speed factor multiplies, while the blend-out that follows it
/// runs on the wall clock at any speed — it is the ramp that keeps the machine's
/// per-tick step bounds satisfied, and speeding a motion up must not shorten it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayWindow {
    /// How long the motion runs at its recorded speed, holds included.
    pub duration_ms: u64,
    /// How long the motion's overlay takes to fade out afterwards.
    pub blend_out_ms: u64,
}

impl PlayWindow {
    /// How long the whole overlay occupies the timeline when played at `speed`.
    ///
    /// Rounded up, so the window closes no earlier than the last frame the
    /// player has to produce.
    #[must_use]
    pub fn span_ms(self, speed: f64) -> u64 {
        debug_assert!(speed > 0.0, "a validated play step names a positive speed");
        #[expect(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a duration in milliseconds, divided by a speed in 0.25..=2.0"
        )]
        let scaled = (self.duration_ms as f64 / speed).ceil() as u64;
        scaled.saturating_add(self.blend_out_ms)
    }
}

/// An overlay running at an instant, and where in its own motion it is.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActiveOverlay<'a> {
    /// Which step started it, which is also its composition order.
    pub index: usize,
    /// What it plays.
    pub play: &'a Play,
    /// The offset its step named.
    pub started_ms: u64,
    /// How long it has been running — wall-clock milliseconds since it started,
    /// which a player scales by the invocation speed to find its own clock.
    pub elapsed_ms: u64,
}

/// Why a script's overlays cannot be run against a given library.
///
/// Answered at acceptance, so a script that fails one of these never replaces
/// the timeline already running.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OverlayError {
    /// A play step names a motion the library does not hold. Refused rather
    /// than skipped: a script that plays three motions and finds two is not the
    /// script anybody wrote.
    #[error("step {index} plays `{name}`, which this library does not hold")]
    UnknownMotion {
        /// Which step named it.
        index: usize,
        /// The name that did not resolve.
        name: String,
    },

    /// The timeline would run more overlays at once than the machine composes.
    #[error(
        "{count} overlays would be running at {at_ms} ms; at most \
         {MAX_CONCURRENT_OVERLAYS} may run at once"
    )]
    TooManyOverlays {
        /// The instant, as an offset past receipt, at which the count is
        /// reached.
        at_ms: u64,
        /// How many would be running.
        count: usize,
    },
}

/// Why a script is not one, even though it decoded.
///
/// Separate from [`DecodeError`]'s parse refusals because these are the
/// scripter's bugs rather than the wire's: the text arrived intact and says
/// something no machine should execute.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ScriptError {
    /// A timeout of zero would expire the script the instant it landed, which
    /// is a stow request wearing a timeline's clothes. Every script carries a
    /// real bound; a script that lapses is what makes the head's exposure
    /// finite when the scripter dies mid-conversation.
    #[error("`timeout_ms` is {timeout_ms}; a script's timeout must be positive")]
    TimeoutNotPositive {
        /// What the script asked for.
        timeout_ms: u64,
    },

    /// Steps out of order, or two at the same instant. Either way the timeline
    /// does not say what the head should do: the executor takes the last due
    /// step, and "last" is only meaningful when the offsets ascend.
    #[error("step {index} is at {after_ms} ms, at or before its predecessor's {previous_ms} ms")]
    StepsNotAscending {
        /// Which step broke the order.
        index: usize,
        /// Its offset.
        after_ms: u64,
        /// The offset it should have exceeded.
        previous_ms: u64,
    },

    /// The timeline runs to or past the instant the same message calls its
    /// bound. Whichever of the two numbers is wrong, the message contradicts
    /// itself about how long the head is up, and the timeout is the one that
    /// claims to be the answer — so the script is refused and the publisher
    /// sizes its timeout from its own timeline.
    ///
    /// The last step must fall *strictly* inside the timeout: level with it the
    /// lapse resolves first, and the step it was waiting for would be swallowed
    /// on every such timeline.
    #[error(
        "the last step is at {last_ms} ms, at or past the script's own {timeout_ms} ms timeout"
    )]
    TimelinePastTimeout {
        /// The last step's offset.
        last_ms: u64,
        /// The timeout it had to fall inside.
        timeout_ms: u64,
    },

    /// The timeout exceeds [`MAX_TIMEOUT_MS`]. The independent bound: a
    /// publisher that got its own arithmetic wrong keeps the timeline and the
    /// timeout consistent with each other, so only a number neither of them
    /// can justify catches it.
    #[error("`timeout_ms` is {timeout_ms}; no script may exceed {MAX_TIMEOUT_MS} ms")]
    TimeoutPastCeiling {
        /// What the script asked for.
        timeout_ms: u64,
    },

    /// A play step asks for a speed outside the bounds both ends agree on. A
    /// refusal rather than a clamp: a publisher that meant 1.5 and wrote 15 gets
    /// told, instead of watching a motion play at a speed it did not ask for.
    #[error(
        "step {index} asks for speed {speed}; an overlay plays between \
         {MIN_SPEED} and {MAX_SPEED} times its recorded speed"
    )]
    SpeedOutOfBounds {
        /// Which step asked.
        index: usize,
        /// What it asked for.
        speed: f64,
    },

    /// A play step names nothing, or names something longer than any library
    /// entry can be. Neither can resolve, and both are the publisher's bug
    /// rather than a missing asset.
    ///
    /// Length and emptiness only: the library's charset is not checked here
    /// (see [`MAX_MOTION_NAME_LEN`]), so a name of legal length spelled in a
    /// wrong alphabet passes this and is refused at the daemon as a motion no
    /// library holds.
    #[error("step {index} names a motion of {len} characters; a name is 1..={MAX_MOTION_NAME_LEN}")]
    MotionNameUnusable {
        /// Which step named it.
        index: usize,
        /// How long the name was.
        len: usize,
    },

    /// A play step comes due before any base step does.
    ///
    /// An overlay is a delta on a base, so the timeline has to have said what
    /// the base is first — `keep` counts, since it defines the base as "where it
    /// is". Without that rule an overlay-only script would also expire its
    /// overlays against a resting daemon it never woke, which looks like a
    /// broken motion rather than a refused script.
    #[error("step {index} plays `{name}` at {after_ms} ms, before any base step has come due")]
    PlayBeforeBase {
        /// Which step plays too early.
        index: usize,
        /// What it plays.
        name: String,
        /// When it plays.
        after_ms: u64,
    },
}

/// One motion script, addressed to one pod.
///
/// Construct through [`MotionScript::new`] or [`MotionScript::decode`]; both
/// validate, so a value of this type is always a lawful timeline.
#[derive(Debug, Clone, PartialEq)]
pub struct MotionScript {
    /// Whose head this is about. A consumer obeys its own id and reports the
    /// rest; the channel is not assumed to carry one machine's traffic.
    pod: String,
    /// Ordering authority for this pod's scripts. The latest accepted script
    /// wholly replaces the previous one, and one numbered at or below the last
    /// accepted is a redelivery to drop — so the scripter's numbers must
    /// survive its own restarts, which is what [`crate::seq::SeqSource`] is
    /// for.
    seq: u64,
    /// The timeline, ascending by offset. Empty is lawful: it commands no
    /// posture change, and the script's only effect is its timeout.
    steps: Vec<Step>,
    /// How long after receipt the daemon stows and rests regardless. This is
    /// the loss-of-instruction bound — the head's exposure stays finite even if
    /// every later message is lost.
    timeout_ms: u64,
}

/// The JSON shape, kept separate from the public struct so the discriminator is
/// a wire detail rather than a field a caller has to set correctly.
///
/// No `deny_unknown_fields`: tolerance of unknown fields is the point.
///
/// TODO(script-timebase): a `base` field carrying an absolute start instant on
/// a timebase both ends share, so speech and motion begin together regardless
/// of delivery jitter. Offsets are measured from receipt until then.
#[derive(Serialize, Deserialize)]
struct Wire {
    #[serde(rename = "type")]
    kind: String,
    pod: String,
    seq: u64,
    steps: Vec<Step>,
    timeout_ms: u64,
}

/// Why a body did not yield a script.
///
/// Four refusals rather than one string, because they mean different things to
/// whoever reads the log line: the channel carried something that is not JSON,
/// something that is JSON of another kind, a script body with a field missing
/// or unreadable, or a well-formed body whose timeline is not executable.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum DecodeError {
    /// The body is not JSON at all.
    #[error("the body is not json: {detail}")]
    NotJson {
        /// What the parser said.
        detail: String,
    },

    /// The body is JSON, and is some other message on this channel. Expected in
    /// normal operation once the vocabulary grows; worth reporting, never worth
    /// alarming about.
    #[error("the body is a `{kind}` message, not a `{MOTION_SCRIPT_TYPE}` one")]
    WrongType {
        /// The discriminator the body carried.
        kind: String,
    },

    /// The body claims to be a script and does not hold one — a missing field,
    /// a field of the wrong type, or a posture nobody has defined.
    #[error("the body is not a well-formed `{MOTION_SCRIPT_TYPE}` message: {detail}")]
    Malformed {
        /// What the deserializer said.
        detail: String,
    },

    /// The body decoded and the timeline it holds is not executable.
    #[error("the script is not executable: {0}")]
    Invalid(#[from] ScriptError),
}

impl MotionScript {
    /// A script for `pod`, numbered `seq`, running `steps` under `timeout_ms`.
    ///
    /// Refuses the same timelines [`Self::decode`] refuses, so a scripter
    /// cannot emit something its own daemon would drop.
    pub fn new(
        pod: impl Into<String>,
        seq: u64,
        steps: Vec<Step>,
        timeout_ms: u64,
    ) -> Result<Self, ScriptError> {
        validate(&steps, timeout_ms)?;
        Ok(Self {
            pod: pod.into(),
            seq,
            steps,
            timeout_ms,
        })
    }

    /// Whose head this script is about.
    #[must_use]
    pub fn pod(&self) -> &str {
        &self.pod
    }

    /// This script's ordering number.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// The timeline, ascending by offset.
    #[must_use]
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// How long after receipt the script lapses.
    #[must_use]
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    /// The base command this script asks for `elapsed_ms` after it arrived, or
    /// `None` while no base step has come due.
    ///
    /// The last due base step wins, so steps whose time had already passed when
    /// the script landed collapse into the one that matters — a daemon that was
    /// away for a second does not replay a timeline at it. Play steps are not
    /// part of this collapse: an overlay is a window (see
    /// [`Self::overlays_at`]), not a state the next step supersedes.
    #[must_use]
    pub fn base_at(&self, elapsed_ms: u64) -> Option<Base> {
        self.steps
            .iter()
            .rev()
            .filter(|step| step.after_ms <= elapsed_ms)
            .find_map(|step| step.action.base())
    }

    /// The overlays running `elapsed_ms` after this script arrived, in wire step
    /// order — which is the order they compose in.
    ///
    /// `window` supplies what this crate cannot know: how long the named motion
    /// runs at its recorded speed, and how long its blend-out takes. A name
    /// `window` cannot answer for is skipped, which is the disposition a script
    /// that reached the machine at all should never need — acceptance resolves
    /// every name first ([`Self::check_overlays`]).
    ///
    /// A daemon that wakes inside a window joins the overlay in progress, at the
    /// offset [`ActiveOverlay::elapsed_ms`] gives it, rather than starting it
    /// from the top; one that wakes past a window skips it entirely. The
    /// timeline stays authoritative in absolute time, and nothing is ever
    /// replayed.
    #[must_use]
    pub fn overlays_at<F>(&self, elapsed_ms: u64, mut window: F) -> Vec<ActiveOverlay<'_>>
    where
        F: FnMut(&Play) -> Option<PlayWindow>,
    {
        self.steps
            .iter()
            .enumerate()
            .filter_map(|(index, step)| {
                let play = step.action.play()?;
                let span = window(play)?.span_ms(play.speed);
                (step.after_ms <= elapsed_ms && elapsed_ms < step.after_ms.saturating_add(span))
                    .then(|| ActiveOverlay {
                        index,
                        play,
                        started_ms: step.after_ms,
                        elapsed_ms: elapsed_ms - step.after_ms,
                    })
            })
            .collect()
    }

    /// The rules a script's overlays have to keep against a library: every name
    /// resolves, and the timeline never runs more than
    /// [`MAX_CONCURRENT_OVERLAYS`] of them at once.
    ///
    /// Checked at acceptance rather than at decode, because both questions need
    /// the library the publisher may not have. The count is exact rather than a
    /// count of steps: overlays that do not overlap cost nothing at the same
    /// time, and a script scheduling twenty short motions in sequence is not
    /// four-deep in anything.
    ///
    /// The motion's own speed ceiling is deliberately not checked here — that
    /// number lives in the library beside the durations, and the caller reading
    /// it out has already read it.
    pub fn check_overlays<F>(&self, mut window: F) -> Result<(), OverlayError>
    where
        F: FnMut(&Play) -> Option<PlayWindow>,
    {
        let mut spans = Vec::new();
        for (index, step) in self.steps.iter().enumerate() {
            let Some(play) = step.action.play() else {
                continue;
            };
            let span = window(play).ok_or_else(|| OverlayError::UnknownMotion {
                index,
                name: play.name.clone(),
            })?;
            spans.push((
                step.after_ms,
                step.after_ms.saturating_add(span.span_ms(play.speed)),
            ));
        }

        // The count can only rise where an overlay starts, so those instants are
        // the whole search.
        for &(start, _) in &spans {
            let count = spans
                .iter()
                .filter(|&&(from, until)| from <= start && start < until)
                .count();
            if count > MAX_CONCURRENT_OVERLAYS {
                return Err(OverlayError::TooManyOverlays {
                    at_ms: start,
                    count,
                });
            }
        }
        Ok(())
    }

    /// The offset of the first step still ahead of `elapsed_ms`, if any.
    #[must_use]
    pub fn next_step_ms(&self, elapsed_ms: u64) -> Option<u64> {
        self.steps
            .iter()
            .find(|step| step.after_ms > elapsed_ms)
            .map(|step| step.after_ms)
    }

    /// The offset at which this script lapses: the timeout it named, and
    /// nothing else.
    ///
    /// Kept as a method rather than folded into the executor because the
    /// *concept* is the executor's — "when does this stop being an
    /// instruction" — and validation is what makes the answer this simple:
    /// every step is strictly inside the timeout, so no step can be waiting
    /// when the lapse arrives.
    #[must_use]
    pub fn expiry_ms(&self) -> u64 {
        self.timeout_ms
    }

    /// This script as the JSON text a bus body carries.
    ///
    /// Infallible: every field is a string, an integer, or an enum, and none of
    /// them can fail to serialize. The `expect` documents that rather than
    /// pushing an impossible error onto the scripter.
    #[must_use]
    pub fn encode(&self) -> String {
        let wire = Wire {
            kind: MOTION_SCRIPT_TYPE.to_owned(),
            pod: self.pod.clone(),
            seq: self.seq,
            steps: self.steps.clone(),
            timeout_ms: self.timeout_ms,
        };
        serde_json::to_string(&wire).expect("a motion script holds nothing that can fail to encode")
    }

    /// A script out of the JSON text a delivery carried.
    pub fn decode(text: &str) -> Result<Self, DecodeError> {
        // Two passes: the discriminator first, so a body of another kind is
        // reported as another kind rather than as a malformed script — the
        // difference between "not for me" and "somebody broke the scripter".
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|error| DecodeError::NotJson {
                detail: error.to_string(),
            })?;
        let kind = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| DecodeError::Malformed {
                detail: "no `type` field".to_owned(),
            })?;
        if kind != MOTION_SCRIPT_TYPE {
            return Err(DecodeError::WrongType {
                kind: kind.to_owned(),
            });
        }

        let wire: Wire = serde_json::from_value(value).map_err(|error| DecodeError::Malformed {
            detail: error.to_string(),
        })?;
        validate(&wire.steps, wire.timeout_ms)?;
        Ok(Self {
            pod: wire.pod,
            seq: wire.seq,
            steps: wire.steps,
            timeout_ms: wire.timeout_ms,
        })
    }
}

/// The rules a script has to keep, in one place so the constructor and the
/// decoder cannot come to different conclusions about the same script.
///
/// Everything checkable without a library: the timeout bounds, the ascending
/// timeline, and — for play steps — a usable name, a speed inside the bounds
/// both ends share, and a base step ahead of every overlay.
fn validate(steps: &[Step], timeout_ms: u64) -> Result<(), ScriptError> {
    if timeout_ms == 0 {
        return Err(ScriptError::TimeoutNotPositive { timeout_ms });
    }
    if timeout_ms > MAX_TIMEOUT_MS {
        return Err(ScriptError::TimeoutPastCeiling { timeout_ms });
    }
    for (index, step) in steps.iter().enumerate().skip(1) {
        let previous_ms = steps[index - 1].after_ms;
        if step.after_ms <= previous_ms {
            return Err(ScriptError::StepsNotAscending {
                index,
                after_ms: step.after_ms,
                previous_ms,
            });
        }
    }
    // Last, so a timeline that is out of order is reported as out of order:
    // "the last step" means nothing until the offsets ascend.
    if let Some(last) = steps.last()
        && last.after_ms >= timeout_ms
    {
        return Err(ScriptError::TimelinePastTimeout {
            last_ms: last.after_ms,
            timeout_ms,
        });
    }

    let mut base_seen = false;
    for (index, step) in steps.iter().enumerate() {
        let Some(play) = step.action.play() else {
            base_seen = true;
            continue;
        };
        if play.name.is_empty() || play.name.len() > MAX_MOTION_NAME_LEN {
            return Err(ScriptError::MotionNameUnusable {
                index,
                len: play.name.len(),
            });
        }
        if !play.speed.is_finite() || play.speed < MIN_SPEED || play.speed > MAX_SPEED {
            return Err(ScriptError::SpeedOutOfBounds {
                index,
                speed: play.speed,
            });
        }
        if !base_seen {
            return Err(ScriptError::PlayBeforeBase {
                index,
                name: play.name.clone(),
                after_ms: step.after_ms,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(steps: Vec<Step>, timeout_ms: u64) -> MotionScript {
        MotionScript::new("reachy00", 1, steps, timeout_ms).expect("a lawful script")
    }

    /// A library that answers the same window for every name, which is what a
    /// window test wants: the arithmetic under test is the timeline's, not the
    /// library's.
    fn window(duration_ms: u64, blend_out_ms: u64) -> impl FnMut(&Play) -> Option<PlayWindow> {
        move |_| {
            Some(PlayWindow {
                duration_ms,
                blend_out_ms,
            })
        }
    }

    /// What one side writes, the other side reads — including the empty
    /// timeline, which is lawful and whose only effect is its timeout.
    #[test]
    fn a_script_survives_the_wire() {
        for steps in [
            vec![],
            vec![Step::new(0, Posture::Up)],
            vec![Step::new(0, Posture::Up), Step::new(6740, Posture::Stow)],
        ] {
            let script = MotionScript::new("reachy00", 1_786_543_210_123, steps, 30_000)
                .expect("a lawful script");
            let text = script.encode();
            assert_eq!(MotionScript::decode(&text).expect("it decodes"), script);
        }
    }

    /// The encoded shape, spelled out field by field rather than inferred from
    /// a round trip: a round trip agrees with itself even when both halves are
    /// wrong, and the other end of this channel is written against the text,
    /// not against this crate's opinion of it.
    #[test]
    fn the_encoding_is_the_documented_one() {
        let script = MotionScript::new(
            "reachy00",
            1_786_543_210_123,
            vec![Step::new(0, Posture::Up), Step::new(6740, Posture::Stow)],
            30_000,
        )
        .expect("a lawful script");
        let value: serde_json::Value = serde_json::from_str(&script.encode()).expect("it is json");

        assert_eq!(value["type"], "motion-script");
        assert_eq!(value["pod"], "reachy00");
        assert_eq!(value["seq"], 1_786_543_210_123u64);
        assert_eq!(value["timeout_ms"], 30_000);
        assert_eq!(value["steps"][0]["after_ms"], 0);
        assert_eq!(value["steps"][0]["posture"], "up");
        assert_eq!(value["steps"][1]["after_ms"], 6740);
        assert_eq!(value["steps"][1]["posture"], "stow");
    }

    /// The spelling a consumer logs and the spelling that crosses the wire are
    /// the same one, checked against the encoding rather than against a second
    /// copy of the table.
    #[test]
    fn a_posture_names_itself_the_way_the_wire_does() {
        for posture in [Posture::Up, Posture::Stow] {
            let text = script(vec![Step::new(0, posture)], 1000).encode();
            let value: serde_json::Value = serde_json::from_str(&text).expect("it is json");
            assert_eq!(value["steps"][0]["posture"], posture.as_str());
            assert_eq!(posture.to_string(), posture.as_str());
        }
    }

    /// A field this daemon has never heard of is not a reason to drop the
    /// script. The schema is meant to grow — `base` is already named for the
    /// shared timebase — and the scripter that grows first must not take the
    /// head down with it.
    #[test]
    fn a_field_nobody_here_knows_is_ignored() {
        let text = r#"{"type":"motion-script","pod":"reachy00","seq":3,
                       "steps":[{"after_ms":0,"posture":"up","ease":"quintic"}],
                       "timeout_ms":30000,"base":1786543210123}"#;
        let decoded = MotionScript::decode(text).expect("the fields it needs are all there");
        assert_eq!(decoded.steps(), [Step::new(0, Posture::Up)]);
        assert_eq!(decoded.seq(), 3);
    }

    /// Another tenant of the same channel is reported as another tenant. The
    /// daemon skips it; nothing about it is a failure.
    #[test]
    fn a_message_of_another_kind_says_so() {
        let text = r#"{"type":"gaze","pod":"reachy00","az":0.2}"#;
        match MotionScript::decode(text).expect_err("not a script") {
            DecodeError::WrongType { kind } => assert_eq!(kind, "gaze"),
            other => panic!("expected a wrong-type report, got {other}"),
        }
    }

    /// The malformed shapes, each reported as what it is. A posture nobody has
    /// defined is refused rather than guessed at: the postures are the
    /// message's whole meaning, and a guess here moves a head.
    #[test]
    fn a_body_that_is_not_a_script_is_reported_rather_than_guessed_at() {
        let not_json = MotionScript::decode("{not json").expect_err("it is not json");
        assert!(
            matches!(not_json, DecodeError::NotJson { .. }),
            "{not_json}"
        );

        for text in [
            // No discriminator at all.
            r#"{"pod":"reachy00","seq":1,"steps":[],"timeout_ms":30000}"#,
            // Each required field, missing in turn.
            r#"{"type":"motion-script","seq":1,"steps":[],"timeout_ms":30000}"#,
            r#"{"type":"motion-script","pod":"reachy00","steps":[],"timeout_ms":30000}"#,
            r#"{"type":"motion-script","pod":"reachy00","seq":1,"timeout_ms":30000}"#,
            r#"{"type":"motion-script","pod":"reachy00","seq":1,"steps":[]}"#,
            // A posture outside the vocabulary.
            r#"{"type":"motion-script","pod":"reachy00","seq":1,
                "steps":[{"after_ms":0,"posture":"lurking"}],"timeout_ms":30000}"#,
            // A step missing its offset.
            r#"{"type":"motion-script","pod":"reachy00","seq":1,
                "steps":[{"posture":"up"}],"timeout_ms":30000}"#,
            // Numbers that are not counts.
            r#"{"type":"motion-script","pod":"reachy00","seq":-1,"steps":[],"timeout_ms":30000}"#,
            r#"{"type":"motion-script","pod":"reachy00","seq":1,"steps":[],"timeout_ms":-1}"#,
        ] {
            let refused = MotionScript::decode(text).expect_err("a field is missing or unreadable");
            assert!(
                matches!(refused, DecodeError::Malformed { .. }),
                "{text}: {refused}"
            );
        }
    }

    /// A timeline that decodes and cannot be executed is refused whole, and by
    /// both doors: the daemon that reads it and the scripter that builds it
    /// come to the same conclusion, so a bug cannot escape the host.
    #[test]
    fn an_unexecutable_timeline_is_refused_by_both_doors() {
        let zero_timeout = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                               "steps":[],"timeout_ms":0}"#;
        assert_eq!(
            MotionScript::decode(zero_timeout).expect_err("no bound"),
            DecodeError::Invalid(ScriptError::TimeoutNotPositive { timeout_ms: 0 })
        );
        assert_eq!(
            MotionScript::new("reachy00", 1, vec![], 0).expect_err("no bound"),
            ScriptError::TimeoutNotPositive { timeout_ms: 0 }
        );

        let backwards = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                            "steps":[{"after_ms":500,"posture":"up"},
                                     {"after_ms":200,"posture":"stow"}],
                            "timeout_ms":30000}"#;
        assert_eq!(
            MotionScript::decode(backwards).expect_err("out of order"),
            DecodeError::Invalid(ScriptError::StepsNotAscending {
                index: 1,
                after_ms: 200,
                previous_ms: 500,
            })
        );

        // Two steps at the same instant: the executor takes the last due one,
        // and "last" means nothing when two share an offset.
        assert_eq!(
            MotionScript::new(
                "reachy00",
                1,
                vec![Step::new(0, Posture::Up), Step::new(0, Posture::Stow)],
                30_000,
            )
            .expect_err("simultaneous"),
            ScriptError::StepsNotAscending {
                index: 1,
                after_ms: 0,
                previous_ms: 0,
            }
        );
    }

    /// The refusals read as sentences naming the message type and the offending
    /// numbers, because they go into a log line an operator reads without this
    /// file open beside it.
    #[test]
    fn a_refusal_names_what_was_expected() {
        let wrong_type = MotionScript::decode(r#"{"type":"gaze"}"#).expect_err("not a script");
        let printed = wrong_type.to_string();
        assert!(printed.contains("motion-script"), "{printed}");
        assert!(printed.contains("gaze"), "{printed}");

        let printed = DecodeError::Invalid(ScriptError::StepsNotAscending {
            index: 2,
            after_ms: 200,
            previous_ms: 500,
        })
        .to_string();
        assert!(printed.contains("200"), "{printed}");
        assert!(printed.contains("500"), "{printed}");
    }

    /// The timeline resolved at an instant: nothing before the first step, the
    /// last due step once several have passed, and the last step forever after.
    #[test]
    fn the_posture_is_the_last_step_that_has_come_due() {
        let script = script(
            vec![Step::new(500, Posture::Up), Step::new(6740, Posture::Stow)],
            30_000,
        );

        assert_eq!(script.base_at(0), None, "nothing is due yet");
        assert_eq!(script.base_at(499), None);
        assert_eq!(script.base_at(500), Some(Base::Posture(Posture::Up)));
        assert_eq!(script.base_at(6739), Some(Base::Posture(Posture::Up)));
        assert_eq!(script.base_at(6740), Some(Base::Posture(Posture::Stow)));
        assert_eq!(script.base_at(600_000), Some(Base::Posture(Posture::Stow)));

        // A script that landed late collapses to the one posture that matters
        // rather than replaying its timeline.
        assert_eq!(script.base_at(10_000), Some(Base::Posture(Posture::Stow)));
    }

    /// An empty timeline never asks for a posture. It is a lawful script whose
    /// whole content is its timeout.
    #[test]
    fn an_empty_timeline_commands_nothing() {
        let script = script(vec![], 30_000);
        assert_eq!(script.base_at(0), None);
        assert_eq!(script.base_at(u64::MAX), None);
        assert_eq!(script.next_step_ms(0), None);
        assert_eq!(script.expiry_ms(), 30_000);
    }

    /// What the executor needs to know about the future: when the next step
    /// falls due, and when the whole script lapses.
    #[test]
    fn the_next_step_and_the_expiry_are_both_offsets() {
        let script = script(
            vec![Step::new(500, Posture::Up), Step::new(6740, Posture::Stow)],
            30_000,
        );

        assert_eq!(script.next_step_ms(0), Some(500));
        assert_eq!(script.next_step_ms(500), Some(6740));
        assert_eq!(script.next_step_ms(6740), None);
        assert_eq!(script.expiry_ms(), 30_000);
    }

    /// The expiry is the timeout and nothing else, on every shape of timeline —
    /// which is what makes the number in the message the number the head is
    /// exposed for.
    #[test]
    fn the_expiry_is_the_timeout_the_script_named() {
        for steps in [
            vec![],
            vec![Step::new(0, Posture::Up)],
            vec![Step::new(0, Posture::Up), Step::new(29_999, Posture::Stow)],
        ] {
            assert_eq!(script(steps, 30_000).expiry_ms(), 30_000);
        }
    }

    /// A timeline that runs to or past its own timeout is refused by both
    /// doors, and the last step has to be *strictly* inside: level with the
    /// timeout the lapse resolves first, and the step would be swallowed.
    #[test]
    fn a_timeline_reaching_its_own_timeout_is_refused() {
        assert_eq!(
            MotionScript::new(
                "reachy00",
                1,
                vec![Step::new(0, Posture::Up), Step::new(9_000, Posture::Stow)],
                5_000,
            )
            .expect_err("the timeline outruns the bound it states"),
            ScriptError::TimelinePastTimeout {
                last_ms: 9_000,
                timeout_ms: 5_000,
            }
        );

        let level = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                        "steps":[{"after_ms":5000,"posture":"stow"}],
                        "timeout_ms":5000}"#;
        assert_eq!(
            MotionScript::decode(level).expect_err("level with the lapse"),
            DecodeError::Invalid(ScriptError::TimelinePastTimeout {
                last_ms: 5_000,
                timeout_ms: 5_000,
            })
        );

        // One millisecond inside is lawful, and the step resolves.
        let inside = script(vec![Step::new(4_999, Posture::Stow)], 5_000);
        assert_eq!(inside.base_at(4_999), Some(Base::Posture(Posture::Stow)));

        // A timeline out of order is reported as out of order rather than as a
        // timeline past its timeout: "the last step" means nothing until the
        // offsets ascend.
        assert_eq!(
            MotionScript::new(
                "reachy00",
                1,
                vec![Step::new(9_000, Posture::Up), Step::new(10, Posture::Stow)],
                5_000,
            )
            .expect_err("out of order"),
            ScriptError::StepsNotAscending {
                index: 1,
                after_ms: 10,
                previous_ms: 9_000,
            }
        );
    }

    /// The second bound, which exists for the slip that keeps the timeline and
    /// the timeout consistent with each other: no message may name an exposure
    /// past ten minutes, whatever its steps say.
    #[test]
    fn a_timeout_past_the_ceiling_is_refused() {
        assert_eq!(
            MotionScript::new("reachy00", 1, vec![], MAX_TIMEOUT_MS + 1)
                .expect_err("past the ceiling"),
            ScriptError::TimeoutPastCeiling {
                timeout_ms: MAX_TIMEOUT_MS + 1,
            }
        );

        // The seconds-for-milliseconds accident: an hour-long exposure under a
        // timeline that agrees with it perfectly.
        let slipped = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                          "steps":[{"after_ms":0,"posture":"up"},
                                   {"after_ms":3600000,"posture":"stow"}],
                          "timeout_ms":3605000}"#;
        assert_eq!(
            MotionScript::decode(slipped).expect_err("an hour is nobody's turn"),
            DecodeError::Invalid(ScriptError::TimeoutPastCeiling {
                timeout_ms: 3_605_000,
            })
        );

        // The ceiling itself is lawful; it is a bound, not a limit to stay
        // under.
        let at_ceiling = script(vec![Step::new(0, Posture::Up)], MAX_TIMEOUT_MS);
        assert_eq!(at_ceiling.expiry_ms(), MAX_TIMEOUT_MS);
    }

    /// The refusals read as sentences carrying both numbers, because the
    /// operator reading the daemon's refusal line is looking for which of the
    /// two the publisher got wrong.
    #[test]
    fn the_ceiling_refusals_name_their_numbers() {
        let printed = ScriptError::TimelinePastTimeout {
            last_ms: 40_500,
            timeout_ms: 30_000,
        }
        .to_string();
        assert!(printed.contains("40500"), "{printed}");
        assert!(printed.contains("30000"), "{printed}");

        let printed = ScriptError::TimeoutPastCeiling {
            timeout_ms: 3_605_000,
        }
        .to_string();
        assert!(printed.contains("3605000"), "{printed}");
        assert!(printed.contains("600000"), "{printed}");
    }

    /// A timeline of both kinds of step survives the wire intact — including the
    /// speed a play step names and the `keep` a base step names.
    #[test]
    fn a_layered_script_survives_the_wire() {
        let script = MotionScript::new(
            "reachy00",
            7,
            vec![
                Step::new(0, Posture::Up),
                Step::play(400, Play::at_speed("pollen/emotions/loving1", 1.5)),
                Step::keep(2_000),
                Step::play(2_100, Play::new("pod/wiggle")),
                Step::new(9_000, Posture::Stow),
            ],
            30_000,
        )
        .expect("a lawful script");

        assert_eq!(
            MotionScript::decode(&script.encode()).expect("it decodes"),
            script
        );
    }

    /// The encoded shape of base and play steps, spelled out: a posture step
    /// encodes as before, `keep` rides the same field, and a play step carries
    /// no `posture` at all — which is how an older daemon comes to refuse it.
    #[test]
    fn the_layered_encoding_is_the_documented_one() {
        let script = script(
            vec![
                Step::new(0, Posture::Up),
                Step::play(400, Play::at_speed("pollen/emotions/loving1", 1.5)),
                Step::keep(2_000),
                Step::play(2_100, Play::new("pod/wiggle")),
            ],
            30_000,
        );
        let value: serde_json::Value = serde_json::from_str(&script.encode()).expect("it is json");

        assert_eq!(value["steps"][0]["posture"], "up");
        assert!(value["steps"][0].get("play").is_none());
        assert_eq!(value["steps"][1]["after_ms"], 400);
        assert_eq!(value["steps"][1]["play"]["name"], "pollen/emotions/loving1");
        assert_eq!(value["steps"][1]["play"]["speed"], 1.5);
        assert!(
            value["steps"][1].get("posture").is_none(),
            "an older daemon refuses this step because there is no posture in it"
        );
        assert_eq!(value["steps"][2]["posture"], "keep");
        assert_eq!(value["steps"][3]["play"]["speed"], 1.0);
    }

    /// A capture of a step is the step's own wire shape, both kinds of it, so
    /// the surfaces that record steps carry no second rendering to keep current.
    #[test]
    fn a_captured_step_is_the_wire_shape() {
        let steps = vec![
            Step::new(0, Posture::Up),
            Step::keep(2_000),
            Step::play(2_100, Play::at_speed("pod/wiggle", 1.5)),
        ];
        let script = script(steps.clone(), 30_000);
        let encoded: serde_json::Value =
            serde_json::from_str(&script.encode()).expect("it is json");

        for (index, step) in steps.iter().enumerate() {
            assert_eq!(step.capture(), encoded["steps"][index]);
        }
    }

    /// `keep` is a base command and never a posture. The daemon's posture state,
    /// its target sets, and its captures are all typed on [`Posture`], and this
    /// is what keeps `keep` out of them.
    #[test]
    fn keep_is_a_base_command_and_not_a_posture() {
        let script = script(vec![Step::keep(0)], 30_000);
        assert_eq!(script.base_at(0), Some(Base::Keep));
        assert_eq!(script.base_at(0).and_then(Base::posture), None);

        serde_json::from_str::<Posture>("\"keep\"")
            .expect_err("the posture vocabulary is still two values");
        assert_eq!(Base::Keep.as_str(), "keep");
        assert_eq!(Base::Keep.to_string(), "keep");
    }

    /// The base collapses across a mixed timeline: play steps are not part of
    /// it, and the last due *base* step is the answer however many overlays
    /// started since.
    #[test]
    fn the_base_collapses_past_the_play_steps_between() {
        let script = script(
            vec![
                Step::new(500, Posture::Up),
                Step::play(600, Play::new("pod/nod")),
                Step::play(1_200, Play::new("pod/wiggle")),
                Step::keep(2_000),
                Step::play(2_100, Play::new("pod/nod")),
                Step::new(9_000, Posture::Stow),
            ],
            30_000,
        );

        assert_eq!(script.base_at(499), None);
        assert_eq!(script.base_at(500), Some(Base::Posture(Posture::Up)));
        assert_eq!(script.base_at(1_999), Some(Base::Posture(Posture::Up)));
        assert_eq!(script.base_at(2_000), Some(Base::Keep));
        assert_eq!(script.base_at(2_100), Some(Base::Keep));
        assert_eq!(script.base_at(9_000), Some(Base::Posture(Posture::Stow)));

        // A daemon that woke late reads one base, not a replay of three.
        assert_eq!(script.base_at(20_000), Some(Base::Posture(Posture::Stow)));

        // And a play step still moves the clock the executor waits on.
        assert_eq!(script.next_step_ms(500), Some(600));
    }

    /// A step does exactly one thing. Both fields or neither is the publisher
    /// having built a step it could not have meant, and either is refused where
    /// the body is read rather than resolved by a precedence rule.
    #[test]
    fn a_step_that_is_not_exactly_one_action_is_refused() {
        for steps in [
            r#"[{"after_ms":0,"posture":"up","play":{"name":"pod/nod"}}]"#,
            r#"[{"after_ms":0}]"#,
        ] {
            let text = format!(
                r#"{{"type":"motion-script","pod":"reachy00","seq":1,
                     "steps":{steps},"timeout_ms":30000}}"#
            );
            let refused = MotionScript::decode(&text).expect_err("one action per step");
            assert!(
                matches!(refused, DecodeError::Malformed { .. }),
                "{steps}: {refused}"
            );
        }
    }

    /// An overlay rides a base, so the timeline has to have defined one first.
    /// A play-only script is the same refusal — and it is also the script that
    /// would never wake a resting daemon, so its overlays would lapse having
    /// moved nothing.
    #[test]
    fn a_play_before_any_base_step_is_refused_by_both_doors() {
        assert_eq!(
            MotionScript::new(
                "reachy00",
                1,
                vec![
                    Step::play(0, Play::new("pod/wiggle")),
                    Step::new(10, Posture::Up),
                ],
                30_000,
            )
            .expect_err("the base is not defined yet"),
            ScriptError::PlayBeforeBase {
                index: 0,
                name: "pod/wiggle".to_owned(),
                after_ms: 0,
            }
        );

        let play_only = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                            "steps":[{"after_ms":0,"play":{"name":"pod/wiggle"}}],
                            "timeout_ms":30000}"#;
        assert_eq!(
            MotionScript::decode(play_only).expect_err("no base at all"),
            DecodeError::Invalid(ScriptError::PlayBeforeBase {
                index: 0,
                name: "pod/wiggle".to_owned(),
                after_ms: 0,
            })
        );

        // The rule asks for a base that is *defined*, not one that is *named* —
        // so the mid-flight overlay change, which deliberately moves nothing,
        // satisfies it.
        MotionScript::new(
            "reachy00",
            1,
            vec![Step::keep(0), Step::play(0, Play::new("pod/wiggle"))],
            30_000,
        )
        .expect_err("two steps at one instant are still out of order");
        let keep_first = script(
            vec![Step::keep(0), Step::play(10, Play::new("pod/wiggle"))],
            30_000,
        );
        assert_eq!(keep_first.base_at(10), Some(Base::Keep));
    }

    /// The speed bounds are checked by both doors, and the default is the
    /// recorded speed. A refusal rather than a clamp: the publisher hears about
    /// its own arithmetic instead of watching a motion play at a speed nobody
    /// asked for.
    #[test]
    fn a_speed_outside_the_global_bounds_is_refused() {
        for speed in [0.1, 2.5, f64::NAN, f64::INFINITY, 0.0, -1.0] {
            let refused = MotionScript::new(
                "reachy00",
                1,
                vec![
                    Step::new(0, Posture::Up),
                    Step::play(10, Play::at_speed("pod/nod", speed)),
                ],
                30_000,
            )
            .expect_err("outside the bounds");
            assert!(
                matches!(refused, ScriptError::SpeedOutOfBounds { index: 1, .. }),
                "{speed}: {refused}"
            );
        }

        // The bounds themselves are lawful, and a step that names no speed asks
        // for the recorded one.
        for speed in [MIN_SPEED, MAX_SPEED] {
            script(
                vec![
                    Step::new(0, Posture::Up),
                    Step::play(10, Play::at_speed("pod/nod", speed)),
                ],
                30_000,
            );
        }
        let defaulted = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                            "steps":[{"after_ms":0,"posture":"up"},
                                     {"after_ms":10,"play":{"name":"pod/nod"}}],
                            "timeout_ms":30000}"#;
        let decoded = MotionScript::decode(defaulted).expect("a lawful script");
        assert_eq!(decoded.steps()[1].action.play().expect("a play").speed, 1.0);

        let over = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                       "steps":[{"after_ms":0,"posture":"up"},
                                {"after_ms":10,"play":{"name":"pod/nod","speed":9.0}}],
                       "timeout_ms":30000}"#;
        assert!(matches!(
            MotionScript::decode(over).expect_err("nine times"),
            DecodeError::Invalid(ScriptError::SpeedOutOfBounds { index: 1, .. })
        ));
    }

    /// A name that cannot join against any library is the publisher's bug, not
    /// a missing asset, and is refused without one.
    #[test]
    fn a_name_no_library_could_hold_is_refused() {
        for name in [String::new(), "a".repeat(MAX_MOTION_NAME_LEN + 1)] {
            let len = name.len();
            assert_eq!(
                MotionScript::new(
                    "reachy00",
                    1,
                    vec![Step::new(0, Posture::Up), Step::play(10, Play::new(name))],
                    30_000,
                )
                .expect_err("no library holds that"),
                ScriptError::MotionNameUnusable { index: 1, len }
            );
        }

        // The bound itself is lawful.
        script(
            vec![
                Step::new(0, Posture::Up),
                Step::play(10, Play::new("a".repeat(MAX_MOTION_NAME_LEN))),
            ],
            30_000,
        );
    }

    /// The window a play step occupies: the motion's own clock divided by the
    /// speed, plus a blend-out that runs on the wall clock at any speed —
    /// because the blend is a step-bound constraint in real time, and speeding
    /// a motion up must not shorten it.
    #[test]
    fn a_play_windows_span_scales_the_motion_and_not_the_blend() {
        let window = PlayWindow {
            duration_ms: 2_000,
            blend_out_ms: 200,
        };
        assert_eq!(window.span_ms(1.0), 2_200);
        assert_eq!(window.span_ms(2.0), 1_200);
        assert_eq!(window.span_ms(0.25), 8_200);
        // Rounded up, so the window never closes before the last frame.
        assert_eq!(
            PlayWindow {
                duration_ms: 999,
                blend_out_ms: 0,
            }
            .span_ms(1.5),
            666
        );
    }

    /// Overlays resolve as windows, in step order: one starts without ending
    /// another, a daemon that wakes mid-window joins at the offset the timeline
    /// says rather than from the top, and a window that has wholly passed is
    /// skipped rather than replayed.
    #[test]
    fn overlays_resolve_as_windows_in_step_order() {
        let script = script(
            vec![
                Step::new(0, Posture::Up),
                Step::play(1_000, Play::new("pod/nod")),
                Step::play(1_500, Play::new("pod/wiggle")),
            ],
            30_000,
        );
        // 2 s of motion and a 200 ms fade: [1000, 3200) and [1500, 3700).
        let active = |at| {
            script
                .overlays_at(at, window(2_000, 200))
                .into_iter()
                .map(|overlay| (overlay.play.name.clone(), overlay.elapsed_ms))
                .collect::<Vec<_>>()
        };

        assert_eq!(active(999), vec![]);
        assert_eq!(active(1_000), vec![("pod/nod".to_owned(), 0)]);
        assert_eq!(
            active(1_600),
            vec![("pod/nod".to_owned(), 600), ("pod/wiggle".to_owned(), 100),],
            "step order is composition order, and one overlay does not end the other"
        );
        assert_eq!(
            active(3_199),
            vec![
                ("pod/nod".to_owned(), 2_199),
                ("pod/wiggle".to_owned(), 1_699),
            ]
        );
        assert_eq!(
            active(3_200),
            vec![("pod/wiggle".to_owned(), 1_700)],
            "the window is half-open: the blend-out has finished"
        );
        assert_eq!(active(3_700), vec![], "wholly passed, never replayed");

        // A name the library cannot answer for occupies no window. Acceptance
        // refuses such a script outright, so this is the disposition of a case
        // that should not arrive.
        assert!(script.overlays_at(1_600, |_| None).is_empty());

        // The whole answer for one instant, fields included. `index` is a
        // position in `steps`, not an ordinal among the plays — the two agree
        // only for a script that is all plays, which no fixture here is, and
        // `check_overlays` names steps the same way. If they disagreed, the
        // acceptance refusal a publisher reads would point at the wrong step.
        let running = script.overlays_at(1_600, window(2_000, 200));
        assert_eq!(
            running[0],
            ActiveOverlay {
                index: 1,
                play: script.steps()[1].action.play().expect("a play step"),
                started_ms: 1_000,
                elapsed_ms: 600,
            }
        );
        assert_eq!(
            running[1],
            ActiveOverlay {
                index: 2,
                play: script.steps()[2].action.play().expect("a play step"),
                started_ms: 1_500,
                elapsed_ms: 100,
            }
        );
    }

    /// The concurrency cap counts overlaps rather than steps: a timeline that
    /// plays twenty motions one after another is never more than one deep, and
    /// the refusal names the instant it was reached.
    #[test]
    fn the_concurrency_cap_counts_overlapping_windows() {
        let mut steps = vec![Step::new(0, Posture::Up)];
        for index in 0..MAX_CONCURRENT_OVERLAYS {
            steps.push(Step::play(1_000 + index as u64 * 10, Play::new("pod/nod")));
        }
        let packed = script(steps.clone(), 30_000);
        assert_eq!(packed.check_overlays(window(2_000, 200)), Ok(()));

        // One more inside the same window is one too many.
        let mut crowded = steps;
        crowded.push(Step::play(1_100, Play::new("pod/nod")));
        assert_eq!(
            script(crowded, 30_000).check_overlays(window(2_000, 200)),
            Err(OverlayError::TooManyOverlays {
                at_ms: 1_100,
                count: MAX_CONCURRENT_OVERLAYS + 1,
            })
        );

        // The same five, spread out so none of them overlaps, are fine.
        let mut spread = vec![Step::new(0, Posture::Up)];
        for index in 0..=MAX_CONCURRENT_OVERLAYS {
            spread.push(Step::play(
                1_000 + index as u64 * 3_000,
                Play::new("pod/nod"),
            ));
        }
        assert_eq!(
            script(spread, 30_000).check_overlays(window(2_000, 200)),
            Ok(())
        );

        // A name the library does not hold is refused before any of that.
        assert_eq!(
            packed.check_overlays(|_| None),
            Err(OverlayError::UnknownMotion {
                index: 1,
                name: "pod/nod".to_owned(),
            })
        );
    }

    /// The refusals a publisher and an operator read, each naming what went
    /// wrong and the number that says so.
    #[test]
    fn the_overlay_refusals_name_their_numbers() {
        let printed = ScriptError::SpeedOutOfBounds {
            index: 2,
            speed: 9.0,
        }
        .to_string();
        assert!(printed.contains('9'), "{printed}");
        assert!(printed.contains("0.25"), "{printed}");

        let printed = OverlayError::TooManyOverlays {
            at_ms: 1_100,
            count: 5,
        }
        .to_string();
        assert!(printed.contains("1100"), "{printed}");
        assert!(printed.contains('4'), "{printed}");

        let printed = OverlayError::UnknownMotion {
            index: 1,
            name: "pod/nod".to_owned(),
        }
        .to_string();
        assert!(printed.contains("pod/nod"), "{printed}");

        assert_eq!(
            Play::at_speed("pod/nod", 1.5).to_string(),
            "play pod/nod at 1.50x"
        );
        assert_eq!(Play::new("pod/nod").to_string(), "play pod/nod");
    }

    /// A posture-only script round-trips unchanged: the encoding is
    /// byte-compatible with the shape that carried only postures.
    #[test]
    fn an_old_posture_only_script_is_unchanged_in_both_directions() {
        let old = r#"{"type":"motion-script","pod":"reachy00","seq":1,
                      "steps":[{"after_ms":0,"posture":"up"},
                               {"after_ms":6740,"posture":"stow"}],
                      "timeout_ms":30000}"#;
        let decoded = MotionScript::decode(old).expect("a lawful script");
        assert_eq!(
            decoded.steps(),
            [Step::new(0, Posture::Up), Step::new(6740, Posture::Stow)]
        );
        assert_eq!(decoded.base_at(0), Some(Base::Posture(Posture::Up)));
        assert!(decoded.overlays_at(0, window(2_000, 200)).is_empty());
        assert_eq!(decoded.check_overlays(|_| None), Ok(()));

        let value: serde_json::Value = serde_json::from_str(&decoded.encode()).expect("it is json");
        assert_eq!(value["steps"][0], json_step(0, "up"));
        assert_eq!(value["steps"][1], json_step(6740, "stow"));
    }

    fn json_step(after_ms: u64, posture: &str) -> serde_json::Value {
        serde_json::json!({ "after_ms": after_ms, "posture": posture })
    }
}
