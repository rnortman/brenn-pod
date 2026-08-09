//! The message body: a timed motion script, and how it survives the wire.
//!
//! JSON, because that is what a bus body is here, and with a `"type"` field
//! because this channel carries more than one kind of intent over time. A
//! consumer that filters on the discriminator keeps working when a second kind
//! arrives; one that assumed every body on the channel was a script would start
//! misreading them.
//!
//! A script is three things: whose head it is about, an ordering number, and a
//! timeline — zero or more postures at offsets from the moment the script
//! arrives, plus the timeout after which the head goes back down whether or not
//! anything else arrives. The timeout is an **unconditional ceiling on the
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

/// One posture, due at an offset from the script's arrival.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    /// Milliseconds after receipt at which this posture becomes the desired
    /// one.
    pub after_ms: u64,
    /// What the head should be doing from then until the next step.
    pub posture: Posture,
}

impl Step {
    /// A step naming `posture` at `after_ms` past receipt.
    #[must_use]
    pub const fn new(after_ms: u64, posture: Posture) -> Self {
        Self { after_ms, posture }
    }
}

/// Why a script is not one, even though it decoded.
///
/// Separate from [`DecodeError`]'s parse refusals because these are the
/// scripter's bugs rather than the wire's: the text arrived intact and says
/// something no machine should execute.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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
}

/// One motion script, addressed to one pod.
///
/// Construct through [`MotionScript::new`] or [`MotionScript::decode`]; both
/// validate, so a value of this type is always a lawful timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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

    /// The posture this script asks for `elapsed_ms` after it arrived, or
    /// `None` while no step has come due.
    ///
    /// The last due step wins, so steps whose time had already passed when the
    /// script landed collapse into the one that matters — a daemon that was
    /// away for a second does not replay a timeline at it.
    #[must_use]
    pub fn posture_at(&self, elapsed_ms: u64) -> Option<Posture> {
        self.steps
            .iter()
            .rev()
            .find(|step| step.after_ms <= elapsed_ms)
            .map(|step| step.posture)
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

/// The four rules a script has to keep, in one place so the constructor and
/// the decoder cannot come to different conclusions about the same script.
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script(steps: Vec<Step>, timeout_ms: u64) -> MotionScript {
        MotionScript::new("reachy00", 1, steps, timeout_ms).expect("a lawful script")
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

        assert_eq!(script.posture_at(0), None, "nothing is due yet");
        assert_eq!(script.posture_at(499), None);
        assert_eq!(script.posture_at(500), Some(Posture::Up));
        assert_eq!(script.posture_at(6739), Some(Posture::Up));
        assert_eq!(script.posture_at(6740), Some(Posture::Stow));
        assert_eq!(script.posture_at(600_000), Some(Posture::Stow));

        // A script that landed late collapses to the one posture that matters
        // rather than replaying its timeline.
        assert_eq!(script.posture_at(10_000), Some(Posture::Stow));
    }

    /// An empty timeline never asks for a posture. It is a lawful script whose
    /// whole content is its timeout.
    #[test]
    fn an_empty_timeline_commands_nothing() {
        let script = script(vec![], 30_000);
        assert_eq!(script.posture_at(0), None);
        assert_eq!(script.posture_at(u64::MAX), None);
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
        assert_eq!(inside.posture_at(4_999), Some(Posture::Stow));

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
}
