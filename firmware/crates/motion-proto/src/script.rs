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
//! anything else arrives. A timeline running past its own timeout carries the
//! lapse out to its last step, so the bound is the later of the two;
//! [`MotionScript::expiry_ms`] is the one place that arithmetic lives. There is
//! no vocabulary here for a conversation, a lease, or a turn: the daemon
//! executes timed posture intents and knows nothing else.
//!
//! Tolerance runs in one direction only. Unknown *fields* are ignored, so a
//! newer scripter may add one without a lockstep deploy. An unknown *posture* is
//! a refusal: the postures are the whole meaning of the message, and guessing at
//! one nobody has defined would move a head on a guess.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The discriminator every motion script carries.
pub const MOTION_SCRIPT_TYPE: &str = "motion-script";

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

    /// The offset at which this script lapses.
    ///
    /// The timeout, except where the timeline outlasts it: a script never
    /// expires with a step still unexecuted, because a timeline that stopped
    /// short would be a script whose own instructions never ran. The last step
    /// therefore stands for at least the millisecond after it comes due — the
    /// expiry is one past it, not level with it, or the lapse would resolve
    /// first and swallow the step it was waiting for. The ordinary script's
    /// last step is the stow that ends it well inside the timeout, and this
    /// arithmetic never fires for one.
    ///
    /// TODO(script-timeout-bound): a timeline that outlasts its own timeout
    /// carries the head past the instant the timeout named, so the bound on the
    /// head's exposure is the larger of the two numbers the script chose rather
    /// than the timeout alone.
    #[must_use]
    pub fn expiry_ms(&self) -> u64 {
        let after_last = self
            .steps
            .last()
            .map_or(0, |step| step.after_ms.saturating_add(1));
        self.timeout_ms.max(after_last)
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

/// The two rules a timeline has to keep, in one place so the constructor and
/// the decoder cannot come to different conclusions about the same script.
fn validate(steps: &[Step], timeout_ms: u64) -> Result<(), ScriptError> {
    if timeout_ms == 0 {
        return Err(ScriptError::TimeoutNotPositive { timeout_ms });
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

    /// A script never lapses with a step still unexecuted: a timeout shorter
    /// than its own timeline would be a script whose instructions never ran.
    ///
    /// The expiry is one millisecond *past* the last step and not level with
    /// it, because the lapse is checked first: level with it, the step would
    /// resolve as a lapse rather than as the posture it names, and the script's
    /// last instruction would be swallowed on every such timeline.
    #[test]
    fn a_timeline_outlasting_its_timeout_still_runs_to_its_end() {
        let script = script(
            vec![Step::new(0, Posture::Up), Step::new(9_000, Posture::Stow)],
            5_000,
        );
        assert_eq!(script.expiry_ms(), 9_001);
        assert_eq!(script.posture_at(9_000), Some(Posture::Stow));
    }

    /// The same, for a last step that is not the stow the ordinary script ends
    /// on: the vocabulary is meant to grow, and a swallowed final step would be
    /// invisible for as long as every script happened to end in a stow.
    #[test]
    fn a_final_step_that_is_not_a_stow_survives_a_short_timeout() {
        let script = script(vec![Step::new(9_000, Posture::Up)], 5_000);
        assert_eq!(script.expiry_ms(), 9_001);
        assert_eq!(script.posture_at(9_000), Some(Posture::Up));
    }
}
