//! The message body: what a presence intent says, and how it survives the wire.
//!
//! JSON, because that is what a bus body is here, and with a `"type"` field
//! because this channel is expected to carry more than presence eventually —
//! gaze and direction-of-arrival intents are the named next tenants. A consumer
//! that filters on the discriminator keeps working when they arrive; one that
//! assumed every body on the channel was a presence body would start
//! misreading them.
//!
//! Decoding is tolerant in one direction only. Unknown *fields* are ignored, so
//! a newer publisher may add one without a lockstep deploy. An unknown *state*
//! is a refusal: the states are the whole meaning of the message, and guessing
//! at one nobody has defined would move a head on a guess.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The discriminator every presence body carries.
pub const PRESENCE_TYPE: &str = "presence";

/// What the head should be doing.
///
/// Two states and no third: attending, or stowed. Anything richer — where to
/// look, which emote to play — is a different message type on the same channel,
/// not another value here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PresenceState {
    /// An interaction is live: the head belongs up.
    Engaged,
    /// Nothing is happening: the head belongs stowed.
    Idle,
}

impl PresenceState {
    /// The state as the wire spells it.
    ///
    /// Defined here, beside the serde rename that decides the spelling, because
    /// every consumer that logs a state would otherwise hand-copy it: a JSONL
    /// line whose spelling drifts from the wire's stops joining against the
    /// publisher's capture, and the drift is silent.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Engaged => "engaged",
            Self::Idle => "idle",
        }
    }
}

impl std::fmt::Display for PresenceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One presence intent, addressed to one pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceBody {
    /// Whose head this is about. A consumer obeys its own id and reports the
    /// rest; the channel is not assumed to carry one machine's traffic.
    pub pod: String,
    /// The desired state.
    pub state: PresenceState,
    /// The publisher's counter for this stream of intents. Carried for
    /// observability — it makes a log of deliveries readable as a sequence, and
    /// it makes a gap visible. It is deliberately not authority over which
    /// intent wins: the lease's arrival order is, and a publisher that restarts
    /// counts from zero again, which would leave a seq-ordered consumer
    /// ignoring every intent it ever sent afterwards.
    pub seq: u64,
}

/// The JSON shape, kept separate from the public struct so the discriminator is
/// a wire detail rather than a field a caller has to set correctly.
///
/// No `deny_unknown_fields`: tolerance of unknown fields is the point.
#[derive(Serialize, Deserialize)]
struct Wire {
    #[serde(rename = "type")]
    kind: String,
    pod: String,
    state: PresenceState,
    seq: u64,
}

/// Why a body did not decode.
///
/// Three refusals rather than one string, because they mean different things to
/// whoever reads the log line: the channel carried something that is not JSON,
/// something that is JSON of another kind, or a presence body with a field
/// missing or unreadable.
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
    #[error("the body is a `{kind}` message, not a `{PRESENCE_TYPE}` one")]
    WrongType {
        /// The discriminator the body carried.
        kind: String,
    },

    /// The body claims to be a presence body and does not hold one.
    #[error("the body is not a well-formed `{PRESENCE_TYPE}` message: {detail}")]
    Malformed {
        /// What the deserializer said.
        detail: String,
    },
}

impl PresenceBody {
    /// A body for `pod` in `state`, numbered `seq`.
    pub fn new(pod: impl Into<String>, state: PresenceState, seq: u64) -> Self {
        Self {
            pod: pod.into(),
            state,
            seq,
        }
    }

    /// This body as the JSON text a bus body carries.
    ///
    /// Infallible: every field is a string, an enum, or an integer, and none of
    /// them can fail to serialize. The `expect` documents that rather than
    /// pushing an impossible error onto the publisher.
    #[must_use]
    pub fn encode(&self) -> String {
        let wire = Wire {
            kind: PRESENCE_TYPE.to_owned(),
            pod: self.pod.clone(),
            state: self.state,
            seq: self.seq,
        };
        serde_json::to_string(&wire).expect("a presence body holds nothing that can fail to encode")
    }

    /// A body out of the JSON text a delivery carried.
    pub fn decode(text: &str) -> Result<Self, DecodeError> {
        // Two passes: the discriminator first, so a body of another kind is
        // reported as another kind rather than as a malformed presence body —
        // the difference between "not for me" and "somebody broke the
        // publisher".
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
        if kind != PRESENCE_TYPE {
            return Err(DecodeError::WrongType {
                kind: kind.to_owned(),
            });
        }

        let wire: Wire = serde_json::from_value(value).map_err(|error| DecodeError::Malformed {
            detail: error.to_string(),
        })?;
        Ok(Self {
            pod: wire.pod,
            state: wire.state,
            seq: wire.seq,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What one side writes, the other side reads — for both states, which is
    /// the whole of the vocabulary today.
    #[test]
    fn a_body_survives_the_wire() {
        for state in [PresenceState::Engaged, PresenceState::Idle] {
            let body = PresenceBody::new("reachy00", state, 7);
            let text = body.encode();
            assert_eq!(PresenceBody::decode(&text).expect("it decodes"), body);
        }
    }

    /// The encoded shape, spelled out field by field rather than inferred from
    /// a round trip: a round trip agrees with itself even
    /// when both halves are wrong, and the other end of this channel is written
    /// against the text, not against this crate's opinion of it.
    #[test]
    fn the_encoding_is_the_documented_one() {
        let text = PresenceBody::new("reachy00", PresenceState::Engaged, 42).encode();
        let value: serde_json::Value = serde_json::from_str(&text).expect("it is json");
        assert_eq!(value["type"], "presence");
        assert_eq!(value["pod"], "reachy00");
        assert_eq!(value["state"], "engaged");
        assert_eq!(value["seq"], 42);

        let idle = PresenceBody::new("reachy00", PresenceState::Idle, 0).encode();
        let value: serde_json::Value = serde_json::from_str(&idle).expect("it is json");
        assert_eq!(value["state"], "idle");
    }

    /// The spelling a consumer logs and the spelling that crosses the wire are
    /// the same one, checked against the encoding rather than against a second
    /// copy of the table.
    #[test]
    fn the_state_names_itself_the_way_the_wire_does() {
        for state in [PresenceState::Engaged, PresenceState::Idle] {
            let text = PresenceBody::new("reachy00", state, 1).encode();
            let value: serde_json::Value = serde_json::from_str(&text).expect("it is json");
            assert_eq!(value["state"], state.as_str());
            assert_eq!(state.to_string(), state.as_str());
        }
    }

    /// A field this consumer has never heard of is not a reason to drop the
    /// intent. The channel is meant to grow, and the publisher that grows first
    /// must not take the head down with it.
    #[test]
    fn a_field_nobody_here_knows_is_ignored() {
        let text = r#"{"type":"presence","pod":"reachy00","state":"engaged","seq":3,
                       "reason":"wake","gaze":{"az":0.2,"el":-0.1}}"#;
        let body = PresenceBody::decode(text).expect("the fields it needs are all there");
        assert_eq!(
            body,
            PresenceBody::new("reachy00", PresenceState::Engaged, 3)
        );
    }

    /// Another tenant of the same channel is reported as another tenant. The
    /// consumer skips it; nothing about it is a failure.
    #[test]
    fn a_message_of_another_kind_says_so() {
        let text = r#"{"type":"gaze","pod":"reachy00","az":0.2}"#;
        match PresenceBody::decode(text).expect_err("not a presence body") {
            DecodeError::WrongType { kind } => assert_eq!(kind, "gaze"),
            other => panic!("expected a wrong-type report, got {other}"),
        }
    }

    /// The three malformed shapes, each reported as what it is. A state nobody
    /// has defined is refused rather than guessed at: the states are the
    /// message's whole meaning, and a guess here moves a head.
    #[test]
    fn a_body_that_is_not_one_is_reported_rather_than_guessed_at() {
        let not_json = PresenceBody::decode("{not json").expect_err("it is not json");
        assert!(
            matches!(not_json, DecodeError::NotJson { .. }),
            "{not_json}"
        );

        let untyped = PresenceBody::decode(r#"{"pod":"reachy00","state":"idle","seq":1}"#)
            .expect_err("no discriminator");
        assert!(
            matches!(untyped, DecodeError::Malformed { .. }),
            "{untyped}"
        );

        for text in [
            r#"{"type":"presence","pod":"reachy00","seq":1}"#,
            r#"{"type":"presence","state":"idle","seq":1}"#,
            r#"{"type":"presence","pod":"reachy00","state":"idle"}"#,
            r#"{"type":"presence","pod":"reachy00","state":"lurking","seq":1}"#,
            r#"{"type":"presence","pod":"reachy00","state":"idle","seq":-1}"#,
        ] {
            let refused = PresenceBody::decode(text).expect_err("a field is missing or unreadable");
            assert!(
                matches!(refused, DecodeError::Malformed { .. }),
                "{text}: {refused}"
            );
        }
    }

    /// The refusal reads as a sentence naming the message type, because it goes
    /// into a log line an operator reads without this file open beside it.
    #[test]
    fn a_refusal_names_what_was_expected() {
        let refused = PresenceBody::decode(r#"{"type":"gaze"}"#).expect_err("not presence");
        let printed = refused.to_string();
        assert!(printed.contains("presence"), "{printed}");
        assert!(printed.contains("gaze"), "{printed}");
    }
}
