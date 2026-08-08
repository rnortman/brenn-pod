//! The canonical JSONL rendering of the facts this crate hands its embedders.
//!
//! The crate still emits nothing and logs nothing: these are functions an
//! embedder calls from its own sink, at its own event names. What they buy is
//! one spelling of the fields across every binary that embeds a bridge — a
//! capture off the motion daemon and a capture off the speech surface describe
//! an attachment with the same keys, so a reader joining two captures is not
//! reconciling two hand-copies of the same struct. Rendering each embedder's
//! own way is exactly how one of them came to drop two fields the other kept.

use brenn_attach_client::conn::{AttachmentFacts, DetachReason};
use brenn_attach_proto::{GapInfo, GapReason};
use serde_json::{Value, json};

/// What an attachment negotiated, as JSONL fields.
#[must_use]
pub fn attached_fields(facts: &AttachmentFacts) -> Value {
    json!({
        "participant_id": facts.participant_id,
        "session_id": facts.session_id,
        "version": facts.version,
        "heartbeat_secs": facts.heartbeat_secs,
        "max_body_bytes": facts.max_body_bytes,
        "max_frame_bytes": facts.max_frame_bytes,
        "alert_granted": facts.alert_granted,
    })
}

/// Why an attachment ended, as JSONL fields.
#[must_use]
pub fn detached_fields(reason: &DetachReason) -> Value {
    match reason {
        DetachReason::LivenessTimeout => json!({ "reason": "liveness_timeout" }),
        // Peer-supplied text: rendered as text, never interpolated anywhere.
        DetachReason::TransportClosed { code, reason } => json!({
            "reason": "transport_closed",
            "code": code,
            "detail": reason,
        }),
    }
}

/// Why a subscription's replay was gapped. A staleness report, not an error.
#[must_use]
pub fn gap_reason(gap: &GapInfo) -> &'static str {
    match gap.reason {
        GapReason::EpochChanged => "epoch_changed",
        GapReason::BeyondRetained => "beyond_retained",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every field of the negotiated facts reaches the line. The assertion is
    /// the count as well as the names: a field added upstream and forgotten
    /// here is the divergence this module exists to prevent.
    #[test]
    fn an_attachment_renders_every_fact_it_negotiated() {
        let facts = AttachmentFacts {
            participant_id: "reachy-motiond".to_owned(),
            session_id: "0198f0".to_owned(),
            version: 1,
            heartbeat_secs: 20,
            max_body_bytes: 65_536,
            max_frame_bytes: 131_072,
            alert_granted: true,
        };

        let fields = attached_fields(&facts);
        let object = fields.as_object().expect("an object");
        assert_eq!(object.len(), 7, "{fields}");
        assert_eq!(fields["participant_id"], json!("reachy-motiond"));
        assert_eq!(fields["heartbeat_secs"], json!(20));
        assert_eq!(fields["max_frame_bytes"], json!(131_072));
        assert_eq!(fields["alert_granted"], json!(true));
    }

    #[test]
    fn a_detachment_says_which_of_the_two_shapes_it_was() {
        assert_eq!(
            detached_fields(&DetachReason::LivenessTimeout),
            json!({ "reason": "liveness_timeout" })
        );
        assert_eq!(
            detached_fields(&DetachReason::TransportClosed {
                code: Some(1006),
                reason: "no close frame".to_owned(),
            }),
            json!({
                "reason": "transport_closed",
                "code": 1006,
                "detail": "no close frame",
            })
        );
    }

    #[test]
    fn a_gap_names_what_made_it() {
        assert_eq!(
            gap_reason(&GapInfo {
                reason: GapReason::EpochChanged,
            }),
            "epoch_changed"
        );
    }
}
