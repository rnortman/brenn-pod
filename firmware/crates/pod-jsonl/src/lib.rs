//! The host workspace's JSONL event-line envelope: `{"ts_ms", "event", ...fields}`.
//!
//! One owner of the shape, so every binary that narrates itself in JSONL — the
//! speech surface's async sink, its offline export tools, the bus probe — speaks
//! one dialect. A consumer parses lines from any of them off the same two keys,
//! and the next evolution of the envelope reaches all of them at once instead of
//! whichever copy the author happened to be editing.
//!
//! A leaf by construction: serde and serde_json, nothing else. Crates that carry
//! weight of their own depend on this one, never the reverse.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// The wire envelope: `ts_ms` and `event` first, then the caller's fields
/// flattened in. `fields` must serialize to a JSON object.
#[derive(Serialize)]
struct Envelope<'a, T: Serialize> {
    ts_ms: u64,
    event: &'a str,
    #[serde(flatten)]
    fields: T,
}

/// Serialize one event into the envelope at a caller-supplied `ts_ms`.
///
/// The stamp is the caller's: a caller teeing one event to two sinks stamps once
/// and hands both the same reading, and a caller with a clock domain of its own
/// stamps from that rather than from this crate's wall clock. Callers with
/// neither concern use [`now_ms`].
///
/// A serialization failure — including a `fields` that is not a JSON object, and
/// so cannot be flattened — yields a self-describing `jsonl_encode_error` line
/// naming the event that produced it, so the miss is visible rather than silent.
/// The fallback is built through serde too: interpolating a serde error into a
/// hand-written literal could itself emit malformed JSON.
pub fn format_line_at<T: Serialize>(ts_ms: u64, event: &str, fields: &T) -> String {
    match serde_json::to_string(&Envelope {
        ts_ms,
        event,
        fields,
    }) {
        Ok(line) => line,
        Err(err) => serde_json::json!({
            "ts_ms": ts_ms,
            "event": "jsonl_encode_error",
            "target": event,
            "detail": err.to_string(),
        })
        .to_string(),
    }
}

/// Milliseconds since the UNIX epoch, or 0 on a clock reading before it — a
/// stamp is a diagnostic aid, never a reason to refuse a line.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn parsed(line: &str) -> Value {
        serde_json::from_str(line).expect("the envelope is JSON")
    }

    #[test]
    fn the_envelope_carries_the_stamp_the_event_and_the_flattened_fields() {
        let line = parsed(&format_line_at(
            1_700_000_000_123,
            "segment_opened",
            &json!({ "segment_id": 7, "is_resume": false }),
        ));
        assert_eq!(line["ts_ms"], 1_700_000_000_123u64);
        assert_eq!(line["event"], "segment_opened");
        assert_eq!(line["segment_id"], 7);
        assert_eq!(line["is_resume"], false);
    }

    #[test]
    fn a_typed_struct_flattens_like_a_map() {
        #[derive(Serialize)]
        struct Fields<'a> {
            pod_id: &'a str,
        }
        let line = parsed(&format_line_at(
            0,
            "conn_hello",
            &Fields {
                pod_id: "pod-a1b2c3",
            },
        ));
        assert_eq!(line["pod_id"], "pod-a1b2c3");
    }

    #[test]
    fn fields_that_cannot_flatten_yield_a_visible_encode_error() {
        // A bare value has no keys to flatten, so the envelope cannot be built.
        // The line still lands, and it names the event that was lost.
        let line = parsed(&format_line_at(5, "odd", &json!(42)));
        assert_eq!(line["event"], "jsonl_encode_error");
        assert_eq!(line["target"], "odd");
        assert_eq!(line["ts_ms"], 5);
        assert!(line["detail"].is_string(), "{line}");
    }

    #[test]
    fn a_failing_serializer_is_reported_rather_than_panicking() {
        struct AlwaysErr;
        impl Serialize for AlwaysErr {
            fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("unserializable"))
            }
        }
        let line = parsed(&format_line_at(0, "stt_failed", &AlwaysErr));
        assert_eq!(line["event"], "jsonl_encode_error");
        assert_eq!(line["target"], "stt_failed");
    }

    #[test]
    fn the_wall_clock_stamp_is_after_the_epoch() {
        // 2020-01-01, comfortably behind any host that can build this crate and
        // far enough ahead of 0 to catch a stamp that silently fell back.
        assert!(now_ms() > 1_577_836_800_000, "{}", now_ms());
    }
}
