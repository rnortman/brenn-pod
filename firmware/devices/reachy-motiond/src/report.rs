//! Where the daemon says what it is doing.
//!
//! Two streams, and the split is the point. This daemon is run in the foreground
//! over ssh with somebody watching the head move, and the same run is worth
//! reading back afterwards by machine.
//!
//! - **stderr** carries the narration: the motion libraries' own per-move and
//!   per-event lines, plus the daemon's, as prose in the order they happened.
//!   That is what an operator reads while it runs.
//! - **stdout** carries JSONL in this workspace's envelope — one object per
//!   line, `ts_ms` and `event` first. That is what a capture parses.
//!
//! Interleaving the two on one stream would give a reader neither. Separating
//! them costs a redirect.

use std::io::Write;

use serde_json::Value;

/// Where the daemon's two kinds of output go.
///
/// A trait rather than two free functions so a test can hold what was said.
/// Both halves take `&self` and neither answers: two threads narrate — the one
/// holding the servo bus and the one holding the attachment — and a thread
/// mid-move must never wait on the other's line.
pub trait Sink: Send + Sync {
    /// One line of narration for whoever is watching.
    fn line(&self, text: &str);

    /// One JSONL event. `fields` must be a JSON object; the envelope renders
    /// anything else as an encode error naming `event`, so a mistake is visible
    /// rather than silent.
    fn event(&self, event: &str, fields: &Value);
}

/// The process's own streams: narration to stderr, events to stdout.
#[derive(Debug, Clone, Copy)]
pub struct Streams;

impl Sink for Streams {
    fn line(&self, text: &str) {
        // One write per line: two would interleave with the other thread's.
        let _ = std::io::stderr().write_all(format!("{text}\n").as_bytes());
    }

    fn event(&self, event: &str, fields: &Value) {
        let _ = std::io::stdout().write_all(event_line(event, fields).as_bytes());
    }
}

/// One capture line: the workspace envelope, newline-terminated.
///
/// Separate from the write so the shape a capture parser keys on is assertable
/// without owning the process's stdout.
fn event_line(event: &str, fields: &Value) -> String {
    format!(
        "{}\n",
        pod_jsonl::format_line_at(pod_jsonl::now_ms(), event, fields)
    )
}

/// A sink that keeps what it was told, for tests that assert on it.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct Collect {
    said: std::sync::Mutex<Said>,
}

/// What a [`Collect`] has been told.
#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub struct Said {
    /// Narration, in order.
    pub lines: Vec<String>,
    /// Events, in order, as `(event, fields)`.
    pub events: Vec<(String, Value)>,
}

#[cfg(test)]
impl Collect {
    /// A copy of everything said so far.
    pub fn said(&self) -> Said {
        self.said
            .lock()
            .expect("no test panics holding this")
            .clone()
    }

    /// Whether an event of this name has been emitted.
    pub fn saw(&self, event: &str) -> bool {
        self.said().events.iter().any(|(name, _)| name == event)
    }

    /// The fields of the first event of this name.
    pub fn fields(&self, event: &str) -> Option<Value> {
        self.said()
            .events
            .iter()
            .find(|(name, _)| name == event)
            .map(|(_, fields)| fields.clone())
    }
}

#[cfg(test)]
impl Sink for Collect {
    fn line(&self, text: &str) {
        self.said
            .lock()
            .expect("no test panics holding this")
            .lines
            .push(text.to_owned());
    }

    fn event(&self, event: &str, fields: &Value) {
        self.said
            .lock()
            .expect("no test panics holding this")
            .events
            .push((event.to_owned(), fields.clone()));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_collecting_sink_keeps_both_kinds_in_order() {
        let sink = Collect::default();
        sink.line("first");
        sink.event("presence_intent", &json!({ "state": "engaged" }));
        sink.line("second");

        let said = sink.said();
        assert_eq!(said.lines, vec!["first".to_owned(), "second".to_owned()]);
        assert!(sink.saw("presence_intent"));
        assert!(!sink.saw("motion_fault"));
        assert_eq!(
            sink.fields("presence_intent"),
            Some(json!({"state":"engaged"}))
        );
    }

    /// The shape every capture parser for this daemon keys on: one object per
    /// line, `ts_ms` and `event` alongside the caller's fields, and exactly one
    /// newline so two records never glue together.
    #[test]
    fn a_capture_line_is_one_object_carrying_the_stamp_the_name_and_the_fields() {
        let line = event_line("presence_intent", &json!({ "state": "engaged" }));

        assert_eq!(line.matches('\n').count(), 1, "{line:?}");
        assert!(line.ends_with('\n'), "{line:?}");
        let parsed: Value = serde_json::from_str(line.trim_end()).expect("one JSON object");
        assert!(parsed["ts_ms"].is_u64(), "{parsed}");
        assert_eq!(parsed["event"], json!("presence_intent"));
        assert_eq!(parsed["state"], json!("engaged"));
    }

    /// What the [`Sink`] trait promises for a `fields` that is not an object: a
    /// line naming the event that produced it, rather than nothing at all.
    #[test]
    fn fields_that_are_not_an_object_render_as_an_error_naming_the_event() {
        let line = event_line("presence_intent", &json!("not an object"));

        let parsed: Value = serde_json::from_str(line.trim_end()).expect("one JSON object");
        assert_eq!(parsed["event"], json!("jsonl_encode_error"));
        assert_eq!(parsed["target"], json!("presence_intent"));
    }
}
