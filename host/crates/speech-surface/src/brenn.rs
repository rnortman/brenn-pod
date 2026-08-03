//! The bus side of the `BrennBrain`: the [`BrainLink`] implementation that turns
//! the brain's transport-free vocabulary into bridge publishes.
//!
//! The split is deliberate. `speech-pipeline` owns the turn protocol and the wire
//! bodies and knows nothing about a bridge; this module owns the bridge and knows
//! nothing about turns. Everything asynchronous stays here too: the brain's
//! `wake` and `barge_declined` hooks run on the pipeline's select loop and must
//! not await, so those two publishes are queued as [`Notice`]s for the bridge
//! driver to carry.

pub mod driver;
/// In-crate model bus peer. Crate-visible so the server's wiring tests can
/// mint a real [`BridgeHandle`] without a socket.
#[cfg(test)]
pub(crate) mod scripted;

use brenn_bridge::{BridgeHandle, PublishOutcome, PublishRequest, Urgency};
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::json;
use speech_pipeline::{BrainLink, LinkError};
use tokio::sync::mpsc;

use crate::jsonl::JsonlHandle;

/// Depth of the fire-and-forget notice queue between the link and the driver.
///
/// Small on purpose. Both notice kinds are advisory and rare — one per wake, one
/// per interrupted turn that produced no command — so a backlog here means the
/// bridge is already wedged, and a deep queue would only delay stale nudges
/// further instead of dropping them.
pub const NOTICE_QUEUE_DEPTH: usize = 4;

/// An outbound message the driver publishes on the link's behalf, because the
/// caller could not await one.
///
/// One queue for both kinds: neither gates a turn, and interleaving them costs
/// nothing a separate queue would buy back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notice {
    /// A wake nudge, for the wake channel at [`Urgency::Low`].
    Wake(String),
    /// An interruption notice, for the publish channel at [`Urgency::High`] —
    /// conversation content, ordered with the utterances it sits between.
    Interruption(String),
}

/// The brain's transport: publishes on a [`BridgeHandle`], and queues what it
/// cannot await.
#[derive(Clone)]
pub struct BridgeLink {
    handle: BridgeHandle,
    publish_channel: String,
    attribution: Option<String>,
    notice_tx: mpsc::Sender<Notice>,
    jsonl: JsonlHandle,
}

impl BridgeLink {
    /// Build a link over `handle`, returning it with the notice receiver the
    /// bridge driver drains. The queue is created here so its depth has one owner
    /// and the two ends cannot be paired wrongly.
    pub fn new(
        handle: BridgeHandle,
        publish_channel: String,
        attribution: Option<String>,
        jsonl: JsonlHandle,
    ) -> (BridgeLink, mpsc::Receiver<Notice>) {
        let (notice_tx, notice_rx) = mpsc::channel(NOTICE_QUEUE_DEPTH);
        (
            BridgeLink {
                handle,
                publish_channel,
                attribution,
                notice_tx,
                jsonl,
            },
            notice_rx,
        )
    }

    /// Queue a notice, or report the drop. `event` names the JSONL line so the
    /// two kinds stay distinguishable in the stream (their loudness differs: a
    /// lost wake nudge costs latency, a lost interruption notice costs
    /// conversational truth).
    fn queue(&self, event: &str, notice: Notice) {
        if let Err(err) = self.notice_tx.try_send(notice) {
            let reason = match err {
                mpsc::error::TrySendError::Full(_) => "queue_full",
                mpsc::error::TrySendError::Closed(_) => "driver_gone",
            };
            self.jsonl.emit(event, &json!({ "reason": reason }));
        }
    }
}

impl BrainLink for BridgeLink {
    /// Publish the utterance and wait for the peer's own word on it.
    ///
    /// A refusal reaches the turn as a failure, which is what parks it for its
    /// whole response budget otherwise: see [`publish_once`].
    fn publish_utterance(&self, body: String) -> BoxFuture<'static, Result<(), LinkError>> {
        let handle = self.handle.clone();
        let request = PublishRequest {
            channel: self.publish_channel.clone(),
            attribution: self.attribution.clone(),
            body,
            urgency: Urgency::High,
        };
        async move { publish_once(&handle, request).await.map_err(LinkError::new) }.boxed()
    }

    fn notify_wake(&self, body: String) {
        self.queue("brenn_wake_dropped", Notice::Wake(body));
    }

    fn notify_interruption(&self, body: String) {
        self.queue("brenn_interruption_dropped", Notice::Interruption(body));
    }
}

/// One publish, with the peer's refusal rendered for an operator.
///
/// Every non-`Ok` [`PublishOutcome`] is a failure: in all of them the message was
/// not delivered. Both publish paths — the awaited utterance and the driver's
/// fire-and-forget notices and help document — end here, so that policy has one
/// statement to keep true when the outcome set grows.
pub(crate) async fn publish_once(
    handle: &BridgeHandle,
    request: PublishRequest,
) -> Result<(), String> {
    match handle.publish(request).await {
        Ok(PublishOutcome::Ok) => Ok(()),
        Ok(refused) => Err(describe_outcome(refused)),
        Err(err) => Err(err.to_string()),
    }
}

/// Render a publish outcome for an operator. [`PublishOutcome`] carries no
/// `Display`, and its `Debug` is not a sentence; the sizes on `BodyTooLarge` are
/// the whole diagnosis when a body outgrows the bus, so they are kept.
pub(crate) fn describe_outcome(outcome: PublishOutcome) -> String {
    match outcome {
        PublishOutcome::Ok => "accepted".to_string(),
        PublishOutcome::RateLimited => "the peer rate-limited the publish".to_string(),
        PublishOutcome::BodyTooLarge { len, max } => {
            format!("the body is {len} bytes, over the peer's {max}-byte limit")
        }
        PublishOutcome::Failed => "the peer accepted the frame but the publish failed".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use brenn_bridge::Bridge;

    use super::scripted::{ScriptedConnector, scripted};
    use super::*;
    use crate::config::JsonlSink;

    /// A link over a bridge that never connects, its notice receiver, the JSONL
    /// path, and the pieces a test must keep alive: the bridge (its command
    /// receiver keeps the handle live) and the writer join.
    ///
    /// These tests exercise the link's own judgement — what it queues, and what it
    /// says when it cannot — and none of them needs a socket. An empty script gives
    /// exactly that: the scripted connector parks once its attempts are spent, so
    /// nothing re-dials in the background behind the assertions.
    struct Fixture {
        link: BridgeLink,
        notices: mpsc::Receiver<Notice>,
        jsonl_path: std::path::PathBuf,
        _dir: tempfile::TempDir,
        _bridge: Bridge<ScriptedConnector>,
        writer: tokio::task::JoinHandle<()>,
    }

    async fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("events.jsonl");
        let (jsonl, writer) = crate::jsonl::spawn_quiet(&JsonlSink::File(jsonl_path.clone()))
            .await
            .unwrap();
        let (bridge, handle, _events, _peers) = scripted(&[], 1);
        let (link, notices) = BridgeLink::new(
            handle,
            "brenn:pod.utterance".to_string(),
            Some("voice".to_string()),
            jsonl,
        );
        Fixture {
            link,
            notices,
            jsonl_path,
            _dir: dir,
            _bridge: bridge,
            writer,
        }
    }

    impl Fixture {
        /// Drop the emit handle, drain the writer, and read back the event lines.
        async fn lines(self) -> Vec<serde_json::Value> {
            let Fixture {
                link,
                notices,
                jsonl_path,
                _dir,
                _bridge,
                writer,
            } = self;
            drop(link);
            drop(notices);
            writer.await.unwrap();
            std::fs::read_to_string(&jsonl_path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).expect("each line is JSON"))
                .collect()
        }
    }

    #[tokio::test]
    async fn a_wake_nudge_is_queued_for_the_driver() {
        let mut fx = fixture().await;
        fx.link.notify_wake("{\"type\":\"wake\"}".to_string());
        assert_eq!(
            fx.notices.try_recv().unwrap(),
            Notice::Wake("{\"type\":\"wake\"}".to_string())
        );
        assert!(
            fx.lines().await.is_empty(),
            "a queued nudge is not an event"
        );
    }

    #[tokio::test]
    async fn an_interruption_notice_is_queued_for_the_driver() {
        let mut fx = fixture().await;
        fx.link
            .notify_interruption("{\"type\":\"interruption\"}".to_string());
        assert_eq!(
            fx.notices.try_recv().unwrap(),
            Notice::Interruption("{\"type\":\"interruption\"}".to_string())
        );
    }

    #[tokio::test]
    async fn a_full_queue_drops_the_newest_notice_loudly() {
        let fx = fixture().await;
        for i in 0..NOTICE_QUEUE_DEPTH {
            fx.link.notify_wake(format!("wake-{i}"));
        }
        fx.link.notify_wake("overrun".to_string());
        fx.link.notify_interruption("overrun".to_string());

        let lines = fx.lines().await;
        let events: Vec<&str> = lines
            .iter()
            .map(|line| line["event"].as_str().unwrap())
            .collect();
        assert_eq!(
            events,
            vec!["brenn_wake_dropped", "brenn_interruption_dropped"],
            "{lines:?}"
        );
        for line in &lines {
            assert_eq!(line["reason"], "queue_full", "{line}");
        }
    }

    #[tokio::test]
    async fn a_departed_driver_is_reported_not_silent() {
        let mut fx = fixture().await;
        fx.notices.close();
        fx.link.notify_wake("wake".to_string());
        fx.link.notify_interruption("interruption".to_string());

        let lines = fx.lines().await;
        let reasons: Vec<(&str, &str)> = lines
            .iter()
            .map(|line| {
                (
                    line["event"].as_str().unwrap(),
                    line["reason"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            reasons,
            vec![
                ("brenn_wake_dropped", "driver_gone"),
                ("brenn_interruption_dropped", "driver_gone"),
            ]
        );
    }

    #[test]
    fn every_refusal_renders_as_a_sentence_with_its_numbers() {
        assert_eq!(
            describe_outcome(PublishOutcome::RateLimited),
            "the peer rate-limited the publish"
        );
        assert_eq!(
            describe_outcome(PublishOutcome::BodyTooLarge { len: 40, max: 32 }),
            "the body is 40 bytes, over the peer's 32-byte limit"
        );
        assert_eq!(
            describe_outcome(PublishOutcome::Failed),
            "the peer accepted the frame but the publish failed"
        );
    }
}
