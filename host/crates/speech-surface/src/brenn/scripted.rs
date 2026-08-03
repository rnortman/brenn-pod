//! A scripted bus peer, for the tests that drive the bridge from outside the
//! bridge's own crate.
//!
//! The transport is a pair of channels, so a test states exactly what the server
//! said and exactly when the socket died. Frames are handled as JSON values
//! rather than the typed `ClientFrame`/`ServerFrame`: those types belong to the
//! pinned upstream crates, which only `brenn-bridge` may depend on, so this peer
//! reads and writes the wire as text — which is also what a real peer does.

use std::collections::VecDeque;
use std::time::Duration;

use brenn_bridge::{
    Bridge, BridgeEvent, BridgeHandle, ConnConfig, TransportConnection, TransportConnector,
    TransportError, TransportEvent,
};
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Long enough that reaching it means something is wedged, not slow: this peer
/// answers in microseconds.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// What one connect attempt does.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Attempt {
    /// The socket opens and the test drives the peer behind it.
    Open,
    /// The connect fails before a socket exists.
    Fail,
}

/// The bridge's end of one scripted socket.
struct Wire {
    inbound: UnboundedReceiver<TransportEvent>,
    sent: UnboundedSender<String>,
}

pub(crate) struct ScriptedConnector {
    attempts: VecDeque<Option<Wire>>,
}

pub(crate) struct ScriptedConnection {
    wire: Wire,
}

impl TransportConnector for ScriptedConnector {
    type Conn = ScriptedConnection;

    async fn connect(&mut self, _url: &str) -> Result<ScriptedConnection, TransportError> {
        match self.attempts.pop_front() {
            Some(Some(wire)) => Ok(ScriptedConnection { wire }),
            Some(None) => Err(TransportError::new("scripted connect failure")),
            // The script is spent. Pending rather than failing keeps the tail of
            // a test quiet: no re-dial storm behind the assertions that follow.
            None => std::future::pending().await,
        }
    }
}

impl TransportConnection for ScriptedConnection {
    async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
        self.wire
            .sent
            .send(text)
            .map_err(|_| TransportError::new("the scripted peer is gone"))
    }

    async fn next_event(&mut self) -> TransportEvent {
        match self.wire.inbound.recv().await {
            Some(event) => event,
            None => TransportEvent::Failed("the scripted peer is gone".to_string()),
        }
    }

    async fn close(&mut self) {
        self.wire.inbound.close();
    }
}

/// The test's end of one scripted socket.
pub(crate) struct Peer {
    inbound: UnboundedSender<TransportEvent>,
    sent: UnboundedReceiver<String>,
}

impl Peer {
    /// The next frame the bridge wrote, as JSON.
    pub(crate) async fn next_frame(&mut self) -> Value {
        let text = tokio::time::timeout(WAIT, self.sent.recv())
            .await
            .expect("the bridge wrote a frame before the timeout")
            .expect("the bridge's socket is still open");
        serde_json::from_str(&text).expect("the bridge writes parseable frames")
    }

    /// The next frame, asserted to be of `kind`.
    pub(crate) async fn expect_frame(&mut self, kind: &str) -> Value {
        let frame = self.next_frame().await;
        assert_eq!(frame["type"], kind, "expected a {kind}, got {frame}");
        frame
    }

    /// Whether the bridge has written anything not yet read.
    pub(crate) fn wrote_nothing(&mut self) -> bool {
        self.sent.try_recv().is_err()
    }

    pub(crate) fn say(&self, frame: Value) {
        self.inbound
            .send(TransportEvent::Text(frame.to_string()))
            .expect("the bridge is still reading");
    }

    pub(crate) fn close(&self) {
        self.inbound
            .send(TransportEvent::Closed {
                code: None,
                reason: String::new(),
            })
            .expect("the bridge is still reading");
    }

    /// Read the bridge's `Hello` and answer with a matching one plus a `Welcome`,
    /// which is what makes the attachment live. The version range is echoed back
    /// from what the bridge stated, so this peer stays compatible with a moved
    /// pin without knowing the constant.
    pub(crate) async fn handshake(&mut self) {
        let hello = self.expect_frame("Hello").await;
        let versions = hello["versions"].clone();
        self.say(json!({
            "type": "Hello",
            "versions": versions,
            "ident": "scripted-peer",
        }));
        self.say(json!({
            "type": "Welcome",
            "version": versions["max"].clone(),
            "participant_id": "remote:pod-kitchen",
            "session_id": "sess-1",
            "heartbeat_secs": 20,
            "max_body_bytes": 65_536,
            "max_frame_bytes": 532_480,
            "alert_granted": true,
        }));
    }

    /// Answer the bridge's outstanding `Subscribe` on `channel` with `kind`
    /// (`"Ok"` or `"Unavailable"`), after checking that is what it asked for.
    pub(crate) async fn answer_subscribe(&mut self, channel: &str, kind: &str) -> Value {
        let frame = self.expect_frame("Subscribe").await;
        assert_eq!(frame["channel"], channel, "unexpected channel: {frame}");
        self.say(json!({
            "type": "SubscribeResult",
            "channel": channel,
            "outcome": { "kind": kind },
            "replay_count": 0,
        }));
        frame
    }

    /// Read the next `Publish` and answer it with `kind` — one of the outcome
    /// kinds that carry no fields (`"Ok"`, `"RateLimited"`, `"Failed"`); a sized
    /// refusal needs its own frame. Returns the frame, so the test can assert
    /// what was published where and at what urgency.
    pub(crate) async fn answer_publish(&mut self, kind: &str) -> Value {
        let frame = self.expect_frame("Publish").await;
        self.say(json!({
            "type": "PublishResult",
            "correlation": frame["correlation"].clone(),
            "outcome": { "kind": kind },
        }));
        frame
    }

    /// Deliver one message on `channel`.
    pub(crate) fn deliver(&self, channel: &str, body: &str, seq: u64) {
        self.deliver_after_gap(channel, body, seq, 0);
    }

    /// Deliver one message on `channel` behind `dropped` messages the bus lost
    /// before it — what a real peer reports when the push window rolled.
    pub(crate) fn deliver_after_gap(&self, channel: &str, body: &str, seq: u64, dropped: u64) {
        self.say(json!({
            "type": "Deliver",
            "channel": channel,
            "rows": [{
                "envelope": {
                    "message_id": "00000000-0000-0000-0000-000000000001",
                    "source": "brenn",
                    "channel": channel,
                    "sender": "system:harness",
                    "publish_ts": "2023-11-14T22:13:20Z",
                    "body": body,
                    "urgency": "high",
                    "envelope_type": "brenn",
                },
                "seq": seq,
                "cursor": format!("opaque-token-{seq}"),
                "dropped": dropped,
            }],
        }));
    }
}

fn conn_config() -> ConnConfig {
    ConnConfig {
        url: "wss://peer.example.net/remote/pod-kitchen/ws".to_string(),
        ident: "speech-surface/test".to_string(),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        connect_timeout: Duration::from_secs(5),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0,
        terminal_close_code: None,
    }
}

/// A bridge over a scripted connector, one [`Peer`] per [`Attempt::Open`] in
/// script order.
pub(crate) fn scripted(
    script: &[Attempt],
    max_futile: u32,
) -> (
    Bridge<ScriptedConnector>,
    BridgeHandle,
    mpsc::Receiver<BridgeEvent>,
    VecDeque<Peer>,
) {
    let mut attempts = VecDeque::new();
    let mut peers = VecDeque::new();
    for step in script {
        match step {
            Attempt::Fail => attempts.push_back(None),
            Attempt::Open => {
                let (inbound_tx, inbound_rx) = unbounded_channel();
                let (sent_tx, sent_rx) = unbounded_channel();
                attempts.push_back(Some(Wire {
                    inbound: inbound_rx,
                    sent: sent_tx,
                }));
                peers.push_back(Peer {
                    inbound: inbound_tx,
                    sent: sent_rx,
                });
            }
        }
    }
    let (bridge, handle, events) =
        Bridge::with_connector(conn_config(), max_futile, ScriptedConnector { attempts });
    (bridge, handle, events, peers)
}
