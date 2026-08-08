//! A scripted bus peer, so the daemon's own `select!` loop can be driven with no
//! socket and no server.
//!
//! The transport is a pair of channels: a test states exactly what the peer said
//! and exactly when the socket died. Frames are handled as JSON values rather
//! than the typed client and server frames — those belong to the pinned upstream
//! crates that only `brenn-bridge` may depend on, so this peer reads and writes
//! the wire as text, which is what a real peer does anyway.
//!
//! Only what this daemon's loop needs is here: an attachment, the presence
//! subscription, deliveries, and the two ways a run ends.

use std::collections::VecDeque;
use std::time::Duration;

use brenn_bridge::{
    Bridge, BridgeEvent, BridgeHandle, ConnConfig, TransportConnection, TransportConnector,
    TransportError, TransportEvent,
};
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Long enough that reaching it means something is wedged rather than slow: this
/// peer answers in microseconds.
pub const WAIT: Duration = Duration::from_secs(10);

/// The bridge's end of one scripted socket.
struct Wire {
    inbound: UnboundedReceiver<TransportEvent>,
    sent: UnboundedSender<String>,
}

pub struct ScriptedConnector {
    sockets: VecDeque<Wire>,
}

pub struct ScriptedConnection {
    wire: Wire,
}

impl TransportConnector for ScriptedConnector {
    type Conn = ScriptedConnection;

    async fn connect(&mut self, _url: &str) -> Result<ScriptedConnection, TransportError> {
        match self.sockets.pop_front() {
            Some(wire) => Ok(ScriptedConnection { wire }),
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
            None => TransportEvent::Failed("the scripted peer is gone".to_owned()),
        }
    }

    async fn close(&mut self) {
        self.wire.inbound.close();
    }
}

/// The test's end of one scripted socket.
pub struct Peer {
    inbound: UnboundedSender<TransportEvent>,
    sent: UnboundedReceiver<String>,
}

impl Peer {
    /// The next frame the bridge wrote, as JSON.
    pub async fn next_frame(&mut self) -> Value {
        let text = tokio::time::timeout(WAIT, self.sent.recv())
            .await
            .expect("the bridge wrote a frame before the timeout")
            .expect("the bridge's socket is still open");
        serde_json::from_str(&text).expect("the bridge writes parseable frames")
    }

    /// The next frame, asserted to be of `kind`.
    pub async fn expect_frame(&mut self, kind: &str) -> Value {
        let frame = self.next_frame().await;
        assert_eq!(frame["type"], kind, "expected a {kind}, got {frame}");
        frame
    }

    fn say(&self, frame: Value) {
        self.inbound
            .send(TransportEvent::Text(frame.to_string()))
            .expect("the bridge is still reading");
    }

    /// Read the bridge's `Hello` and answer with a matching one plus a
    /// `Welcome`, which is what makes the attachment live. The version range is
    /// echoed back from what the bridge stated, so this peer stays compatible
    /// with a moved pin without knowing the constant.
    pub async fn handshake(&mut self, alert_granted: bool) {
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
            "participant_id": "remote:reachy-motiond",
            "session_id": "sess-1",
            "heartbeat_secs": 20,
            "max_body_bytes": 65_536,
            "max_frame_bytes": 532_480,
            "alert_granted": alert_granted,
        }));
    }

    /// Answer the bridge's outstanding `Subscribe` on `channel` with `kind`
    /// (`"Ok"` or `"Unavailable"`), after checking that is what it asked for.
    /// Returns the frame, so a test can assert the depths it stated.
    pub async fn answer_subscribe(&mut self, channel: &str, kind: &str) -> Value {
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

    /// Deliver one message on `channel`.
    pub fn deliver(&self, channel: &str, body: &str, seq: u64) {
        self.say(json!({
            "type": "Deliver",
            "channel": channel,
            "rows": [{
                "envelope": {
                    "message_id": "00000000-0000-0000-0000-000000000001",
                    "source": "brenn",
                    "channel": channel,
                    "sender": "system:speech-host",
                    "publish_ts": "2026-08-07T00:00:00Z",
                    "body": body,
                    "urgency": "normal",
                    "envelope_type": "brenn",
                },
                "seq": seq,
                "cursor": format!("opaque-token-{seq}"),
                "dropped": 0,
            }],
        }));
    }

    /// Say something the protocol does not admit, which is how a peer ends an
    /// attachment nothing will reconnect after.
    pub fn break_the_protocol(&self) {
        self.say(json!({ "type": "DeferredView", "channel": "brenn:whatever" }));
    }
}

fn conn_config() -> ConnConfig {
    ConnConfig {
        url: "wss://peer.example.net/remote/reachy-motiond/ws".to_owned(),
        ident: "reachy-motiond/test".to_owned(),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        connect_timeout: Duration::from_secs(5),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0,
        terminal_close_code: None,
    }
}

/// A bridge over a scripted connector, with `sockets` connect attempts that
/// succeed and one [`Peer`] behind each. A dial past the last one never
/// completes.
pub fn scripted(
    sockets: usize,
    max_futile: u32,
) -> (
    Bridge<ScriptedConnector>,
    BridgeHandle,
    mpsc::Receiver<BridgeEvent>,
    VecDeque<Peer>,
) {
    let mut wires = VecDeque::new();
    let mut peers = VecDeque::new();
    for _ in 0..sockets {
        let (inbound_tx, inbound_rx) = unbounded_channel();
        let (sent_tx, sent_rx) = unbounded_channel();
        wires.push_back(Wire {
            inbound: inbound_rx,
            sent: sent_tx,
        });
        peers.push_back(Peer {
            inbound: inbound_tx,
            sent: sent_rx,
        });
    }
    let (bridge, handle, events) = Bridge::with_connector(
        conn_config(),
        max_futile,
        ScriptedConnector { sockets: wires },
    );
    (bridge, handle, events, peers)
}
