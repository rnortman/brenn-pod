//! The supervision loop, against a scripted peer.
//!
//! The transport is a pair of channels, so a test states exactly what the server
//! said and exactly when the socket died. What is under test is the loop's own
//! judgement: what reaches the wire, what reaches the embedder, and when the
//! bridge decides it is better off dead.

use std::collections::VecDeque;
use std::time::Duration;

use brenn_attach_client::transport::{TransportConnection, TransportError, TransportEvent};
use brenn_attach_proto::{SUPPORTED_VERSIONS, Urgency, VersionRange};
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::*;

const URL: &str = "wss://peer.example.net/remote/pod-kitchen/ws";
const ROSTER: &str = "brenn:chat.app.home.roster";

/// Long enough that reaching it means something is wedged, not slow: the peer
/// here answers in microseconds.
const WAIT: Duration = Duration::from_secs(10);

fn depths() -> SubscriptionDepths {
    SubscriptionDepths {
        push_depth: 1,
        retain_depth: 1,
    }
}

fn conn_config() -> ConnConfig {
    ConnConfig {
        url: URL.to_string(),
        ident: "brenn-bridge/test".to_string(),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        connect_timeout: Duration::from_secs(5),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0,
        terminal_close_code: None,
    }
}

// ── the scripted transport ────────────────────────────────────────────────

/// What one connect attempt does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Attempt {
    /// The socket opens and the test drives the peer behind it.
    Open,
    /// The connect fails before a socket exists.
    Fail,
}

struct ScriptedConnector {
    attempts: VecDeque<Option<Wire>>,
}

/// The bridge's end of one scripted socket.
struct Wire {
    inbound: UnboundedReceiver<TransportEvent>,
    sent: UnboundedSender<String>,
}

struct ScriptedConnection {
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
struct Peer {
    inbound: UnboundedSender<TransportEvent>,
    sent: UnboundedReceiver<String>,
}

impl Peer {
    /// The next frame the bridge wrote, parsed.
    async fn next_frame(&mut self) -> ClientFrame {
        let text = tokio::time::timeout(WAIT, self.sent.recv())
            .await
            .expect("the bridge wrote a frame before the timeout")
            .expect("the bridge's socket is still open");
        serde_json::from_str(&text).expect("the bridge writes parseable frames")
    }

    /// Whether the bridge has written anything not yet read.
    fn wrote_nothing(&mut self) -> bool {
        self.sent.try_recv().is_err()
    }

    fn say(&self, frame: Value) {
        self.inbound
            .send(TransportEvent::Text(frame.to_string()))
            .expect("the bridge is still reading");
    }

    fn close(&self) {
        self.inbound
            .send(TransportEvent::Closed {
                code: None,
                reason: String::new(),
            })
            .expect("the bridge is still reading");
    }

    /// Close carrying a code, which is what a peer that means something by the
    /// close does.
    fn close_with(&self, code: u16, reason: &str) {
        self.inbound
            .send(TransportEvent::Closed {
                code: Some(code),
                reason: reason.to_string(),
            })
            .expect("the bridge is still reading");
    }

    /// Read the bridge's `Hello` and answer with a matching one plus a
    /// `Welcome`, which is what makes the attachment live.
    async fn handshake(&mut self) {
        match self.next_frame().await {
            ClientFrame::Hello { .. } => {}
            other => panic!("the first frame must be a Hello, got {other:?}"),
        }
        self.say(json!({
            "type": "Hello",
            "versions": {"min": SUPPORTED_VERSIONS.min, "max": SUPPORTED_VERSIONS.max},
            "ident": "scripted-peer",
        }));
        self.say(json!({
            "type": "Welcome",
            "version": SUPPORTED_VERSIONS.max,
            "participant_id": "remote:pod-kitchen",
            "session_id": "sess-1",
            "heartbeat_secs": 20,
            "max_body_bytes": 65_536,
            "max_frame_bytes": 532_480,
            "alert_granted": true,
        }));
    }
}

fn subscribe_result(channel: &str, kind: &str) -> Value {
    json!({
        "type": "SubscribeResult",
        "channel": channel,
        "outcome": {"kind": kind},
        "replay_count": 0,
    })
}

fn row(channel: &str, body: &str, seq: u64, dropped: u64) -> Value {
    json!({
        "envelope": {
            "message_id": "00000000-0000-0000-0000-000000000001",
            "source": "brenn",
            "channel": channel,
            "sender": "system:chat-roster",
            "publish_ts": "2023-11-14T22:13:20Z",
            "body": body,
            "urgency": "normal",
            "envelope_type": "brenn",
        },
        "seq": seq,
        "cursor": format!("opaque-token-{seq}"),
        "dropped": dropped,
    })
}

fn deliver(channel: &str, body: &str, seq: u64) -> Value {
    json!({
        "type": "Deliver",
        "channel": channel,
        "rows": [row(channel, body, seq, 0)],
    })
}

/// One `Deliver` pass carrying several rows. A pass's loss belongs to the
/// subscription and rides its head row, which is where the plane requires it.
fn deliver_rows(channel: &str, rows: Vec<Value>) -> Value {
    json!({
        "type": "Deliver",
        "channel": channel,
        "rows": rows,
    })
}

/// Build a bridge over a scripted connector, one peer per `Attempt::Open`.
fn scripted(
    script: &[Attempt],
    max_futile: u32,
) -> (
    Bridge<ScriptedConnector>,
    BridgeHandle,
    mpsc::Receiver<BridgeEvent>,
    Vec<Peer>,
) {
    scripted_with(conn_config(), script, max_futile)
}

/// [`scripted`] over connection parameters the test states — a deadline it wants
/// to see fire, a close code it wants treated as terminal.
fn scripted_with(
    conn: ConnConfig,
    script: &[Attempt],
    max_futile: u32,
) -> (
    Bridge<ScriptedConnector>,
    BridgeHandle,
    mpsc::Receiver<BridgeEvent>,
    Vec<Peer>,
) {
    let mut attempts = VecDeque::new();
    let mut peers = Vec::new();
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
                peers.push(Peer {
                    inbound: inbound_tx,
                    sent: sent_rx,
                });
            }
        }
    }
    let (bridge, handle, events) =
        Bridge::with_connector(conn, max_futile, ScriptedConnector { attempts });
    (bridge, handle, events, peers)
}

async fn next_event(events: &mut mpsc::Receiver<BridgeEvent>) -> BridgeEvent {
    tokio::time::timeout(WAIT, events.recv())
        .await
        .expect("an event arrived before the timeout")
        .expect("the bridge is still running")
}

async fn expect_attached(events: &mut mpsc::Receiver<BridgeEvent>) -> AttachmentFacts {
    match next_event(events).await {
        BridgeEvent::Attached(facts) => facts,
        other => panic!("expected an attachment, got {other:?}"),
    }
}

async fn expect_detached(events: &mut mpsc::Receiver<BridgeEvent>) {
    match next_event(events).await {
        BridgeEvent::Detached { .. } => {}
        other => panic!("expected a detach, got {other:?}"),
    }
}

// ── the tests ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn an_attachment_carries_a_subscription_and_its_deliveries_to_the_embedder() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    let facts = expect_attached(&mut events).await;
    assert_eq!(facts.participant_id, "remote:pod-kitchen");
    assert_eq!(facts.max_body_bytes, 65_536);
    assert!(facts.alert_granted);

    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Cursorless)
        .await
        .expect("the bridge is running");
    match peer.next_frame().await {
        ClientFrame::Subscribe {
            channel,
            push_depth,
            retain_depth,
            resume,
        } => {
            assert_eq!(channel, ROSTER);
            assert_eq!(push_depth, 1);
            assert_eq!(retain_depth, 1);
            assert_eq!(resume, None, "a first subscribe presents no cursor");
        }
        other => panic!("expected a Subscribe, got {other:?}"),
    }

    peer.say(subscribe_result(ROSTER, "Ok"));
    match next_event(&mut events).await {
        BridgeEvent::Subscribed {
            channel,
            replay_count,
            gap,
        } => {
            assert_eq!(channel, ROSTER);
            assert_eq!(replay_count, 0);
            assert_eq!(gap, None);
        }
        other => panic!("expected a subscription, got {other:?}"),
    }

    peer.say(deliver(ROSTER, r#"{"v":1,"conversations":[{"id":42}]}"#, 1));
    match next_event(&mut events).await {
        BridgeEvent::Delivered(delivery) => {
            assert_eq!(delivery.channel, ROSTER);
            assert_eq!(delivery.seq, 1);
            assert_eq!(delivery.dropped, 0);
            assert_eq!(delivery.envelope.sender, "system:chat-roster");
            assert!(delivery.envelope.body.contains("\"id\":42"));
        }
        other => panic!("expected a delivery, got {other:?}"),
    }

    handle.shutdown().await.expect("the bridge is running");
    let outcome = task.await.expect("the task joins");
    assert_eq!(outcome, BridgeOutcome::Closed);
    assert_eq!(outcome.exit_code(), 0);
    assert!(
        outcome.commanded(),
        "a shutdown that was acted on is the one commanded ending"
    );
}

#[tokio::test]
async fn a_dial_that_reaches_no_socket_is_reported_to_the_embedder() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Fail, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());

    // The failure is the only thing the embedder is told about the first
    // attempt: nothing attached, so no `Detached` follows it.
    match next_event(&mut events).await {
        BridgeEvent::ConnectFailed { timed_out } => assert!(
            !timed_out,
            "the connector refused the dial; the deadline never fired"
        ),
        other => panic!("expected a failed dial, got {other:?}"),
    }

    let mut peer = peers.remove(0);
    peer.handshake().await;
    expect_attached(&mut events).await;

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_run_of_failed_dials_keeps_reporting_rather_than_going_quiet() {
    // What a stale token looks like from here: every attempt refused, forever.
    // The bridge stays up — a refusal is indistinguishable from a server that is
    // down — so the one thing it owes an operator is that it keeps saying so.
    let (bridge, handle, mut events, mut peers) = scripted(
        &[Attempt::Fail, Attempt::Fail, Attempt::Fail, Attempt::Open],
        3,
    );
    let task = tokio::spawn(bridge.run());

    for _ in 0..3 {
        assert_eq!(
            next_event(&mut events).await,
            BridgeEvent::ConnectFailed { timed_out: false }
        );
    }

    // The run of refusals is not itself a verdict: the fourth dial attaches, and
    // a futile budget of 3 was never touched, because nothing was ever sent.
    let mut peer = peers.remove(0);
    peer.handshake().await;
    expect_attached(&mut events).await;

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_publish_with_no_live_attachment_is_lost_rather_than_queued() {
    let (bridge, handle, _events, _peers) = scripted(&[Attempt::Fail], 3);
    let task = tokio::spawn(bridge.run());

    let error = handle
        .publish(PublishRequest {
            channel: "brenn:chat.app.home.in.42".to_string(),
            attribution: None,
            body: "hello".to_string(),
            urgency: Urgency::Normal,
        })
        .await
        .expect_err("nothing can be carried off a live attachment");
    assert_eq!(error, PublishError::Lost);

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_publish_is_answered_with_the_peers_outcome() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;

    let answering = async {
        let correlation = match peer.next_frame().await {
            ClientFrame::Publish {
                channel,
                attribution,
                body,
                correlation,
                ..
            } => {
                assert_eq!(channel, "brenn:chat.app.home.in.42");
                assert_eq!(attribution, None, "v1 publishes as the bare principal");
                assert_eq!(body, "hello");
                correlation.expect("a publish awaiting an answer carries a correlation")
            }
            other => panic!("expected a Publish, got {other:?}"),
        };
        peer.say(json!({
            "type": "PublishResult",
            "correlation": correlation,
            "outcome": {"kind": "RateLimited"},
        }));
        peer
    };
    let publishing = handle.publish(PublishRequest {
        channel: "brenn:chat.app.home.in.42".to_string(),
        attribution: None,
        body: "hello".to_string(),
        urgency: Urgency::Normal,
    });
    let (outcome, peer) = tokio::join!(publishing, answering);
    assert_eq!(
        outcome.expect("the peer answered"),
        PublishOutcome::RateLimited
    );

    drop(peer);
    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_publish_outstanding_when_the_socket_dies_is_lost() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;

    let publisher = handle.clone();
    let publishing = tokio::spawn(async move {
        publisher
            .publish(PublishRequest {
                channel: "brenn:chat.app.home.in.42".to_string(),
                attribution: None,
                body: "hello".to_string(),
                urgency: Urgency::Normal,
            })
            .await
    });
    peer.next_frame().await;
    peer.close();

    let answer = publishing.await.expect("the publisher task joins");
    assert_eq!(
        answer.expect_err("a publish the socket outlived is not an outcome"),
        PublishError::Lost
    );
    expect_detached(&mut events).await;

    // The bridge is still alive and reconnecting; the drop is a link event, not
    // a verdict on this process.
    let mut next = peers.remove(0);
    next.handshake().await;
    expect_attached(&mut events).await;
    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn an_unavailable_channel_is_reported_and_not_resubscribed_on_reattach() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);

    first.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    first.next_frame().await;
    first.say(subscribe_result(ROSTER, "Unavailable"));
    match next_event(&mut events).await {
        BridgeEvent::Unavailable { channel } => assert_eq!(channel, ROSTER),
        other => panic!("expected an unavailable channel, got {other:?}"),
    }

    first.close();
    expect_detached(&mut events).await;
    second.handshake().await;
    expect_attached(&mut events).await;

    // The hold was dropped with the refusal, so the reattach owes the channel
    // nothing. Asking again is the application's call.
    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
    assert!(
        second.wrote_nothing(),
        "a refused channel must not be resubscribed behind the application's back"
    );
}

#[tokio::test]
async fn a_held_channel_is_resubscribed_on_reattach_presenting_its_cursor() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);

    first.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    first.next_frame().await;
    first.say(subscribe_result(ROSTER, "Ok"));
    next_event(&mut events).await;
    first.say(deliver(ROSTER, "{}", 1));
    next_event(&mut events).await;

    first.close();
    expect_detached(&mut events).await;
    second.handshake().await;
    expect_attached(&mut events).await;
    match second.next_frame().await {
        ClientFrame::Subscribe {
            channel, resume, ..
        } => {
            assert_eq!(channel, ROSTER);
            assert!(
                resume.is_some(),
                "an in-window blip is lossless because the cursor is presented again"
            );
        }
        other => panic!("expected a resubscribe, got {other:?}"),
    }

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn no_version_in_common_ends_the_bridge_with_its_own_exit_code() {
    let (bridge, _handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    match peer.next_frame().await {
        ClientFrame::Hello { .. } => {}
        other => panic!("expected a Hello, got {other:?}"),
    }
    peer.say(json!({
        "type": "Hello",
        "versions": {"min": SUPPORTED_VERSIONS.max + 7, "max": SUPPORTED_VERSIONS.max + 9},
        "ident": "a-much-newer-peer",
    }));

    let outcome = tokio::time::timeout(WAIT, task)
        .await
        .expect("the bridge ended before the timeout")
        .expect("the task joins");
    assert_eq!(
        outcome,
        BridgeOutcome::Incompatible {
            ours: SUPPORTED_VERSIONS,
            theirs: VersionRange {
                min: SUPPORTED_VERSIONS.max + 7,
                max: SUPPORTED_VERSIONS.max + 9,
            },
        }
    );
    assert_eq!(outcome.exit_code(), crate::exit::VERSION_INCOMPATIBLE);
    assert!(!outcome.commanded(), "nobody commands a version refusal");
    let rendered = outcome.to_string();
    assert!(
        rendered.contains(&format!("{}", SUPPORTED_VERSIONS.max))
            && rendered.contains(&format!("{}", SUPPORTED_VERSIONS.max + 9)),
        "the operator has to be told which side to update: {rendered}"
    );
    assert!(events.try_recv().is_err(), "nothing ever attached");
}

#[tokio::test]
async fn a_frame_this_bridge_cannot_own_is_fatal() {
    let (bridge, _handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    // This bridge parks nothing, so the peer owes it no parked-set mirror.
    peer.say(json!({
        "type": "DeferredView",
        "channel": ROSTER,
        "entries": [],
    }));

    let outcome = tokio::time::timeout(WAIT, task)
        .await
        .expect("the bridge ended before the timeout")
        .expect("the task joins");
    match &outcome {
        BridgeOutcome::Fatal { detail } => assert!(
            detail.contains("deferred view"),
            "the detail names what could not be reconciled: {detail}"
        ),
        other => panic!("expected a protocol error, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), crate::exit::HARD_FAILURE);
    assert!(!outcome.commanded(), "nobody commands a protocol error");
}

#[tokio::test]
async fn a_run_of_attachments_answered_nothing_ends_the_bridge() {
    let (bridge, handle, mut events, mut peers) =
        scripted(&[Attempt::Open, Attempt::Open, Attempt::Open], 2);
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);
    let mut third = peers.remove(0);

    // The shape a refused frame produces: the bridge states something, the peer
    // answers by closing, and the held statement is re-sent at the next
    // attachment to earn the same close.
    first.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    first.next_frame().await;
    first.close();
    expect_detached(&mut events).await;

    second.handshake().await;
    expect_attached(&mut events).await;
    second.next_frame().await;
    second.close();
    expect_detached(&mut events).await;

    let outcome = tokio::time::timeout(WAIT, task)
        .await
        .expect("the bridge ended before the timeout")
        .expect("the task joins");
    assert_eq!(outcome, BridgeOutcome::Futile { attachments: 2 });
    assert_eq!(outcome.exit_code(), crate::exit::HARD_FAILURE);
    assert!(
        !outcome.commanded(),
        "giving up on a silent peer is nobody's command"
    );
    // The budget is a bound on attempts, not just an ending: a dialed socket
    // receives a `Hello` immediately, so silence on the third scripted wire is
    // proof the third attachment was never made.
    assert!(
        third.wrote_nothing(),
        "the budget must stop the bridge dialing again, not merely end it afterwards"
    );
}

#[tokio::test]
async fn an_answered_attachment_resets_the_futile_run() {
    // The heuristic is *consecutive* futile attachments, and that is the whole
    // reason it is safe on a flaky link: counting them cumulatively would kill a
    // healthy long-lived pod after `max_futile` unanswered drops spread over its
    // whole life.
    let (bridge, handle, mut events, mut peers) = scripted(
        &[Attempt::Open, Attempt::Open, Attempt::Open, Attempt::Open],
        2,
    );
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);
    let mut third = peers.remove(0);
    let mut fourth = peers.remove(0);

    // One futile attachment: the subscribe goes out and the peer closes on it.
    first.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    first.next_frame().await;
    first.close();
    expect_detached(&mut events).await;

    // An answered one: the resubscribe is acknowledged before the socket dies,
    // so the run resets to zero.
    second.handshake().await;
    expect_attached(&mut events).await;
    second.next_frame().await;
    second.say(subscribe_result(ROSTER, "Ok"));
    next_event(&mut events).await;
    second.close();
    expect_detached(&mut events).await;

    // Another futile one. With the reset, this is the run's first; without it,
    // it would be the second and the budget would fire here.
    third.handshake().await;
    expect_attached(&mut events).await;
    third.next_frame().await;
    third.close();
    expect_detached(&mut events).await;

    fourth.handshake().await;
    expect_attached(&mut events).await;

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_dial_the_deadline_outlives_is_reported_as_a_timeout() {
    // `timed_out` is the only bit separating "the host refused the connect" —
    // wrong port, wrong host, server down — from "the host swallowed it", which
    // is what an operator uses to decide between the config and the network.
    let conn = ConnConfig {
        connect_timeout: Duration::from_millis(50),
        ..conn_config()
    };
    // An empty script: the connector never answers, so the armed deadline is
    // what resolves the attempt.
    let (bridge, handle, mut events, _peers) = scripted_with(conn, &[], 3);
    let task = tokio::spawn(bridge.run());

    assert_eq!(
        next_event(&mut events).await,
        BridgeEvent::ConnectFailed { timed_out: true }
    );

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn every_row_of_one_delivery_pass_reaches_the_embedder_with_its_own_facts() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    peer.next_frame().await;
    peer.say(subscribe_result(ROSTER, "Ok"));
    next_event(&mut events).await;

    // One pass, two rows, and a loss report that belongs to the pass — the plane
    // refuses a non-zero `dropped` on any row but the head.
    peer.say(deliver_rows(
        ROSTER,
        vec![
            row(ROSTER, r#"{"n":1}"#, 1, 3),
            row(ROSTER, r#"{"n":2}"#, 2, 0),
        ],
    ));

    let mut delivered = Vec::new();
    for _ in 0..2 {
        match next_event(&mut events).await {
            BridgeEvent::Delivered(delivery) => delivered.push(delivery),
            other => panic!("expected a delivery, got {other:?}"),
        }
    }
    assert_eq!(
        delivered.iter().map(|d| d.seq).collect::<Vec<_>>(),
        vec![1, 2],
        "each row is its own event, in wire order"
    );
    assert_eq!(
        delivered.iter().map(|d| d.dropped).collect::<Vec<_>>(),
        vec![3, 0],
        "the pass's loss rides the row it precedes and is not restated on the rest"
    );
    assert!(delivered[1].envelope.body.contains("\"n\":2"));

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_subscription_stated_before_the_first_attachment_reaches_the_first_wire() {
    // The startup order every embedder has — bridge-probe states its whole CLI
    // set before anything is attached, and a pod application states its at boot.
    // A frame written while detached is dropped at the wire, so the resend at
    // attach is the only thing that carries it.
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Fail, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());

    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Cursorless)
        .await
        .expect("the bridge is running");

    match next_event(&mut events).await {
        BridgeEvent::ConnectFailed { .. } => {}
        other => panic!("expected the first dial to fail, got {other:?}"),
    }

    let mut peer = peers.remove(0);
    peer.handshake().await;
    expect_attached(&mut events).await;
    match peer.next_frame().await {
        ClientFrame::Subscribe { channel, .. } => assert_eq!(channel, ROSTER),
        other => panic!("a subscription stated while detached must be re-sent, got {other:?}"),
    }

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn a_close_on_the_declared_terminal_code_ends_the_bridge() {
    let conn = ConnConfig {
        terminal_close_code: Some(4001),
        ..conn_config()
    };
    let (bridge, _handle, mut events, mut peers) =
        scripted_with(conn, &[Attempt::Open, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    peer.close_with(4001, "this build is not welcome here");

    let outcome = tokio::time::timeout(WAIT, task)
        .await
        .expect("the bridge ended before the timeout")
        .expect("the task joins");
    match &outcome {
        BridgeOutcome::PeerClosedTerminal { code, reason } => {
            assert_eq!(*code, 4001);
            assert_eq!(reason, "this build is not welcome here");
        }
        other => panic!("expected a terminal close, got {other:?}"),
    }
    assert_eq!(outcome.exit_code(), crate::exit::HARD_FAILURE);
    assert!(
        !outcome.commanded(),
        "the peer's terminal close is not the embedder's command"
    );
    assert!(
        outcome.to_string().contains("4001"),
        "the operator is told which code stopped the pod: {outcome}"
    );
}

#[tokio::test]
async fn two_publishes_in_flight_each_get_their_own_answer() {
    // Two tasks publishing on one attachment is the ordinary embedder shape, and
    // the correlations are what keep the answers apart: upstream asserts on a
    // duplicate, and a crossed pair would hand a caller another caller's outcome.
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;

    let request = |channel: &str| PublishRequest {
        channel: channel.to_string(),
        attribution: None,
        body: "hello".to_string(),
        urgency: Urgency::Normal,
    };
    let first = handle.clone();
    let first =
        tokio::spawn(async move { first.publish(request("brenn:chat.app.home.in.1")).await });
    let second = handle.clone();
    let second =
        tokio::spawn(async move { second.publish(request("brenn:chat.app.home.in.2")).await });

    let mut correlations = Vec::new();
    for _ in 0..2 {
        match peer.next_frame().await {
            ClientFrame::Publish {
                channel,
                correlation,
                ..
            } => correlations.push((
                channel,
                correlation.expect("a publish awaiting an answer carries a correlation"),
            )),
            other => panic!("expected a Publish, got {other:?}"),
        }
    }
    assert_ne!(
        correlations[0].1, correlations[1].1,
        "two publishes in flight must not share a correlation"
    );

    // Answered out of order and distinguishably, so a crossed pair cannot pass.
    for (channel, correlation) in correlations.iter().rev() {
        let kind = if channel.ends_with(".1") {
            "RateLimited"
        } else {
            "Ok"
        };
        peer.say(json!({
            "type": "PublishResult",
            "correlation": correlation,
            "outcome": {"kind": kind},
        }));
    }

    assert_eq!(
        first.await.expect("the publisher joins").expect("answered"),
        PublishOutcome::RateLimited
    );
    assert_eq!(
        second
            .await
            .expect("the publisher joins")
            .expect("answered"),
        PublishOutcome::Ok
    );

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn an_unsubscribe_racing_an_unavailable_answer_is_a_no_op() {
    // The settlement drops every hold on the channel, so an `unsubscribe` the
    // application had already issued — or issues afterwards from a generic
    // "release what I held" path — releases a hold nobody has. That is the peer's
    // ordinary answer to a deprovisioned channel, not an application bug, and it
    // must not take the bridge task down with it.
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    peer.next_frame().await;
    peer.say(subscribe_result(ROSTER, "Unavailable"));
    match next_event(&mut events).await {
        BridgeEvent::Unavailable { channel } => assert_eq!(channel, ROSTER),
        other => panic!("expected an unavailable channel, got {other:?}"),
    }

    handle
        .unsubscribe(ROSTER)
        .await
        .expect("the bridge is running");
    // Never held, so nothing is owed the peer either.
    handle
        .unsubscribe("brenn:chat.app.home.out.42")
        .await
        .expect("the bridge is running");

    // Still alive and still serving: an alert proves the task survived both.
    handle
        .alert(AlertSeverity::Info, "still here", "the task did not die")
        .await
        .expect("the bridge is running");
    match peer.next_frame().await {
        ClientFrame::Alert { title, .. } => assert_eq!(title, "still here"),
        other => panic!("expected the alert, got {other:?}"),
    }

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[tokio::test]
async fn an_idle_attachment_that_drops_is_not_futile() {
    // A budget of one: any futile attachment at all would end the bridge, so a
    // surviving reattachment is the whole assertion.
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open, Attempt::Open], 1);
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);

    first.handshake().await;
    expect_attached(&mut events).await;
    first.close();
    expect_detached(&mut events).await;

    second.handshake().await;
    expect_attached(&mut events).await;

    // Nothing is left to drive the bridge or to hear it, which is its own
    // orderly ending.
    drop(handle);
    drop(events);
    let outcome = tokio::time::timeout(WAIT, task)
        .await
        .expect("the bridge ended before the timeout")
        .expect("the task joins");
    assert_eq!(outcome, BridgeOutcome::EmbedderGone);
    assert_eq!(outcome.exit_code(), 0);
    assert!(
        !outcome.commanded(),
        "orderly, but nobody asked: the embedder vanished"
    );
}

#[tokio::test]
async fn an_unsubscribe_reaches_the_wire_and_releases_the_hold() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open, Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut first = peers.remove(0);
    let mut second = peers.remove(0);

    first.handshake().await;
    expect_attached(&mut events).await;
    handle
        .subscribe(ROSTER, depths(), ResumePolicy::Resume)
        .await
        .expect("the bridge is running");
    first.next_frame().await;
    first.say(subscribe_result(ROSTER, "Ok"));
    next_event(&mut events).await;

    handle
        .unsubscribe(ROSTER)
        .await
        .expect("the bridge is running");
    match first.next_frame().await {
        ClientFrame::Unsubscribe { channel } => assert_eq!(channel, ROSTER),
        other => panic!("expected an Unsubscribe, got {other:?}"),
    }

    first.close();
    expect_detached(&mut events).await;
    second.handshake().await;
    expect_attached(&mut events).await;

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
    assert!(
        second.wrote_nothing(),
        "a released channel is not this bridge's to resubscribe"
    );
}

#[tokio::test]
async fn an_alert_reaches_the_wire_unanswered() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    handle
        .alert(
            AlertSeverity::Warning,
            "pod-kitchen",
            "the mic array is mute",
        )
        .await
        .expect("the bridge is running");
    match peer.next_frame().await {
        ClientFrame::Alert {
            severity,
            title,
            body,
            attribution,
        } => {
            assert_eq!(severity, AlertSeverity::Warning);
            assert_eq!(title, "pod-kitchen");
            assert_eq!(body, "the mic array is mute");
            // Absent, so the alert on the wire is what a wire-3 peer read.
            assert_eq!(attribution, None);
        }
        other => panic!("expected an Alert, got {other:?}"),
    }

    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);
}

#[test]
fn building_a_bridge_validates_the_config_it_is_handed() {
    // `Config`'s fields are public and `parse` skips validation by design, so
    // without this an embedder composing a config in memory would lose the
    // wss-only refusal and the timing floors that `Config::load` enforces —
    // silently, at runtime, rather than at startup with a named error.
    let cleartext = Config::parse(
        "server_url = \"ws://brenn.example.net/remote/pod-kitchen/ws\"\n\
         token_file = \"/etc/brenn/token\"\n",
    )
    .expect("the fixture parses");
    match Bridge::new(&cleartext) {
        Err(ConfigError::Rejected { message }) => assert!(message.contains("wss://"), "{message}"),
        Err(other) => panic!("expected the wss refusal, got {other:?}"),
        Ok(_) => panic!("a cleartext URL must never reach a connector"),
    }

    // Including the timings, whose refusals live on `ReconnectConfig`: a zero
    // backoff is a re-dial spin loop against a server that is down.
    let spinning = Config::parse(
        "server_url = \"wss://brenn.example.net/remote/pod-kitchen/ws\"\n\
         token_file = \"/etc/brenn/token\"\n\
         [reconnect]\n\
         initial_backoff_ms = 0\n",
    )
    .expect("the fixture parses");
    match Bridge::new(&spinning) {
        Err(ConfigError::Rejected { message }) => {
            assert!(message.contains("initial_backoff_ms"), "{message}")
        }
        Err(other) => panic!("expected the timing refusal, got {other:?}"),
        Ok(_) => panic!("a spin-loop backoff must be refused at build time"),
    }
}

#[tokio::test]
async fn a_wss_dial_reaches_the_network_rather_than_a_missing_tls_backend() {
    // The config refuses everything but `wss://`, so this is the only kind of
    // URL the native connector is ever handed. Built without a TLS backend,
    // the transport refuses it the moment the TCP connect lands, with an error
    // this bridge cannot tell from a server that is down: every dial fails,
    // nothing is ever sent, the futile budget never trips, and the pod runs
    // forever looking like it is pointed at a dead host.
    //
    // The socket has to be reachable for the question to be asked at all — a
    // refused connect fails first and says nothing about TLS — so this accepts
    // one connection and drops it. Whatever the handshake then fails with, it
    // must not be the backend's absence.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let address = listener.local_addr().expect("the bound address");
    let accepting = tokio::spawn(async move {
        let _ = listener.accept().await;
    });

    let mut connector = NativeConnector::with_bearer("s3cret-token");
    let detail = match tokio::time::timeout(
        WAIT,
        connector.connect(&format!("wss://{address}/remote/pod-kitchen/ws")),
    )
    .await
    .expect("the handshake answered before the timeout")
    {
        Err(err) => err.to_string(),
        Ok(_) => panic!("a peer that says nothing cannot complete a handshake"),
    };
    assert!(
        !detail.to_ascii_lowercase().contains("tls support"),
        "the dial failed for want of a TLS backend, not for want of a server: {detail}"
    );
    accepting.await.expect("the listener task joins");
}

#[tokio::test]
async fn a_handle_outliving_the_task_answers_gone() {
    let (bridge, handle, mut events, mut peers) = scripted(&[Attempt::Open], 3);
    let task = tokio::spawn(bridge.run());
    let mut peer = peers.remove(0);

    peer.handshake().await;
    expect_attached(&mut events).await;
    handle.shutdown().await.expect("the bridge is running");
    assert_eq!(task.await.expect("the task joins"), BridgeOutcome::Closed);

    assert_eq!(
        handle
            .subscribe(ROSTER, depths(), ResumePolicy::Resume)
            .await,
        Err(BridgeGone)
    );
    assert_eq!(
        handle
            .publish(PublishRequest {
                channel: "brenn:chat.app.home.in.42".to_string(),
                attribution: None,
                body: "hello".to_string(),
                urgency: Urgency::Normal,
            })
            .await,
        Err(PublishError::Gone)
    );
}
