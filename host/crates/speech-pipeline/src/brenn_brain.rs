//! `BrennBrain` — the `Brain` that answers over a pub/sub link instead of in
//! process. This module holds the wire contract: the outbound message bodies the
//! pod composes, the tolerant scanner for the marked-up text a language model
//! sends back, and the instruction document that tells that model what to send.
//!
//! The transport itself is behind [`BrainLink`], so this crate carries no
//! transport dependency and the codec stays a set of pure functions.
//!
//! The two directions are deliberately asymmetric. **Outbound** bodies are
//! composed by code, so they are strict JSON built from serde structs. **Inbound**
//! bodies are composed by a language model with no harness code stamping
//! envelopes, so strict parsing would fail routinely: the response body is the
//! speech itself, with control information in tag-shaped islands that [`scan`]
//! lifts out and everything else passed through verbatim.

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use regex::Regex;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::brain::{BrainEvent, BrainEventFn, BrainStats, send_or_report};
use crate::traits::{Brain, ResponseSink};
use crate::types::{
    ContextSegment, InterruptProgress, PodId, RoomId, SpeakBody, SpeakCmd, Utterance, UtteranceId,
};

/// The transport seam a brenn-side brain publishes through.
pub trait BrainLink: Send + Sync {
    /// Publish an utterance body and await the peer's outcome. The returned future
    /// is awaited inline by the pipeline's dispatch, so a turn is not underway
    /// until it resolves.
    fn publish_utterance(&self, body: String) -> BoxFuture<'static, Result<(), LinkError>>;
    /// Fire-and-forget wake nudge. Called from `Brain::wake` on the pipeline's
    /// select loop, so it must be cheap and must not block.
    fn notify_wake(&self, body: String);
    /// Fire-and-forget interruption notice. Called from `Brain::barge_declined` on
    /// the pipeline's select loop, so it must be cheap and must not block.
    fn notify_interruption(&self, body: String);
}

/// Why a link refused to carry a message. `detail` is the transport's own
/// rendering, carried onto the failure event verbatim rather than re-categorized:
/// the brain has no use for the distinction, and the operator wants the transport's
/// own words.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct LinkError {
    pub detail: String,
}

impl LinkError {
    /// Wrap a transport's rendering of a refusal.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

// --- outbound bodies ---------------------------------------------------------

/// The estimate of where an interrupted reply was cut, as it appears on the wire.
/// One shape for both the `interrupted` field of an utterance body and the
/// standalone interruption notice, so the peer has a single schema to learn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterruptedBody {
    /// The turn whose reply was cut.
    pub utterance: u64,
    pub heard_ms: u64,
    pub total_ms: u64,
    /// The estimated words the listener heard before the cut. Serialized as `null`
    /// — not omitted — when there is no estimate: a `Pcm` reply has no text to cut,
    /// and a cut before the first word boundary yields no whole word.
    pub heard_text: Option<String>,
}

impl InterruptedBody {
    /// The wire estimate for one interrupted turn, taking the heard text from the
    /// segment's own recorded reply text.
    pub fn from_segment(seg: &ContextSegment) -> Self {
        let heard_text = seg
            .response_text
            .as_deref()
            .and_then(|text| seg.interrupted.heard_prefix(text))
            .map(str::to_owned);
        Self {
            utterance: seg.utterance.0,
            heard_ms: seg.interrupted.heard_ms,
            total_ms: seg.interrupted.total_ms,
            heard_text,
        }
    }

    /// The estimate for the turn this utterance interrupted: the last link of its
    /// barge-in chain (the reply the speech actually cut). `None` when the utterance
    /// interrupted nothing.
    pub fn for_utterance(u: &Utterance) -> Option<Self> {
        u.barge_in
            .as_ref()
            .and_then(|b| b.chain.last())
            .map(Self::from_segment)
    }
}

#[derive(Serialize)]
struct WakeWire<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    pod: &'a str,
}

#[derive(Serialize)]
struct UtteranceWire<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    utterance: u64,
    pod: &'a str,
    room: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    speaker: Option<&'a str>,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    interrupted: Option<InterruptedBody>,
}

#[derive(Serialize)]
struct InterruptionWire<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    pod: &'a str,
    room: &'a str,
    interrupted: InterruptedBody,
}

/// Render an outbound body. Every one of these shapes is a flat struct of strings
/// and integers, so serialization cannot fail; a failure here would be a serde
/// contract violation, not a runtime condition to handle.
fn render<T: Serialize>(body: &T) -> String {
    serde_json::to_string(body).expect("outbound body serializes")
}

/// The advisory pre-warm nudge: a wake word fired on `pod`, a command may follow.
pub fn wake_body(pod: &PodId) -> String {
    render(&WakeWire {
        kind: "wake",
        pod: &pod.0,
    })
}

/// The conversation turn: who said what, where, and — on a barge-in dispatch —
/// where the reply it cut off had reached. One atomic message carries both, so the
/// interruption estimate can neither be reordered against the new speech nor
/// half-delivered.
pub fn utterance_body(u: &Utterance, text: &str) -> String {
    render(&UtteranceWire {
        kind: "utterance",
        utterance: u.id.0,
        pod: &u.pod.0,
        room: &u.room.0,
        speaker: u.speaker.as_ref().map(|s| s.0.as_str()),
        text,
        interrupted: InterruptedBody::for_utterance(u),
    })
}

/// The notify-only notice for a barge-in that captured no usable command: a reply
/// was cut, and no utterance message will follow to say so. No reply is expected
/// or accepted for it.
pub fn interruption_body(pod: &PodId, room: &RoomId, interrupted: InterruptedBody) -> String {
    render(&InterruptionWire {
        kind: "interruption",
        pod: &pod.0,
        room: &room.0,
        interrupted,
    })
}

// --- inbound codec -----------------------------------------------------------

/// One control marker lifted out of a response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tag {
    /// Turn correlation. `to` is `None` when the marker carried no id — accepted
    /// optimistically for the pending turn by the delivery policy.
    Reply { to: Option<u64> },
    /// Flush what came with this message and expect a follow-up message.
    Continued,
    /// Hold the microphone open after this reply. In the wire vocabulary, not yet
    /// implemented.
    Listen,
    /// A tag-shaped island the vocabulary does not cover, or one mangled past
    /// parsing. Stripped from the speech and reported loudly, never spoken.
    Unknown { raw: String },
}

/// Tag-shaped islands in a response body. Deliberately not an XML grammar: the
/// body is speech with markers in it, and a parser that rejected the whole body
/// over one malformed marker would silence a turn.
///
/// An island opens at a `<` immediately followed by an ASCII letter (optionally
/// after a `/`), which is what keeps a bare `3 < 4` out of the scan, and closes at
/// the first `>`. The second alternative catches an opener that never closes —
/// the body ended, or the next `<` arrived first — so a mangled marker is stripped
/// instead of read aloud, and the markers after it still parse. An island that
/// reaches the scanner through that alternative is mangled by definition and never
/// a known marker.
static TAG_ISLAND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"</?[A-Za-z][^<>]*>|</?[A-Za-z][^<>]*").expect("valid regex"));

/// The `to` attribute of a reply marker, permissively: double-quoted,
/// single-quoted, or bare, with whitespace anywhere around the `=`.
static REPLY_TO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?:^|\s)to\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'/>]+))"#).expect("valid regex")
});

/// Whether an attribute list mentions `to` at all, so a marker whose id is present
/// but unreadable reads as mangled rather than as a marker without an id.
static REPLY_TO_KEY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|\s)to\s*=").expect("valid regex"));

/// Split a response body into the text to speak and the markers it carried, each
/// with its offset into that text. Known markers are lifted out and acted on by
/// the caller; unknown or mangled tag-shaped islands are lifted out as
/// [`Tag::Unknown`] and reported; everything else is speech, verbatim — including a
/// `<` that is not tag-shaped, and including `&amp;`, since the body is not XML and
/// nothing is entity-decoded.
///
/// The offsets are what a future speech-synchronized marker (an emote, a gaze
/// target) fires on: such a marker's meaning is its position in the speech. Nothing
/// acts on them today, but the scan never discards the position.
pub fn scan(body: &str) -> (String, Vec<(Tag, usize)>) {
    let mut text = String::with_capacity(body.len());
    let mut tags = Vec::new();
    let mut cursor = 0;
    for island in TAG_ISLAND.find_iter(body) {
        text.push_str(&body[cursor..island.start()]);
        tags.push((parse_island(island.as_str()), text.len()));
        cursor = island.end();
    }
    text.push_str(&body[cursor..]);
    (text, tags)
}

/// Classify one tag-shaped island.
///
/// Two shapes are deliberately admitted beyond the documented vocabulary, because
/// punishing a language model's markup habits costs a turn and buys nothing: the
/// trailing `/` of a self-closing marker is optional, and attributes the marker has
/// no use for (`continued="true"` on a reply) are ignored rather than fatal. Two
/// are deliberately not admitted: a closing form (`</reply>`) is not a marker in
/// this vocabulary, and an island the body ended before closing is mangled — both
/// are [`Tag::Unknown`], stripped and reported, so a divergence between the peer's
/// vocabulary and ours stays visible.
fn parse_island(raw: &str) -> Tag {
    let unknown = || Tag::Unknown { raw: raw.into() };
    let inner = raw
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'));
    // No closing `>`: the body ended mid-island.
    let Some(inner) = inner else { return unknown() };
    // A closing form carries no marker of its own.
    if inner.starts_with('/') {
        return unknown();
    }
    let inner = inner.strip_suffix('/').unwrap_or(inner);
    let name_len = inner
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == ':'))
        .unwrap_or(inner.len());
    let (name, attrs) = inner.split_at(name_len);
    match name {
        "reply" => parse_reply(attrs, raw),
        "continued" => Tag::Continued,
        "listen" => Tag::Listen,
        _ => unknown(),
    }
}

/// The reply marker's turn id: absent (the model forgot it), present and readable,
/// or present and unreadable — which mangles the whole marker, so it is stripped and
/// reported with its raw text instead of carrying a half-read id into the
/// correlation check.
///
/// Stripping the id is deliberate: an unreadable id is evidence of a typo in *this*
/// turn's marker, not a stale echo from the peer's context — so a partial read must
/// not reach the correlation check.
fn parse_reply(attrs: &str, raw: &str) -> Tag {
    let value = REPLY_TO
        .captures(attrs)
        .and_then(|caps| (1..=3).find_map(|i| caps.get(i)))
        .map(|m| m.as_str());
    match value {
        Some(value) => match value.trim().parse::<u64>() {
            Ok(to) => Tag::Reply { to: Some(to) },
            Err(_) => Tag::Unknown { raw: raw.into() },
        },
        None if REPLY_TO_KEY.is_match(attrs) => Tag::Unknown { raw: raw.into() },
        None => Tag::Reply { to: None },
    }
}

// --- the instruction document ------------------------------------------------

/// The channel names the instruction document names for its reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelpChannels {
    /// Where the pod publishes utterances — where they *arrive*, from the reader's
    /// side.
    pub publish: String,
    /// Where the reader publishes replies.
    pub response: String,
    /// Where the pod publishes wake nudges, when it publishes them at all.
    pub wake: Option<String>,
}

/// The contract document, written at the language model that answers on the bus —
/// not at a human reading reference documentation. It is prompt-style: direct
/// imperatives, the exact markers to emit, and a worked example. The channel names
/// are its only dynamic content.
///
/// It lives next to the codec on purpose. The instructions and the parser can only
/// diverge if someone edits one of them alone, and here that is one file.
pub fn response_contract_help(channels: &HelpChannels) -> String {
    let wake_section = match &channels.wake {
        Some(wake) => format!(
            "\n\
             A `{{\"type\": \"wake\", \"pod\": \"...\"}}` notice may arrive on `{wake}`: a wake\n\
             word just fired on that pod and a command may follow within a few seconds. Nothing\n\
             has been asked yet. Do not reply to it. It exists so you can start warming up.\n"
        ),
        None => String::new(),
    };
    format!(
        "{HELP_INTRO}\n\
         Utterances from the pods arrive on the channel `{publish}`.\n\
         Publish every reply on the channel `{response}`.\n\
         {wake_section}{HELP_BODY}",
        publish = channels.publish,
        response = channels.response,
    )
}

const HELP_INTRO: &str = "\
You are the voice of a home speech pod. Every message you publish on the response
channel is synthesized and spoken aloud to whoever is standing in the room, so
write speech: plain sentences, no markdown, no lists, no emoji, no stage
directions, nothing that only makes sense on a screen.
";

const HELP_BODY: &str = "
# What arrives

Each message on the utterance channel is one JSON object with a `type` field.

`{\"type\": \"utterance\", ...}` — somebody spoke to a pod. Answer it. Fields:

  utterance    integer id for this turn. Your reply must name it.
  pod          which pod heard the speech.
  room         which room that pod is in.
  speaker      who was recognized speaking, when known; absent when not.
  text         the speech-to-text transcript. This is what to answer.
  interrupted  present only when this person cut your previous reply off
               mid-sentence. See below.

`{\"type\": \"interruption\", ...}` — you were cut off mid-reply, and no usable
command came out of the speech that cut you. Carries `pod`, `room`, and
`interrupted`. Never reply to this message. No turn is pending, so anything you
publish in response to it is dropped. Note it and wait.

# The `interrupted` object

  utterance    id of the turn whose reply was cut off — one of your earlier answers.
  heard_ms     milliseconds of that reply the listener heard before the cut.
  total_ms     estimated length of the whole reply.
  heard_text   best-effort estimate of the words they actually heard, or null when
               there is no estimate. It is an estimate from timing, not a
               recording: do not quote it back as if it were exact.

Where it appears is the whole difference:

- Inside an `utterance`: they cut you off AND then said this. Answer the new
  `text`, and use the estimate to know what they had already heard so you do not
  repeat it.
- As a standalone `interruption`: they cut you off and nothing usable was
  captured. They may not even have been talking to you. Say nothing.

# What to publish

Publish the words to be spoken, with control markers inline. The body is text, not
JSON, and not XML — only the markers below are lifted out of it.

Start every reply with `<reply to=\"N\"/>`, where N is the `utterance` field of the
message you are answering. That is the only way the pod knows which turn your words
belong to. Then write what should be said.

The pod sends:

  {\"type\": \"utterance\", \"utterance\": 41, \"pod\": \"kitchen\", \"room\": \"kitchen\",
   \"text\": \"what's the weather like\"}

You publish:

  <reply to=\"41\"/>Sixty-eight and clear right now, with rain expected after nine.

# Answering in parts

If the first sentence is ready before the rest, end a partial reply with
`<continued/>` and send the remainder as a follow-up message. Everything before the
marker is spoken immediately, which is how a slow answer stops sounding slow.

  <reply to=\"42\"/>Let me check the forecast.<continued/>
  <reply to=\"42\" continued=\"true\"/>Sixty-eight and clear, with rain after nine.

The last message of a turn must NOT carry `<continued/>`: that marker is a promise
of another message, and the pod waits for it before the turn is over.

# The markers

  <reply to=\"N\"/>   first thing in a reply; N is the utterance id you answer.
  <continued/>      at the end of a partial reply; another message follows.
  <listen/>         at the end of a reply, to keep the microphone open so the
                    person can keep talking without the wake word. Accepted, but
                    not implemented yet — it is stripped and ignored today.

One reply per turn, plus its continuations. Anything else tag-shaped is stripped
out of the speech and reported as an error, so do not invent markers and do not
wrap your reply in XML or markdown.
";

// --- the brain ---------------------------------------------------------------

/// Upper bound on the continuations one turn may chain. A constant rather than
/// config: it guards against a peer that promises a follow-up forever, and a
/// deployment has no reason to want a different bound — a chain this long is a
/// runaway either way. It also bounds how long the pipeline's dispatch stays parked,
/// and with it how stale a queued barge-in can get.
const MAX_CONTINUATIONS: u32 = 16;

/// Depth of one turn's response slot. A turn is answered in a handful of messages;
/// the depth exists so a follow-up arriving while `handle` is between segments is
/// held rather than refused, not to buffer a stream.
const SLOT_CAPACITY: usize = 4;

/// How many of one message's markers are reported individually. Nothing bounds a
/// body but the bus's size limit, and every `<` followed by a letter opens an
/// island — so a peer that wrapped its answer in HTML could otherwise mint
/// thousands of loud events from a single message and bury the line an operator
/// needs. The counters are not capped, only the events.
const MAX_MARKER_REPORTS: usize = 4;

/// The turn awaiting a response and the channel its segments arrive on.
///
/// A channel, not a oneshot, and that is load-bearing for a multi-part turn: a
/// oneshot would have to be re-armed between segments, and a follow-up message
/// landing in the re-arm gap would be dropped as though no turn were pending. Armed
/// once per turn by [`SlotGuard::arm`] and released by its [`Drop`];
/// [`BrennBrain::deliver`] never takes it, so no such gap exists.
struct PendingTurn {
    utterance: UtteranceId,
    tx: mpsc::Sender<ParsedResponse>,
}

/// One response message with its markers already lifted out.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedResponse {
    /// The speech, verbatim but for the stripped markers.
    text: String,
    /// The message promised a follow-up.
    continued: bool,
}

/// What [`BrennBrain::deliver`] did with a response message. Everything but
/// `Delivered` is a drop, rendered by the caller: a message no turn owns produces no
/// brain event, since there is no turn to attribute it to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverOutcome {
    /// Queued to the pending turn.
    Delivered,
    /// No turn was awaiting a response.
    NoTurnPending,
    /// The message named a different turn than the pending one. The slot stays armed.
    ReplyMismatch { pending: UtteranceId },
    /// The pending turn's slot was full. A should-not-happen guard.
    Backlogged,
}

/// The `Brain` that answers over a pub/sub link: publish the utterance, await the
/// peer's response messages, speak each one as it arrives.
///
/// One turn is in flight at a time, which the pipeline guarantees by awaiting
/// `handle` inline — so a single pending slot is the whole correlation mechanism, and
/// the `reply to` marker is a check on it rather than a router.
pub struct BrennBrain {
    link: Arc<dyn BrainLink>,
    /// Publish → first response message.
    response_timeout: Duration,
    /// Between promised continuation segments. Shorter than the initial window by
    /// convention: once the peer has started answering, a long silence is evidence of
    /// a lost continuation rather than a slow think.
    continuation_timeout: Duration,
    /// Spoken when a turn ends on a link failure. Whitespace-only disables it.
    failure_message: String,
    events: BrainEventFn,
    stats: Arc<BrainStats>,
    /// Shared, not a plain field: `handle` returns a `'static` future that cannot
    /// borrow `&self`, so the slot it arms and the slot `deliver` reads have to be the
    /// same shared cell. Held only for slot operations, never across an await.
    pending: Arc<Mutex<Option<PendingTurn>>>,
}

impl BrennBrain {
    /// Build a brain over `link`. `failure_message` is spoken when a turn ends on a
    /// link failure; a whitespace-only string keeps such a turn silent.
    pub fn new(
        link: Arc<dyn BrainLink>,
        response_timeout: Duration,
        continuation_timeout: Duration,
        failure_message: String,
        events: BrainEventFn,
        stats: Arc<BrainStats>,
    ) -> Self {
        Self {
            link,
            response_timeout,
            continuation_timeout,
            failure_message,
            events,
            stats,
            pending: Arc::new(Mutex::new(None)),
        }
    }

    /// Hand one response-channel message to the turn awaiting it.
    ///
    /// Synchronous and cheap throughout — a regex scan and a `try_send` — because the
    /// caller drives the transport's event channel, which back-pressures the socket
    /// read while it is not being drained.
    pub fn deliver(&self, body: &str) -> DeliverOutcome {
        let slot = self.pending.lock().expect("pending turn slot");
        let Some(turn) = slot.as_ref() else {
            // Nothing asked for this. Dropped whole: no turn owns it, so it earns no
            // per-marker events either.
            return DeliverOutcome::NoTurnPending;
        };
        let utterance = turn.utterance;
        let (text, tags) = scan(body);
        let named = tags.iter().find_map(|(tag, _)| match tag {
            Tag::Reply { to } => *to,
            _ => None,
        });
        let assumed = match named {
            // A specific but wrong id is positive evidence the message is not ours —
            // a stale id echoed out of the peer's own context. The slot stays armed,
            // so a stray cannot starve the real response out of its window.
            Some(to) if to != utterance.0 => {
                return DeliverOutcome::ReplyMismatch { pending: utterance };
            }
            Some(_) => false,
            // The marker was forgotten, or mangled past reading and stripped to the
            // same place. With one pending slot there is only one turn this could
            // belong to, so accept it — and say so, once it is in.
            None => true,
        };
        let parsed = ParsedResponse {
            text,
            continued: tags.iter().any(|(tag, _)| *tag == Tag::Continued),
        };
        // The queue push comes before the per-marker events on purpose. A message the
        // turn never receives has had none of its markers acted on, and events saying
        // otherwise would sit in the stream beside the drop that contradicts them.
        if turn.tx.try_send(parsed).is_err() {
            // Full, or — unreachable, since the slot outlives the receiver on every
            // path including a dropped turn — closed. Either way the message is gone
            // and the caller reports it.
            return DeliverOutcome::Backlogged;
        }
        if assumed {
            (self.events)(BrainEvent::LinkReplyAssumed { utterance });
            self.stats.record_link_reply_assumed();
        }
        // `link_tags_stripped` stays the complete tally an operator reads off
        // `stage_health`; the per-marker events are capped so an oversized body
        // cannot mint them by the thousand.
        let mut reported = 0usize;
        for (tag, _) in &tags {
            match tag {
                Tag::Unknown { raw } => {
                    self.stats.record_link_tag_stripped();
                    if reported < MAX_MARKER_REPORTS {
                        (self.events)(BrainEvent::LinkTagStripped {
                            utterance,
                            tag: raw.clone(),
                        });
                        reported += 1;
                    }
                }
                Tag::Listen => {
                    // TODO(brenn-brain-listen): hold the mic open for a follow-up
                    // instead of reporting the request unsupported.
                    self.stats.record_link_listen_unsupported();
                    if reported < MAX_MARKER_REPORTS {
                        (self.events)(BrainEvent::LinkListenUnsupported { utterance });
                        reported += 1;
                    }
                }
                Tag::Reply { .. } | Tag::Continued => {}
            }
        }
        DeliverOutcome::Delivered
    }
}

/// Holds the pending slot for one turn and releases it however that turn ends —
/// including a `handle` future dropped part-way through, which no explicit release
/// at a return path can cover. A leaked slot would be quietly misleading rather than
/// loud: later messages would be refused as a backlog, and the next turn would find
/// the slot occupied.
///
/// The release clears only this turn's arming. A slot some later dispatch has
/// already re-armed belongs to that turn, and taking it would strand it.
struct SlotGuard {
    pending: Arc<Mutex<Option<PendingTurn>>>,
    utterance: UtteranceId,
}

impl SlotGuard {
    fn arm(
        pending: Arc<Mutex<Option<PendingTurn>>>,
        utterance: UtteranceId,
        tx: mpsc::Sender<ParsedResponse>,
    ) -> SlotGuard {
        {
            let mut slot = pending.lock().expect("pending turn slot");
            debug_assert!(
                slot.is_none(),
                "inline dispatch guarantees one turn in flight, so the slot is free"
            );
            *slot = Some(PendingTurn { utterance, tx });
        }
        SlotGuard { pending, utterance }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let mut slot = self.pending.lock().expect("pending turn slot");
        if slot
            .as_ref()
            .is_some_and(|turn| turn.utterance == self.utterance)
        {
            *slot = None;
        }
    }
}

/// Queue one spoken segment for `u`. Whitespace-only text queues nothing: a message
/// that was all markers has nothing to say, and the same rule is what makes a
/// whitespace-only failure message the way to disable the spoken fallback.
fn speak(
    out: &mut ResponseSink,
    u: &Utterance,
    text: String,
    events: &BrainEventFn,
    stats: &BrainStats,
) {
    if text.trim().is_empty() {
        return;
    }
    let cmd = SpeakCmd {
        target: u.pod.clone(),
        in_reply_to: Some(u.id),
        body: SpeakBody::Text(text),
        interruptible: true,
        timings: u.timings.clone(),
    };
    send_or_report(out, cmd, u.id, events, stats);
}

impl Brain for BrennBrain {
    /// Publish the turn, then speak each response segment as it arrives, returning
    /// only when the peer stops promising more.
    ///
    /// The pipeline awaits this inline, and the turn ledger's settlement rests on the
    /// return being the proof that no further command is coming for this turn — which
    /// the segment loop preserves: nothing is queued after it returns. What the loop
    /// does cost is a longer park, with flushed segments already playing while it
    /// waits, so a barge-in over one of them is not processed until it returns.
    ///
    /// A caller that drops this future part-way — a cancelling `select!` arm, an
    /// aborted task — ends the turn with whatever was already spoken. The pending
    /// slot is released on the drop ([`SlotGuard`]), so the next turn arms a free
    /// slot and messages for the abandoned one are refused rather than queued.
    fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, ()> {
        let link = Arc::clone(&self.link);
        let events = Arc::clone(&self.events);
        let stats = Arc::clone(&self.stats);
        let pending = Arc::clone(&self.pending);
        let response_timeout = self.response_timeout;
        let continuation_timeout = self.continuation_timeout;
        let failure_message = self.failure_message.clone();
        async move {
            let utterance = u.id;
            let text = u
                .transcript
                .as_ref()
                .map(|t| t.text.trim())
                .filter(|t| !t.is_empty());
            let Some(text) = text else {
                // A barge that carved nothing usable still cut a reply, and no
                // utterance message will follow to say so — the notice is the peer's
                // only way to learn it was interrupted. Not a link failure, so
                // nothing is spoken.
                if let Some(interrupted) = InterruptedBody::for_utterance(&u) {
                    link.notify_interruption(interruption_body(&u.pod, &u.room, interrupted));
                }
                (events)(BrainEvent::NoTranscript { utterance });
                stats.record_no_transcript();
                return;
            };
            let body = utterance_body(&u, text);
            let (tx, mut rx) = mpsc::channel::<ParsedResponse>(SLOT_CAPACITY);
            let slot = SlotGuard::arm(pending, utterance, tx);
            if let Err(err) = link.publish_utterance(body).await {
                // No retry: a publish lost between the socket and its answer may
                // still have landed, and a second copy would have the peer answer
                // twice.
                (events)(BrainEvent::LinkPublishFailed {
                    utterance,
                    detail: err.detail,
                });
                stats.record_link_publish_failure();
                // Released before the apology is queued, so a message racing in
                // behind the refusal finds no turn to join.
                drop(slot);
                speak(&mut out, &u, failure_message, &events, &stats);
                return;
            }
            let mut window = response_timeout;
            let mut continuation = false;
            let mut segments: u32 = 0;
            loop {
                match tokio::time::timeout(window, rx.recv()).await {
                    Err(_) => {
                        // Whatever was already flushed stays spoken; the apology marks
                        // a truncated turn as over and broken rather than leaving the
                        // user waiting on silence.
                        (events)(BrainEvent::LinkResponseTimeout {
                            utterance,
                            waited_ms: window.as_millis() as u64,
                            continuation,
                        });
                        stats.record_link_response_timeout();
                        drop(slot);
                        speak(&mut out, &u, failure_message, &events, &stats);
                        return;
                    }
                    Ok(Some(parsed)) => {
                        let promised = parsed.continued;
                        speak(&mut out, &u, parsed.text, &events, &stats);
                        if !promised {
                            return;
                        }
                        segments += 1;
                        if segments >= MAX_CONTINUATIONS {
                            // The capping segment was real content, so the turn ends
                            // as if it were terminal — no apology.
                            (events)(BrainEvent::LinkContinuationCapped {
                                utterance,
                                segments,
                            });
                            stats.record_link_continuation_capped();
                            return;
                        }
                        window = continuation_timeout;
                        continuation = true;
                    }
                    Ok(None) => {
                        debug_assert!(
                            false,
                            "the slot holds the only sender, so the channel cannot close here"
                        );
                        return;
                    }
                }
            }
        }
        .boxed()
    }

    fn interrupt(&self, _id: UtteranceId, _progress: InterruptProgress) {
        // Nothing to do. The peer learns about the cut from the `interrupted` field of
        // the next dispatched utterance, or — when the barge captured no usable
        // command — from a standalone interruption notice. The ledger has already
        // captured the segment those are built from by the time this fires, so
        // recording anything here would only duplicate it.
    }

    fn wake(&self, pod: &PodId) {
        self.link.notify_wake(wake_body(pod));
    }

    fn barge_declined(&self, u: &Utterance) {
        // No chain means nothing was interrupted — a barge whose flush was stale, so
        // there is nothing to report.
        let Some(interrupted) = InterruptedBody::for_utterance(u) else {
            return;
        };
        self.link
            .notify_interruption(interruption_body(&u.pod, &u.room, interrupted));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        BargeInContext, InterruptProgress, SpeakerId, Transcript, UtteranceId, test_utterance,
    };
    use serde_json::{Value, json};

    fn segment(response: Option<&str>, heard_ms: u64, total_ms: u64) -> ContextSegment {
        ContextSegment {
            utterance: UtteranceId(122),
            transcript: Some("earlier question".into()),
            response_text: response.map(String::from),
            interrupted: InterruptProgress { heard_ms, total_ms },
        }
    }

    fn with_chain(u: Utterance, chain: Vec<ContextSegment>) -> Utterance {
        Utterance {
            barge_in: Some(BargeInContext {
                chain: chain.into(),
            }),
            ..u
        }
    }

    fn parsed(body: &str) -> Value {
        serde_json::from_str(body).expect("body is JSON")
    }

    fn tags(body: &str) -> Vec<Tag> {
        scan(body).1.into_iter().map(|(tag, _)| tag).collect()
    }

    // --- outbound bodies ---

    #[test]
    fn wake_body_names_the_pod() {
        assert_eq!(
            parsed(&wake_body(&PodId("kitchen".into()))),
            json!({ "type": "wake", "pod": "kitchen" })
        );
    }

    #[test]
    fn utterance_body_carries_the_turn_and_omits_an_unknown_speaker() {
        let u = test_utterance();
        assert_eq!(
            parsed(&utterance_body(&u, "what is the weather")),
            json!({
                "type": "utterance",
                "utterance": 42,
                "pod": "pod-x",
                "room": "kitchen",
                "text": "what is the weather",
            })
        );
    }

    #[test]
    fn utterance_body_carries_a_known_speaker() {
        let u = Utterance {
            speaker: Some(SpeakerId("alice".into())),
            ..test_utterance()
        };
        assert_eq!(parsed(&utterance_body(&u, "hi"))["speaker"], json!("alice"));
    }

    #[test]
    fn utterance_body_carries_the_cut_estimate_of_the_turn_it_interrupted() {
        // Half of a 26-char reply, trimmed back to the last whole word.
        let u = with_chain(
            test_utterance(),
            vec![segment(Some("the weather today is sunny"), 500, 1_000)],
        );
        assert_eq!(
            parsed(&utterance_body(&u, "no cancel that"))["interrupted"],
            json!({
                "utterance": 122,
                "heard_ms": 500,
                "total_ms": 1_000,
                "heard_text": "the weather",
            })
        );
    }

    #[test]
    fn utterance_body_reports_the_last_chain_segment_only() {
        let u = with_chain(
            test_utterance(),
            vec![
                segment(Some("first reply text"), 900, 1_000),
                ContextSegment {
                    utterance: UtteranceId(130),
                    ..segment(Some("second reply here"), 400, 1_000)
                },
            ],
        );
        assert_eq!(
            parsed(&utterance_body(&u, "no the other one"))["interrupted"]["utterance"],
            json!(130)
        );
    }

    #[test]
    fn a_pcm_reply_has_a_null_heard_text() {
        // Nothing to cut: the estimate is explicitly null, not omitted, so the peer
        // sees one schema whether or not there were words.
        let u = with_chain(test_utterance(), vec![segment(None, 300, 1_000)]);
        let interrupted = parsed(&utterance_body(&u, "wait"))["interrupted"].clone();
        assert_eq!(interrupted["heard_text"], Value::Null);
        assert_eq!(interrupted["heard_ms"], json!(300));
    }

    #[test]
    fn a_cut_before_the_first_word_has_a_null_heard_text() {
        let u = with_chain(
            test_utterance(),
            vec![segment(Some("the weather today"), 0, 1_000)],
        );
        assert_eq!(
            parsed(&utterance_body(&u, "stop"))["interrupted"]["heard_text"],
            Value::Null
        );
    }

    #[test]
    fn an_utterance_that_interrupted_nothing_omits_the_estimate() {
        let body = parsed(&utterance_body(&test_utterance(), "hello"));
        assert!(
            body.get("interrupted").is_none(),
            "no barge chain, no estimate: {body}"
        );
    }

    #[test]
    fn interruption_body_reuses_the_estimate_shape() {
        let interrupted =
            InterruptedBody::from_segment(&segment(Some("one two three"), 700, 1_000));
        assert_eq!(
            parsed(&interruption_body(
                &PodId("pod-x".into()),
                &RoomId("kitchen".into()),
                interrupted,
            )),
            json!({
                "type": "interruption",
                "pod": "pod-x",
                "room": "kitchen",
                "interrupted": {
                    "utterance": 122,
                    "heard_ms": 700,
                    "total_ms": 1_000,
                    "heard_text": "one two",
                },
            })
        );
    }

    #[test]
    fn the_spoken_text_is_the_one_passed_in() {
        // The dispatched text is a parameter, not read from `u.transcript`: the
        // caller has already trimmed it and decided it is usable.
        let u = Utterance {
            transcript: Some(Transcript {
                text: "  padded  ".into(),
                confidence: None,
            }),
            ..test_utterance()
        };
        assert_eq!(
            parsed(&utterance_body(&u, "padded"))["text"],
            json!("padded")
        );
    }

    // --- inbound codec ---

    #[test]
    fn a_reply_marker_is_lifted_out_with_its_id() {
        let (text, tags) = scan("<reply to=\"123\"/>Hello there.");
        assert_eq!(text, "Hello there.");
        assert_eq!(tags, vec![(Tag::Reply { to: Some(123) }, 0)]);
    }

    #[test]
    fn reply_attribute_quoting_is_permissive() {
        for body in [
            "<reply to=\"123\"/>",
            "<reply to='123'/>",
            "<reply to=123/>",
            "<reply  to = \"123\" />",
            "<reply to=123>",
            "<reply to=\"123\" continued=\"true\"/>",
        ] {
            assert_eq!(
                tags(body),
                vec![Tag::Reply { to: Some(123) }],
                "body: {body}"
            );
        }
    }

    #[test]
    fn a_continued_attribute_on_the_reply_marker_is_inert() {
        // Sloppy marker placement is not punished: the attribute parses and carries
        // no protocol weight of its own.
        assert_eq!(
            tags("<reply to=\"7\" continued=\"true\"/>more"),
            vec![Tag::Reply { to: Some(7) }]
        );
    }

    #[test]
    fn a_reply_marker_without_an_id_carries_none() {
        assert_eq!(tags("<reply/>Hello."), vec![Tag::Reply { to: None }]);
    }

    #[test]
    fn a_reply_marker_with_an_unreadable_id_is_unknown() {
        // Present but unreadable is mangled: the half-read id never reaches the
        // correlation check, and the raw marker is reported instead. What the
        // delivery policy then does with it is pinned by
        // `a_message_whose_reply_id_is_mangled_is_accepted_and_reported`.
        assert_eq!(
            tags("<reply to=\"abc\"/>Hello."),
            vec![Tag::Unknown {
                raw: "<reply to=\"abc\"/>".into()
            }]
        );
        assert_eq!(
            tags("<reply to=/>Hello."),
            vec![Tag::Unknown {
                raw: "<reply to=/>".into()
            }]
        );
    }

    #[test]
    fn the_continuation_and_listen_markers_are_lifted_out() {
        let (text, tags) = scan("Checking.<continued/>");
        assert_eq!(text, "Checking.");
        assert_eq!(tags, vec![(Tag::Continued, 9)]);

        let (text, tags) = scan("Go on.<listen/>");
        assert_eq!(text, "Go on.");
        assert_eq!(tags, vec![(Tag::Listen, 6)]);
    }

    #[test]
    fn a_marker_anywhere_in_the_body_counts() {
        let (text, tags) = scan("one <continued/>two");
        assert_eq!(text, "one two");
        assert_eq!(tags, vec![(Tag::Continued, 4)]);
    }

    #[test]
    fn an_unknown_well_formed_tag_is_stripped_and_reported_raw() {
        let (text, tags) = scan("Hi <emote:happy/>there.");
        assert_eq!(text, "Hi there.");
        assert_eq!(
            tags,
            vec![(
                Tag::Unknown {
                    raw: "<emote:happy/>".into()
                },
                3
            )]
        );
    }

    #[test]
    fn a_closing_form_is_unknown() {
        // The vocabulary is self-closing markers; a closing tag is markup we neither
        // speak nor act on, and the divergence should be visible.
        let (text, tags) = scan("<reply to=\"1\">Hi.</reply>");
        assert_eq!(text, "Hi.");
        assert_eq!(
            tags,
            vec![
                (Tag::Reply { to: Some(1) }, 0),
                (
                    Tag::Unknown {
                        raw: "</reply>".into()
                    },
                    3
                ),
            ]
        );
    }

    #[test]
    fn an_island_the_body_ended_before_closing_is_stripped_as_unknown() {
        let (text, tags) = scan("Hello there. <reply to=\"12");
        assert_eq!(text, "Hello there. ");
        assert_eq!(
            tags,
            vec![(
                Tag::Unknown {
                    raw: "<reply to=\"12".into()
                },
                13
            )]
        );
    }

    #[test]
    fn a_mangled_island_does_not_poison_the_rest_of_the_message() {
        let (text, tags) = scan("<reply to=\"5\"/>One <foo two <continued/>three");
        assert_eq!(text, "One three");
        assert_eq!(
            tags,
            vec![
                (Tag::Reply { to: Some(5) }, 0),
                (
                    Tag::Unknown {
                        raw: "<foo two ".into()
                    },
                    4
                ),
                (Tag::Continued, 4),
            ]
        );
    }

    #[test]
    fn a_bare_less_than_is_spoken_verbatim() {
        let (text, tags) = scan("three < four and 4 > 3");
        assert_eq!(text, "three < four and 4 > 3");
        assert!(tags.is_empty());
    }

    #[test]
    fn entities_are_not_decoded() {
        // The body is not XML; `&amp;` is five characters of speech input.
        let (text, tags) = scan("Bed &amp; breakfast");
        assert_eq!(text, "Bed &amp; breakfast");
        assert!(tags.is_empty());
    }

    #[test]
    fn a_tags_only_body_scans_to_empty_text() {
        let (text, tags) = scan("<reply to=\"9\"/><continued/>");
        assert!(text.is_empty(), "text: {text:?}");
        assert_eq!(
            tags,
            vec![(Tag::Reply { to: Some(9) }, 0), (Tag::Continued, 0)]
        );
    }

    #[test]
    fn tag_offsets_index_into_the_stripped_text() {
        // The offset is where the marker fell in the *speech*, which is what a
        // future speech-synchronized marker fires on.
        let (text, tags) = scan("Hello<emote:happy/> there<listen/>");
        assert_eq!(text, "Hello there");
        let offsets: Vec<usize> = tags.iter().map(|(_, at)| *at).collect();
        assert_eq!(offsets, vec![5, 11]);
        assert_eq!(&text[..offsets[0]], "Hello");
    }

    #[test]
    fn an_empty_body_scans_to_nothing() {
        let (text, tags) = scan("");
        assert!(text.is_empty());
        assert!(tags.is_empty());
    }

    // --- the instruction document ---

    fn help_channels() -> HelpChannels {
        HelpChannels {
            publish: "brenn:pod.utterance".into(),
            response: "brenn:pod.speak".into(),
            wake: Some("brenn:pod.wake".into()),
        }
    }

    #[test]
    fn the_help_document_names_every_marker_type_and_channel() {
        // The document and the codec live in one file so they cannot diverge
        // silently; this is the assertion that makes "cannot" true. Every marker the
        // codec knows, every outbound message type, and every configured channel has
        // to appear verbatim.
        let help = response_contract_help(&help_channels());
        for needle in [
            "<reply to=\"N\"/>",
            "<continued/>",
            "<listen/>",
            "\"type\": \"wake\"",
            "\"type\": \"utterance\"",
            "\"type\": \"interruption\"",
            "brenn:pod.utterance",
            "brenn:pod.speak",
            "brenn:pod.wake",
        ] {
            assert!(help.contains(needle), "help text is missing {needle:?}");
        }
    }

    #[test]
    fn the_help_document_draws_the_two_interruption_cases_apart() {
        let help = response_contract_help(&help_channels());
        assert!(help.contains("Never reply to this message."));
        assert!(help.contains("cut you off AND then said this"));
        // It must also say what each field of the estimate means.
        for field in ["heard_ms", "total_ms", "heard_text"] {
            assert!(help.contains(field), "help text is missing {field:?}");
        }
    }

    // --- the brain ---

    /// What a [`BrainLink`] was asked to carry, in order.
    #[derive(Default)]
    struct LinkLog {
        utterances: Vec<String>,
        wakes: Vec<String>,
        interruptions: Vec<String>,
    }

    /// A recording `BrainLink` that answers every publish with a scripted result.
    struct FakeLink {
        log: Arc<Mutex<LinkLog>>,
        publish: Result<(), LinkError>,
    }

    impl BrainLink for FakeLink {
        fn publish_utterance(&self, body: String) -> BoxFuture<'static, Result<(), LinkError>> {
            self.log.lock().unwrap().utterances.push(body);
            let result = self.publish.clone();
            async move { result }.boxed()
        }
        fn notify_wake(&self, body: String) {
            self.log.lock().unwrap().wakes.push(body);
        }
        fn notify_interruption(&self, body: String) {
            self.log.lock().unwrap().interruptions.push(body);
        }
    }

    const FAILURE_MESSAGE: &str = "Sorry, something's not working right now.";
    const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
    const CONTINUATION_TIMEOUT: Duration = Duration::from_secs(10);

    struct Fixture {
        brain: Arc<BrennBrain>,
        log: Arc<Mutex<LinkLog>>,
        events: Arc<Mutex<Vec<BrainEvent>>>,
        stats: Arc<BrainStats>,
        speak_tx: futures::channel::mpsc::Sender<SpeakCmd>,
        speak_rx: futures::channel::mpsc::Receiver<SpeakCmd>,
    }

    impl Fixture {
        fn new() -> Fixture {
            Fixture::with(Ok(()), FAILURE_MESSAGE)
        }

        fn with(publish: Result<(), LinkError>, failure_message: &str) -> Fixture {
            let log = Arc::new(Mutex::new(LinkLog::default()));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&seen);
            let events: BrainEventFn = Arc::new(move |e| sink.lock().unwrap().push(e));
            let stats = Arc::new(BrainStats::default());
            let link = Arc::new(FakeLink {
                log: Arc::clone(&log),
                publish,
            });
            let brain = Arc::new(BrennBrain::new(
                link,
                RESPONSE_TIMEOUT,
                CONTINUATION_TIMEOUT,
                failure_message.into(),
                events,
                Arc::clone(&stats),
            ));
            // Deep enough that a full continuation chain never fills it: an unrelated
            // `SinkFull` in the middle of a turn would be a confusing failure.
            let (speak_tx, speak_rx) = futures::channel::mpsc::channel::<SpeakCmd>(32);
            Fixture {
                brain,
                log,
                events: seen,
                stats,
                speak_tx,
                speak_rx,
            }
        }

        /// Dispatch `u` the way the pipeline does — awaited concurrently, so the test
        /// can play the peer while the turn is parked.
        fn dispatch(&self, u: Utterance) -> tokio::task::JoinHandle<()> {
            let sink = ResponseSink::new(self.speak_tx.clone());
            tokio::spawn(self.brain.handle(u, sink))
        }

        /// Deliver `body` once the turn has armed its slot. The turn has to reach its
        /// response await first, and only a spawned task can get there.
        async fn deliver_armed(&self, body: &str) -> DeliverOutcome {
            for _ in 0..64 {
                let outcome = self.brain.deliver(body);
                if outcome != DeliverOutcome::NoTurnPending {
                    return outcome;
                }
                tokio::task::yield_now().await;
            }
            panic!("the turn never armed its response slot");
        }

        fn spoken(&mut self) -> Vec<String> {
            let mut out = Vec::new();
            while let Ok(cmd) = self.speak_rx.try_recv() {
                assert_eq!(cmd.in_reply_to, Some(UtteranceId(42)));
                assert!(cmd.interruptible, "a reply is always interruptible");
                match cmd.body {
                    SpeakBody::Text(text) => out.push(text),
                    SpeakBody::Pcm(_) => panic!("a link brain replies with text"),
                }
            }
            out
        }

        fn events(&self) -> Vec<BrainEvent> {
            self.events.lock().unwrap().clone()
        }

        fn published(&self) -> Vec<Value> {
            self.log
                .lock()
                .unwrap()
                .utterances
                .iter()
                .map(|body| parsed(body))
                .collect()
        }
    }

    #[tokio::test]
    async fn a_turn_publishes_its_utterance_and_speaks_the_reply() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        assert_eq!(
            f.deliver_armed("<reply to=\"42\"/>Sixty-eight and clear.")
                .await,
            DeliverOutcome::Delivered
        );
        turn.await.unwrap();

        assert_eq!(
            f.published(),
            vec![json!({
                "type": "utterance",
                "utterance": 42,
                "pod": "pod-x",
                "room": "kitchen",
                "text": "what is the weather",
            })]
        );
        assert_eq!(f.spoken(), ["Sixty-eight and clear."]);
        assert!(
            f.events().is_empty(),
            "a clean turn is silent: {:?}",
            f.events()
        );
        // The turn is over, so the slot is free and a late message finds nothing.
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>late"),
            DeliverOutcome::NoTurnPending
        );
    }

    #[tokio::test]
    async fn a_barge_dispatch_publishes_the_cut_estimate() {
        let mut f = Fixture::new();
        let u = with_chain(
            test_utterance(),
            vec![segment(Some("the weather today is sunny"), 500, 1_000)],
        );
        let turn = f.dispatch(u);
        f.deliver_armed("<reply to=\"42\"/>Cancelled.").await;
        turn.await.unwrap();

        assert_eq!(
            f.published()[0]["interrupted"],
            json!({
                "utterance": 122,
                "heard_ms": 500,
                "total_ms": 1_000,
                "heard_text": "the weather",
            })
        );
        assert_eq!(f.spoken(), ["Cancelled."]);
    }

    #[tokio::test]
    async fn a_multi_part_response_speaks_each_segment_and_returns_on_the_terminal_one() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Let me check.<continued/>")
            .await;
        assert!(!turn.is_finished());
        f.deliver_armed("<reply to=\"42\" continued=\"true\"/>Sixty-eight and clear.")
            .await;
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Let me check.", "Sixty-eight and clear."]);
        assert!(f.events().is_empty(), "{:?}", f.events());
    }

    #[tokio::test]
    async fn a_follow_up_delivered_before_the_turn_polls_is_not_lost() {
        // Both messages land while the turn is parked on the first `recv`. With a
        // re-armed oneshot the second would arrive to an empty slot and be dropped;
        // the slot stays armed for the turn's whole life precisely so it cannot.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>First.<continued/>")
            .await;
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>Second."),
            DeliverOutcome::Delivered
        );
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["First.", "Second."]);
    }

    #[tokio::test(start_paused = true)]
    async fn no_response_at_all_times_out_and_apologizes() {
        let mut f = Fixture::new();
        f.dispatch(test_utterance()).await.unwrap();

        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkResponseTimeout {
                utterance: UtteranceId(42),
                waited_ms: 30_000,
                continuation: false,
            }]
        );
        assert_eq!(f.stats.snapshot().link_response_timeouts, 1);
        assert_eq!(f.spoken(), [FAILURE_MESSAGE]);
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>too late"),
            DeliverOutcome::NoTurnPending
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_promised_continuation_that_never_arrives_times_out_after_what_was_spoken() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Let me check.<continued/>")
            .await;
        turn.await.unwrap();

        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkResponseTimeout {
                utterance: UtteranceId(42),
                waited_ms: 10_000,
                continuation: true,
            }],
            "the continuation window is the shorter one"
        );
        assert_eq!(f.spoken(), ["Let me check.", FAILURE_MESSAGE]);
    }

    #[tokio::test]
    async fn a_runaway_continuation_chain_is_capped_without_an_apology() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        // Every message promises another, so the bound — not the peer — ends the turn.
        // Each is handed over singly: the slot is a handful of messages deep, not a
        // stream buffer.
        for n in 0..MAX_CONTINUATIONS {
            assert_eq!(
                f.deliver_armed(&format!("<reply to=\"42\"/>part {n}<continued/>"))
                    .await,
                DeliverOutcome::Delivered
            );
            tokio::task::yield_now().await;
        }
        turn.await.unwrap();

        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkContinuationCapped {
                utterance: UtteranceId(42),
                segments: MAX_CONTINUATIONS,
            }]
        );
        assert_eq!(f.stats.snapshot().link_continuations_capped, 1);
        let spoken = f.spoken();
        assert_eq!(spoken.len() as u32, MAX_CONTINUATIONS);
        assert_eq!(
            spoken.last().map(String::as_str),
            Some("part 15"),
            "the capping segment was real content and is spoken"
        );
    }

    #[tokio::test]
    async fn a_publish_failure_apologizes_without_awaiting_a_response() {
        let mut f = Fixture::with(Err(LinkError::new("bridge gone")), FAILURE_MESSAGE);
        // No timeout is needed to complete: the turn never waits for a response.
        f.dispatch(test_utterance()).await.unwrap();

        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkPublishFailed {
                utterance: UtteranceId(42),
                detail: "bridge gone".into(),
            }]
        );
        assert_eq!(f.stats.snapshot().link_publish_failures, 1);
        assert_eq!(f.spoken(), [FAILURE_MESSAGE]);
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>unexpected"),
            DeliverOutcome::NoTurnPending
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_whitespace_only_failure_message_keeps_a_broken_turn_silent() {
        // The disable knob: the same whitespace rule that drops an all-markers
        // segment.
        let mut failed = Fixture::with(Err(LinkError::new("gone")), "  ");
        failed.dispatch(test_utterance()).await.unwrap();
        assert!(failed.spoken().is_empty());
        assert_eq!(failed.stats.snapshot().link_publish_failures, 1);

        let mut timed_out = Fixture::with(Ok(()), "");
        timed_out.dispatch(test_utterance()).await.unwrap();
        assert!(timed_out.spoken().is_empty());
        assert_eq!(timed_out.stats.snapshot().link_response_timeouts, 1);
    }

    #[tokio::test]
    async fn a_whitespace_only_terminal_message_ends_the_turn_silently() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>   ").await;
        turn.await.unwrap();

        assert!(f.spoken().is_empty(), "nothing to say, nothing queued");
        assert!(f.events().is_empty(), "{:?}", f.events());
    }

    #[tokio::test]
    async fn a_mismatched_reply_id_is_dropped_and_the_real_response_still_lands() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        assert_eq!(
            f.deliver_armed("<reply to=\"7\"/>Not yours.").await,
            DeliverOutcome::ReplyMismatch {
                pending: UtteranceId(42)
            }
        );
        // The slot stayed armed, so a stray cannot starve the answer.
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>Yours."),
            DeliverOutcome::Delivered
        );
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Yours."]);
        assert!(
            f.events().is_empty(),
            "a rejected message earns no per-marker events: {:?}",
            f.events()
        );
    }

    #[tokio::test]
    async fn a_message_with_no_reply_marker_is_accepted_for_the_pending_turn() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        assert_eq!(
            f.deliver_armed("Sixty-eight and clear.").await,
            DeliverOutcome::Delivered
        );
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Sixty-eight and clear."]);
        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkReplyAssumed {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(f.stats.snapshot().link_replies_assumed, 1);
    }

    #[tokio::test]
    async fn unknown_and_listen_markers_are_reported_and_the_speech_survives() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Go on.<emote:happy/><listen/>")
            .await;
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Go on."]);
        assert_eq!(
            f.events(),
            vec![
                BrainEvent::LinkTagStripped {
                    utterance: UtteranceId(42),
                    tag: "<emote:happy/>".into(),
                },
                BrainEvent::LinkListenUnsupported {
                    utterance: UtteranceId(42)
                },
            ]
        );
        assert_eq!(f.stats.snapshot().link_tags_stripped, 1);
        assert_eq!(f.stats.snapshot().link_listen_unsupported, 1);
    }

    #[tokio::test]
    async fn a_message_whose_reply_id_is_mangled_is_accepted_and_reported() {
        // An unreadable id never reaches the correlation check, so the message lands
        // where a forgotten marker lands — spoken for the pending turn, with the raw
        // marker reported loudly beside the accept, which is what tells the two
        // apart in the stream.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        assert_eq!(
            f.deliver_armed("<reply to=\"4x2\"/>Sixty-eight and clear.")
                .await,
            DeliverOutcome::Delivered
        );
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Sixty-eight and clear."]);
        assert_eq!(
            f.events(),
            vec![
                BrainEvent::LinkReplyAssumed {
                    utterance: UtteranceId(42)
                },
                BrainEvent::LinkTagStripped {
                    utterance: UtteranceId(42),
                    tag: "<reply to=\"4x2\"/>".into(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_message_thick_with_markers_reports_a_bounded_number_of_them() {
        // A body is bounded only by the bus's size limit, so the per-marker events
        // have to be bounded here instead. The counter still sees every one.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        let flood = "<a/>".repeat(MAX_MARKER_REPORTS * 5);
        f.deliver_armed(&format!("<reply to=\"42\"/>{flood}Still here."))
            .await;
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Still here."]);
        assert_eq!(
            f.events().len(),
            MAX_MARKER_REPORTS,
            "one message cannot mint events without bound: {:?}",
            f.events()
        );
        assert_eq!(
            f.stats.snapshot().link_tags_stripped,
            (MAX_MARKER_REPORTS * 5) as u64,
            "the counter is the complete tally"
        );
    }

    #[tokio::test]
    async fn a_turn_dropped_part_way_releases_its_slot() {
        // Nothing cancels `handle` today, but a caller that did would otherwise leave
        // the slot armed forever: later messages refused as a backlog, the next turn
        // arming over a slot that is not free.
        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        assert_eq!(
            f.deliver_armed("<reply to=\"42\"/>Half of it.<continued/>")
                .await,
            DeliverOutcome::Delivered
        );

        turn.abort();
        assert!(
            turn.await.unwrap_err().is_cancelled(),
            "the turn's future is dropped, not run to completion"
        );
        assert_eq!(
            f.brain.deliver("<reply to=\"42\"/>the rest"),
            DeliverOutcome::NoTurnPending,
            "the abandoned turn owns nothing"
        );
    }

    #[tokio::test]
    async fn a_message_with_no_turn_pending_is_dropped_whole() {
        let f = Fixture::new();
        assert_eq!(
            f.brain
                .deliver("<reply to=\"42\"/>unsolicited<emote:happy/>"),
            DeliverOutcome::NoTurnPending
        );
        assert!(
            f.events().is_empty(),
            "no turn owns it, so no marker of it is reported: {:?}",
            f.events()
        );
    }

    #[tokio::test]
    async fn a_backlogged_message_reports_none_of_its_markers() {
        let f = Fixture::new();
        // A slot nothing is draining. `handle` empties the channel as fast as
        // `deliver` fills it, so holding the receiver here is the only way to reach
        // the should-not-happen guard.
        let (tx, _rx) = mpsc::channel::<ParsedResponse>(SLOT_CAPACITY);
        *f.brain.pending.lock().unwrap() = Some(PendingTurn {
            utterance: UtteranceId(42),
            tx,
        });
        for _ in 0..SLOT_CAPACITY {
            assert_eq!(
                f.brain.deliver("<reply to=\"42\"/>in"),
                DeliverOutcome::Delivered
            );
        }

        assert_eq!(
            f.brain
                .deliver("<reply to=\"42\"/>overrun<emote:happy/><listen/>"),
            DeliverOutcome::Backlogged
        );
        assert!(
            f.events().is_empty(),
            "the turn never got the message, so nothing of it was acted on: {:?}",
            f.events()
        );
        assert_eq!(f.stats.snapshot().link_tags_stripped, 0);
        assert_eq!(f.stats.snapshot().link_listen_unsupported, 0);
    }

    #[tokio::test]
    async fn a_missing_transcript_declines_without_publishing() {
        let mut f = Fixture::new();
        let u = Utterance {
            transcript: None,
            ..test_utterance()
        };
        f.dispatch(u).await.unwrap();

        assert!(f.published().is_empty(), "nothing to say to the peer");
        assert!(f.log.lock().unwrap().interruptions.is_empty());
        assert!(f.spoken().is_empty(), "not a link failure, so no apology");
        assert_eq!(
            f.events(),
            vec![BrainEvent::NoTranscript {
                utterance: UtteranceId(42)
            }]
        );
        assert_eq!(f.stats.snapshot().no_transcript, 1);
    }

    #[tokio::test]
    async fn a_barge_that_transcribed_to_nothing_still_reports_the_interruption() {
        let f = Fixture::new();
        let u = with_chain(
            Utterance {
                transcript: Some(Transcript {
                    text: "  ".into(),
                    confidence: None,
                }),
                ..test_utterance()
            },
            vec![segment(Some("one two three"), 700, 1_000)],
        );
        f.dispatch(u).await.unwrap();

        assert!(f.published().is_empty());
        let notices = &f.log.lock().unwrap().interruptions;
        assert_eq!(notices.len(), 1);
        assert_eq!(
            parsed(&notices[0]),
            json!({
                "type": "interruption",
                "pod": "pod-x",
                "room": "kitchen",
                "interrupted": {
                    "utterance": 122,
                    "heard_ms": 700,
                    "total_ms": 1_000,
                    "heard_text": "one two",
                },
            })
        );
    }

    #[tokio::test]
    async fn a_wake_nudges_the_peer() {
        let f = Fixture::new();
        f.brain.wake(&PodId("kitchen".into()));
        assert_eq!(
            f.log.lock().unwrap().wakes,
            [r#"{"type":"wake","pod":"kitchen"}"#]
        );
    }

    #[test]
    fn a_declined_barge_reports_the_interruption_and_nothing_else_does() {
        let f = Fixture::new();
        let with_cut = with_chain(test_utterance(), vec![segment(None, 300, 1_000)]);
        f.brain.barge_declined(&with_cut);
        // Nothing was interrupted, so there is nothing to report.
        f.brain.barge_declined(&test_utterance());

        let notices = &f.log.lock().unwrap().interruptions;
        assert_eq!(notices.len(), 1, "one chain, one notice");
        assert_eq!(
            parsed(&notices[0])["interrupted"],
            json!({
                "utterance": 122,
                "heard_ms": 300,
                "total_ms": 1_000,
                "heard_text": Value::Null,
            })
        );
    }

    #[test]
    fn interrupt_is_a_no_op() {
        let f = Fixture::new();
        f.brain.interrupt(
            UtteranceId(42),
            InterruptProgress {
                heard_ms: 300,
                total_ms: 1_000,
            },
        );
        assert!(f.events().is_empty());
        assert!(f.log.lock().unwrap().interruptions.is_empty());
    }

    #[test]
    fn an_unconfigured_wake_channel_is_absent_from_the_help_document() {
        // No wake channel means no wake notices, so the document must not promise
        // them.
        let help = response_contract_help(&HelpChannels {
            wake: None,
            ..help_channels()
        });
        assert!(!help.contains("brenn:pod.wake"));
        assert!(!help.contains("\"type\": \"wake\""));
        assert!(help.contains("brenn:pod.utterance"));
    }
}
