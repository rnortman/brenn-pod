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

use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use regex::Regex;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::brain::{BrainEvent, BrainEventFn, BrainStats, send_or_report};
use crate::traits::{Brain, ResponseSink, TurnEnd};
use crate::types::{
    ContextSegment, Cue, InterruptProgress, PodId, RoomId, SpeakBody, SpeakCmd, Utterance,
    UtteranceId, UtteranceLevel,
};
use crate::volume::{
    AutoPolicy, OperatorBounds, VolumeBook, VolumeCommand, VolumeState, resolve_shared,
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
    volume: &'a VolumeState,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<UtteranceLevel>,
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
///
/// `volume` is the pod's volume state when the turn is sent: the peer's only view
/// of it, so a hold lost to a restart or a value clamped into the operator's
/// bounds shows on the next turn.
pub fn utterance_body(u: &Utterance, text: &str, volume: &VolumeState) -> String {
    render(&UtteranceWire {
        kind: "utterance",
        utterance: u.id.0,
        pod: &u.pod.0,
        room: &u.room.0,
        speaker: u.speaker.as_ref().map(|s| s.0.as_str()),
        text,
        interrupted: InterruptedBody::for_utterance(u),
        volume,
        level: u.level,
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
#[derive(Debug, Clone, PartialEq)]
pub enum Tag {
    /// Turn correlation. `to` is `None` when the marker carried no id — accepted
    /// optimistically for the pending turn by the delivery policy.
    Reply { to: Option<u64> },
    /// Flush what came with this message and expect a follow-up message.
    Continued,
    /// Hold the microphone open after this reply.
    Listen,
    /// Move the head: a named pose to take for the rest of the reply, or a named
    /// motion to play over the standing one. Carries the [`Cue`] the response
    /// delivers verbatim, so the marker and the movement cannot drift apart.
    Cue(Cue),
    /// Change the reply volume: this message's gain, the pod's persistent
    /// volume state, or both.
    Volume(VolumeTag),
    /// A tag-shaped island the vocabulary does not cover, one mangled past
    /// parsing, or a `<volume/>` superseded by a later one in the same message.
    /// Stripped from the speech and reported loudly, never spoken or acted on.
    Unknown { raw: String },
}

/// What one `<volume/>` marker asked for. At least one attribute is present;
/// every value is a finite number. Range is not checked here: the operator's
/// bounds belong to the volume book, not to the codec.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct VolumeTag {
    /// The gain for this message's speech only (`db`).
    pub db: Option<f32>,
    /// The persistent attributes, applied to the pod's state.
    pub persistent: VolumeCommand,
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

/// One named attribute, permissively: double-quoted, single-quoted, or bare,
/// with whitespace anywhere around the `=`. One pattern per key, built once.
fn attr_pattern(key: &str) -> Regex {
    Regex::new(&format!(
        r#"(?:^|\s){key}\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'/>]+))"#
    ))
    .expect("valid regex")
}

/// Whether an attribute list mentions `key` at all, so a marker whose value is
/// present but unreadable reads as mangled rather than as one without the
/// attribute.
fn attr_key_pattern(key: &str) -> Regex {
    Regex::new(&format!(r"(?:^|\s){key}\s*=")).expect("valid regex")
}

/// The value of an attribute, whichever quoting the peer used.
fn attr_value<'a>(pattern: &Regex, attrs: &'a str) -> Option<&'a str> {
    pattern
        .captures(attrs)
        .and_then(|caps| (1..=3).find_map(|i| caps.get(i)))
        .map(|m| m.as_str())
}

/// An optional marker attribute: the pattern that reads its value and the one
/// that notices its key, so a present but unreadable value mangles the marker
/// instead of reading as an absent attribute.
struct MarkerAttr {
    value: Regex,
    key: Regex,
}

impl MarkerAttr {
    fn new(name: &str) -> Self {
        Self {
            value: attr_pattern(name),
            key: attr_key_pattern(name),
        }
    }

    /// Absent, or present and readable, or `Err` — present but unreadable.
    fn read<'a>(&self, attrs: &'a str) -> Result<Option<&'a str>, ()> {
        match attr_value(&self.value, attrs) {
            Some(value) => Ok(Some(value.trim())),
            None if self.key.is_match(attrs) => Err(()),
            None => Ok(None),
        }
    }

    /// Absent, or a value that parses as `T`, or `Err`.
    fn parsed<T: FromStr>(&self, attrs: &str) -> Result<Option<T>, ()> {
        self.read(attrs)?
            .map(|value| value.parse::<T>().map_err(|_| ()))
            .transpose()
    }

    /// Absent, or a finite number, or `Err`.
    fn number<T: FromStr + Into<f64> + Copy>(&self, attrs: &str) -> Result<Option<T>, ()> {
        match self.parsed::<T>(attrs)? {
            Some(n) if !n.into().is_finite() => Err(()),
            n => Ok(n),
        }
    }
}

/// The `to` attribute of a reply marker.
static REPLY_TO: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("to"));

/// The `name` attribute of a pose or motion marker. Required, so it has no key
/// check: absent and unreadable both mangle the marker.
static CUE_NAME: LazyLock<Regex> = LazyLock::new(|| attr_pattern("name"));

/// The `speed` attribute of a pose or motion marker.
static CUE_SPEED: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("speed"));

static VOLUME_DB: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("db"));
static VOLUME_HOLD: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("hold"));
static VOLUME_AUTO: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("auto"));
static VOLUME_K_AMBIENT: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("k_ambient"));
static VOLUME_AMBIENT_REF: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("ambient_ref"));
static VOLUME_MIN: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("min"));
static VOLUME_MAX: LazyLock<MarkerAttr> = LazyLock::new(|| MarkerAttr::new("max"));

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
///
/// A message carries at most one [`Tag::Volume`]: when it has several, the last is
/// kept and every earlier one becomes [`Tag::Unknown`], so it is reported as
/// stripped rather than dropped unseen. One marker's attributes are applied as
/// one transaction, and the vocabulary has no rule for combining two.
pub fn scan(body: &str) -> (String, Vec<(Tag, usize)>) {
    let mut text = String::with_capacity(body.len());
    let mut tags: Vec<(Tag, usize)> = Vec::new();
    let mut cursor = 0;
    let mut volume_at: Option<(usize, &str)> = None;
    for island in TAG_ISLAND.find_iter(body) {
        text.push_str(&body[cursor..island.start()]);
        let tag = parse_island(island.as_str());
        if matches!(tag, Tag::Volume(_))
            && let Some((earlier, raw)) = volume_at.replace((tags.len(), island.as_str()))
        {
            tags[earlier].0 = Tag::Unknown { raw: raw.into() };
        }
        tags.push((tag, text.len()));
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
        "pose" => parse_cue(attrs, raw, false),
        "motion" => parse_cue(attrs, raw, true),
        "volume" => parse_volume(attrs).map_or_else(unknown, Tag::Volume),
        _ => unknown(),
    }
}

/// A volume marker: every attribute optional, at least one present. Like a cue,
/// a marker with any attribute present but unreadable — a number that is not a
/// finite number, `auto` other than `on`/`off` — is not read at all: the peer
/// meant something this side cannot recover, and half of a volume change is not
/// what it asked for.
fn parse_volume(attrs: &str) -> Option<VolumeTag> {
    let auto = match VOLUME_AUTO.read(attrs).ok()? {
        None => None,
        Some("on") => Some(true),
        Some("off") => Some(false),
        Some(_) => return None,
    };
    let tag = VolumeTag {
        db: VOLUME_DB.number(attrs).ok()?,
        persistent: VolumeCommand {
            hold_db: VOLUME_HOLD.number(attrs).ok()?,
            auto,
            k_ambient: VOLUME_K_AMBIENT.number(attrs).ok()?,
            ambient_ref_dbfs: VOLUME_AMBIENT_REF.number(attrs).ok()?,
            auto_min_db: VOLUME_MIN.number(attrs).ok()?,
            auto_max_db: VOLUME_MAX.number(attrs).ok()?,
        },
    };
    (tag != VolumeTag::default()).then_some(tag)
}

/// A pose or motion marker: a required `name` and an optional `speed`.
///
/// Neither attribute is defaulted when it is there but unreadable. A cue is a
/// command to move a machine, and the only honest reading of a mangled one is
/// that the peer meant something this side cannot recover — so it is stripped
/// and reported like any other marker the codec cannot read, and the head does
/// not move. The speed's range is not checked here; this crate does not link
/// the motion protocol.
fn parse_cue(attrs: &str, raw: &str, motion: bool) -> Tag {
    let unknown = || Tag::Unknown { raw: raw.into() };
    let name = attr_value(&CUE_NAME, attrs).map(str::trim).unwrap_or("");
    if name.is_empty() {
        return unknown();
    }
    let Ok(speed) = CUE_SPEED.number::<f64>(attrs) else {
        return unknown();
    };
    let name = name.to_owned();
    Tag::Cue(if motion {
        Cue::Motion { name, speed }
    } else {
        Cue::Pose { name, speed }
    })
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
    match REPLY_TO.parsed::<u64>(attrs) {
        Ok(to) => Tag::Reply { to },
        Err(()) => Tag::Unknown { raw: raw.into() },
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

/// The volume facts the instruction document states: the operator's bounds and
/// the automatic policy every pod starts from. Fixed for the process; the live
/// per-pod state rides on each utterance instead.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct HelpVolume {
    pub bounds: OperatorBounds,
    pub auto: AutoPolicy,
}

/// What the instruction document may tell its reader to ask the pod for: the
/// movement vocabulary the deployment resolved, the window a `<continued/>`
/// buys, and the volume bounds. All are deployment facts, so the document states
/// the ones this run actually holds rather than an example of them.
#[derive(Debug, Clone, PartialEq)]
pub struct HelpCues {
    /// Pose names the reader may name, already filtered to the ones it is
    /// allowed to command. Empty renders as `(none)`.
    pub poses: Vec<String>,
    /// Motion names the reader may name. Empty renders as `(none)`.
    pub motions: Vec<String>,
    /// How long the pod waits for a promised continuation, as configured.
    pub continued_ms: u64,
    pub volume: HelpVolume,
}

/// The contract document, written at the language model that answers on the bus —
/// not at a human reading reference documentation. It is prompt-style: direct
/// imperatives, the exact markers to emit, and a worked example. The channel names,
/// the movement vocabulary and the continuation window are its dynamic content.
///
/// It lives next to the codec on purpose. The instructions and the parser can only
/// diverge if someone edits one of them alone, and here that is one file.
pub fn response_contract_help(channels: &HelpChannels, cues: &HelpCues) -> String {
    let wake_section = match &channels.wake {
        Some(wake) => format!(
            "\n\
             A `{{\"type\": \"wake\", \"pod\": \"...\"}}` notice may arrive on `{wake}`: a wake\n\
             word just fired on that pod and a command may follow within a few seconds. Nothing\n\
             has been asked yet. Do not reply to it. It exists so you can start warming up.\n"
        ),
        None => String::new(),
    };
    // Rounded to whole seconds: the document is spoken-language instruction, and
    // a millisecond figure invites the reader to treat a scheduling budget as a
    // promise.
    let continued_s = cues.continued_ms.div_ceil(1_000);
    let markers = format!(
        "\
# The markers

  <reply to=\"N\"/>        first thing in a reply; N is the utterance id you answer.
  <continued/>           at the end of a partial reply when another message
                         follows. The pod waits about {continued_s} seconds for it and
                         the robot stays attentive; a continuation you never send
                         simply ends the turn.
  <listen/>              at the end of a reply, to keep the microphone open so the
                         person can keep talking without the wake word.
  <pose name=\"P\" speed=\"S\"/>      move the head to pose P and stay there for the
                                  rest of the reply. S is optional, 1.0 is the
                                  pose's own pace, 0.25–2.0.
  <motion name=\"M\" speed=\"S\"/>    play motion M over the current pose, once. A
                                  second motion replaces one still playing. S as
                                  above.

Poses you may name: {poses}.
Motions you may name: {motions}.
",
        poses = name_list(&cues.poses),
        motions = name_list(&cues.motions),
    );
    let volume = volume_section(&cues.volume);
    format!(
        "{HELP_INTRO}\n\
         Utterances from the pods arrive on the channel `{publish}`.\n\
         Publish every reply on the channel `{response}`.\n\
         {wake_section}{HELP_BODY}\n{markers}\n{volume}{HELP_TAIL}",
        publish = channels.publish,
        response = channels.response,
    )
}

/// The `# Volume` section: the marker, its attributes, the configured bounds and
/// defaults, and where the live state is reported.
fn volume_section(v: &HelpVolume) -> String {
    let a = &v.auto;
    format!(
        "\
# Volume

All volume numbers are decibels relative to the voice's natural level: 0 is as
produced, negative is quieter, positive is louder. The operator allows
{min} to {max} dB; anything outside that is clamped, whatever you ask.

  <volume db=\"N\"/>         play this message's speech at N dB. This message only.
  <volume hold=\"N\"/>       set the pod's standing volume to N dB. It stays until you
                           change it or the pod restarts.
  <volume auto=\"on\"/>      let the pod adjust the volume to how loud the room is
                           (\"off\" turns that off). Starts {auto_state}.
  <volume k_ambient=\"A\" ambient_ref=\"B\"/>
                           tune the automatic adjustment: A dB louder per dB the
                           room is above B dBFS, and A dB quieter per dB it is
                           below. B is the room level at which speech plays at the
                           standing volume. Defaults: A {k_ambient}, B {ambient_ref}.
  <volume min=\"L\" max=\"H\"/> limit how far the automatic adjustment may move the
                           volume, L below to H above the standing volume.
                           Defaults: {auto_min} and {auto_max}. L must not exceed H.

Attributes combine in one marker: `<volume hold=\"-10\" auto=\"off\"/>`. Use one
`<volume/>` per message and put every attribute in it: when a message has more,
only the last is acted on and the others are discarded. A marker can be a whole
message with no speech. A standing change takes effect from the message that
carries it: earlier messages of the same reply keep the volume they were sent
under, so put the marker in the message where the change should start.

Every utterance carries a `volume` object with the pod's current state:
`held_db` (the standing volume), `auto`, `level_source`, `k_ambient`,
`ambient_ref_dbfs`, `auto_min_db`, `auto_max_db`, and the operator's `min_db`
and `max_db`. Read it rather than assuming your last marker took effect.

Every utterance also carries a `level` object saying how loudly you were
addressed and how loud the room was: `speech_dbfs` (how loudly you were
addressed, in dB below full scale: the wake phrase for an utterance that
followed one, the utterance's own speech for a follow-up or an interruption;
null when it could not be measured; it does not drive the volume), `ambient`
(`dbfs`: the room's level over up to the last ten seconds in which the pod was
silent and nobody was addressing it; `blocks`: how many 100 ms readings that
rests on; `age_ms`: how old the newest of them is; `carried`: true when nothing
recent was usable and this is the last good reading; null when the room has
never been heard), and `source` — `raw_mic`, an unprocessed microphone, or
`beam`, the processed speech channel, whose loudness is flattened and means
little. It is absent when nothing was measured. The automatic adjustment reads
`ambient`; with `level_source` `raw_mic` it ignores a `beam` reading, and a
reading five minutes old or more no longer counts.

",
        min = v.bounds.min_db(),
        max = v.bounds.max_db(),
        auto_state = if a.enabled { "on" } else { "off" },
        k_ambient = a.k_ambient,
        ambient_ref = a.ambient_ref_dbfs,
        auto_min = a.min_db,
        auto_max = a.max_db,
    )
}

/// A vocabulary line's names, or `(none)` when the deployment configured no
/// library: the reader is told the list is empty rather than left to guess that
/// a missing line means anything goes.
fn name_list(names: &[String]) -> String {
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}

const HELP_INTRO: &str = "\
You are the voice of a home speech pod. Every message you publish on the response
channel is synthesized and spoken aloud to whoever is standing in the room, so
write speech: plain sentences, no markdown, no lists, no emoji, no stage
directions, nothing that only makes sense on a screen. Write like people speak,
not like they write: Long sentences create response latency; every syllable must
be spoken, and that takes real time. Be concise, be informal, no extra scaffolding.
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

The only feedback a user gets that you're working on something is if you say something.
So before doing any thinking, searching, or tool calls, answer quickly to acknowledge
that you're working, in a few words, followed by `<continued/>` to hold the line open.

For long responses, chunk them up with `<continued/>`, one message per chunk, because
that lets the TTS start running, so the speach runs while you generate the remaining
tokens.

  <reply to=\"42\"/>Let me check the forecast.<continued/>
  <reply to=\"42\" continued=\"true\"/>Sixty-eight and clear, with rain after nine.

The last message of a turn must NOT carry `<continued/>`: that marker is a promise
of another message, and the pod waits for it before the turn is over.
";

const HELP_TAIL: &str = "\
One reply per turn, plus its continuations. Anything else tag-shaped is stripped
out of the speech and reported as an error, so do not invent markers and do not
wrap your reply in XML or markdown.

# Remember that STT is imperfect

You'll often see STT errors. You may need to infer what the user really said,
especially with similar-sounding words.
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
#[derive(Debug, Clone, PartialEq)]
struct ParsedResponse {
    /// The speech, verbatim but for the stripped markers.
    text: String,
    /// The message promised a follow-up.
    continued: bool,
    /// The message asked for the microphone to stay open. Read only as the turn's
    /// disposition; nothing here holds the floor open.
    listen: bool,
    /// The movements the message asked for, in the order they appeared.
    cues: Vec<Cue>,
    /// The volume change the message asked for. [`scan`] leaves at most one
    /// `<volume/>` marker in a message.
    volume: Option<VolumeTag>,
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
    /// The per-pod volume state, read when a turn is sent so the utterance can
    /// report it, and when each segment is queued to resolve its gain. Written
    /// by each turn's volume tap, never by the brain.
    volume: Arc<Mutex<VolumeBook>>,
}

impl BrennBrain {
    /// Build a brain over `link`. `failure_message` is spoken when a turn ends on a
    /// link failure; a whitespace-only string keeps such a turn silent. `volume`
    /// is the book each turn's volume tap writes.
    pub fn new(
        link: Arc<dyn BrainLink>,
        response_timeout: Duration,
        continuation_timeout: Duration,
        failure_message: String,
        events: BrainEventFn,
        stats: Arc<BrainStats>,
        volume: Arc<Mutex<VolumeBook>>,
    ) -> Self {
        Self {
            link,
            response_timeout,
            continuation_timeout,
            failure_message,
            events,
            stats,
            pending: Arc::new(Mutex::new(None)),
            volume,
        }
    }

    /// The volume facts the instruction document states: the operator's bounds
    /// and the default automatic policy, both fixed for the process.
    pub fn help_volume(&self) -> HelpVolume {
        let book = self
            .volume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        HelpVolume {
            bounds: book.bounds(),
            auto: book.auto_defaults(),
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
            listen: tags.iter().any(|(tag, _)| *tag == Tag::Listen),
            cues: tags
                .iter()
                .filter_map(|(tag, _)| match tag {
                    Tag::Cue(cue) => Some(cue.clone()),
                    _ => None,
                })
                .collect(),
            volume: tags.iter().rev().find_map(|(tag, _)| match tag {
                Tag::Volume(volume) => Some(*volume),
                _ => None,
            }),
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
                Tag::Reply { .. } | Tag::Continued | Tag::Listen | Tag::Cue(_) | Tag::Volume(_) => {
                }
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
///
/// `gain_db` is the message's own `<volume db=…/>`; `None` everywhere else. The
/// segment's gain is resolved from `volume` here, as it is queued, so a
/// persistent change a later message writes to the book never reaches speech
/// already queued.
fn speak(
    out: &mut ResponseSink,
    u: &Utterance,
    text: String,
    gain_db: Option<f32>,
    volume: &Mutex<VolumeBook>,
    events: &BrainEventFn,
    stats: &BrainStats,
) {
    if text.trim().is_empty() {
        return;
    }
    let gain = resolve_shared(volume, &u.pod, gain_db, u.level.as_ref());
    let cmd = SpeakCmd {
        target: u.pod.clone(),
        in_reply_to: Some(u.id),
        body: SpeakBody::Text(text),
        interruptible: true,
        timings: u.timings.clone(),
        gain,
        level: u.level,
    };
    send_or_report(out, cmd, u.id, events, stats);
}

impl Brain for BrennBrain {
    /// Publish the turn, then speak each response segment as it arrives, returning
    /// only when the peer stops promising more.
    ///
    /// The turn ends [`TurnEnd::Open`] exactly when the message that ended the chain
    /// carried `<listen/>`; every other ending — no transcript, a refused publish, a
    /// window that timed out, the continuation cap — is [`TurnEnd::Closed`], because
    /// none of them delivered a final response at all.
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
    fn handle(&self, u: Utterance, mut out: ResponseSink) -> BoxFuture<'static, TurnEnd> {
        let link = Arc::clone(&self.link);
        let events = Arc::clone(&self.events);
        let stats = Arc::clone(&self.stats);
        let pending = Arc::clone(&self.pending);
        let response_timeout = self.response_timeout;
        let continuation_timeout = self.continuation_timeout;
        let failure_message = self.failure_message.clone();
        let book = Arc::clone(&self.volume);
        // By value, and the lock released here: the future never holds it.
        let volume = book
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state(&u.pod);
        async move {
            let utterance = u.id;
            let text = u
                .spoken_text()
                .expect("the gate declines an utterance with no text before dispatch");
            let body = utterance_body(&u, text, &volume);
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
                speak(&mut out, &u, failure_message, None, &book, &events, &stats);
                return TurnEnd::Closed;
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
                        speak(&mut out, &u, failure_message, None, &book, &events, &stats);
                        return TurnEnd::Closed;
                    }
                    Ok(Some(parsed)) => {
                        let promised = parsed.continued;
                        // The turn's disposition is the *final* message's: a
                        // `<listen/>` on a segment that promised a follow-up is
                        // superseded by whatever ends the chain.
                        let listen = parsed.listen;
                        // Before the speech, because the cue is what the reply
                        // is delivered *with*: a head that starts moving as the
                        // words are queued is as close to "while speaking" as a
                        // delivery-time cue gets.
                        // TODO(cue-timing-delivery-not-playback): the head moves
                        // a whole TTS round-trip ahead of the words the cue sits
                        // beside; `scan`'s marker offsets are the way to the
                        // spoken instant.
                        if !parsed.cues.is_empty() {
                            out.cue(parsed.cues);
                        }
                        // Before the speech too, so a hold asked for in this
                        // message is in the book when `speak` resolves the
                        // message's own speech. Speech earlier messages queued
                        // already carries its resolution and keeps it.
                        let gain_db = parsed.volume.and_then(|volume| {
                            if !volume.persistent.is_empty() {
                                out.volume(volume.persistent);
                            }
                            volume.db
                        });
                        speak(&mut out, &u, parsed.text, gain_db, &book, &events, &stats);
                        if !promised {
                            return if listen {
                                TurnEnd::Open
                            } else {
                                TurnEnd::Closed
                            };
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
                            // The peer was still promising more when the cap cut it
                            // off, so no message ever ended the turn: nothing asked
                            // to keep listening.
                            return TurnEnd::Closed;
                        }
                        window = continuation_timeout;
                        continuation = true;
                    }
                    Ok(None) => {
                        debug_assert!(
                            false,
                            "the slot holds the only sender, so the channel cannot close here"
                        );
                        return TurnEnd::Closed;
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
        AmbientReading, BargeInContext, InterruptProgress, LevelSource, SpeakerId, Transcript,
        UtteranceId, test_utterance,
    };
    use crate::volume::GainResolution;
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
            parsed(&utterance_body(
                &u,
                "what is the weather",
                &VolumeState::default()
            )),
            json!({
                "type": "utterance",
                "utterance": 42,
                "pod": "pod-x",
                "room": "kitchen",
                "text": "what is the weather",
                "volume": serde_json::to_value(VolumeState::default()).unwrap(),
            })
        );
    }

    #[test]
    fn utterance_body_carries_the_measured_level_when_there_is_one() {
        let u = Utterance {
            level: Some(UtteranceLevel {
                speech_dbfs: Some(-27.5),
                ambient: Some(AmbientReading {
                    dbfs: -58.25,
                    blocks: 100,
                    age_ms: 300,
                    carried: false,
                }),
                source: LevelSource::RawMic,
            }),
            ..test_utterance()
        };
        assert_eq!(
            parsed(&utterance_body(&u, "hi", &VolumeState::default()))["level"],
            json!({
                "speech_dbfs": -27.5,
                "ambient": {"dbfs": -58.25, "blocks": 100, "age_ms": 300, "carried": false},
                "source": "raw_mic",
            })
        );
        let quiet_room_unknown = Utterance {
            level: Some(UtteranceLevel {
                speech_dbfs: Some(-40.0),
                ambient: None,
                source: LevelSource::Beam,
            }),
            ..test_utterance()
        };
        assert_eq!(
            parsed(&utterance_body(
                &quiet_room_unknown,
                "hi",
                &VolumeState::default()
            ))["level"],
            json!({"speech_dbfs": -40.0, "ambient": null, "source": "beam"})
        );
        // A dead speech window keeps the room.
        let speech_unmeasured = Utterance {
            level: Some(UtteranceLevel {
                speech_dbfs: None,
                ambient: Some(AmbientReading {
                    dbfs: -70.0,
                    blocks: 3,
                    age_ms: 40_000,
                    carried: true,
                }),
                source: LevelSource::RawMic,
            }),
            ..test_utterance()
        };
        assert_eq!(
            parsed(&utterance_body(
                &speech_unmeasured,
                "hi",
                &VolumeState::default()
            ))["level"]["speech_dbfs"],
            Value::Null
        );
    }

    #[test]
    fn utterance_body_carries_a_known_speaker() {
        let u = Utterance {
            speaker: Some(SpeakerId("alice".into())),
            ..test_utterance()
        };
        assert_eq!(
            parsed(&utterance_body(&u, "hi", &VolumeState::default()))["speaker"],
            json!("alice")
        );
    }

    #[test]
    fn utterance_body_carries_the_cut_estimate_of_the_turn_it_interrupted() {
        // Half of a 26-char reply, trimmed back to the last whole word.
        let u = with_chain(
            test_utterance(),
            vec![segment(Some("the weather today is sunny"), 500, 1_000)],
        );
        assert_eq!(
            parsed(&utterance_body(
                &u,
                "no cancel that",
                &VolumeState::default()
            ))["interrupted"],
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
            parsed(&utterance_body(
                &u,
                "no the other one",
                &VolumeState::default()
            ))["interrupted"]["utterance"],
            json!(130)
        );
    }

    #[test]
    fn a_pcm_reply_has_a_null_heard_text() {
        // Nothing to cut: the estimate is explicitly null, not omitted, so the peer
        // sees one schema whether or not there were words.
        let u = with_chain(test_utterance(), vec![segment(None, 300, 1_000)]);
        let interrupted =
            parsed(&utterance_body(&u, "wait", &VolumeState::default()))["interrupted"].clone();
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
            parsed(&utterance_body(&u, "stop", &VolumeState::default()))["interrupted"]["heard_text"],
            Value::Null
        );
    }

    #[test]
    fn an_utterance_that_interrupted_nothing_omits_the_estimate() {
        let body = parsed(&utterance_body(
            &test_utterance(),
            "hello",
            &VolumeState::default(),
        ));
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
            parsed(&utterance_body(&u, "padded", &VolumeState::default()))["text"],
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

    // --- cue markers ---

    #[test]
    fn a_pose_marker_carries_its_name_and_optional_speed() {
        assert_eq!(
            tags("<pose name=\"peek\"/>"),
            vec![Tag::Cue(Cue::Pose {
                name: "peek".into(),
                speed: None
            })]
        );
        assert_eq!(
            tags("<pose name='peek' speed=1.5/>"),
            vec![Tag::Cue(Cue::Pose {
                name: "peek".into(),
                speed: Some(1.5)
            })]
        );
        assert_eq!(
            tags("<motion name=\"a/b\" speed=\"0.5\"/>"),
            vec![Tag::Cue(Cue::Motion {
                name: "a/b".into(),
                speed: Some(0.5)
            })]
        );
    }

    #[test]
    fn a_cue_marker_the_codec_cannot_read_is_stripped_like_any_other() {
        // Nothing is defaulted: a cue moves a machine, and a marker whose name or
        // pace is unreadable has not said where to move it.
        for body in [
            "<pose/>",
            "<pose name=\"\"/>",
            "<motion speed=\"1.0\"/>",
            "<pose name=\"peek\" speed=\"fast\"/>",
            "<motion name=\"a/b\" speed=\"\"/>",
            "<pose name=\"peek\" speed=\"inf\"/>",
        ] {
            assert_eq!(
                tags(body),
                vec![Tag::Unknown { raw: body.into() }],
                "body: {body}"
            );
        }
    }

    #[test]
    fn a_hold_marker_is_not_in_the_vocabulary() {
        // `<hold/>` was never a marker; `<continued/>` is what keeps the head up.
        // Pin that it stays unknown, so a peer using it is told rather than
        // silently obeyed.
        assert_eq!(
            tags("<hold/>"),
            vec![Tag::Unknown {
                raw: "<hold/>".into()
            }]
        );
    }

    #[test]
    fn cue_markers_are_stripped_from_the_speech_in_order() {
        let (text, tags) = scan("<motion name=\"a/b\"/>Hello!<pose name=\"peek\"/> Bye.");
        assert_eq!(text, "Hello! Bye.");
        assert_eq!(
            tags.into_iter().map(|(tag, _)| tag).collect::<Vec<_>>(),
            vec![
                Tag::Cue(Cue::Motion {
                    name: "a/b".into(),
                    speed: None
                }),
                Tag::Cue(Cue::Pose {
                    name: "peek".into(),
                    speed: None
                }),
            ]
        );
    }

    // --- the instruction document ---

    fn help_channels() -> HelpChannels {
        HelpChannels {
            publish: "brenn:pod.utterance".into(),
            response: "brenn:pod.speak".into(),
            wake: Some("brenn:pod.wake".into()),
        }
    }

    fn volume(body: &str) -> VolumeTag {
        match tags(body).as_slice() {
            [Tag::Volume(v)] => *v,
            other => panic!("{body}: expected one volume marker, got {other:?}"),
        }
    }

    #[test]
    fn a_volume_marker_reads_each_attribute() {
        assert_eq!(volume("<volume db=\"-6\"/>").db, Some(-6.0));
        assert_eq!(
            volume("<volume hold=\"-10\"/>").persistent.hold_db,
            Some(-10.0)
        );
        assert_eq!(volume("<volume auto=\"on\"/>").persistent.auto, Some(true));
        assert_eq!(volume("<volume auto='off'/>").persistent.auto, Some(false));
        let policy = volume("<volume k_ambient=0.25 ambient_ref=\"-50\"/>").persistent;
        assert_eq!(
            (policy.k_ambient, policy.ambient_ref_dbfs),
            (Some(0.25), Some(-50.0))
        );
        let swing = volume("<volume min=\"-5\" max='5'>").persistent;
        assert_eq!(
            (swing.auto_min_db, swing.auto_max_db),
            (Some(-5.0), Some(5.0))
        );
    }

    /// The speech-driven knobs are not attributes: beside a live attribute they
    /// are ignored, and alone they leave nothing to read, so the marker is
    /// stripped and reported as unknown.
    #[test]
    fn the_removed_speech_knobs_are_not_read() {
        assert_eq!(
            volume("<volume k_speech=\"1\" k_ambient=\"0.5\"/>").persistent,
            VolumeCommand {
                k_ambient: Some(0.5),
                ..VolumeCommand::default()
            }
        );
        for raw in ["<volume k_speech=\"1\"/>", "<volume speech_ref=\"-40\"/>"] {
            assert_eq!(tags(raw), [Tag::Unknown { raw: raw.into() }], "{raw}");
        }
    }

    #[test]
    fn a_volume_marker_combines_attributes() {
        let v = volume("<volume db=\"-3\" hold=\"-10\" auto=\"on\" max=\"60\"/>");
        assert_eq!(v.db, Some(-3.0));
        assert_eq!(
            v.persistent,
            VolumeCommand {
                hold_db: Some(-10.0),
                auto: Some(true),
                auto_max_db: Some(60.0),
                ..VolumeCommand::default()
            }
        );
    }

    #[test]
    fn only_the_last_volume_marker_in_a_message_is_kept() {
        let hold = VolumeTag {
            db: None,
            persistent: VolumeCommand {
                hold_db: Some(-10.0),
                ..VolumeCommand::default()
            },
        };
        let auto = VolumeTag {
            db: None,
            persistent: VolumeCommand {
                auto: Some(true),
                ..VolumeCommand::default()
            },
        };
        assert_eq!(
            tags("<volume db=\"-3\"/>Hi<volume hold=\"-10\"/> there<listen/><volume auto=\"on\"/>"),
            vec![
                Tag::Unknown {
                    raw: "<volume db=\"-3\"/>".into()
                },
                Tag::Unknown {
                    raw: "<volume hold=\"-10\"/>".into()
                },
                Tag::Listen,
                Tag::Volume(auto),
            ]
        );
        // A mangled marker supersedes nothing.
        assert_eq!(
            tags("<volume hold=\"-10\"/><volume db=\"loud\"/>"),
            vec![
                Tag::Volume(hold),
                Tag::Unknown {
                    raw: "<volume db=\"loud\"/>".into()
                },
            ]
        );
    }

    #[test]
    fn a_mangled_volume_marker_is_unknown() {
        for body in [
            "<volume db=\"loud\"/>",
            "<volume auto=\"maybe\"/>",
            "<volume/>",
            "<volume>",
            "<volume hold=\"-10\"",
            "<volume db=\"inf\"/>",
            "<volume db=\"1e40\"/>",
            "<volume hold=\"-10\" db=/>",
            "<volume min=\"\"/>",
        ] {
            assert_eq!(
                tags(body),
                vec![Tag::Unknown { raw: body.into() }],
                "{body} must be stripped and reported"
            );
        }
    }

    #[test]
    fn a_volume_marker_is_lifted_out_of_the_speech() {
        let (text, tags) = scan("<reply to=\"3\"/><volume db=\"-6\"/>Quietly now.");
        assert_eq!(text, "Quietly now.");
        assert_eq!(tags.len(), 2);
    }

    fn help_cues() -> HelpCues {
        HelpCues {
            poses: vec!["neutral".into(), "peek".into()],
            motions: vec!["bench/nod".into()],
            continued_ms: 30_000,
            volume: HelpVolume::default(),
        }
    }

    #[test]
    fn the_help_document_names_every_marker_type_and_channel() {
        // The document and the codec live in one file so they cannot diverge
        // silently; this is the assertion that makes "cannot" true. Every marker the
        // codec knows, every outbound message type, and every configured channel has
        // to appear verbatim.
        let help = response_contract_help(&help_channels(), &help_cues());
        for needle in [
            "<reply to=\"N\"/>",
            "<continued/>",
            "<listen/>",
            "<pose name=\"P\" speed=\"S\"/>",
            "<motion name=\"M\" speed=\"S\"/>",
            "<volume db=\"N\"/>",
            "<volume hold=\"N\"/>",
            "<volume auto=\"on\"/>",
            "<volume k_ambient=\"A\" ambient_ref=\"B\"/>",
            "<volume min=\"L\" max=\"H\"/>",
            "`level` object",
            "`speech_dbfs`",
            "`ambient`",
            "`carried`",
            "`raw_mic`",
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
        let help = response_contract_help(&help_channels(), &help_cues());
        assert!(help.contains("Never reply to this message."));
        assert!(help.contains("cut you off AND then said this"));
        // It must also say what each field of the estimate means.
        for field in ["heard_ms", "total_ms", "heard_text"] {
            assert!(help.contains(field), "help text is missing {field:?}");
        }
    }

    #[test]
    fn the_help_document_names_the_vocabulary_this_deployment_resolved() {
        let help = response_contract_help(&help_channels(), &help_cues());
        assert!(help.contains("Poses you may name: neutral, peek."));
        assert!(help.contains("Motions you may name: bench/nod."));
    }

    #[test]
    fn an_empty_vocabulary_says_so_rather_than_saying_nothing() {
        // A missing line would read as "name anything"; `(none)` is the honest
        // rendering of a deployment that configured no library.
        let help = response_contract_help(
            &help_channels(),
            &HelpCues {
                poses: Vec::new(),
                motions: Vec::new(),
                ..help_cues()
            },
        );
        assert!(help.contains("Poses you may name: (none)."));
        assert!(help.contains("Motions you may name: (none)."));
    }

    #[test]
    fn the_continuation_row_states_the_configured_window() {
        // The document cannot contradict the configuration, so the number is
        // rendered from it rather than written into the prose.
        let help = response_contract_help(
            &help_channels(),
            &HelpCues {
                continued_ms: 12_000,
                ..help_cues()
            },
        );
        assert!(help.contains("waits about 12 seconds for it"));
        assert!(!help.contains("not implemented yet"));

        // Rounded up, never down: the document must not promise the peer a
        // shorter window than the pod will actually wait.
        let help = response_contract_help(
            &help_channels(),
            &HelpCues {
                continued_ms: 1_500,
                ..help_cues()
            },
        );
        assert!(
            help.contains("waits about 2 seconds for it"),
            "1500 ms is two seconds' worth of promise: {help}"
        );
    }

    #[test]
    fn the_volume_section_states_the_configured_bounds_and_defaults() {
        let help = response_contract_help(
            &help_channels(),
            &HelpCues {
                volume: HelpVolume {
                    bounds: OperatorBounds::new(-24.0, 3.0).unwrap(),
                    auto: AutoPolicy {
                        enabled: true,
                        k_ambient: 0.5,
                        ambient_ref_dbfs: -40.0,
                        min_db: -9.0,
                        ..AutoPolicy::default()
                    },
                },
                ..help_cues()
            },
        );
        assert!(help.contains("The operator allows\n-24 to 3 dB"), "{help}");
        assert!(help.contains("Starts on."));
        assert!(help.contains("Defaults: A 0.5, B -40."), "{help}");
        assert!(
            help.contains("A dB quieter per dB it is"),
            "both directions: {help}"
        );
        for removed in ["k_speech", "speech_ref"] {
            assert!(!help.contains(removed), "help text still names {removed:?}");
        }
        assert!(help.contains("Defaults: -9 and 6."));
        for field in ["`volume` object", "held_db", "auto_min_db", "max_db"] {
            assert!(help.contains(field), "help text is missing {field:?}");
        }
        for rule in [
            "Every utterance also carries a `level` object",
            "own speech for a follow-up or an interruption",
            "silent and nobody was addressing it",
            "A standing change takes effect from the message that",
            "earlier messages of the same reply keep the volume they were sent",
        ] {
            assert!(help.contains(rule), "help text is missing {rule:?}");
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
        /// Every batch of cues the sink's cue tap was handed, in order.
        cued: Arc<Mutex<Vec<Vec<Cue>>>>,
        /// The volume book the brain reads and the fixture's volume tap writes.
        book: Arc<Mutex<VolumeBook>>,
        /// What reached the sink, in order: `speak` per offered command and
        /// `volume` per persistent volume change.
        order: Arc<Mutex<Vec<&'static str>>>,
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
            let book = Arc::new(Mutex::new(VolumeBook::default()));
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
                Arc::clone(&book),
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
                cued: Arc::new(Mutex::new(Vec::new())),
                book,
                order: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Dispatch `u` the way the pipeline does — awaited concurrently, so the test
        /// can play the peer while the turn is parked.
        fn dispatch(&self, u: Utterance) -> tokio::task::JoinHandle<TurnEnd> {
            let cued = Arc::clone(&self.cued);
            let (book, pod) = (Arc::clone(&self.book), u.pod.clone());
            let (speak_order, volume_order) = (Arc::clone(&self.order), Arc::clone(&self.order));
            let sink = ResponseSink::with_taps(
                self.speak_tx.clone(),
                Some(Arc::new(move |_: &SpeakCmd| {
                    speak_order.lock().unwrap().push("speak");
                })),
                Some(Arc::new(move |cues| cued.lock().unwrap().push(cues))),
                Some(Arc::new(move |cmd: VolumeCommand| {
                    volume_order.lock().unwrap().push("volume");
                    let _ = book.lock().unwrap().apply(&pod, &cmd);
                })),
            );
            tokio::spawn(self.brain.handle(u, sink))
        }

        fn order(&self) -> Vec<&'static str> {
            self.order.lock().unwrap().clone()
        }

        /// The gain resolution of every queued command, in order.
        fn gains(&mut self) -> Vec<GainResolution> {
            let mut out = Vec::new();
            while let Ok(cmd) = self.speak_rx.try_recv() {
                out.push(cmd.gain);
            }
            out
        }

        /// The per-message override of every queued command, in order.
        fn overrides(&mut self) -> Vec<Option<f32>> {
            self.gains()
                .iter()
                .map(GainResolution::override_db)
                .collect()
        }

        fn cued(&self) -> Vec<Vec<Cue>> {
            self.cued.lock().unwrap().clone()
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
                "volume": serde_json::to_value(VolumeState::default()).unwrap(),
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
    async fn a_volume_db_rides_on_every_command_of_its_message_only() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><volume db=\"-6\"/>Quietly.<continued/>")
            .await;
        f.deliver_armed("<reply to=\"42\"/>Back to normal.").await;
        turn.await.unwrap();
        assert_eq!(f.overrides(), [Some(-6.0), None]);
        assert!(
            f.order().iter().all(|o| *o == "speak"),
            "db is not persistent"
        );
    }

    #[tokio::test]
    async fn persistent_volume_reaches_the_tap_before_the_speech() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Okay, quieter.<volume hold=\"-10\" db=\"-12\"/>")
            .await;
        turn.await.unwrap();
        assert_eq!(f.order(), ["volume", "speak"]);
        let gains = f.gains();
        assert_eq!(gains.len(), 1);
        assert_eq!(gains[0].override_db(), Some(-12.0));
        assert_eq!(
            gains[0].held_db(),
            -10.0,
            "the message's own hold is in the book"
        );
        assert_eq!(gains[0].applied_db(), -12.0);
        assert!(f.events().is_empty(), "an acted-on marker is not reported");
    }

    #[tokio::test]
    async fn a_hold_in_a_later_message_leaves_earlier_queued_speech_alone() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        // Back to back, with nothing routed in between: the first segment is
        // still queued when the second message writes the hold.
        f.deliver_armed("<reply to=\"42\"/>Sure, one moment.<continued/>")
            .await;
        f.deliver_armed("<reply to=\"42\"/><volume hold=\"-20\"/>Here is the quiet part.")
            .await;
        turn.await.unwrap();
        let held: Vec<f32> = f.gains().iter().map(GainResolution::held_db).collect();
        assert_eq!(held, [0.0, -20.0]);
        assert_eq!(f.order(), ["speak", "volume", "speak"]);
    }

    #[tokio::test]
    async fn a_volume_only_message_queues_no_speech() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><volume hold=\"-10\"/>")
            .await;
        assert_eq!(turn.await.unwrap(), TurnEnd::Closed);
        assert_eq!(f.order(), ["volume"]);
        assert!(f.overrides().is_empty());
    }

    #[tokio::test]
    async fn the_utterance_reports_the_pods_volume_state_after_a_hold() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><volume hold=\"-10\" auto=\"on\"/>Sure.")
            .await;
        turn.await.unwrap();
        assert_eq!(f.published()[0]["volume"]["held_db"], json!(0.0));
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Again.").await;
        turn.await.unwrap();
        let second = &f.published()[1]["volume"];
        assert_eq!(second["held_db"], json!(-10.0));
        assert_eq!(second["auto"], json!(true));
        assert_eq!(second["min_db"], json!(-30.0));
        let _ = f.overrides();
    }

    #[test]
    fn help_volume_reads_the_books_configuration() {
        let f = Fixture::new();
        *f.book.lock().unwrap() = VolumeBook::new(
            OperatorBounds::new(-20.0, 0.0).unwrap(),
            crate::volume::GainBaselines::default(),
            AutoPolicy::default(),
        );
        assert_eq!(f.brain.help_volume().bounds.max_db(), 0.0);
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
        // A turn nobody answered delivered no final response, so it is closed.
        assert_eq!(f.dispatch(test_utterance()).await.unwrap(), TurnEnd::Closed);

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
        assert_eq!(turn.await.unwrap(), TurnEnd::Closed);

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
        assert_eq!(f.dispatch(test_utterance()).await.unwrap(), TurnEnd::Closed);

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
    async fn an_unknown_marker_is_reported_and_the_speech_survives() {
        // A marker the vocabulary knows is acted on silently, whatever it asks
        // for; only the one nobody can read earns a line.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Go on.<emote:happy/><listen/>")
            .await;
        turn.await.unwrap();

        assert_eq!(f.spoken(), ["Go on."]);
        assert_eq!(
            f.events(),
            vec![BrainEvent::LinkTagStripped {
                utterance: UtteranceId(42),
                tag: "<emote:happy/>".into(),
            }]
        );
        assert_eq!(f.stats.snapshot().link_tags_stripped, 1);
    }

    #[tokio::test]
    async fn a_reply_with_cues_hands_them_over_before_its_speech() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><motion name=\"a/b\" speed=\"0.5\"/>Hello!")
            .await;
        turn.await.unwrap();

        assert_eq!(
            f.cued(),
            vec![vec![Cue::Motion {
                name: "a/b".into(),
                speed: Some(0.5)
            }]]
        );
        assert_eq!(f.spoken(), ["Hello!"]);
    }

    #[tokio::test]
    async fn a_cue_only_message_moves_the_head_and_queues_no_speech() {
        // A movement is not speech, so it does not ride the response queue: a
        // whitespace-only reply queues nothing, and the head still moves.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><pose name=\"peek\"/>")
            .await;
        turn.await.unwrap();

        assert_eq!(
            f.cued(),
            vec![vec![Cue::Pose {
                name: "peek".into(),
                speed: None
            }]]
        );
        assert!(f.spoken().is_empty());
    }

    /// A chain's cues are handed over one batch per message, as they arrive. The
    /// head is meant to move with the chunk that asked for it, and the scripter's
    /// "last of each kind wins" rule is per batch: collecting a chain into one
    /// batch would drop the first pose and start the second motion at the end.
    #[tokio::test]
    async fn a_continuation_chain_hands_over_one_batch_per_message() {
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/><pose name=\"peek\"/>part one<continued/>")
            .await;
        f.deliver_armed("<reply to=\"42\" continued=\"true\"/><motion name=\"a/b\"/>part two")
            .await;
        turn.await.unwrap();

        assert_eq!(
            f.cued(),
            vec![
                vec![Cue::Pose {
                    name: "peek".into(),
                    speed: None
                }],
                vec![Cue::Motion {
                    name: "a/b".into(),
                    speed: None
                }]
            ]
        );
        assert_eq!(f.spoken(), ["part one", "part two"]);
    }

    #[tokio::test]
    async fn a_reply_with_no_cues_never_calls_the_cue_tap() {
        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Sixty-eight and clear.")
            .await;
        turn.await.unwrap();

        assert!(f.cued().is_empty());
    }

    // --- how the turn ended ---

    #[tokio::test]
    async fn a_plain_final_response_closes_the_turn() {
        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Sixty-eight and clear.")
            .await;

        assert_eq!(turn.await.unwrap(), TurnEnd::Closed);
    }

    #[tokio::test]
    async fn a_final_response_that_asks_to_listen_leaves_the_turn_open() {
        // The turn's disposition is read off the marker, which is what tells the
        // head that the exchange is expected to continue and the listener that a
        // capture window is owed.
        let mut f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Which one did you mean?<listen/>")
            .await;

        assert_eq!(turn.await.unwrap(), TurnEnd::Open);
        assert_eq!(f.spoken(), ["Which one did you mean?"]);
    }

    #[tokio::test]
    async fn a_continuation_chain_takes_its_disposition_from_the_message_that_ends_it() {
        // The mid-chain `<listen/>` is spent by the segment that carried it: the peer
        // promised more in the same breath, and what it says at the end is the answer.
        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Let me check.<listen/><continued/>")
            .await;
        f.deliver_armed("<reply to=\"42\" continued=\"true\"/>Sixty-eight and clear.")
            .await;

        assert_eq!(turn.await.unwrap(), TurnEnd::Closed);

        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        f.deliver_armed("<reply to=\"42\"/>Let me check.<continued/>")
            .await;
        f.deliver_armed("<reply to=\"42\" continued=\"true\"/>Which city?<listen/>")
            .await;

        assert_eq!(turn.await.unwrap(), TurnEnd::Open);
    }

    #[tokio::test]
    async fn a_capped_chain_closes_the_turn_whatever_its_last_segment_asked_for() {
        // Nothing ended this turn — the bound did — so no message spoke for it.
        let f = Fixture::new();
        let turn = f.dispatch(test_utterance());
        for n in 0..MAX_CONTINUATIONS {
            f.deliver_armed(&format!("<reply to=\"42\"/>part {n}<listen/><continued/>"))
                .await;
            tokio::task::yield_now().await;
        }

        assert_eq!(turn.await.unwrap(), TurnEnd::Closed);
        assert_eq!(f.stats.snapshot().link_continuations_capped, 1);
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
        let help = response_contract_help(
            &HelpChannels {
                wake: None,
                ..help_channels()
            },
            &help_cues(),
        );
        assert!(!help.contains("brenn:pod.wake"));
        assert!(!help.contains("\"type\": \"wake\""));
        assert!(help.contains("brenn:pod.utterance"));
    }
}
