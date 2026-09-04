//! Bringing the XVF3800 up in a known state, and saying what state it is in.
//!
//! The board hands the host two output channels. The left one is the chip's
//! post-processed beam — adaptive noise suppression, AGC and echo suppression —
//! and those stages adapt to whatever they hear, including the robot's own servos.
//! The right one can be routed to the ASR output, which is taken straight off the
//! beamformer and never enters the post-processor. That is what speech recognition
//! is meant to consume, and this module is what puts it there.
//!
//! Three steps, in order, before any audio stream is opened: read the chip's
//! identity, reboot it so no adaptive state survives a restart of this process, and
//! write the routing. The reboot is why this runs first — the board leaves the USB
//! bus and comes back as a fresh sound card, so a capture stream opened before it
//! would be opened on a card about to vanish.
//!
//! Everything here that talks to the chip is generic over [`ControlTransport`], and
//! the bus and the card are reached through function seams, so the whole sequence
//! is decidable off the device.

use std::fmt;
use std::time::{Duration, Instant};

use xvf3800_ctrl::{
    AEC_AECCONVERGED_CMD, AEC_ASROUTGAIN_CMD, AEC_ASROUTONOFF_CMD, AEC_ASROUTONOFF_LABEL,
    AEC_RESID, APPLICATION_SERVICER_RESID, AUDIO_MGR_OP_L_CMD, AUDIO_MGR_OP_L_LABEL,
    AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_R_LABEL, AUDIO_MGR_OP_READ_LEN, AUDIO_MGR_RESID, BLD_MSG_CMD,
    BLD_MSG_LABEL, BLD_MSG_READ_LEN, ControlTransport, PP_AGCGAIN_CMD, PP_AGCONOFF_CMD,
    PP_DTSENSITIVE_CMD, PP_ECHOONOFF_CMD, PP_MIN_NN_CMD, PP_MIN_NS_CMD, PP_RESID, REBOOT_CMD,
    REBOOT_LABEL, REBOOT_WRITE_LEN, RetryPolicy, SCALAR_READ_LEN, USB_RETRY, VERSION_CMD,
    VERSION_LABEL, VERSION_READ_LEN, decode_ascii, decode_f32, decode_i32, encode_i32,
};

use crate::config::POST_PROCESSED_CHANNEL;
use crate::regs::{read_register, write_register};
use crate::usb_ctrl::{Board, log_generation};

/// The routing written to `AUDIO_MGR_OP_R`: category 7 (AEC residual / ASR data),
/// source 3 (the auto-select beam).
pub const ASR_ROUTE: [u8; 2] = [7, 3];

/// How long the board is given to leave the bus and come back after a reboot. The
/// vendor's own recovery note puts it at three to five seconds.
pub const REBOOT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the bus and the card list are looked at while waiting.
pub const REBOOT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long the device nodes are given to become this process's to open, once
/// the board and its card are back.
///
/// Enumeration and the permissions on what it created are two moments: devtmpfs
/// creates `/dev/bus/usb/…` and `/dev/snd/…` owned by root, and the udev rule
/// that hands them to the `audio` group runs afterwards. This pod is not root,
/// so an open between the two fails on permission — a race that did not exist
/// while the pod only ever opened devices that had been settled for as long as
/// the unit had been up.
pub const NODE_ACCESS_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the eleven diagnostic reads of one state line may take together.
///
/// The retry budget bounds the *transactions* per register; this bounds the wall
/// clock of the whole line, which is the number that matters on the thread that
/// also polls the VAD gate every 50 ms. A transfer that times out costs a second
/// of it whatever the retry budget says, so eleven silent registers would be
/// eleven seconds without this. Once it is spent the rest of the line prints as
/// `?`, which is what an unread register prints anyway.
pub const STATE_LINE_BUDGET: Duration = Duration::from_millis(500);

/// How long the board is given to be seen *leaving* the bus, before the wait
/// treats it as gone and back inside that window.
///
/// The vendor's own recovery note puts the whole reboot at three to five seconds,
/// so a board still answering at the end of this one either never left or has
/// already returned; either way the pre-reboot device is no longer what a look
/// finds.
pub const REBOOT_SETTLE: Duration = Duration::from_secs(3);

/// How often the chip's state line is repeated while the VAD gate is open.
pub const STATE_LINE_INTERVAL: Duration = Duration::from_secs(30);

/// The retry budget the state line's eleven reads run on: two transactions, 10 ms
/// apart.
///
/// Not `USB_RETRY`, which spends up to a second per register: these reads happen on
/// the thread that also polls the VAD gate every 50 ms, and they are due precisely
/// while somebody is speaking. Eleven registers on the full budget would stall the
/// gate for eleven seconds and stretch a hangover counted in ticks — the instrument
/// would be perturbing the utterance it was added to explain. A register that will
/// not answer twice prints as `?`, which is what an unread register prints anyway.
pub const STATE_RETRY: RetryPolicy = RetryPolicy {
    max_retries: 1,
    delay_ms: 10,
};

// ── Identity ──────────────────────────────────────────────────────────────────

/// What the chip says it is: the application servicer's version triple and the
/// build string the firmware was compiled with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub version: (u8, u8, u8),
    pub build: String,
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (major, minor, patch) = self.version;
        write!(f, "firmware {major}.{minor}.{patch} {:?}", self.build)
    }
}

/// Read the chip's identity, or say which register would not answer.
///
/// Not fatal on its own — the caller logs it and goes on — but it is the line that
/// says which command table the routing below was written against.
pub fn read_identity<T: ControlTransport>(transport: &mut T) -> Result<Identity, String>
where
    T::Error: fmt::Display,
{
    let mut version = [0u8; VERSION_READ_LEN];
    read_register(
        transport,
        USB_RETRY,
        APPLICATION_SERVICER_RESID,
        VERSION_CMD,
        &mut version,
        VERSION_LABEL,
    )?;
    let mut build = [0u8; BLD_MSG_READ_LEN];
    read_register(
        transport,
        USB_RETRY,
        APPLICATION_SERVICER_RESID,
        BLD_MSG_CMD,
        &mut build,
        BLD_MSG_LABEL,
    )?;
    Ok(Identity {
        version: (version[0], version[1], version[2]),
        build: decode_ascii(&build).to_string(),
    })
}

// ── Reboot ────────────────────────────────────────────────────────────────────

/// Ask the chip to reboot.
///
/// Write-only and unacknowledged in any useful sense: the board is on its way off
/// the bus while the transfer completes, so a transfer error here is reported and
/// the wait below is what decides whether the reboot happened.
pub fn request_reboot<T: ControlTransport>(transport: &mut T) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    let payload = [1u8; REBOOT_WRITE_LEN];
    write_register(
        transport,
        USB_RETRY,
        APPLICATION_SERVICER_RESID,
        REBOOT_CMD,
        &payload,
        REBOOT_LABEL,
    )
}

/// Wait for the rebooted board to come back on the bus *and* for the kernel to
/// present its sound card again.
///
/// Both, because they return at different moments: the USB device enumerates first
/// and the ALSA card appears when `snd_usb_audio` has bound to it, and a capture
/// opened between the two fails on a card that is not there yet.
///
/// The board has to be seen to *go* before its presence means anything. A reboot
/// request returns while the chip is still on the bus, so a look taken immediately
/// finds the pre-reboot device and its card, and a handle opened on that answers
/// the routing writes with transport errors or stale values — which the caller
/// would then read as a firmware that refused the routing. That misreading is
/// worse than the wait: it is deterministic-looking, and it points the next
/// investigation at the wrong register. So the absence is waited for first, and
/// where it is never observed the vendor's [`REBOOT_SETTLE`] floor stands in for
/// it.
///
/// A board that does not come back inside [`REBOOT_TIMEOUT`] is an error naming what
/// was last seen — which of the two halves was missing is the whole finding.
///
/// Whether the absence was actually seen travels with the board, because the two
/// endings are different findings: a board that left and returned was rebooted,
/// and one that answered every look either ignored the request or the write went
/// somewhere it did not reach — and that is the one condition under which the
/// adaptive state this sequence exists to clear is still there.
pub fn wait_for_board(
    find: &dyn Fn() -> Result<Board, String>,
    card_ready: &dyn Fn() -> Result<(), String>,
    now: &dyn Fn() -> Instant,
    sleep: &dyn Fn(Duration),
) -> Result<(Board, bool), String> {
    let deadline = now() + REBOOT_TIMEOUT;
    let settled = now() + REBOOT_SETTLE;
    let mut left_the_bus = false;
    while now() < settled {
        if find()
            .and_then(|board| card_ready().map(|()| board))
            .is_err()
        {
            left_the_bus = true;
            break;
        }
        sleep(REBOOT_POLL_INTERVAL);
    }
    loop {
        match find().and_then(|board| card_ready().map(|()| board)) {
            Ok(board) => return Ok((board, left_the_bus)),
            Err(why) => {
                if now() >= deadline {
                    return Err(format!(
                        "the board did not come back within {}s of the reboot; last seen: {why}",
                        REBOOT_TIMEOUT.as_secs()
                    ));
                }
                sleep(REBOOT_POLL_INTERVAL);
            }
        }
    }
}

/// Try something until it works or the budget is spent, at the reboot poll's
/// cadence.
///
/// For the opens that follow a re-enumeration: the failure is a device node this
/// process may not touch *yet*, which is indistinguishable at the call from one
/// it will never be allowed to touch, and only time tells them apart. The last
/// failure is what is reported, because the first one is the race and the last
/// one is the state that persisted.
pub fn keep_trying<T>(
    attempt: &dyn Fn() -> Result<T, String>,
    budget: Duration,
    now: &dyn Fn() -> Instant,
    sleep: &dyn Fn(Duration),
) -> Result<T, String> {
    let deadline = now() + budget;
    loop {
        match attempt() {
            Ok(got) => return Ok(got),
            Err(why) => {
                if now() >= deadline {
                    return Err(why);
                }
                sleep(REBOOT_POLL_INTERVAL);
            }
        }
    }
}

// ── Routing ───────────────────────────────────────────────────────────────────

/// What became of the attempt to route the ASR output to the right channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Routing {
    /// The chip read back what was written. The channel to stream is the ASR one.
    Applied,
    /// The chip did not take the routing, and why — one line per disagreement.
    ///
    /// Never retried: a firmware that refuses this routing refuses it on every
    /// start, so retrying is a restart loop with no pipeline in it.
    Refused(Vec<String>),
}

impl Routing {
    /// Which capture channel this pod streams, given the channel the configuration
    /// settled on.
    ///
    /// A refusal is the one case the pod overrides the configuration: the routing
    /// the ASR channel depends on is not there, so the run falls back to the
    /// channel whose routing this pod never wrote — exactly the pipeline that ran
    /// before the ASR output was ever asked for.
    pub const fn channel(&self, configured: usize) -> usize {
        match self {
            Routing::Applied => configured,
            Routing::Refused(_) => POST_PROCESSED_CHANNEL,
        }
    }

    /// How the startup line names the channel it settled on.
    pub fn channel_note(&self, configured: usize) -> String {
        let channel = self.channel(configured);
        match self {
            Routing::Applied => format!("channel={channel}"),
            Routing::Refused(_) => format!("channel={channel} (routing refused)"),
        }
    }
}

/// What one attempt at the routing produced: the verdict, what the registers
/// said, and whatever else the read noticed on the way.
///
/// The three are apart because only the verdict decides which channel the run
/// streams. A diagnostic read that failed is worth a line and is not a firmware
/// refusing anything, and folding it into the verdict would turn a USB hiccup
/// into "the routing was refused" — a sentence the runbook tells a reader means
/// something deterministic about this firmware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingRead {
    pub routing: Routing,
    /// What the three registers read back, for the line that reports them.
    pub reading: RoutingReading,
    /// Readings that are not the verdict: one line each, for the log.
    pub notes: Vec<String>,
}

/// What the three routing registers read back, `None` where one would not answer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoutingReading {
    pub left: Option<[u8; AUDIO_MGR_OP_READ_LEN]>,
    pub right: Option<[u8; AUDIO_MGR_OP_READ_LEN]>,
    pub asr_out: Option<i32>,
}

impl fmt::Display for RoutingReading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} | {} | ASROUTONOFF {}",
            pair_text("OP_L", self.left),
            pair_text("OP_R", self.right),
            match self.asr_out {
                Some(value) => value.to_string(),
                None => "unread".to_string(),
            }
        )
    }
}

/// What the two output-routing registers say the board's channels carry.
///
/// Each register answers a `(category, source)` byte pair, carried and printed
/// raw: the only pair anything in-tree interprets is [`ASR_ROUTE`], which this pod
/// wrote itself, so every other reading is evidence to be reviewed rather than a
/// value to conclude from here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputRouting {
    /// `AUDIO_MGR_OP_L`.
    pub left: [u8; AUDIO_MGR_OP_READ_LEN],
    /// `AUDIO_MGR_OP_R`.
    pub right: [u8; AUDIO_MGR_OP_READ_LEN],
}

impl OutputRouting {
    /// Whether the two outputs are routed from different sources, as the registers
    /// have it.
    pub fn channels_differ(&self) -> bool {
        self.left != self.right
    }
}

impl fmt::Display for OutputRouting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} | {}",
            pair_text("OP_L", Some(self.left)),
            pair_text("OP_R", Some(self.right))
        )
    }
}

/// One `(category, source)` pair as every line that prints one renders it, or that
/// it was not read. One spelling, so the pod log and a bench report say the same
/// thing about the same register.
fn pair_text(name: &str, pair: Option<[u8; AUDIO_MGR_OP_READ_LEN]>) -> String {
    match pair {
        Some(pair) => format!("{name} (category {}, source {})", pair[0], pair[1]),
        None => format!("{name} unread"),
    }
}

/// One output-routing register's `(category, source)` pair, or the reading that
/// says why there is none.
pub fn read_routing_register<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    cmd: u8,
    label: &str,
) -> Result<[u8; AUDIO_MGR_OP_READ_LEN], String>
where
    T::Error: fmt::Display,
{
    let mut pair = [0u8; AUDIO_MGR_OP_READ_LEN];
    read_register(transport, policy, AUDIO_MGR_RESID, cmd, &mut pair, label)?;
    Ok(pair)
}

/// Read both output-routing registers, or say why not.
///
/// The failure is a string rather than a verdict because this reading does not
/// decide anything on its own: the attended bench case reports it either way, and
/// [`read_routing`] is what judges the routing this pod writes.
pub fn read_output_routing<T: ControlTransport>(transport: &mut T) -> Result<OutputRouting, String>
where
    T::Error: fmt::Display,
{
    Ok(OutputRouting {
        left: read_routing_register(
            transport,
            USB_RETRY,
            AUDIO_MGR_OP_L_CMD,
            AUDIO_MGR_OP_L_LABEL,
        )?,
        right: read_routing_register(
            transport,
            USB_RETRY,
            AUDIO_MGR_OP_R_CMD,
            AUDIO_MGR_OP_R_LABEL,
        )?,
    })
}

/// Read the three routing registers back and judge them against what this pod
/// writes at startup.
///
/// The one place the judgement lives: the startup sequence calls it after its two
/// writes and the bench's routing case calls it on its own, so `pod_0.log` and a
/// self-test report describe the same disagreement in the same words.
///
/// `AUDIO_MGR_OP_L` is read for the record and never judged — the left channel's
/// routing is the chip's to choose and this pod has no expectation of it, so a
/// read that fails is a note rather than a refusal.
pub fn read_routing<T: ControlTransport>(transport: &mut T, policy: RetryPolicy) -> RoutingRead
where
    T::Error: fmt::Display,
{
    let mut refusals = Vec::new();
    let mut notes = Vec::new();
    let mut reading = RoutingReading::default();

    match read_routing_register(transport, policy, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_R_LABEL) {
        Ok(right) => {
            reading.right = Some(right);
            if right != ASR_ROUTE {
                refusals.push(format!(
                    "{AUDIO_MGR_OP_R_LABEL} was written ({}, {}) and reads back ({}, {})",
                    ASR_ROUTE[0], ASR_ROUTE[1], right[0], right[1]
                ));
            }
        }
        Err(why) => refusals.push(why),
    }

    let mut enabled = [0u8; SCALAR_READ_LEN];
    match read_register(
        transport,
        policy,
        AEC_RESID,
        AEC_ASROUTONOFF_CMD,
        &mut enabled,
        AEC_ASROUTONOFF_LABEL,
    ) {
        Ok(()) => {
            let value = decode_i32(&enabled);
            reading.asr_out = Some(value);
            if value != 1 {
                refusals.push(format!(
                    "{AEC_ASROUTONOFF_LABEL} was written 1 and reads back {value}"
                ));
            }
        }
        Err(why) => refusals.push(why),
    }

    match read_routing_register(transport, policy, AUDIO_MGR_OP_L_CMD, AUDIO_MGR_OP_L_LABEL) {
        Ok(left) => reading.left = Some(left),
        Err(why) => notes.push(why),
    }

    RoutingRead {
        routing: if refusals.is_empty() {
            Routing::Applied
        } else {
            Routing::Refused(refusals)
        },
        reading,
        notes,
    }
}

/// Route the auto-select beam's ASR output to the right channel and confirm it.
///
/// The order is the user guide's: the output mux first, then the switch that makes
/// the ASR extraction the thing the mux is carrying. [`read_routing`] is the
/// arbiter: a write the transport could not carry joins its refusals ahead of them
/// only when the read-back also disagrees or could not be taken. A transport error
/// can mean a write that landed under a status read that hiccupped — registers
/// holding the values this pod wanted are a chip carrying the routing.
pub fn apply_asr_routing<T: ControlTransport>(transport: &mut T) -> RoutingRead
where
    T::Error: fmt::Display,
{
    let mut refusals = Vec::new();
    if let Err(why) = write_register(
        transport,
        USB_RETRY,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_R_CMD,
        &ASR_ROUTE,
        AUDIO_MGR_OP_R_LABEL,
    ) {
        refusals.push(why);
    }
    if let Err(why) = write_register(
        transport,
        USB_RETRY,
        AEC_RESID,
        AEC_ASROUTONOFF_CMD,
        &encode_i32(1),
        AEC_ASROUTONOFF_LABEL,
    ) {
        refusals.push(why);
    }

    let mut read = read_routing(transport, USB_RETRY);
    if !refusals.is_empty() {
        match read.routing {
            Routing::Applied => {
                for why in refusals {
                    read.notes.push(format!(
                        "{why}; the registers read back what was written, so the chip \
                         carries the routing"
                    ));
                }
            }
            Routing::Refused(found) => {
                refusals.extend(found);
                read.routing = Routing::Refused(refusals);
            }
        }
    }
    read
}

// ── The sequence ──────────────────────────────────────────────────────────────

/// Open the chip's control plane and put the chip in the state this pipeline runs
/// on: rebooted, with the beamformer's ASR output routed to the right channel.
///
/// The reboot is what makes the chip's adaptive state — the only state on this path
/// that outlives a process restart — start where every other part of the pipeline
/// starts. It costs the board leaving the USB bus, which is why the caller opens
/// nothing else until this returns.
///
/// A routing the chip does not take is not fatal and is not retried: the refusal is
/// deterministic, so exiting would loop forever with no pipeline ever running. The
/// lines say so and the run streams the post-processed channel instead.
///
/// Every moving part is a seam — the bus, the card, the clock, the sleep and the
/// open — so the order of the whole sequence is decidable off the device, which is
/// where its two load-bearing orderings live: the handle is dropped before the
/// board goes, and the routing is written to the board that came back.
pub fn bring_up<T: ControlTransport>(
    open: &dyn Fn(Board) -> Result<T, String>,
    find: &dyn Fn() -> Result<Board, String>,
    card_ready: &dyn Fn() -> Result<(), String>,
    now: &dyn Fn() -> Instant,
    sleep: &dyn Fn(Duration),
) -> Result<(T, Routing), String>
where
    T::Error: fmt::Display,
{
    let board = find().map_err(|e| format!("no XVF3800 control interface: {e}"))?;
    log_generation(board);
    let mut control = keep_trying(&|| open(board), NODE_ACCESS_TIMEOUT, now, sleep)
        .map_err(|e| format!("cannot open {board} for control transfers: {e}"))?;
    match read_identity(&mut control) {
        Ok(identity) => log::info!("xvf3800: {identity}"),
        Err(why) => log::warn!("xvf3800: {why}"),
    }
    // A transport error here is what a chip that resets while acknowledging the
    // transfer produces, so it is reported and the wait decides whether the
    // reboot happened. Failing the start on it would be a restart loop against a
    // board that is rebooting exactly as asked.
    if let Err(why) = request_reboot(&mut control) {
        log::warn!("xvf3800: {why}; the wait below is what says whether it rebooted");
    }
    // The handle names a device that is on its way off the bus.
    drop(control);
    let (board, left_the_bus) = wait_for_board(find, card_ready, now, sleep)?;
    if left_the_bus {
        log::info!("xvf3800: reboot: the board left the bus and came back");
    } else {
        log::warn!(
            "xvf3800: reboot: the board was never seen leaving the bus, so it may not have \
             rebooted and its adaptive state may be the state it had; proceeding after the {}s \
             settle",
            REBOOT_SETTLE.as_secs()
        );
    }
    log_generation(board);
    let mut control = keep_trying(&|| open(board), NODE_ACCESS_TIMEOUT, now, sleep)
        .map_err(|e| format!("cannot re-open {board} after the reboot: {e}"))?;

    // Dropped intentionally; `state_line` re-reads and prints these registers.
    let RoutingRead { routing, notes, .. } = apply_asr_routing(&mut control);
    for note in notes {
        log::warn!("xvf3800: {note}");
    }
    if let Routing::Refused(refusals) = &routing {
        for line in refusals {
            log::error!("xvf3800: {line}");
        }
        log::error!(
            "xvf3800: the ASR routing was refused; this run streams channel \
             {POST_PROCESSED_CHANNEL}, the post-processed output, and does not ask again"
        );
    }
    // Read before it is logged: a macro argument is not evaluated when no logger
    // wants the line, and the state line is a reading and not a format.
    let state = state_line(&mut control, now);
    log::info!("{state}");
    Ok((control, routing))
}

// ── The state line ────────────────────────────────────────────────────────────

/// How a register's value renders once it is a number, an id or a refusal.
enum Field {
    Pair([u8; AUDIO_MGR_OP_READ_LEN]),
    Int(i32),
    Float(f32),
    Unread,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::Pair([category, source]) => write!(f, "({category},{source})"),
            Field::Int(v) => write!(f, "{v}"),
            Field::Float(v) => write!(f, "{v:.4}"),
            // A register that would not answer prints as unread rather than as a
            // plausible value; the transport has already logged why.
            Field::Unread => write!(f, "?"),
        }
    }
}

/// Read the chip's routing and post-processing state as one line.
///
/// Every field is read independently and a register that will not answer prints as
/// `?`, so one dead register does not cost the reading of the other ten. The
/// post-processing values are here even though nothing downstream hears that stage
/// any more: what the suppressor and the AGC are doing is exactly what a degraded
/// run needs to be readable against.
///
/// The whole line is bounded by [`STATE_LINE_BUDGET`], read off `now`: a bus that
/// has stopped answering costs a timeout per register, and this line is due on the
/// gate thread while somebody is speaking. Registers past the budget are not asked
/// for and print unread.
pub fn state_line<T: ControlTransport>(transport: &mut T, now: &dyn Fn() -> Instant) -> String
where
    T::Error: fmt::Display,
{
    let deadline = now() + STATE_LINE_BUDGET;
    let spent = || now() >= deadline;
    let mut fields = vec![
        ("OP_L", pair(transport, AUDIO_MGR_OP_L_CMD, "OP_L", &spent)),
        ("OP_R", pair(transport, AUDIO_MGR_OP_R_CMD, "OP_R", &spent)),
        (
            "ASROUTONOFF",
            scalar(
                transport,
                AEC_RESID,
                AEC_ASROUTONOFF_CMD,
                false,
                "ASROUTONOFF",
                &spent,
            ),
        ),
        (
            "ASROUTGAIN",
            scalar(
                transport,
                AEC_RESID,
                AEC_ASROUTGAIN_CMD,
                true,
                "ASROUTGAIN",
                &spent,
            ),
        ),
        (
            "AECCONVERGED",
            scalar(
                transport,
                AEC_RESID,
                AEC_AECCONVERGED_CMD,
                false,
                "AECCONVERGED",
                &spent,
            ),
        ),
    ];
    for (name, cmd, is_float) in [
        ("AGCONOFF", PP_AGCONOFF_CMD, false),
        ("AGCGAIN", PP_AGCGAIN_CMD, true),
        ("MIN_NS", PP_MIN_NS_CMD, true),
        ("MIN_NN", PP_MIN_NN_CMD, true),
        ("ECHOONOFF", PP_ECHOONOFF_CMD, false),
        ("DTSENSITIVE", PP_DTSENSITIVE_CMD, false),
    ] {
        fields.push((
            name,
            scalar(transport, PP_RESID, cmd, is_float, name, &spent),
        ));
    }
    let rendered: Vec<String> = fields
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    format!("chip state: {}", rendered.join(" "))
}

/// One output-routing register as a category/source pair.
fn pair<T: ControlTransport>(
    transport: &mut T,
    cmd: u8,
    label: &str,
    spent: &dyn Fn() -> bool,
) -> Field
where
    T::Error: fmt::Display,
{
    if spent() {
        return Field::Unread;
    }
    let mut bytes = [0u8; AUDIO_MGR_OP_READ_LEN];
    match read_register(
        transport,
        STATE_RETRY,
        AUDIO_MGR_RESID,
        cmd,
        &mut bytes,
        label,
    ) {
        Ok(()) => Field::Pair(bytes),
        Err(_) => Field::Unread,
    }
}

/// One four-byte register, read as a float or as an int.
fn scalar<T: ControlTransport>(
    transport: &mut T,
    resid: u8,
    cmd: u8,
    is_float: bool,
    label: &str,
    spent: &dyn Fn() -> bool,
) -> Field
where
    T::Error: fmt::Display,
{
    if spent() {
        return Field::Unread;
    }
    let mut bytes = [0u8; SCALAR_READ_LEN];
    match read_register(transport, STATE_RETRY, resid, cmd, &mut bytes, label) {
        Ok(()) if is_float => Field::Float(decode_f32(&bytes)),
        Ok(()) => Field::Int(decode_i32(&bytes)),
        Err(_) => Field::Unread,
    }
}

/// When the state line is due.
///
/// The chip's state is worth a line while there is speech to explain and worth
/// nothing at all while the room is quiet, so the cadence follows the VAD gate: one
/// line when the gate opens, one every [`STATE_LINE_INTERVAL`] it stays open, one
/// when it closes, and nothing in between.
#[derive(Debug, Default)]
pub struct StateLineCadence {
    open: bool,
    said_at: Option<Instant>,
}

impl StateLineCadence {
    /// A cadence that has said nothing, with the gate closed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether this tick should print the state line, given the gate's state now.
    pub fn tick(&mut self, gate_open: bool, now: Instant) -> bool {
        let was_open = self.open;
        self.open = gate_open;
        let due = match (was_open, gate_open) {
            (false, true) | (true, false) => true,
            (true, true) => self
                .said_at
                .is_none_or(|said| now.duration_since(said) >= STATE_LINE_INTERVAL),
            (false, false) => false,
        };
        if due {
            self.said_at = Some(now);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ASR_OUTPUT_CHANNEL;
    use crate::test_support::{Clock, Scripted};
    use crate::usb_ctrl::{DeviceId, Generation};
    use xvf3800_ctrl::STATUS_DONE;

    /// A four-byte little-endian payload, as the chip answers a scalar register.
    fn i32_bytes(v: i32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }

    fn f32_bytes(v: f32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }

    fn build_bytes(text: &str) -> Vec<u8> {
        let mut payload = vec![0u8; BLD_MSG_READ_LEN];
        payload[..text.len()].copy_from_slice(text.as_bytes());
        payload
    }

    fn a_board() -> Board {
        Board {
            id: DeviceId {
                vendor: 0x38fb,
                product: 0x1001,
                generation: Generation::ReachyFirmware,
            },
            bus: 1,
            address: 4,
        }
    }

    // ── Identity ──────────────────────────────────────────────────────────────

    #[test]
    fn the_identity_read_takes_the_version_and_then_the_build_string() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, vec![2, 1, 2]),
            (
                STATUS_DONE,
                build_bytes("XMOS XVF3800 v2.1.2 (ua-io16-lin)"),
            ),
        ]);
        let identity = read_identity(&mut transport).expect("identity");
        assert_eq!(identity.version, (2, 1, 2));
        assert_eq!(identity.build, "XMOS XVF3800 v2.1.2 (ua-io16-lin)");
        assert_eq!(
            transport.registers,
            vec![
                (APPLICATION_SERVICER_RESID, VERSION_CMD),
                (APPLICATION_SERVICER_RESID, BLD_MSG_CMD)
            ]
        );
        assert!(identity.to_string().contains("firmware 2.1.2"));
    }

    #[test]
    fn an_identity_register_that_will_not_answer_names_itself() {
        let mut transport = Scripted::answering(0x02, vec![0; VERSION_READ_LEN]);
        let why = read_identity(&mut transport).expect_err("status 0x02");
        assert!(
            why.contains("VERSION (resid 48 cmd 0)") && why.contains("0x02"),
            "{why}"
        );
    }

    // ── Reboot ────────────────────────────────────────────────────────────────

    #[test]
    fn the_reboot_is_one_byte_written_to_the_application_servicer() {
        let mut transport = Scripted::answering(STATUS_DONE, Vec::new());
        request_reboot(&mut transport).expect("reboot");
        assert_eq!(
            transport.writes,
            vec![(APPLICATION_SERVICER_RESID, REBOOT_CMD, vec![1u8])]
        );
    }

    /// A chip that resets while acknowledging the transfer is a transport error
    /// on this write, and that is the only way it fails: the board returns no
    /// status for a write at all. Reported, never fatal.
    #[test]
    fn a_reboot_the_transport_could_not_carry_is_reported_rather_than_assumed() {
        let mut transport = Scripted::failing("pipe error");
        let why = request_reboot(&mut transport).expect_err("the transport died");
        assert!(
            why.contains("REBOOT (resid 48 cmd 7)") && why.contains("write failed: pipe error"),
            "{why}"
        );
    }

    #[test]
    fn the_wait_returns_once_the_bus_and_the_card_are_both_back() {
        let clock = Clock::new();
        // The bus answers on the third look and the card two looks after that,
        // which is the order the two actually return in.
        let bus_looks = std::cell::Cell::new(0);
        let card_looks = std::cell::Cell::new(0);
        let board = wait_for_board(
            &|| {
                bus_looks.set(bus_looks.get() + 1);
                if bus_looks.get() >= 3 {
                    Ok(a_board())
                } else {
                    Err("no XVF3800 board on the bus".to_string())
                }
            },
            &|| {
                card_looks.set(card_looks.get() + 1);
                if card_looks.get() >= 3 {
                    Ok(())
                } else {
                    Err("no sound card yet".to_string())
                }
            },
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect("the board came back");
        assert_eq!(board, (a_board(), true), "the absence was seen");
        assert_eq!(
            bus_looks.get(),
            5,
            "the card is looked at only once the bus is back"
        );
        assert_eq!(card_looks.get(), 3);
    }

    /// The board answering on the first look is the *pre-reboot* board: the
    /// reboot request returns while the chip is still on the bus. Returning it
    /// hands the routing writes a device that is about to vanish, and the
    /// transport errors that follow read as a firmware refusing the routing.
    #[test]
    fn a_board_that_has_not_gone_yet_is_not_the_board_that_came_back() {
        let clock = Clock::new();
        let started = clock.now();
        let looks = std::cell::Cell::new(0);
        let board = wait_for_board(
            &|| {
                looks.set(looks.get() + 1);
                Ok(a_board())
            },
            &|| Ok(()),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect("a board that is there throughout is still a board");
        assert_eq!(
            board,
            (a_board(), false),
            "and the caller is told the absence was never seen"
        );
        assert!(
            clock.now().duration_since(started) >= REBOOT_SETTLE,
            "the vendor's settle floor stands in for an absence nobody saw"
        );
        assert!(looks.get() > 1, "and it was looked at more than once");
    }

    #[test]
    fn a_board_that_never_comes_back_names_what_was_last_seen() {
        let clock = Clock::new();
        let why = wait_for_board(
            &|| Err("no XVF3800 board on the bus; looked for 38fb:1001".to_string()),
            &|| panic!("the card is not looked at while the bus is empty"),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect_err("never came back");
        assert!(why.contains("did not come back within 10s"), "{why}");
        assert!(why.contains("38fb:1001"), "{why}");
    }

    #[test]
    fn a_card_that_never_binds_is_reported_as_the_card_and_not_as_the_bus() {
        let clock = Clock::new();
        let why = wait_for_board(
            &|| Ok(a_board()),
            &|| Err("no sound card named any of [\"reachy mini audio\"]".to_string()),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect_err("no card");
        assert!(why.contains("no sound card named"), "{why}");
    }

    // ── Routing ───────────────────────────────────────────────────────────────

    /// The chip taking the routing: two writes in the user guide's order, three
    /// reads back, and the ASR channel to stream.
    #[test]
    fn the_routing_writes_the_mux_then_the_switch_and_reads_all_three_back() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, ASR_ROUTE.to_vec()),
            (STATUS_DONE, i32_bytes(1)),
            (STATUS_DONE, vec![8, 0]),
        ]);
        let read = apply_asr_routing(&mut transport);
        assert_eq!(read.routing, Routing::Applied);
        assert!(read.notes.is_empty(), "{:?}", read.notes);
        assert_eq!(
            transport.writes,
            vec![
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, ASR_ROUTE.to_vec()),
                (AEC_RESID, AEC_ASROUTONOFF_CMD, i32_bytes(1)),
            ]
        );
        assert_eq!(
            transport.registers,
            vec![
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD),
                (AEC_RESID, AEC_ASROUTONOFF_CMD),
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD),
            ]
        );
        assert_eq!(
            Routing::Applied.channel(ASR_OUTPUT_CHANNEL),
            ASR_OUTPUT_CHANNEL
        );
        assert_eq!(
            Routing::Applied.channel_note(ASR_OUTPUT_CHANNEL),
            "channel=1"
        );
    }

    /// A firmware that acknowledges the write and does not take it: the disagreement
    /// names the register and both values, the process runs on the post-processed
    /// channel, and nothing is written a second time.
    #[test]
    fn a_routing_read_back_differently_is_refused_once_and_never_retried() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, vec![8, 0]),
            (STATUS_DONE, i32_bytes(0)),
            (STATUS_DONE, vec![8, 0]),
        ]);
        let RoutingRead { routing, notes, .. } = apply_asr_routing(&mut transport);
        let Routing::Refused(why) = &routing else {
            panic!("expected a refusal, got {routing:?}");
        };
        assert!(notes.is_empty(), "{notes:?}");
        assert_eq!(why.len(), 2, "{why:?}");
        assert!(
            why[0].contains("AUDIO_MGR_OP_R")
                && why[0].contains("(7, 3)")
                && why[0].contains("(8, 0)"),
            "{why:?}"
        );
        assert!(
            why[1].contains("AEC_ASROUTONOFF") && why[1].contains("reads back 0"),
            "{why:?}"
        );
        assert_eq!(transport.writes.len(), 2, "one write each, no retry");
        // A refusal overrides even a configuration that named the ASR channel.
        assert_eq!(routing.channel(ASR_OUTPUT_CHANNEL), POST_PROCESSED_CHANNEL);
        assert_eq!(
            routing.channel_note(ASR_OUTPUT_CHANNEL),
            "channel=0 (routing refused)"
        );
    }

    /// The transport dying under the writes: the chip returns no status for a
    /// write, so a transfer error is the only way one fails, and it is the path
    /// the pod actually takes when the board goes away mid-sequence.
    #[test]
    fn a_write_the_transport_could_not_carry_is_itself_a_refusal() {
        let mut transport = Scripted::failing("the board went away");
        let RoutingRead { routing, notes, .. } = apply_asr_routing(&mut transport);
        let Routing::Refused(why) = &routing else {
            panic!("expected a refusal, got {routing:?}");
        };
        // Two writes and two read-backs, all of them the same dead transport;
        // the left channel's read is a note rather than a fifth refusal. The two
        // write failures come first: they are what caused the read-backs to
        // disagree, and a reader wants the cause above the symptom.
        assert_eq!(why.len(), 4, "{why:?}");
        assert!(
            why[0].contains("write failed: the board went away"),
            "{why:?}"
        );
        assert!(
            why[1].contains("write failed: the board went away"),
            "{why:?}"
        );
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("AUDIO_MGR_OP_L"), "{notes:?}");
    }

    /// A write the transport could not carry over a chip that reads back exactly
    /// what was asked for: the registers are the arbiter, so the failures are
    /// notes, not refusals.
    #[test]
    fn a_write_the_transport_could_not_carry_is_no_refusal_when_the_chip_reads_back_right() {
        let mut bank =
            crate::test_support::RegisterBank::new().failing_writes("the transfer timed out");
        bank.set(AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, ASR_ROUTE.to_vec());
        bank.set(AEC_RESID, AEC_ASROUTONOFF_CMD, i32_bytes(1));
        bank.set(AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD, vec![8, 0]);
        let mut transport = Handle(std::rc::Rc::new(std::cell::RefCell::new(bank)));
        let RoutingRead {
            routing,
            reading,
            notes,
        } = apply_asr_routing(&mut transport);
        assert_eq!(routing, Routing::Applied);
        assert_eq!(routing.channel(ASR_OUTPUT_CHANNEL), ASR_OUTPUT_CHANNEL);
        assert_eq!(reading.right, Some(ASR_ROUTE));
        assert_eq!(reading.asr_out, Some(1));
        assert_eq!(notes.len(), 2, "one note per write, no refusal: {notes:?}");
        assert!(
            notes[0].contains("AUDIO_MGR_OP_R") && notes[0].contains("the transfer timed out"),
            "{notes:?}"
        );
        assert!(
            notes[1].contains("AEC_ASROUTONOFF") && notes[1].contains("carries the routing"),
            "{notes:?}"
        );
        assert_eq!(
            transport.0.borrow().writes.len(),
            2,
            "one write each, still never retried"
        );
    }

    /// The left channel's routing is read for the record and this pod has no
    /// expectation of it, so a read that fails is a line and not a verdict.
    /// Folding it in would turn a USB hiccup into the sentence the runbook says
    /// means this firmware refuses the ASR routing — and send the next
    /// investigation after a chip that took it.
    #[test]
    fn the_read_this_pod_never_wrote_cannot_refuse_the_routing() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, ASR_ROUTE.to_vec()),
            (STATUS_DONE, i32_bytes(1)),
            // The third read is OP_L, and this chip will not answer it.
            (0x02, vec![0, 0]),
        ]);
        let RoutingRead { routing, notes, .. } = apply_asr_routing(&mut transport);
        assert_eq!(routing, Routing::Applied);
        assert_eq!(routing.channel(ASR_OUTPUT_CHANNEL), ASR_OUTPUT_CHANNEL);
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(
            notes[0].contains("AUDIO_MGR_OP_L") && notes[0].contains("0x02"),
            "{notes:?}"
        );
    }

    /// The read-back and its judgement without the writes: what the bench's
    /// routing case runs, and the reason it says what `pod_0.log` said. It reads
    /// the three registers, judges the two this pod writes, and writes nothing.
    #[test]
    fn the_shared_read_judges_without_writing_and_renders_all_three() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, vec![0, 0]),
            (STATUS_DONE, i32_bytes(0)),
            (STATUS_DONE, vec![4, 0]),
        ]);
        let read = read_routing(&mut transport, USB_RETRY);
        assert!(transport.writes.is_empty(), "{:?}", transport.writes);
        let Routing::Refused(why) = &read.routing else {
            panic!("expected a refusal, got {:?}", read.routing);
        };
        assert_eq!(why.len(), 2, "{why:?}");
        assert_eq!(
            read.reading.to_string(),
            "OP_L (category 4, source 0) | OP_R (category 0, source 0) | ASROUTONOFF 0"
        );
    }

    /// A register that would not answer prints as unread rather than as a value
    /// nobody read.
    #[test]
    fn an_unread_register_says_so_in_the_reading() {
        let mut transport = Scripted::failing("pipe error");
        let read = read_routing(&mut transport, USB_RETRY);
        assert_eq!(
            read.reading.to_string(),
            "OP_L unread | OP_R unread | ASROUTONOFF unread"
        );
    }

    // ── The state line ────────────────────────────────────────────────────────

    #[test]
    fn the_state_line_prints_the_routing_and_the_post_processing_stage() {
        let mut transport = Scripted::sequenced(vec![
            (STATUS_DONE, vec![8, 0]),
            (STATUS_DONE, ASR_ROUTE.to_vec()),
            (STATUS_DONE, i32_bytes(1)),
            (STATUS_DONE, f32_bytes(1.0)),
            (STATUS_DONE, i32_bytes(0)),
            (STATUS_DONE, i32_bytes(1)),
            (STATUS_DONE, f32_bytes(32.0)),
            (STATUS_DONE, f32_bytes(0.15)),
            (STATUS_DONE, f32_bytes(0.51)),
            (STATUS_DONE, i32_bytes(1)),
            (STATUS_DONE, i32_bytes(1)),
        ]);
        assert_eq!(
            state_line(&mut transport, &Instant::now),
            "chip state: OP_L=(8,0) OP_R=(7,3) ASROUTONOFF=1 ASROUTGAIN=1.0000 \
             AECCONVERGED=0 AGCONOFF=1 AGCGAIN=32.0000 MIN_NS=0.1500 MIN_NN=0.5100 \
             ECHOONOFF=1 DTSENSITIVE=1"
        );
        assert_eq!(transport.reads, 11);
    }

    #[test]
    fn a_register_that_will_not_answer_prints_as_unread_and_costs_no_other_field() {
        let mut transport = Scripted::failing("the board went away");
        let line = state_line(&mut transport, &Instant::now);
        assert_eq!(
            line,
            "chip state: OP_L=? OP_R=? ASROUTONOFF=? ASROUTGAIN=? AECCONVERGED=? AGCONOFF=? \
             AGCGAIN=? MIN_NS=? MIN_NN=? ECHOONOFF=? DTSENSITIVE=?"
        );
    }

    /// A bus that answers slowly costs a timeout per register whatever the retry
    /// budget says, and this line is read on the thread that polls the VAD gate.
    /// The budget stops the line rather than the gate: what is left prints
    /// unread, and no further transaction is issued.
    #[test]
    fn a_state_line_that_runs_out_of_budget_stops_asking() {
        let clock = Clock::new();
        // Every look at the clock costs three fifths of the budget, so the first
        // field is read and the rest are not asked for.
        let slow = || {
            clock.advance(STATE_LINE_BUDGET * 3 / 5);
            clock.now()
        };
        let mut transport = Scripted::answering(STATUS_DONE, vec![0, 0]);
        let line = state_line(&mut transport, &slow);
        assert!(
            line.starts_with("chip state: OP_L=(0,0) OP_R=? ASROUTONOFF=?"),
            "{line}"
        );
        assert!(line.ends_with("DTSENSITIVE=?"), "{line}");
        assert_eq!(transport.reads, 1, "the rest was never asked for");
    }

    // ── The opens that follow a re-enumeration ────────────────────────────────

    /// The device node exists before this process may open it: udev's group
    /// grant lands after devtmpfs created the node. Only time tells that apart
    /// from a permission that will never come, so the open is retried.
    #[test]
    fn an_open_that_is_not_permitted_yet_is_tried_again_until_it_is() {
        let clock = Clock::new();
        let tries = std::cell::Cell::new(0);
        let opened = keep_trying(
            &|| {
                tries.set(tries.get() + 1);
                if tries.get() >= 4 {
                    Ok("the handle")
                } else {
                    Err("permission denied".to_string())
                }
            },
            NODE_ACCESS_TIMEOUT,
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect("the grant landed");
        assert_eq!(opened, "the handle");
        assert_eq!(tries.get(), 4);
    }

    /// And one that never comes is the failure it always was, reported as the
    /// last thing the open actually said.
    #[test]
    fn an_open_that_never_becomes_permitted_reports_what_it_last_said() {
        let clock = Clock::new();
        let why = keep_trying(
            &|| Err::<(), String>("permission denied".to_string()),
            NODE_ACCESS_TIMEOUT,
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect_err("never permitted");
        assert_eq!(why, "permission denied");
    }

    // ── The sequence ──────────────────────────────────────────────────────────

    /// A control handle several opens can share, so a case can read what the
    /// whole sequence asked of one chip across the reboot.
    #[derive(Clone, Debug)]
    struct Handle(std::rc::Rc<std::cell::RefCell<crate::test_support::RegisterBank>>);

    impl ControlTransport for Handle {
        type Error = &'static str;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.0
                .borrow_mut()
                .control_read_once(resid, cmd, payload, attempt)
        }

        fn control_write_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &[u8],
            attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.0
                .borrow_mut()
                .control_write_once(resid, cmd, payload, attempt)
        }

        fn delay_ms(&mut self, _ms: u32) {}
    }

    /// A chip whose routing registers read back whatever it is given.
    fn a_chip(right: Vec<u8>, enabled: i32) -> Handle {
        let mut bank = crate::test_support::RegisterBank::new();
        bank.set(AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, right);
        bank.set(AEC_RESID, AEC_ASROUTONOFF_CMD, i32_bytes(enabled));
        bank.set(AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD, vec![8, 0]);
        Handle(std::rc::Rc::new(std::cell::RefCell::new(bank)))
    }

    /// The whole sequence on a chip that takes the routing: identity, the reboot
    /// write, the wait, the two routing writes to the board that came back, the
    /// three read-backs and one state line.
    #[test]
    fn the_bring_up_reboots_then_routes_the_board_that_came_back() {
        let clock = Clock::new();
        let chip = a_chip(ASR_ROUTE.to_vec(), 1);
        let opens = std::cell::Cell::new(0);
        let looks = std::cell::Cell::new(0);
        let (_control, routing) = bring_up(
            &|_board| {
                opens.set(opens.get() + 1);
                Ok(chip.clone())
            },
            &|| {
                looks.set(looks.get() + 1);
                // Away for the second and third look: the reboot, seen.
                if (2..=3).contains(&looks.get()) {
                    Err("no XVF3800 board on the bus".to_string())
                } else {
                    Ok(a_board())
                }
            },
            &|| Ok(()),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect("the chip came up");
        assert_eq!(routing, Routing::Applied);
        assert_eq!(opens.get(), 2, "one handle before the reboot and one after");
        let bank = chip.0.borrow();
        assert_eq!(
            bank.writes,
            vec![
                (APPLICATION_SERVICER_RESID, REBOOT_CMD, vec![1u8]),
                (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, ASR_ROUTE.to_vec()),
                (AEC_RESID, AEC_ASROUTONOFF_CMD, i32_bytes(1)),
            ],
            "the routing is written after the reboot, in the guide's order"
        );
        assert_eq!(
            &bank.registers[..2],
            &[
                (APPLICATION_SERVICER_RESID, VERSION_CMD),
                (APPLICATION_SERVICER_RESID, BLD_MSG_CMD)
            ],
            "the identity is read off the board that is about to go"
        );
        // The three read-backs and the eleven of the state line.
        assert_eq!(bank.registers.len(), 2 + 3 + 11);
    }

    /// A board that never comes back is a startup failure naming what was last
    /// seen, and nothing is written to a chip that is not there.
    #[test]
    fn a_bring_up_whose_board_never_returns_fails_and_names_it() {
        let clock = Clock::new();
        let chip = a_chip(ASR_ROUTE.to_vec(), 1);
        let looks = std::cell::Cell::new(0);
        let why = bring_up(
            &|_board| Ok(chip.clone()),
            &|| {
                looks.set(looks.get() + 1);
                if looks.get() == 1 {
                    Ok(a_board())
                } else {
                    Err("no XVF3800 board on the bus; looked for 38fb:1001".to_string())
                }
            },
            &|| Ok(()),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect_err("it never came back");
        assert!(why.contains("did not come back within 10s"), "{why}");
        assert!(why.contains("38fb:1001"), "{why}");
        assert_eq!(
            chip.0.borrow().writes.len(),
            1,
            "the reboot, and no routing written to a board that is gone"
        );
    }

    /// A firmware that will not take the routing: not fatal, written once, and
    /// the run streams the post-processed channel whatever the configuration
    /// asked for.
    #[test]
    fn a_bring_up_the_chip_refused_runs_on_the_post_processed_channel() {
        let clock = Clock::new();
        let chip = a_chip(vec![8, 0], 0);
        let looks = std::cell::Cell::new(0);
        let (_control, routing) = bring_up(
            &|_board| Ok(chip.clone()),
            &|| {
                looks.set(looks.get() + 1);
                if (2..=3).contains(&looks.get()) {
                    Err("no XVF3800 board on the bus".to_string())
                } else {
                    Ok(a_board())
                }
            },
            &|| Ok(()),
            &|| clock.now(),
            &|d| clock.advance(d),
        )
        .expect("a refusal is not a startup failure");
        assert!(matches!(routing, Routing::Refused(_)), "{routing:?}");
        assert_eq!(routing.channel(ASR_OUTPUT_CHANNEL), POST_PROCESSED_CHANNEL);
        assert_eq!(
            chip.0.borrow().writes.len(),
            3,
            "the reboot and one write each, never asked twice"
        );
    }

    // ── Cadence ───────────────────────────────────────────────────────────────

    #[test]
    fn the_state_line_is_due_on_every_gate_edge_and_never_while_the_gate_is_shut() {
        let clock = Clock::new();
        let mut cadence = StateLineCadence::new();
        assert!(
            !cadence.tick(false, clock.now()),
            "nothing to say when quiet"
        );
        assert!(
            cadence.tick(true, clock.now()),
            "the gate opening is a line"
        );
        clock.advance(Duration::from_secs(1));
        assert!(!cadence.tick(true, clock.now()), "not a line per tick");
        clock.advance(Duration::from_secs(1));
        assert!(
            cadence.tick(false, clock.now()),
            "the gate closing is a line"
        );
        clock.advance(STATE_LINE_INTERVAL * 4);
        assert!(!cadence.tick(false, clock.now()), "silence stays silent");
    }

    #[test]
    fn a_gate_that_stays_open_says_the_line_every_thirty_seconds() {
        let clock = Clock::new();
        let mut cadence = StateLineCadence::new();
        assert!(cadence.tick(true, clock.now()));
        // The whole interval, one poll tick at a time.
        let mut said = 0;
        for _ in 0..(STATE_LINE_INTERVAL.as_millis() as u64 * 2 / 50) {
            clock.advance(Duration::from_millis(50));
            if cadence.tick(true, clock.now()) {
                said += 1;
            }
        }
        assert_eq!(said, 2, "one line per interval the gate stays open");
    }
}
