//! The telemetry/VAD poll core: one XVF3800 control-plane tick, start to finish.
//!
//! Every pod gates its outbound audio on the chip's own SPENERGY reading rather
//! than on a software VAD, and forwards SPENERGY + direction-of-arrival in band
//! while a segment is open. That policy — the poll cadence, the FSM driving, the
//! segment-open bookkeeping, and what happens when the streamer channel is full —
//! is identical on I2C and on USB, so it lives here.
//!
//! The platform supplies four things: a [`TelemetryBus`] (which reading arrives
//! over which transport, and any locking that needs), a monotonic microsecond
//! clock, the capture ring's current write head, and the sleep between ticks.
//! The transport-generic half of a reading — framing, retry policy, status
//! classification, payload decode — is [`read_f32x4_reading`], which any
//! [`ControlTransport`] can drive.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};

use audio_pipeline::vad::{VadSource, VadStateMachine, VadTransition, vad_hangover_ticks_ms};
use audio_pipeline::wire::{Telemetry as WireTelemetry, TelemetryKind};
use xvf3800_ctrl::{
    AEC_AZIMUTH_READ_LEN, AEC_AZIMUTH_VALUES_CMD, AEC_RESID, AEC_SPENERGY_READ_LEN,
    AEC_SPENERGY_VALUES_CMD, ControlTransport, RetryPolicy, STATUS_DONE, control_read,
    decode_f32x4,
};

use crate::segment::StreamerMsg;

// ── Cadence ───────────────────────────────────────────────────────────────────

/// SPENERGY polling rate (Hz). 20 Hz → 50 ms poll interval for the VAD FSM.
pub const VAD_POLL_HZ: u32 = 20;

/// Direction-of-arrival polling rate (Hz). One extra control transaction per 100 ms.
pub const DOA_POLL_HZ: u32 = 10;

/// Sleep between poll ticks, in milliseconds.
pub const VAD_POLL_INTERVAL_MS: u32 = 1000 / VAD_POLL_HZ;

/// SPENERGY ticks between DoA reads.
pub const DOA_EVERY_N_TICKS: u32 = VAD_POLL_HZ / DOA_POLL_HZ;

/// Default VAD gate threshold (dimensionless SPENERGY unit, max over four beams).
/// The value each platform falls back to when its own configuration carries no
/// threshold — NVS on the ESP32, `audio.conf` on the Linux pod.
pub const VAD_THRESHOLD_DEFAULT: f32 = 1.0;

// ── Readings ──────────────────────────────────────────────────────────────────

const _: () = assert!(
    AEC_SPENERGY_READ_LEN == 16 && AEC_AZIMUTH_READ_LEN == 16,
    "the telemetry loop assumes both AEC readings are 4 × f32"
);

/// One of the two four-f32 AEC readings the telemetry loop takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    /// `AEC_SPENERGY_VALUES` — per-beam speech energy, the VAD gate's input.
    SpEnergy,
    /// `AEC_AZIMUTH_VALUES` — per-beam direction of arrival, advisory telemetry.
    Azimuths,
}

impl Reading {
    /// Resource ID to address. Both readings live on the AEC servicer.
    pub const fn resid(self) -> u8 {
        AEC_RESID
    }

    /// Command ID to issue (without the read bit; the transport adds it).
    pub const fn cmd(self) -> u8 {
        match self {
            Reading::SpEnergy => AEC_SPENERGY_VALUES_CMD,
            Reading::Azimuths => AEC_AZIMUTH_VALUES_CMD,
        }
    }

    /// Payload length in bytes, excluding the status byte.
    pub const fn read_len(self) -> usize {
        match self {
            Reading::SpEnergy => AEC_SPENERGY_READ_LEN,
            Reading::Azimuths => AEC_AZIMUTH_READ_LEN,
        }
    }

    /// The reading's stable log token.
    pub const fn label(self) -> &'static str {
        match self {
            Reading::SpEnergy => "SPENERGY",
            Reading::Azimuths => "DoA",
        }
    }

    /// Wrap decoded values in the wire telemetry variant this reading maps to.
    pub const fn kind(self, values: [f32; 4]) -> TelemetryKind {
        match self {
            Reading::SpEnergy => TelemetryKind::SpEnergy { values },
            Reading::Azimuths => TelemetryKind::Azimuths { values },
        }
    }
}

/// Take one four-f32 AEC reading over any control transport.
///
/// Returns `None` — and logs the cause — when the chip answers a status other
/// than [`STATUS_DONE`] or the transport itself fails. A failed reading is
/// skipped, never substituted: the caller leaves the FSM untouched for this tick
/// rather than feeding it a fabricated energy.
pub fn read_f32x4_reading<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    reading: Reading,
) -> Option<[f32; 4]>
where
    T::Error: core::fmt::Debug,
{
    let mut payload = [0u8; 16];
    match control_read(
        transport,
        policy,
        reading.resid(),
        reading.cmd(),
        &mut payload,
    ) {
        Ok((STATUS_DONE, _)) => Some(decode_f32x4(&payload)),
        Ok((status, _)) => {
            log::warn!("telemetry: {} status=0x{:02x}", reading.label(), status);
            None
        }
        Err(e) => {
            log::warn!("telemetry: {} read error: {:?}", reading.label(), e);
            None
        }
    }
}

/// The chip's control plane as the telemetry loop needs it: one reading per call.
///
/// Implementations are a lock (or a handle deref) plus one call to
/// [`read_f32x4_reading`] with the transport's [`RetryPolicy`]. `None` means "no
/// reading this tick" for any reason — transport unavailable, bad status, bus
/// error — and the loop treats all of those the same way.
pub trait TelemetryBus {
    /// Take `reading`, or `None` if it could not be taken.
    fn read(&mut self, reading: Reading) -> Option<[f32; 4]>;
}

// ── Poll core ─────────────────────────────────────────────────────────────────

/// The per-tick platform seams, all borrowed for the life of the loop.
pub struct TelemetryCtx<'a> {
    /// Telemetry → streamer channel. Bounded: audio has priority, so a full
    /// channel drops telemetry rather than blocking the poll.
    pub tx: &'a SyncSender<StreamerMsg>,
    /// Lossless VAD-closed flag, the streamer's backup for a dropped
    /// [`StreamerMsg::VadClosed`]. Cleared on onset, set on release.
    pub vad_closed_flag: &'a AtomicBool,
    /// The capture ring's current write head, read at onset so the streamer can
    /// place the pre-roll cursor.
    pub write_head: &'a dyn Fn() -> u64,
    /// Platform monotonic clock in microseconds — the `device_ts_us` this pod
    /// stamps on telemetry frames.
    pub now_us: &'a dyn Fn() -> u64,
    /// Whether capture is currently quiesced. While it is, the gate is fed
    /// silence so no new onset can fire (an onset would have the loop read a
    /// ring nobody is filling); an already-open gate still releases through the
    /// normal hangover path.
    pub capture_quiesced: &'a dyn Fn() -> bool,
}

/// Energy sample handed to the VAD FSM: the max across the four beams.
struct MaxBeamEnergy(f32);

impl VadSource for MaxBeamEnergy {
    fn energy(&self) -> f32 {
        self.0
    }
}

/// The VAD gate and the bookkeeping around it, driven one tick at a time.
pub struct TelemetryCore {
    vad: VadStateMachine,
    segment_open: bool,
    telemetry_drops: u32,
    tick: u32,
}

impl TelemetryCore {
    /// Build the core for a gate threshold and a hangover in milliseconds.
    ///
    /// The hangover converts to poll ticks at [`VAD_POLL_HZ`], which is the
    /// cadence [`run_telemetry_loop`] runs at — the FSM itself has no clock.
    pub fn new(threshold: f32, hangover_ms: u32) -> Self {
        Self {
            vad: VadStateMachine::new(threshold, vad_hangover_ticks_ms(hangover_ms, VAD_POLL_HZ)),
            segment_open: false,
            telemetry_drops: 0,
            tick: 0,
        }
    }

    /// Whether a segment is open — i.e. whether telemetry is being forwarded.
    pub fn segment_open(&self) -> bool {
        self.segment_open
    }

    /// Messages dropped on a full channel since this core was built.
    pub fn telemetry_drops(&self) -> u32 {
        self.telemetry_drops
    }

    /// Run one poll tick: SPENERGY, the FSM, in-band telemetry, and DoA on its
    /// own cadence.
    ///
    /// Returns the FSM transition, or `None` when the SPENERGY reading failed —
    /// in which case the FSM was not updated at all (a missing reading must not
    /// count as silence and time out an open gate). The DoA read still happens
    /// on its tick regardless, so a one-off SPENERGY failure does not shift the
    /// DoA cadence.
    pub fn poll_tick<B: TelemetryBus>(
        &mut self,
        bus: &mut B,
        ctx: &TelemetryCtx<'_>,
    ) -> Option<VadTransition> {
        let sp_values = bus.read(Reading::SpEnergy);
        let now_us = (ctx.now_us)();

        let transition = sp_values.map(|sp| {
            let max_energy = if (ctx.capture_quiesced)() {
                // NEG_INFINITY sits below every representable threshold under
                // either FSM comparison.
                f32::NEG_INFINITY
            } else {
                sp.iter().copied().fold(f32::NEG_INFINITY, f32::max)
            };
            let transition = self.vad.update(&MaxBeamEnergy(max_energy));
            match transition {
                VadTransition::Opened => self.note_vad_opened(ctx, max_energy),
                VadTransition::Closed => self.note_vad_closed(ctx),
                VadTransition::Unchanged => {}
            }

            if self.segment_open {
                self.forward(ctx, Reading::SpEnergy, sp, now_us);
            }
            transition
        });

        if self.tick.is_multiple_of(DOA_EVERY_N_TICKS) {
            // Read on cadence whether or not a segment is open, so the transaction
            // rate the chip sees does not depend on gate state.
            let az_values = bus.read(Reading::Azimuths);
            if self.segment_open
                && let Some(az) = az_values
            {
                let doa_now_us = (ctx.now_us)();
                self.forward(ctx, Reading::Azimuths, az, doa_now_us);
            }
        }

        self.tick = self.tick.wrapping_add(1);
        transition
    }

    /// Onset: publish the write head, clear the closed flag, open the segment.
    ///
    /// A dropped `VadOpened` re-closes the segment: forwarding telemetry for a
    /// segment the streamer never opened wastes channel slots and could cascade
    /// into dropping the `VadClosed` that ends it.
    fn note_vad_opened(&mut self, ctx: &TelemetryCtx<'_>, max_energy: f32) {
        let write_head = (ctx.write_head)();
        log::info!(
            "telemetry: VAD opened (write_head={} energy={:.3})",
            write_head,
            max_energy
        );
        // Clear before sending so the streamer sees a fresh flag.
        ctx.vad_closed_flag.store(false, Ordering::Release);
        self.segment_open = true;
        if ctx
            .tx
            .try_send(StreamerMsg::VadOpened { write_head })
            .is_err()
        {
            self.telemetry_drops = self.telemetry_drops.saturating_add(1);
            log::warn!(
                "telemetry: VadOpened dropped — streamer channel full; \
                 utterance will be lost (drops so far this boot: {})",
                self.telemetry_drops
            );
            self.segment_open = false;
        }
    }

    /// Release: set the closed flag unconditionally, then try the message. The
    /// flag is the streamer's lossless path when the message is dropped.
    fn note_vad_closed(&mut self, ctx: &TelemetryCtx<'_>) {
        log::info!("telemetry: VAD closed (drops={})", self.telemetry_drops);
        self.segment_open = false;
        ctx.vad_closed_flag.store(true, Ordering::Release);
        if ctx.tx.try_send(StreamerMsg::VadClosed).is_err() {
            self.telemetry_drops = self.telemetry_drops.saturating_add(1);
            log::warn!(
                "telemetry: VadClosed channel message dropped — \
                 streamer will detect close via VAD_CLOSED_FLAG"
            );
        }
    }

    /// Offer one telemetry frame to the streamer. A full channel is counted and
    /// tolerated silently — at 30 offers per second a log line per drop would
    /// itself become the problem.
    ///
    /// A *disconnected* channel is the opposite trade: it is logged and not
    /// counted, because it is not a droppable message but a dead receiver, and it
    /// cannot repeat for long — no further segment can open on a dead channel
    /// (a failed `VadOpened` re-closes it), so the offers stop with this
    /// segment's hangover.
    fn forward(
        &mut self,
        ctx: &TelemetryCtx<'_>,
        reading: Reading,
        values: [f32; 4],
        device_ts_us: u64,
    ) {
        let tel = WireTelemetry {
            device_ts_us,
            kind: reading.kind(values),
        };
        match ctx.tx.try_send(StreamerMsg::Telemetry(tel)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.telemetry_drops = self.telemetry_drops.saturating_add(1);
            }
            Err(TrySendError::Disconnected(_)) => {
                log::warn!(
                    "telemetry: streamer channel disconnected ({})",
                    reading.label()
                );
            }
        }
    }
}

/// Poll forever at [`VAD_POLL_HZ`]. Never returns.
///
/// A disconnected streamer channel is logged, not fatal: the pod keeps polling so
/// a restarted streamer thread finds a live gate rather than a dead one.
pub fn run_telemetry_loop<B: TelemetryBus>(
    mut core: TelemetryCore,
    bus: &mut B,
    ctx: &TelemetryCtx<'_>,
    sleep_ms: &dyn Fn(u32),
) -> ! {
    loop {
        core.poll_tick(bus, ctx);
        sleep_ms(VAD_POLL_INTERVAL_MS);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, sync_channel};

    // ── Fakes ─────────────────────────────────────────────────────────────────

    /// Transport error the scripted transport returns; stands in for `EspError` /
    /// `rusb::Error`.
    #[derive(Debug, PartialEq, Eq)]
    struct FakeError;

    #[derive(Debug, PartialEq, Eq)]
    struct Read {
        resid: u8,
        cmd: u8,
        len: usize,
    }

    /// Replays a scripted status/payload sequence, recording the framing it saw.
    struct Scripted {
        /// `Some(status)` per attempt; `None` returns a transport error.
        answers: Vec<Option<u8>>,
        /// Payload bytes the device "returns" on every attempt.
        payload: [u8; 16],
        reads: Vec<Read>,
        delays: Vec<u32>,
    }

    impl Scripted {
        fn new(answers: Vec<Option<u8>>, values: [f32; 4]) -> Self {
            let mut payload = [0u8; 16];
            for (i, v) in values.iter().enumerate() {
                payload[i * 4..i * 4 + 4].copy_from_slice(&v.to_le_bytes());
            }
            Self {
                answers,
                payload,
                reads: Vec::new(),
                delays: Vec::new(),
            }
        }
    }

    impl ControlTransport for Scripted {
        type Error = FakeError;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            self.reads.push(Read {
                resid,
                cmd,
                len: payload.len(),
            });
            payload.copy_from_slice(&self.payload[..payload.len()]);
            match self.answers.remove(0) {
                Some(status) => Ok(status),
                None => Err(FakeError),
            }
        }

        fn control_write_once(
            &mut self,
            _resid: u8,
            _cmd: u8,
            _payload: &[u8],
            _attempt: u32,
        ) -> Result<u8, Self::Error> {
            unreachable!("the telemetry loop never writes")
        }

        fn delay_ms(&mut self, ms: u32) {
            self.delays.push(ms);
        }
    }

    /// A bus that answers from per-reading scripts and records what was asked.
    struct FakeBus {
        sp: Vec<Option<[f32; 4]>>,
        az: Vec<Option<[f32; 4]>>,
        reads: Vec<Reading>,
    }

    impl FakeBus {
        /// Answers every SPENERGY read with `sp` and every DoA read with `az`.
        fn constant(sp: Option<[f32; 4]>, az: Option<[f32; 4]>) -> Self {
            Self {
                sp: vec![sp],
                az: vec![az],
                reads: Vec::new(),
            }
        }

        /// Answers SPENERGY reads from `sp` in order, holding the last answer once
        /// the script runs out; DoA always answers `az`.
        fn scripted(sp: Vec<Option<[f32; 4]>>, az: Option<[f32; 4]>) -> Self {
            Self {
                sp,
                az: vec![az],
                reads: Vec::new(),
            }
        }

        fn sp_reads(&self) -> usize {
            self.reads
                .iter()
                .filter(|r| **r == Reading::SpEnergy)
                .count()
        }

        fn az_reads(&self) -> usize {
            self.reads
                .iter()
                .filter(|r| **r == Reading::Azimuths)
                .count()
        }
    }

    impl TelemetryBus for FakeBus {
        fn read(&mut self, reading: Reading) -> Option<[f32; 4]> {
            self.reads.push(reading);
            let script = match reading {
                Reading::SpEnergy => &mut self.sp,
                Reading::Azimuths => &mut self.az,
            };
            if script.len() > 1 {
                script.remove(0)
            } else {
                script[0]
            }
        }
    }

    /// Fixed threshold and a two-tick hangover, so a close is cheap to drive.
    const THRESHOLD: f32 = 1.0;
    const HANGOVER_MS: u32 = 100; // 2 ticks at 20 Hz
    const LOUD: [f32; 4] = [0.0, 5.0, 0.0, 0.0];
    const QUIET: [f32; 4] = [0.0, 0.0, 0.0, 0.0];
    const AZ: [f32; 4] = [1.5, 2.5, 3.5, 4.5];

    /// The seams, plus the state the assertions read back.
    struct Harness {
        rx: Option<Receiver<StreamerMsg>>,
        tx: SyncSender<StreamerMsg>,
        flag: AtomicBool,
        write_head: u64,
        now_us: u64,
        quiesced: bool,
    }

    impl Harness {
        fn with_capacity(cap: usize) -> Self {
            let (tx, rx) = sync_channel(cap);
            Self {
                rx: Some(rx),
                tx,
                flag: AtomicBool::new(false),
                write_head: 4_242,
                now_us: 7_000_000,
                quiesced: false,
            }
        }

        fn new() -> Self {
            Self::with_capacity(16)
        }

        fn drain(&self) -> Vec<StreamerMsg> {
            match self.rx.as_ref() {
                Some(rx) => rx.try_iter().collect(),
                None => Vec::new(),
            }
        }
    }

    /// Borrow a [`Harness`] as a [`TelemetryCtx`].
    ///
    /// A macro rather than a method: the seams are `&dyn Fn`, and only a `let`
    /// initializer extends the borrowed closures' temporaries far enough to bind.
    macro_rules! harness_ctx {
        ($h:expr) => {
            TelemetryCtx {
                tx: &$h.tx,
                vad_closed_flag: &$h.flag,
                write_head: &|| $h.write_head,
                now_us: &|| $h.now_us,
                capture_quiesced: &|| $h.quiesced,
            }
        };
    }

    fn opened_heads(msgs: &[StreamerMsg]) -> Vec<u64> {
        msgs.iter()
            .filter_map(|m| match m {
                StreamerMsg::VadOpened { write_head } => Some(*write_head),
                _ => None,
            })
            .collect()
    }

    fn closes(msgs: &[StreamerMsg]) -> usize {
        msgs.iter()
            .filter(|m| matches!(m, StreamerMsg::VadClosed))
            .count()
    }

    fn telemetry(msgs: &[StreamerMsg]) -> Vec<&WireTelemetry> {
        msgs.iter()
            .filter_map(|m| match m {
                StreamerMsg::Telemetry(t) => Some(t),
                _ => None,
            })
            .collect()
    }

    // ── The transport-generic read ────────────────────────────────────────────

    /// A DONE answer decodes four little-endian f32s, and the framing the
    /// transport saw is the reading's resid/cmd/length.
    #[test]
    fn reading_decodes_done_payload_with_expected_framing() {
        for (reading, cmd) in [
            (Reading::SpEnergy, AEC_SPENERGY_VALUES_CMD),
            (Reading::Azimuths, AEC_AZIMUTH_VALUES_CMD),
        ] {
            let mut t = Scripted::new(vec![Some(STATUS_DONE)], AZ);
            let got = read_f32x4_reading(&mut t, xvf3800_ctrl::I2C_RETRY, reading);
            assert_eq!(got, Some(AZ), "{} payload", reading.label());
            assert_eq!(
                t.reads,
                vec![Read {
                    resid: AEC_RESID,
                    cmd,
                    len: 16,
                }],
                "{} framing",
                reading.label()
            );
            assert!(t.delays.is_empty(), "a DONE answer must not sleep");
        }
    }

    /// A fatal status and a transport error both yield "no reading", and neither
    /// is retried past the policy budget.
    #[test]
    fn reading_reports_none_on_bad_status_and_on_transport_error() {
        let mut bad = Scripted::new(vec![Some(0x02)], AZ);
        assert_eq!(
            read_f32x4_reading(&mut bad, xvf3800_ctrl::I2C_RETRY, Reading::SpEnergy),
            None
        );
        assert_eq!(bad.reads.len(), 1, "a fatal status must not be retried");

        let mut broken = Scripted::new(vec![None], AZ);
        assert_eq!(
            read_f32x4_reading(&mut broken, xvf3800_ctrl::I2C_RETRY, Reading::Azimuths),
            None
        );
        assert_eq!(broken.reads.len(), 1);
    }

    /// The policy passed in is the policy applied: a WAIT answer is re-issued
    /// after that policy's delay, and the eventual DONE payload is returned.
    #[test]
    fn reading_applies_the_supplied_retry_policy() {
        let mut t = Scripted::new(vec![Some(xvf3800_ctrl::STATUS_WAIT), Some(STATUS_DONE)], AZ);
        assert_eq!(
            read_f32x4_reading(&mut t, xvf3800_ctrl::USB_RETRY, Reading::SpEnergy),
            Some(AZ)
        );
        assert_eq!(t.reads.len(), 2);
        assert_eq!(
            t.delays,
            vec![xvf3800_ctrl::USB_RETRY.delay_ms],
            "the delay must come from the policy the caller supplied"
        );
    }

    // ── Cadence constants ─────────────────────────────────────────────────────

    /// The cadence arithmetic the loop and the FSM both depend on.
    #[test]
    fn cadence_constants_are_consistent() {
        assert_eq!(VAD_POLL_INTERVAL_MS, 50);
        assert_eq!(DOA_EVERY_N_TICKS, 2);
        assert_eq!(
            vad_hangover_ticks_ms(audio_pipeline::vad::VAD_HANGOVER_MS, VAD_POLL_HZ),
            16
        );
        assert_eq!(Reading::SpEnergy.label(), "SPENERGY");
        assert_eq!(Reading::Azimuths.label(), "DoA");
        assert_eq!(Reading::SpEnergy.read_len(), 16);
        assert_eq!(Reading::Azimuths.read_len(), 16);
    }

    // ── The poll core ─────────────────────────────────────────────────────────

    /// Two above-threshold ticks open the gate: the write head goes out with the
    /// message and the closed flag is cleared.
    #[test]
    fn onset_publishes_write_head_and_clears_the_flag() {
        let h = Harness::new();
        h.flag.store(true, Ordering::Release);
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged)
        );
        assert!(!core.segment_open(), "one loud tick must not open the gate");
        assert_eq!(core.poll_tick(&mut bus, &ctx), Some(VadTransition::Opened));
        assert!(core.segment_open());

        let msgs = h.drain();
        assert_eq!(opened_heads(&msgs), vec![h.write_head]);
        assert!(
            !h.flag.load(Ordering::Acquire),
            "onset must clear the closed flag"
        );
    }

    /// Telemetry is forwarded only while a segment is open, stamped with the
    /// injected clock, and the opening tick's own SPENERGY goes out too.
    #[test]
    fn telemetry_flows_only_while_the_segment_is_open() {
        let h = Harness::new();
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx); // closed: no telemetry
        assert!(telemetry(&h.drain()).is_empty());

        core.poll_tick(&mut bus, &ctx); // opens, and forwards its own reading
        let msgs = h.drain();
        let tel = telemetry(&msgs);
        assert_eq!(tel.len(), 1, "the opening tick forwards its SPENERGY");
        assert_eq!(tel[0].device_ts_us, h.now_us);
        assert_eq!(tel[0].kind, TelemetryKind::SpEnergy { values: LOUD });
    }

    /// The gate closes after the configured hangover, sets the flag, sends
    /// `VadClosed`, and stops forwarding.
    #[test]
    fn release_sets_the_flag_and_stops_telemetry() {
        let h = Harness::new();
        let mut bus = FakeBus::scripted(
            vec![
                Some(LOUD),
                Some(LOUD),
                Some(QUIET),
                Some(QUIET),
                Some(QUIET),
            ],
            Some(AZ),
        );
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        core.poll_tick(&mut bus, &ctx); // open
        let _ = h.drain();

        // 100 ms hangover at 20 Hz = 2 below-threshold ticks.
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged)
        );
        assert!(core.segment_open(), "the hangover must hold the gate open");
        assert_eq!(core.poll_tick(&mut bus, &ctx), Some(VadTransition::Closed));
        assert!(!core.segment_open());
        assert!(h.flag.load(Ordering::Acquire), "release must set the flag");

        let msgs = h.drain();
        assert_eq!(closes(&msgs), 1);
        let kinds: Vec<_> = telemetry(&msgs).iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                TelemetryKind::SpEnergy { values: QUIET },
                TelemetryKind::Azimuths { values: AZ }
            ],
            "the still-open hangover tick forwards; the closing tick does not"
        );

        core.poll_tick(&mut bus, &ctx);
        assert!(
            telemetry(&h.drain()).is_empty(),
            "a closed gate forwards nothing"
        );
    }

    /// An undeliverable `VadClosed` is charged to the drop counter, so the
    /// `drops=` diagnostic does not hide the one case where the close itself
    /// bounced. The flag still ends the segment losslessly.
    #[test]
    fn a_dropped_close_is_counted() {
        let h = Harness::with_capacity(1);
        let mut bus = FakeBus::scripted(
            vec![
                Some(LOUD),
                Some(LOUD),
                Some(QUIET),
                Some(QUIET),
                Some(QUIET),
            ],
            Some(AZ),
        );
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        core.poll_tick(&mut bus, &ctx); // open
        assert!(core.segment_open());
        let _ = h.drain();

        // Hangover tick: still open, so its own telemetry fills the one slot.
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged)
        );
        let before = core.telemetry_drops();

        // Closing tick: the channel is full, so VadClosed cannot be delivered.
        assert_eq!(core.poll_tick(&mut bus, &ctx), Some(VadTransition::Closed));
        assert_eq!(
            core.telemetry_drops(),
            before + 1,
            "a bounced VadClosed must be counted"
        );
        assert!(
            h.flag.load(Ordering::Acquire),
            "the flag is the lossless close path"
        );
        assert_eq!(
            closes(&h.drain()),
            0,
            "the close message really was dropped"
        );
    }

    /// A failed SPENERGY read skips the tick without touching the FSM: the two
    /// loud ticks either side of it still add up to an onset.
    #[test]
    fn a_failed_reading_leaves_the_fsm_untouched() {
        let h = Harness::new();
        let mut bus = FakeBus::scripted(vec![Some(LOUD), None, Some(LOUD)], Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged)
        );
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            None,
            "a failed reading reports no transition"
        );
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Opened),
            "the consecutive-above count must survive a skipped tick"
        );
        assert_eq!(opened_heads(&h.drain()), vec![h.write_head]);
    }

    /// DoA is read every second tick regardless of gate state, and only forwarded
    /// while the gate is open — with its own clock reading.
    #[test]
    fn doa_reads_on_cadence_and_forwards_only_when_open() {
        let h = Harness::new();
        let mut bus = FakeBus::constant(Some(QUIET), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        for _ in 0..4 {
            core.poll_tick(&mut bus, &ctx);
        }
        assert_eq!(bus.sp_reads(), 4);
        assert_eq!(bus.az_reads(), 2, "DoA is read on ticks 0 and 2");
        assert!(
            telemetry(&h.drain()).is_empty(),
            "a closed gate forwards no DoA"
        );

        // Open the gate, then run one more DoA tick.
        let mut loud = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        core.poll_tick(&mut loud, &ctx);
        core.poll_tick(&mut loud, &ctx); // open (tick 1: no DoA)
        let _ = h.drain();
        core.poll_tick(&mut loud, &ctx); // tick 2: DoA read + forwarded
        let msgs = h.drain();
        let kinds: Vec<_> = telemetry(&msgs).iter().map(|t| t.kind).collect();
        assert_eq!(
            kinds,
            vec![
                TelemetryKind::SpEnergy { values: LOUD },
                TelemetryKind::Azimuths { values: AZ }
            ],
            "SPENERGY then DoA, in poll order"
        );
        assert!(telemetry(&msgs).iter().all(|t| t.device_ts_us == h.now_us));
    }

    /// A DoA read that fails is simply not forwarded; the tick is otherwise normal.
    #[test]
    fn a_failed_doa_reading_forwards_nothing() {
        let h = Harness::new();
        let mut bus = FakeBus::constant(Some(LOUD), None);
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        core.poll_tick(&mut bus, &ctx); // open
        let _ = h.drain();
        core.poll_tick(&mut bus, &ctx); // DoA tick, read fails

        let msgs = h.drain();
        assert_eq!(bus.az_reads(), 2);
        assert!(
            telemetry(&msgs)
                .iter()
                .all(|t| matches!(t.kind, TelemetryKind::SpEnergy { .. })),
            "no Azimuths frame may be synthesized from a failed read"
        );
    }

    /// A `VadOpened` that cannot be delivered re-closes the segment, so no
    /// telemetry is forwarded for a segment the streamer never opened.
    #[test]
    fn a_dropped_onset_reopens_nothing() {
        // Capacity 1, pre-filled: the next try_send fails.
        let h = Harness::with_capacity(1);
        h.tx.try_send(StreamerMsg::VadClosed).unwrap();
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        assert_eq!(core.poll_tick(&mut bus, &ctx), Some(VadTransition::Opened));
        assert!(
            !core.segment_open(),
            "a dropped VadOpened must not leave a phantom segment open"
        );
        assert_eq!(core.telemetry_drops(), 1);
        assert!(
            !h.flag.load(Ordering::Acquire),
            "the flag is still cleared — the FSM really did open"
        );

        let msgs = h.drain();
        assert!(opened_heads(&msgs).is_empty());
        assert!(telemetry(&msgs).is_empty());
    }

    /// Telemetry that does not fit is counted and dropped, and the segment stays
    /// open — audio has priority over its own telemetry.
    #[test]
    fn full_channel_drops_telemetry_and_keeps_the_segment() {
        let h = Harness::with_capacity(1);
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        core.poll_tick(&mut bus, &ctx); // open: VadOpened takes the one slot
        assert!(core.segment_open());
        assert_eq!(
            core.telemetry_drops(),
            1,
            "the opening tick's own SPENERGY had nowhere to go"
        );

        // Channel still full (VadOpened is unread): further ticks keep dropping.
        core.poll_tick(&mut bus, &ctx);
        assert!(core.segment_open());
        assert!(core.telemetry_drops() >= 2);
        assert_eq!(opened_heads(&h.drain()), vec![h.write_head]);
    }

    /// A disconnected channel is survivable: the gate keeps ticking so a restarted
    /// streamer finds a live FSM. An undeliverable `VadOpened` re-closes the
    /// segment exactly as a full channel does.
    #[test]
    fn a_disconnected_channel_does_not_stop_the_gate() {
        let h = Harness::new();
        drop(h.rx);
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let ctx = harness_ctx!(h);

        core.poll_tick(&mut bus, &ctx);
        assert_eq!(core.poll_tick(&mut bus, &ctx), Some(VadTransition::Opened));
        assert!(!core.segment_open());
        assert_eq!(core.telemetry_drops(), 1);
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged),
            "the FSM keeps ticking with no receiver"
        );
        assert_eq!(
            core.telemetry_drops(),
            1,
            "a closed gate offers nothing, so nothing more is charged"
        );
    }

    /// A receiver that goes away *after* the segment opened is the one path that
    /// reaches `forward`'s disconnected arm: it logs, leaves the gate alone, and is
    /// not charged as a drop (the message was never deliverable, and the offers stop
    /// with this segment).
    #[test]
    fn a_receiver_lost_mid_segment_is_logged_without_charging_a_drop() {
        let mut h = Harness::new();
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        {
            let ctx = harness_ctx!(h);
            core.poll_tick(&mut bus, &ctx);
            core.poll_tick(&mut bus, &ctx); // open, delivered
            assert!(core.segment_open());
        }
        let before = core.telemetry_drops();

        // The streamer thread is gone; the sender survives it.
        h.rx = None;
        let ctx = harness_ctx!(h);
        core.poll_tick(&mut bus, &ctx);

        assert!(
            core.segment_open(),
            "an undeliverable telemetry frame does not close the segment"
        );
        assert_eq!(
            core.telemetry_drops(),
            before,
            "a disconnected receiver is not charged to the drop counter"
        );
    }

    // ── The shipped loop ──────────────────────────────────────────────────────

    /// The FSM counts hangover in ticks, not time, so the configured hangover is only
    /// the intended wall-clock duration if the loop sleeps exactly one poll interval
    /// per tick. Drives the shipped `run_telemetry_loop` — which never returns — and
    /// stops it by panicking out of the sleep seam.
    #[test]
    fn the_loop_sleeps_one_poll_interval_between_ticks() {
        const TICKS: usize = 5;
        const STOP: &str = "telemetry-loop test: stop after the scripted ticks";

        let h = Harness::new();
        let mut bus = FakeBus::constant(Some(QUIET), Some(AZ));
        let core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        let slept = std::cell::RefCell::new(Vec::new());
        let sleep_ms = |ms: u32| {
            let mut slept = slept.borrow_mut();
            slept.push(ms);
            assert!(slept.len() < TICKS, "{STOP}");
        };
        let ctx = harness_ctx!(h);

        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_telemetry_loop(core, &mut bus, &ctx, &sleep_ms);
        }));

        let payload = stopped.expect_err("the loop only ends by panicking out of the sleep");
        assert!(
            payload
                .downcast_ref::<String>()
                .is_some_and(|m| m.contains(STOP)),
            "the loop must end on the scripted stop, not another panic"
        );
        assert_eq!(
            slept.into_inner(),
            vec![VAD_POLL_INTERVAL_MS; TICKS],
            "every inter-tick sleep is one poll interval"
        );
        assert_eq!(
            bus.sp_reads(),
            TICKS,
            "one SPENERGY reading per tick, and the sleep follows each"
        );
    }

    /// While capture is quiesced the gate sees silence, so no onset can fire —
    /// but an already-open gate still releases through the normal hangover.
    #[test]
    fn quiesced_capture_suppresses_onset_but_not_release() {
        let mut h = Harness::new();
        h.quiesced = true;
        let mut bus = FakeBus::constant(Some(LOUD), Some(AZ));
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        {
            let ctx = harness_ctx!(h);
            for _ in 0..8 {
                assert_eq!(
                    core.poll_tick(&mut bus, &ctx),
                    Some(VadTransition::Unchanged)
                );
            }
            assert!(!core.segment_open());
            assert!(h.drain().is_empty(), "a quiesced gate emits nothing");
        }

        // Open the gate with capture live, then quiesce mid-segment.
        let mut core = TelemetryCore::new(THRESHOLD, HANGOVER_MS);
        h.quiesced = false;
        {
            let ctx = harness_ctx!(h);
            core.poll_tick(&mut bus, &ctx);
            core.poll_tick(&mut bus, &ctx);
            assert!(core.segment_open());
        }
        h.quiesced = true;
        let ctx = harness_ctx!(h);
        let _ = h.drain();
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Unchanged)
        );
        assert_eq!(
            core.poll_tick(&mut bus, &ctx),
            Some(VadTransition::Closed),
            "a quiesced open gate must still release through the hangover"
        );
        assert_eq!(closes(&h.drain()), 1);
    }
}
