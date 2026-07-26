//! Protocol RX accumulation with stale-partial-frame recovery.
//!
//! Wraps the COBS accumulator that reassembles host→device `Request` frames.
//! A frame truncated before its COBS zero delimiter would otherwise sit in the
//! accumulator indefinitely and be concatenated onto the *next* frame. The
//! innocent frame is then lost — either to a `DeserError` or, worse, to a
//! silent mis-decode that dispatches a command the host never sent (see
//! `poison_without_reset_costs_next_frame`). Discarding a partial frame after a
//! stretch of RX silence caps any corruption event at one lost frame.
//!
//! Pure logic — no esp-idf types — so it is unit-tested on the host triple.

// Host view: these items exist for the tests and for the device-gated call site.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use device_protocol::Request;
use postcard::accumulator::{CobsAccumulator, FeedResult};

/// RX silence, in ms, after which a buffered partial frame is discarded.
///
/// The host writes a frame in one syscall and intra-frame gaps are sub-ms over
/// USB-CDC, so 500 ms is ~10^3x margin. It also heals well inside the host's 5 s
/// Identify re-send interval.
pub const STALE_PARTIAL_IDLE_MS: u64 = 500;

/// Reassembles COBS-framed `Request`s and self-heals from truncated frames.
pub struct ProtocolRx {
    acc: CobsAccumulator<512>,
    /// `Some(t)` = a partial frame is buffered; `t` = the last RX activity (ms).
    partial_last_rx_ms: Option<u64>,
    deser_error_count: u32,
    over_full_count: u32,
    stale_reset_count: u32,
}

impl ProtocolRx {
    pub fn new() -> Self {
        Self {
            acc: CobsAccumulator::new(),
            partial_last_rx_ms: None,
            deser_error_count: 0,
            over_full_count: 0,
            stale_reset_count: 0,
        }
    }

    /// Feed a received chunk, invoking `on_request` once per decoded frame.
    ///
    /// `now_ms` is a monotonic millisecond clock; it only has to be consistent
    /// with the value passed to [`poll_idle`](Self::poll_idle).
    pub fn feed(&mut self, now_ms: u64, chunk: &[u8], on_request: &mut dyn FnMut(Request)) {
        if chunk.is_empty() {
            return;
        }
        let mut chunk = chunk;
        // A partial frame is buffered iff the last feed of this chunk returned
        // `Consumed` — every other result ends at a delimiter (or resets) and
        // leaves the accumulator empty.
        let mut partial_pending = false;
        loop {
            match self.acc.feed::<Request>(chunk) {
                FeedResult::Success { data, remaining } => {
                    chunk = remaining;
                    on_request(data);
                }
                FeedResult::Consumed => {
                    partial_pending = true;
                    break;
                }
                FeedResult::OverFull(r) => {
                    self.acc = CobsAccumulator::new();
                    // Rate-limit error logs (every 64th) to avoid livelock under
                    // corrupt-byte storms.
                    if self.over_full_count.is_multiple_of(64) {
                        log::warn!(
                            target: "protocol",
                            "OverFull: accumulator reset (count={})",
                            self.over_full_count
                        );
                    }
                    self.over_full_count = self.over_full_count.saturating_add(1);
                    chunk = r;
                }
                FeedResult::DeserError(r) => {
                    if self.deser_error_count.is_multiple_of(64) {
                        log::warn!(
                            target: "protocol",
                            "COBS DeserError: corrupt or unknown-discriminant frame skipped (count={})",
                            self.deser_error_count
                        );
                    }
                    self.deser_error_count = self.deser_error_count.saturating_add(1);
                    chunk = r;
                }
            }
            if chunk.is_empty() {
                break;
            }
        }
        // Any RX activity restarts the idle clock, so only genuine silence with a
        // partial buffered can trigger a reset.
        self.partial_last_rx_ms = partial_pending.then_some(now_ms);
    }

    /// Call when a read yields no bytes. Discards a buffered partial frame that
    /// has seen no RX activity for [`STALE_PARTIAL_IDLE_MS`].
    pub fn poll_idle(&mut self, now_ms: u64) {
        let Some(last_rx_ms) = self.partial_last_rx_ms else {
            return;
        };
        let idle_ms = now_ms.saturating_sub(last_rx_ms);
        if idle_ms < STALE_PARTIAL_IDLE_MS {
            return;
        }
        self.acc = CobsAccumulator::new();
        self.partial_last_rx_ms = None;
        // Rare by construction (a mid-frame stall of >500 ms on USB-CDC), so every
        // occurrence is logged.
        log::warn!(
            target: "protocol",
            "stale partial frame discarded after {}ms idle (count={})",
            idle_ms,
            self.stale_reset_count
        );
        self.stale_reset_count = self.stale_reset_count.saturating_add(1);
    }

    /// Frames dropped as corrupt or unknown-discriminant since construction.
    #[cfg(test)]
    pub fn deser_error_count(&self) -> u32 {
        self.deser_error_count
    }

    /// Partial frames discarded by the idle guard since construction.
    #[cfg(test)]
    pub fn stale_reset_count(&self) -> u32 {
        self.stale_reset_count
    }

    /// Accumulator resets forced by a frame too long to buffer, since
    /// construction.
    #[cfg(test)]
    pub fn over_full_count(&self) -> u32 {
        self.over_full_count
    }
}

impl Default for ProtocolRx {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use device_protocol::{Command, TestName};

    /// COBS-encode a `Request` the way the host transport does.
    fn frame(id: u32) -> Vec<u8> {
        let req = Request {
            id,
            command: Command::RunTest(TestName::Ping),
        };
        let mut buf = [0u8; 512];
        let len = device_protocol::framing::encode_request(&req, &mut buf).expect("encode");
        buf[..len].to_vec()
    }

    /// Feed a chunk, collecting every request dispatched.
    fn feed_reqs(rx: &mut ProtocolRx, now_ms: u64, chunk: &[u8]) -> Vec<Request> {
        let mut reqs = Vec::new();
        rx.feed(now_ms, chunk, &mut |req| reqs.push(req));
        reqs
    }

    /// Feed a chunk, collecting the ids of every request dispatched.
    fn feed(rx: &mut ProtocolRx, now_ms: u64, chunk: &[u8]) -> Vec<u32> {
        feed_reqs(rx, now_ms, chunk)
            .into_iter()
            .map(|req| req.id)
            .collect()
    }

    /// A frame truncated before its delimiter is discarded after the idle
    /// threshold, so the next frame decodes on its own.
    #[test]
    fn stale_partial_discarded_after_idle() {
        let mut rx = ProtocolRx::new();
        let f = frame(7);
        assert_eq!(feed(&mut rx, 0, &f[..f.len() - 2]), Vec::<u32>::new());
        rx.poll_idle(STALE_PARTIAL_IDLE_MS);
        assert_eq!(rx.stale_reset_count(), 1);
        assert_eq!(feed(&mut rx, 600, &frame(8)), vec![8]);
        assert_eq!(
            rx.deser_error_count(),
            0,
            "the healed frame must not have been consumed by a resync"
        );
    }

    /// Negative control: without the idle reset, a buffered partial costs the
    /// next frame — the cost the guard exists to cap.
    ///
    /// The concatenation need not even fail to decode. Here `[02,07,01]`
    /// (frame 7 minus its last data byte and delimiter) followed by frame 8
    /// COBS-decodes into a well-formed request carrying the *stale* id, and
    /// postcard ignores the trailing bytes — so frame 8 vanishes, the stale
    /// request is executed instead, and no DeserError is raised to show it.
    /// A different truncation point yields a DeserError instead; either way the
    /// next frame is the one that pays.
    #[test]
    fn poison_without_reset_costs_next_frame() {
        let mut rx = ProtocolRx::new();
        let f = frame(7);
        feed(&mut rx, 0, &f[..f.len() - 2]);
        rx.poll_idle(STALE_PARTIAL_IDLE_MS - 1);
        assert_eq!(rx.stale_reset_count(), 0, "below threshold: no reset");

        let dispatched = feed_reqs(&mut rx, 499, &frame(8));
        assert_eq!(
            dispatched.len(),
            1,
            "one request dispatched, and not frame 8"
        );
        assert_eq!(
            dispatched[0].id, 7,
            "the stale partial's id survives; frame 8's is lost"
        );
        assert_eq!(rx.deser_error_count(), 0, "the mis-decode is silent");
        assert_eq!(
            feed(&mut rx, 500, &frame(9)),
            vec![9],
            "accumulator healed at frame 8's delimiter"
        );
    }

    /// Delimiter-terminated garbage is a complete (bad) frame, not a partial:
    /// it is counted as a DeserError and leaves nothing for the guard to discard.
    #[test]
    fn complete_corrupt_frame_is_not_a_partial() {
        let mut rx = ProtocolRx::new();
        assert_eq!(feed(&mut rx, 0, &[0x03, 0xff, 0x7f, 0x11, 0x00]), vec![]);
        assert_eq!(rx.deser_error_count(), 1);
        rx.poll_idle(1_000_000);
        assert_eq!(rx.stale_reset_count(), 0);
    }

    /// Idling with nothing buffered does nothing, however long the silence.
    #[test]
    fn idle_with_empty_accumulator_is_noop() {
        let mut rx = ProtocolRx::new();
        rx.poll_idle(1_000_000);
        assert_eq!(rx.stale_reset_count(), 0);
        assert_eq!(feed(&mut rx, 1_000_000, &frame(1)), vec![1]);
    }

    /// A chunk carrying several complete frames dispatches every one of them.
    ///
    /// Routine, not exotic: the host may have several requests queued in the
    /// driver's RX ring before the protocol loop's first read.
    #[test]
    fn multi_frame_chunk_dispatches_every_frame() {
        let mut rx = ProtocolRx::new();
        let chunk = [frame(1), frame(2), frame(3)].concat();
        assert_eq!(feed(&mut rx, 0, &chunk), vec![1, 2, 3]);
        rx.poll_idle(10_000);
        assert_eq!(rx.stale_reset_count(), 0, "nothing was left buffered");
    }

    /// A chunk ending mid-frame arms the guard even though earlier frames in the
    /// same chunk completed.
    #[test]
    fn trailing_partial_after_complete_frame_arms_guard() {
        let mut rx = ProtocolRx::new();
        let f = frame(2);
        let chunk = [&frame(1)[..], &f[..f.len() - 2]].concat();
        assert_eq!(feed(&mut rx, 0, &chunk), vec![1]);
        rx.poll_idle(STALE_PARTIAL_IDLE_MS);
        assert_eq!(rx.stale_reset_count(), 1);
    }

    /// A partial completed by a later chunk disarms the guard: silence after a
    /// frame split across two reads must not report a discarded partial.
    #[test]
    fn completed_partial_clears_idle_marker() {
        let mut rx = ProtocolRx::new();
        let f = frame(7);
        let split = f.len() - 2;
        assert_eq!(feed(&mut rx, 0, &f[..split]), Vec::<u32>::new());
        assert_eq!(feed(&mut rx, 10, &f[split..]), vec![7]);
        rx.poll_idle(10_000);
        assert_eq!(
            rx.stale_reset_count(),
            0,
            "the partial completed; there is nothing to discard"
        );
    }

    /// Same disarming, when the completing chunk ends in a DeserError rather
    /// than a decodable frame.
    #[test]
    fn partial_completed_by_deser_error_clears_idle_marker() {
        let mut rx = ProtocolRx::new();
        assert_eq!(feed(&mut rx, 0, &[0x03, 0xff]), Vec::<u32>::new());
        assert_eq!(feed(&mut rx, 10, &[0x7f, 0x11, 0x00]), Vec::<u32>::new());
        assert_eq!(rx.deser_error_count(), 1);
        rx.poll_idle(10_000);
        assert_eq!(rx.stale_reset_count(), 0);
    }

    /// A delimiter-terminated frame too long to buffer resets the accumulator,
    /// so the next frame decodes on its own.
    #[test]
    fn over_long_frame_resets_accumulator() {
        let mut rx = ProtocolRx::new();
        let mut chunk = vec![0xa5u8; 600];
        chunk.push(0x00);
        assert_eq!(feed(&mut rx, 0, &chunk), Vec::<u32>::new());
        assert_eq!(rx.over_full_count(), 1);
        assert_eq!(feed(&mut rx, 10, &frame(9)), vec![9]);
        assert_eq!(
            rx.deser_error_count(),
            0,
            "the following frame must not be consumed by a resync"
        );
        rx.poll_idle(10_000);
        assert_eq!(rx.stale_reset_count(), 0);
    }

    /// A delimiter-less run longer than the accumulator overflows, and the tail
    /// that survives the overflow is left as a partial — which the idle guard
    /// then discards, so one boot-console burst costs no frame at all.
    #[test]
    fn delimiter_less_flood_overflows_then_heals() {
        let mut rx = ProtocolRx::new();
        assert_eq!(feed(&mut rx, 0, &[0xa5u8; 600]), Vec::<u32>::new());
        assert_eq!(rx.over_full_count(), 1);
        rx.poll_idle(STALE_PARTIAL_IDLE_MS);
        assert_eq!(rx.stale_reset_count(), 1, "the 88-byte tail was buffered");
        assert_eq!(feed(&mut rx, 600, &frame(9)), vec![9]);
        assert_eq!(rx.deser_error_count(), 0);
    }

    /// More bytes for the same partial restart the idle clock.
    #[test]
    fn activity_refreshes_idle_clock() {
        let mut rx = ProtocolRx::new();
        let f = frame(7);
        let split = f.len() - 3;
        feed(&mut rx, 0, &f[..split]);
        feed(&mut rx, 400, &f[split..f.len() - 1]);
        rx.poll_idle(800);
        assert_eq!(
            rx.stale_reset_count(),
            0,
            "clock restarted at 400ms; 800ms is only 400ms of silence"
        );
        rx.poll_idle(900);
        assert_eq!(rx.stale_reset_count(), 1);
    }
}
