//! XVF3800 output routing: what the two I2S slots carry.
//!
//! The left slot keeps the chip's default, the auto-select communication beam. At boot
//! the pod routes amplified raw microphone 0 into the right slot (silence by default)
//! with one `AUDIO_MGR_OP_R` write, then reads both routing registers back. The outcome
//! is logged as one boot line and kept for the `Xvf3800OutputRouting` self-test. Each
//! boot transaction is re-issued on a transport error (a NAK or a bus fault), a bounded
//! number of times, because the shared driver re-issues only on a transient status.
//!
//! The sequence is generic over [`ControlTransport`] so it runs against a scripted
//! transport on the host; the device wrapper binds it to the shared I2C bus.

// Host view: the sequence exists for the tests and for the device-gated call sites.
#![cfg_attr(not(target_os = "espidf"), allow(dead_code))]

use xvf3800_ctrl::{
    AUDIO_MGR_OP_L_CMD, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_READ_LEN, AUDIO_MGR_RESID,
    ControlTransport, RetryPolicy, STATUS_DONE, control_read, control_write,
};

#[cfg(target_os = "espidf")]
use crate::i2c::I2C_BUS;
#[cfg(target_os = "espidf")]
use crate::xvf3800::I2cControl;
#[cfg(target_os = "espidf")]
use device_protocol::{Payload, Status, TestData, test_report_fail_fmt, test_report_ok};
#[cfg(target_os = "espidf")]
use esp_idf_svc::hal::i2c::I2cDriver;
#[cfg(target_os = "espidf")]
use std::sync::Mutex;
#[cfg(target_os = "espidf")]
use xvf3800_ctrl::I2C_RETRY;

/// What the right I2S slot carries: category 3 (amplified microphone data with system
/// delay — the signal handed to the SHF cores, uncompressed, no AGC, no noise
/// suppression, no beamforming), microphone index 0.
pub(crate) const RIGHT_SLOT_ROUTE: [u8; 2] = [3, 0];

/// The left slot's documented default (`AUDIO_MGR_OP_L = (8,0)`, the auto-select beam).
/// Never written: read back at boot and asserted by the identity test.
pub(crate) const LEFT_SLOT_EXPECTED: [u8; 2] = [8, 0];

/// Re-issues of one routing transaction after a transport error (a NAK or a bus
/// fault), after the first attempt. Three: the one sighting on the bench — a NAK on
/// the boot write that the next transaction, microseconds later, did not see — needs
/// one; the budget is for the busy window the hardware reference describes after a
/// command, not for a held bus or a chip still booting, which it cannot wait out and
/// does not try to. Worst case on a held bus, every transaction hitting
/// `I2C_CTRL_TIMEOUT_TICKS`: 3 calls × (4 × 100 ms + 3 × 5 ms) ≈ 1.3 s added to
/// boot, before any other bus user exists.
const ROUTING_TRANSPORT_REISSUES: u32 = 3;

/// Gap before each re-issue: the settle the hardware reference gives for the chip's
/// post-command busy window (`docs/reference/hardware-reference.md`, the I2C bus and
/// control-protocol sections: ~5 ms).
const ROUTING_REISSUE_GAP_MS: u32 = 5;

/// What the boot-time routing write found and left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingOutcome {
    /// Final status of the `AUDIO_MGR_OP_R` write; `None` when the transport failed and
    /// no status came back.
    pub(crate) write_status: Option<u8>,
    /// `AUDIO_MGR_OP_L` read back, `None` when the read failed or returned a non-DONE
    /// status.
    pub(crate) left: Option<[u8; 2]>,
    /// `AUDIO_MGR_OP_R` read back, under the same rule.
    pub(crate) right: Option<[u8; 2]>,
    /// The write was accepted and the right slot reads back as [`RIGHT_SLOT_ROUTE`].
    pub(crate) applied: bool,
    /// Transactions re-issued after a transport error, summed over the write and the
    /// two reads. 0 on a normal boot. Every transport error counts alike, so a non-zero
    /// count does not say which kind cleared: a NAK is the chip's post-command busy
    /// window the hardware reference describes, a timeout is a held or not-yet-ready
    /// bus that happened to release. The transport's warn lines carry the error.
    pub(crate) reissues: u32,
}

/// Read one routing register: the `(category, source)` pair, or `None` when the
/// transport failed or the chip answered with anything but DONE. Single-issue.
fn read_route<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    cmd: u8,
) -> Option<[u8; 2]> {
    read_route_result(transport, policy, cmd).ok().flatten()
}

/// Read one routing register, keeping a transport failure apart from a refusal: `Err`
/// when the transport failed, `Ok(None)` when the chip answered with anything but DONE,
/// `Ok(Some(pair))` on DONE.
fn read_route_result<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    cmd: u8,
) -> Result<Option<[u8; 2]>, T::Error> {
    let mut pair = [0u8; AUDIO_MGR_OP_READ_LEN];
    let (status, _) = control_read(transport, policy, AUDIO_MGR_RESID, cmd, &mut pair)?;
    Ok((status == STATUS_DONE).then_some(pair))
}

/// Run `call` once and, while it fails at the transport and fewer than
/// [`ROUTING_TRANSPORT_REISSUES`] re-issues have been made for it, wait
/// [`ROUTING_REISSUE_GAP_MS`] and run it again, counting each re-issue into `reissues`.
/// Returns the first success, or the last transport error once the budget is spent.
fn reissued<T: ControlTransport, R>(
    transport: &mut T,
    reissues: &mut u32,
    mut call: impl FnMut(&mut T) -> Result<R, T::Error>,
) -> Result<R, T::Error> {
    let mut result = call(transport);
    let mut made = 0;
    while result.is_err() && made < ROUTING_TRANSPORT_REISSUES {
        transport.delay_ms(ROUTING_REISSUE_GAP_MS);
        made += 1;
        *reissues += 1;
        result = call(transport);
    }
    result
}

/// Write [`RIGHT_SLOT_ROUTE`] to `AUDIO_MGR_OP_R`, then read `OP_L` and `OP_R` back.
///
/// Both reads run whatever the write returned, so a refused write is reported with the
/// registers as the chip holds them. Each of the three transactions is re-issued on a
/// transport error, up to [`ROUTING_TRANSPORT_REISSUES`] times; a refusal (a final
/// non-DONE status) is not.
pub(crate) fn apply_right_slot_routing_via<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
) -> RoutingOutcome {
    let mut reissues = 0;
    let write_status = reissued(transport, &mut reissues, |t| {
        control_write(
            t,
            policy,
            AUDIO_MGR_RESID,
            AUDIO_MGR_OP_R_CMD,
            &RIGHT_SLOT_ROUTE,
        )
    })
    .ok()
    .map(|(status, _)| status);
    let mut read = |cmd: u8| {
        reissued(transport, &mut reissues, |t| {
            read_route_result(t, policy, cmd)
        })
        .ok()
        .flatten()
    };
    let left = read(AUDIO_MGR_OP_L_CMD);
    let right = read(AUDIO_MGR_OP_R_CMD);
    RoutingOutcome {
        write_status,
        left,
        right,
        applied: write_status == Some(STATUS_DONE) && right == Some(RIGHT_SLOT_ROUTE),
        reissues,
    }
}

/// The boot write's outcome, kept for the `Xvf3800OutputRouting` self-test. `None`
/// until boot has run the write.
#[cfg(target_os = "espidf")]
static BOOT_ROUTING: Mutex<Option<RoutingOutcome>> = Mutex::new(None);

/// Route raw microphone 0 into the right I2S slot over the shared I2C bus, log the
/// outcome as the boot `routing` line, and keep it for the self-test.
///
/// The `routing:` line states the write's status and both registers as read back
/// (`?` where a read failed), `applied=true` exactly when the right slot reads back as
/// raw microphone 0 after an accepted write, and `left_is_beam=true` exactly when the
/// left slot reads back as the auto-select beam. With `applied=false` the right slot
/// carries whatever the registers show — silence by the chip's default. `reissues=` is
/// the number of transactions re-issued after a transport error, summed over the write
/// and the two reads: 0 on a normal boot. A non-zero count says only that some
/// transaction failed at the transport and a re-issue followed, not why. The
/// transport's `xvf3800_control_*` warn lines before this line name each error:
/// `ESP_FAIL` is a NAK, the chip's post-command busy window the hardware reference
/// classes as transient; any other code (`ESP_ERR_TIMEOUT` above all) is a held,
/// faulted or not-yet-ready bus that released inside the budget, which is not that
/// known transient and is a reading for a human even when `applied=true`. The line is a
/// warning unless both `applied` and `left_is_beam` hold, whatever the count: the transport has already warned once per
/// failed transaction, with the error that tells the two cases apart.
///
/// Every routing transaction is re-issued on a transport error, a bounded number of
/// times (see [`apply_right_slot_routing_via`]).
#[cfg(target_os = "espidf")]
pub(crate) fn apply_right_slot_routing(driver: &mut I2cDriver<'_>) -> RoutingOutcome {
    let outcome = apply_right_slot_routing_via(&mut I2cControl::new(driver), I2C_RETRY);
    let left_is_beam = outcome.left == Some(LEFT_SLOT_EXPECTED);
    // TODO(test-routing-reissues-log-field-unverified): the level choice and the line's
    // format sit behind the device cfg and no host test pins either.
    let level = if outcome.applied && left_is_beam {
        log::Level::Info
    } else {
        log::Level::Warn
    };
    log::log!(
        level,
        "routing: op_r write_status={} op_l={} op_r={} applied={} left_is_beam={} reissues={}",
        StatusText(outcome.write_status),
        PairText(outcome.left),
        PairText(outcome.right),
        outcome.applied,
        left_is_beam,
        outcome.reissues,
    );
    *BOOT_ROUTING
        .lock()
        .unwrap_or_else(|_| panic!("BOOT_ROUTING mutex poisoned")) = Some(outcome);
    outcome
}

/// `Option<u8>` status as the routing line spells it: hex, or `?` for no status.
#[cfg(target_os = "espidf")]
struct StatusText(Option<u8>);

#[cfg(target_os = "espidf")]
impl core::fmt::Display for StatusText {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(s) => write!(f, "{s:#04x}"),
            None => f.write_str("?"),
        }
    }
}

/// `Option<[u8; 2]>` routing pair as `(category,source)`, or `?` for a failed read.
#[cfg(target_os = "espidf")]
struct PairText(Option<[u8; 2]>);

#[cfg(target_os = "espidf")]
impl core::fmt::Display for PairText {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some([c, s]) => write!(f, "({c},{s})"),
            None => f.write_str("?"),
        }
    }
}

/// HIL self-test for `TestName::Xvf3800OutputRouting`.
///
/// Reports the boot write's status and the routing registers as they read now, so a
/// route that was applied at boot and later lost fails here too. The host gate holds the
/// expectation (`write_status == 0x00`, `right == RIGHT_SLOT_ROUTE`, `left ==
/// LEFT_SLOT_EXPECTED`); the device fails only when it has nothing to report.
#[cfg(target_os = "espidf")]
pub(crate) fn run_xvf3800_output_routing() -> (Status, Payload) {
    let boot = *BOOT_ROUTING
        .lock()
        .unwrap_or_else(|_| panic!("BOOT_ROUTING mutex poisoned"));
    let Some(boot) = boot else {
        return test_report_fail_fmt(format_args!(
            "FAIL src=routing boot routing write never ran — firmware init bug"
        ));
    };
    let Some(write_status) = boot.write_status else {
        return test_report_fail_fmt(format_args!(
            "FAIL src=routing boot op_r write had a transport error op_l={} op_r={}",
            PairText(boot.left),
            PairText(boot.right),
        ));
    };

    let mut bus_guard = I2C_BUS
        .lock()
        .unwrap_or_else(|_| panic!("I2C_BUS mutex poisoned"));
    let Some(driver) = bus_guard.as_mut() else {
        return test_report_fail_fmt(format_args!(
            "FAIL src=routing I2C_BUS not initialized — firmware init bug"
        ));
    };
    let mut transport = I2cControl::new(driver);
    let left = read_route(&mut transport, I2C_RETRY, AUDIO_MGR_OP_L_CMD);
    let right = read_route(&mut transport, I2C_RETRY, AUDIO_MGR_OP_R_CMD);
    match (left, right) {
        (Some(left), Some(right)) => test_report_ok(TestData::Xvf3800OutputRouting {
            write_status,
            left,
            right,
        }),
        _ => test_report_fail_fmt(format_args!(
            "FAIL src=routing read failed write_status={write_status:#04x} op_l={} op_r={}",
            PairText(left),
            PairText(right),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xvf3800_ctrl::{I2C_RETRY, STATUS_RETRY};

    /// One recorded transport call: `(is_read, resid, cmd, write payload)`.
    type Call = (bool, u8, u8, Vec<u8>);

    /// Replays scripted per-call answers, recording every call.
    struct Scripted {
        /// Per call, front to back: `Ok((status, read payload))` or a transport error.
        answers: Vec<Result<(u8, [u8; 2]), ()>>,
        calls: Vec<Call>,
        /// Every `delay_ms` argument, in order.
        delays: Vec<u32>,
    }

    impl Scripted {
        fn new(answers: &[Result<(u8, [u8; 2]), ()>]) -> Self {
            Self {
                answers: answers.to_vec(),
                calls: Vec::new(),
                delays: Vec::new(),
            }
        }

        fn next(&mut self) -> Result<(u8, [u8; 2]), ()> {
            assert!(!self.answers.is_empty(), "transport called past the script");
            self.answers.remove(0)
        }
    }

    impl ControlTransport for Scripted {
        type Error = ();

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, ()> {
            self.calls.push((true, resid, cmd, Vec::new()));
            let (status, pair) = self.next()?;
            payload.copy_from_slice(&pair);
            Ok(status)
        }

        fn control_write_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &[u8],
            _attempt: u32,
        ) -> Result<u8, ()> {
            self.calls.push((false, resid, cmd, payload.to_vec()));
            self.next().map(|(status, _)| status)
        }

        fn delay_ms(&mut self, ms: u32) {
            self.delays.push(ms);
        }
    }

    const DONE: u8 = STATUS_DONE;

    #[test]
    fn writes_the_right_route_then_reads_both_registers() {
        let mut t = Scripted::new(&[
            Ok((DONE, [0, 0])),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, RIGHT_SLOT_ROUTE)),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert_eq!(
            outcome,
            RoutingOutcome {
                write_status: Some(DONE),
                left: Some([8, 0]),
                right: Some([3, 0]),
                applied: true,
                reissues: 0,
            }
        );
        assert_eq!(
            t.calls,
            vec![
                (false, AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, vec![3, 0]),
                (true, AUDIO_MGR_RESID, AUDIO_MGR_OP_L_CMD, vec![]),
                (true, AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, vec![]),
            ]
        );
    }

    #[test]
    fn a_transient_write_status_is_retried() {
        let mut t = Scripted::new(&[
            Ok((STATUS_RETRY, [0, 0])),
            Ok((STATUS_RETRY, [0, 0])),
            Ok((DONE, [0, 0])),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, RIGHT_SLOT_ROUTE)),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert!(outcome.applied);
        assert_eq!(outcome.write_status, Some(DONE));
        assert_eq!(
            t.calls.iter().filter(|c| !c.0).count(),
            3,
            "three write attempts"
        );
    }

    /// A refused write is reported with both registers as the chip holds them: the
    /// right slot still at its default reads as not applied.
    #[test]
    fn a_refused_write_is_reported_with_both_readings() {
        let mut t = Scripted::new(&[
            Ok((0x02, [0, 0])),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, [0, 0])),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert_eq!(
            outcome,
            RoutingOutcome {
                write_status: Some(0x02),
                left: Some([8, 0]),
                right: Some([0, 0]),
                applied: false,
                reissues: 0,
            }
        );
    }

    /// An accepted write whose register reads back as something else is not applied.
    #[test]
    fn an_accepted_write_that_does_not_read_back_is_not_applied() {
        let mut t = Scripted::new(&[
            Ok((DONE, [0, 0])),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, [3, 1])),
        ]);
        assert!(!apply_right_slot_routing_via(&mut t, I2C_RETRY).applied);
    }

    /// A write with no status still gets both reads; a failed or non-DONE read is
    /// `None`, never a guessed pair. A persistent transport error is bounded per call,
    /// and the count is the sum over the calls; the non-DONE read costs one transaction
    /// and no re-issue.
    #[test]
    fn transport_errors_and_bad_read_statuses_leave_gaps() {
        let per_call = ROUTING_TRANSPORT_REISSUES as usize + 1;
        let mut script = vec![Err(()); 2 * per_call];
        script.push(Ok((0x03, [9, 9])));
        let mut t = Scripted::new(&script);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert_eq!(
            outcome,
            RoutingOutcome {
                write_status: None,
                left: None,
                right: None,
                applied: false,
                reissues: 2 * ROUTING_TRANSPORT_REISSUES,
            }
        );
        assert_eq!(t.calls.len(), 9);
        assert_eq!(outcome.reissues, 6);
        assert_eq!(
            t.delays,
            vec![ROUTING_REISSUE_GAP_MS; 2 * ROUTING_TRANSPORT_REISSUES as usize]
        );
        assert!(t.answers.is_empty(), "the whole script was consumed");
    }

    /// A transport error on the write is re-issued after one gap and lands.
    #[test]
    fn a_write_transport_error_is_reissued_after_one_gap() {
        let mut t = Scripted::new(&[
            Err(()),
            Ok((DONE, [0, 0])),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, RIGHT_SLOT_ROUTE)),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert_eq!(
            outcome,
            RoutingOutcome {
                write_status: Some(DONE),
                left: Some([8, 0]),
                right: Some([3, 0]),
                applied: true,
                reissues: 1,
            }
        );
        assert_eq!(
            t.calls.iter().filter(|c| !c.0).count(),
            2,
            "two write calls"
        );
        assert_eq!(t.delays, vec![ROUTING_REISSUE_GAP_MS]);
    }

    /// A transport error on a read is re-issued the same way.
    #[test]
    fn a_read_transport_error_is_reissued_after_one_gap() {
        let mut t = Scripted::new(&[
            Ok((DONE, [0, 0])),
            Err(()),
            Ok((DONE, LEFT_SLOT_EXPECTED)),
            Ok((DONE, RIGHT_SLOT_ROUTE)),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert!(outcome.applied);
        assert_eq!(outcome.left, Some(LEFT_SLOT_EXPECTED));
        assert_eq!(outcome.reissues, 1);
        assert_eq!(
            t.calls
                .iter()
                .filter(|c| c.0 && c.2 == AUDIO_MGR_OP_L_CMD)
                .count(),
            2,
            "two OP_L reads"
        );
        assert_eq!(t.delays, vec![ROUTING_REISSUE_GAP_MS]);
    }

    /// A non-DONE read status is a refusal, not a transport error: one transaction, the
    /// read `None`, nothing re-issued.
    #[test]
    fn a_non_done_read_status_is_not_reissued() {
        let mut t = Scripted::new(&[
            Ok((DONE, [0, 0])),
            Ok((0x03, [9, 9])),
            Ok((DONE, RIGHT_SLOT_ROUTE)),
        ]);
        let outcome = apply_right_slot_routing_via(&mut t, I2C_RETRY);
        assert_eq!(outcome.left, None);
        assert!(outcome.applied);
        assert_eq!(outcome.reissues, 0);
        assert_eq!(t.calls.len(), 3);
        assert!(t.delays.is_empty());
    }

    /// The split read keeps the transport error apart from a refusal; the single-issue
    /// `read_route` folds both into `None`.
    #[test]
    fn read_route_result_tells_a_transport_error_from_a_refusal() {
        let mut t = Scripted::new(&[Err(()), Ok((0x03, [9, 9])), Ok((DONE, [3, 0]))]);
        assert_eq!(
            read_route_result(&mut t, I2C_RETRY, AUDIO_MGR_OP_R_CMD),
            Err(())
        );
        assert_eq!(
            read_route_result(&mut t, I2C_RETRY, AUDIO_MGR_OP_R_CMD),
            Ok(None)
        );
        assert_eq!(
            read_route_result(&mut t, I2C_RETRY, AUDIO_MGR_OP_R_CMD),
            Ok(Some([3, 0]))
        );
        let mut t = Scripted::new(&[Err(())]);
        assert_eq!(read_route(&mut t, I2C_RETRY, AUDIO_MGR_OP_R_CMD), None);
        assert_eq!(t.calls.len(), 1, "read_route is single-issue");
    }
}
