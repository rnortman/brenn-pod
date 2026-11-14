//! The chip's state as a reader of a capture needs it: its firmware identity and
//! the registers that decide what its outputs carry, read in one pass over any
//! [`ControlRead`] transport.
//!
//! Everything here reads; nothing here can be handed a transport that writes
//! through it, since every function is bounded on [`ControlRead`].

use crate::{
    AEC_ASROUTGAIN_CMD, AEC_ASROUTGAIN_LABEL, AEC_ASROUTONOFF_CMD, AEC_ASROUTONOFF_LABEL,
    AEC_RESID, APPLICATION_SERVICER_RESID, AUDIO_MGR_FAR_END_DSP_ENABLE_CMD,
    AUDIO_MGR_FAR_END_DSP_ENABLE_LABEL, AUDIO_MGR_FLAG_READ_LEN, AUDIO_MGR_MIC_GAIN_CMD,
    AUDIO_MGR_MIC_GAIN_LABEL, AUDIO_MGR_OP_ALL_CMD, AUDIO_MGR_OP_ALL_LABEL,
    AUDIO_MGR_OP_ALL_READ_LEN, AUDIO_MGR_OP_L_CMD, AUDIO_MGR_OP_L_LABEL, AUDIO_MGR_OP_PACKED_CMD,
    AUDIO_MGR_OP_PACKED_LABEL, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_R_LABEL, AUDIO_MGR_OP_READ_LEN,
    AUDIO_MGR_OP_UPSAMPLE_CMD, AUDIO_MGR_OP_UPSAMPLE_LABEL, AUDIO_MGR_PAIR_READ_LEN,
    AUDIO_MGR_REF_GAIN_CMD, AUDIO_MGR_REF_GAIN_LABEL, AUDIO_MGR_RESID,
    AUDIO_MGR_SELECTED_CHANNELS_CMD, AUDIO_MGR_SELECTED_CHANNELS_LABEL, AUDIO_MGR_SYS_DELAY_CMD,
    AUDIO_MGR_SYS_DELAY_LABEL, BLD_MODIFIED_CMD, BLD_MODIFIED_LABEL, BLD_MODIFIED_READ_LEN,
    BLD_MSG_CMD, BLD_MSG_LABEL, BLD_MSG_READ_LEN, BLD_REPO_HASH_CMD, BLD_REPO_HASH_LABEL,
    BLD_REPO_HASH_READ_LEN, CTRL_BUF_CAPACITY, ControlRead, I2S_DAC_DSP_ENABLE_CMD,
    I2S_DAC_DSP_ENABLE_LABEL, RetryPolicy, SCALAR_READ_LEN, STATUS_DONE, USB_RETRY, VERSION_CMD,
    VERSION_LABEL, VERSION_READ_LEN, control_read,
};

/// The outcome of one register read under a retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterRead<E> {
    /// The chip answered [`STATUS_DONE`]; the payload is filled.
    Done,
    /// The chip answered a status other than [`STATUS_DONE`], final or after the
    /// retry budget ran out.
    Status {
        /// The last status byte.
        status: u8,
        /// Transactions made, the first included.
        attempts: u32,
    },
    /// The transport reports that the device declined the request
    /// ([`ControlRead::refused`]).
    Declined(E),
    /// The transfer did not complete.
    Failed(E),
}

/// Read one register under `policy` and say how it went: the one status check
/// every reader of a register shares.
pub fn read_one<T: ControlRead>(
    transport: &mut T,
    policy: RetryPolicy,
    resid: u8,
    cmd: u8,
    payload: &mut [u8],
) -> RegisterRead<T::Error> {
    match control_read(transport, policy, resid, cmd, payload) {
        Ok((STATUS_DONE, _)) => RegisterRead::Done,
        Ok((status, attempts)) => RegisterRead::Status { status, attempts },
        Err(e) if T::refused(&e) => RegisterRead::Declined(e),
        Err(e) => RegisterRead::Failed(e),
    }
}

/// How a register's payload decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// One unsigned byte per value.
    U8s,
    /// NUL-padded characters.
    Ascii,
    /// One little-endian `int32`.
    I32,
    /// One little-endian IEEE-754 `float`.
    F32,
}

/// One register of the chip's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Register {
    /// The register's name in a capture's record.
    pub name: &'static str,
    /// The register as an operator finds it in the vendor's table.
    pub label: &'static str,
    /// Resource id.
    pub resid: u8,
    /// Command id, without the read bit.
    pub cmd: u8,
    /// Payload length in bytes.
    pub len: usize,
    /// How the payload decodes.
    pub kind: Kind,
}

const fn reg(
    name: &'static str,
    label: &'static str,
    resid: u8,
    cmd: u8,
    len: usize,
    kind: Kind,
) -> Register {
    Register {
        name,
        label,
        resid,
        cmd,
        len,
        kind,
    }
}

/// The `kind` a capture's record gives a state read by [`read_state`] over
/// [`STATE_REGISTERS`], whichever USB id the chip was reached through.
pub const KIND: &str = "xvf3800";

/// Number of registers in [`STATE_REGISTERS`].
pub const STATE_REGISTER_COUNT: usize = 17;

/// The registers of the chip's state, in the order they are read: the firmware's
/// identity, then what its outputs carry and the reference path that feeds them.
pub const STATE_REGISTERS: [Register; STATE_REGISTER_COUNT] = [
    reg(
        "version",
        VERSION_LABEL,
        APPLICATION_SERVICER_RESID,
        VERSION_CMD,
        VERSION_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "bld_msg",
        BLD_MSG_LABEL,
        APPLICATION_SERVICER_RESID,
        BLD_MSG_CMD,
        BLD_MSG_READ_LEN,
        Kind::Ascii,
    ),
    reg(
        "bld_repo_hash",
        BLD_REPO_HASH_LABEL,
        APPLICATION_SERVICER_RESID,
        BLD_REPO_HASH_CMD,
        BLD_REPO_HASH_READ_LEN,
        Kind::Ascii,
    ),
    reg(
        "bld_modified",
        BLD_MODIFIED_LABEL,
        APPLICATION_SERVICER_RESID,
        BLD_MODIFIED_CMD,
        BLD_MODIFIED_READ_LEN,
        Kind::Ascii,
    ),
    reg(
        "audio_mgr_op_l",
        AUDIO_MGR_OP_L_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_L_CMD,
        AUDIO_MGR_OP_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "audio_mgr_op_r",
        AUDIO_MGR_OP_R_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_R_CMD,
        AUDIO_MGR_OP_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "audio_mgr_op_all",
        AUDIO_MGR_OP_ALL_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_ALL_CMD,
        AUDIO_MGR_OP_ALL_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "audio_mgr_op_packed",
        AUDIO_MGR_OP_PACKED_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_PACKED_CMD,
        AUDIO_MGR_PAIR_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "audio_mgr_op_upsample",
        AUDIO_MGR_OP_UPSAMPLE_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_UPSAMPLE_CMD,
        AUDIO_MGR_PAIR_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "audio_mgr_selected_channels",
        AUDIO_MGR_SELECTED_CHANNELS_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_SELECTED_CHANNELS_CMD,
        AUDIO_MGR_PAIR_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "aec_asroutonoff",
        AEC_ASROUTONOFF_LABEL,
        AEC_RESID,
        AEC_ASROUTONOFF_CMD,
        SCALAR_READ_LEN,
        Kind::I32,
    ),
    reg(
        "aec_asroutgain",
        AEC_ASROUTGAIN_LABEL,
        AEC_RESID,
        AEC_ASROUTGAIN_CMD,
        SCALAR_READ_LEN,
        Kind::F32,
    ),
    reg(
        "audio_mgr_mic_gain",
        AUDIO_MGR_MIC_GAIN_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_MIC_GAIN_CMD,
        SCALAR_READ_LEN,
        Kind::F32,
    ),
    reg(
        "audio_mgr_ref_gain",
        AUDIO_MGR_REF_GAIN_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_REF_GAIN_CMD,
        SCALAR_READ_LEN,
        Kind::F32,
    ),
    reg(
        "audio_mgr_sys_delay",
        AUDIO_MGR_SYS_DELAY_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_SYS_DELAY_CMD,
        SCALAR_READ_LEN,
        Kind::I32,
    ),
    reg(
        "audio_mgr_far_end_dsp_enable",
        AUDIO_MGR_FAR_END_DSP_ENABLE_LABEL,
        AUDIO_MGR_RESID,
        AUDIO_MGR_FAR_END_DSP_ENABLE_CMD,
        AUDIO_MGR_FLAG_READ_LEN,
        Kind::U8s,
    ),
    reg(
        "i2s_dac_dsp_enable",
        I2S_DAC_DSP_ENABLE_LABEL,
        AUDIO_MGR_RESID,
        I2S_DAC_DSP_ENABLE_CMD,
        AUDIO_MGR_FLAG_READ_LEN,
        Kind::U8s,
    ),
];

/// Why a register has no reading although its transfer completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The chip answered a status other than [`STATUS_DONE`].
    Status {
        /// The last status byte.
        status: u8,
        /// Transactions made, the first included.
        attempts: u32,
    },
    /// The device declined the request (on USB, a stalled control transfer).
    Declined,
}

/// One register's part of the state: its payload, or the chip's refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    /// The payload as read, without the status byte.
    Payload {
        /// Storage; the first `len` bytes are the payload.
        buf: [u8; CTRL_BUF_CAPACITY],
        /// The payload's length, the register's [`Register::len`].
        len: usize,
    },
    /// The chip refused this register.
    Refused(Refusal),
}

impl Reading {
    /// The payload bytes, or `None` for a refused register.
    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            Reading::Payload { buf, len } => Some(&buf[..*len]),
            Reading::Refused(_) => None,
        }
    }
}

/// Every register of [`STATE_REGISTERS`] asked once: a payload or a refusal each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChipState {
    readings: [Reading; STATE_REGISTER_COUNT],
}

impl ChipState {
    /// Each register with its reading, in table order.
    pub fn entries(&self) -> impl Iterator<Item = (&'static Register, &Reading)> {
        STATE_REGISTERS.iter().zip(self.readings.iter())
    }

    /// The reading of the register named `name`, if the table has one.
    pub fn get(&self, name: &str) -> Option<&Reading> {
        self.entries()
            .find(|(register, _)| register.name == name)
            .map(|(_, reading)| reading)
    }
}

/// A state read that ended at a transfer that did not complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateFailure<E> {
    /// The register the read stopped at.
    pub register: &'static Register,
    /// The transport's error.
    pub error: E,
}

/// Read every register of [`STATE_REGISTERS`] in table order under [`USB_RETRY`].
///
/// A register the chip answers with a status other than done, or declines, is
/// recorded as refused and the read goes on.
///
/// # Errors
/// The first transfer that does not complete ends the read: the failure names its
/// register and carries the transport's error.
pub fn read_state<T: ControlRead>(transport: &mut T) -> Result<ChipState, StateFailure<T::Error>> {
    let mut readings = [Reading::Refused(Refusal::Declined); STATE_REGISTER_COUNT];
    for (register, slot) in STATE_REGISTERS.iter().zip(readings.iter_mut()) {
        let mut buf = [0u8; CTRL_BUF_CAPACITY];
        let len = register.len;
        *slot = match read_one(
            transport,
            USB_RETRY,
            register.resid,
            register.cmd,
            &mut buf[..len],
        ) {
            RegisterRead::Done => Reading::Payload { buf, len },
            RegisterRead::Status { status, attempts } => {
                Reading::Refused(Refusal::Status { status, attempts })
            }
            RegisterRead::Declined(_) => Reading::Refused(Refusal::Declined),
            RegisterRead::Failed(error) => return Err(StateFailure { register, error }),
        };
    }
    Ok(ChipState { readings })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{STATUS_RETRY, should_retry};

    /// The error the fake returns: whether it stands for a stall.
    #[derive(Debug, PartialEq, Eq)]
    struct FakeError {
        stalled: bool,
    }

    /// How the fake answers one register, every time it is asked.
    #[derive(Debug, Clone, Copy)]
    enum Answer {
        /// Done, with every payload byte set to the value.
        Done(u8),
        /// This status byte.
        Status(u8),
        /// A stalled transfer.
        Stall,
        /// A transfer that does not complete.
        Fail,
    }

    /// A read-only transport answering per (resid, cmd); unscripted registers
    /// answer done with their command id as fill.
    struct ByRegister {
        answers: Vec<((u8, u8), Answer)>,
        asked: Vec<(u8, u8, usize, u32)>,
        delays: u32,
    }

    impl ByRegister {
        fn new(answers: &[((u8, u8), Answer)]) -> Self {
            Self {
                answers: answers.to_vec(),
                asked: Vec::new(),
                delays: 0,
            }
        }
    }

    impl ControlRead for ByRegister {
        type Error = FakeError;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            attempt: u32,
        ) -> Result<u8, FakeError> {
            self.asked.push((resid, cmd, payload.len(), attempt));
            let answer = self
                .answers
                .iter()
                .find(|(at, _)| *at == (resid, cmd))
                .map_or(Answer::Done(cmd), |(_, a)| *a);
            match answer {
                Answer::Done(fill) => {
                    payload.fill(fill);
                    Ok(STATUS_DONE)
                }
                Answer::Status(status) => {
                    payload.fill(0);
                    Ok(status)
                }
                Answer::Stall => Err(FakeError { stalled: true }),
                Answer::Fail => Err(FakeError { stalled: false }),
            }
        }

        fn delay_ms(&mut self, _ms: u32) {
            self.delays += 1;
        }

        fn refused(err: &FakeError) -> bool {
            err.stalled
        }
    }

    fn row(name: &str) -> &'static Register {
        STATE_REGISTERS
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("no row {name}"))
    }

    #[test]
    fn read_one_reports_done_and_fills_the_payload() {
        let mut t = ByRegister::new(&[((48, 0), Answer::Done(7))]);
        let mut payload = [0u8; 3];
        assert_eq!(
            read_one(&mut t, USB_RETRY, 48, 0, &mut payload),
            RegisterRead::Done
        );
        assert_eq!(payload, [7, 7, 7]);
        assert_eq!(t.asked, vec![(48, 0, 3, 1)]);
    }

    #[test]
    fn read_one_reports_a_final_status_with_its_attempts() {
        let mut t = ByRegister::new(&[((48, 0), Answer::Status(3))]);
        let mut payload = [0u8; 3];
        assert_eq!(
            read_one(&mut t, USB_RETRY, 48, 0, &mut payload),
            RegisterRead::Status {
                status: 3,
                attempts: 1
            }
        );
    }

    #[test]
    fn read_one_reports_a_status_after_the_budget() {
        assert!(should_retry(STATUS_RETRY));
        let policy = RetryPolicy {
            max_retries: 4,
            delay_ms: 1,
        };
        let mut t = ByRegister::new(&[((48, 0), Answer::Status(STATUS_RETRY))]);
        let mut payload = [0u8; 3];
        assert_eq!(
            read_one(&mut t, policy, 48, 0, &mut payload),
            RegisterRead::Status {
                status: STATUS_RETRY,
                attempts: 5
            }
        );
        assert_eq!(t.delays, 4);
    }

    #[test]
    fn read_one_tells_a_declined_transfer_from_a_failed_one() {
        let mut payload = [0u8; 3];
        let mut stalls = ByRegister::new(&[((48, 0), Answer::Stall)]);
        assert_eq!(
            read_one(&mut stalls, USB_RETRY, 48, 0, &mut payload),
            RegisterRead::Declined(FakeError { stalled: true })
        );
        let mut fails = ByRegister::new(&[((48, 0), Answer::Fail)]);
        assert_eq!(
            read_one(&mut fails, USB_RETRY, 48, 0, &mut payload),
            RegisterRead::Failed(FakeError { stalled: false })
        );
    }

    #[test]
    fn every_row_fits_the_buffer_and_names_are_unique() {
        for (i, r) in STATE_REGISTERS.iter().enumerate() {
            assert!(r.len > 0 && r.len <= CTRL_BUF_CAPACITY, "{}", r.name);
            for other in &STATE_REGISTERS[i + 1..] {
                assert_ne!(r.name, other.name);
                assert_ne!((r.resid, r.cmd), (other.resid, other.cmd), "{}", r.name);
            }
        }
    }

    #[test]
    fn every_row_states_its_label_and_a_length_its_kind_decodes() {
        for r in &STATE_REGISTERS {
            assert!(
                r.label
                    .ends_with(&format!("(resid {} cmd {})", r.resid, r.cmd)),
                "{}: {}",
                r.name,
                r.label
            );
            assert_eq!(
                r.label.split(' ').next().map(str::to_lowercase).as_deref(),
                Some(r.name)
            );
            match r.kind {
                Kind::I32 | Kind::F32 => assert_eq!(r.len, 4, "{}", r.name),
                Kind::U8s | Kind::Ascii => {}
            }
        }
    }

    #[test]
    fn read_state_reads_every_row_in_order() {
        let mut t = ByRegister::new(&[]);
        let state = read_state(&mut t).expect("every read completes");
        let asked: Vec<_> = t.asked.iter().map(|&(r, c, l, _)| (r, c, l)).collect();
        let table: Vec<_> = STATE_REGISTERS
            .iter()
            .map(|r| (r.resid, r.cmd, r.len))
            .collect();
        assert_eq!(asked, table);
        for (r, reading) in state.entries() {
            assert_eq!(
                reading.payload(),
                Some(&vec![r.cmd; r.len][..]),
                "{}",
                r.name
            );
        }
    }

    #[test]
    fn read_state_records_refusals_and_goes_on() {
        let repo_hash = row("bld_repo_hash");
        let gain = row("aec_asroutgain");
        let mut t = ByRegister::new(&[
            ((repo_hash.resid, repo_hash.cmd), Answer::Status(3)),
            ((gain.resid, gain.cmd), Answer::Stall),
        ]);
        let state = read_state(&mut t).expect("refusals do not end the read");
        assert_eq!(t.asked.len(), STATE_REGISTER_COUNT);
        assert_eq!(
            state.get("bld_repo_hash"),
            Some(&Reading::Refused(Refusal::Status {
                status: 3,
                attempts: 1
            }))
        );
        assert_eq!(
            state.get("aec_asroutgain"),
            Some(&Reading::Refused(Refusal::Declined))
        );
        assert_eq!(
            state.get("version").and_then(Reading::payload),
            Some(&[0u8; 3][..])
        );
        assert_eq!(state.get("no_such_register"), None);
    }

    #[test]
    fn read_state_ends_at_the_first_failed_transfer() {
        let op_r = row("audio_mgr_op_r");
        let index = STATE_REGISTERS
            .iter()
            .position(|r| r.name == "audio_mgr_op_r")
            .expect("row exists");
        let mut t = ByRegister::new(&[((op_r.resid, op_r.cmd), Answer::Fail)]);
        let failure = read_state(&mut t).expect_err("a failed transfer ends the read");
        assert_eq!(failure.register.name, "audio_mgr_op_r");
        assert_eq!(failure.error, FakeError { stalled: false });
        assert_eq!(
            t.asked.len(),
            index + 1,
            "nothing is asked after the failure"
        );
    }
}
