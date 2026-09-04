//! One register access, one contract.
//!
//! Every read and every write this pod issues to the XVF3800 — the self-test
//! registry's readings, the chip's bring-up sequence, the state line — goes
//! through the two functions here. The retry budget is the caller's, because a
//! diagnostic read on the thread that also runs the VAD gate cannot afford the
//! budget a startup write can; the status check and the two failure readings are
//! not, so a `pod_0.log` line and a self-test line describe the same failure with
//! the same words.
//!
//! The failure is a `String` rather than a typed error because not every caller's
//! verdict turns on it: some print it, some fail a case on it, and none parse it.

use std::fmt;

use xvf3800_ctrl::{ControlTransport, RetryPolicy, STATUS_DONE, control_read, control_write};

/// One register's payload under `policy`, or the one-line reading that says why
/// there is none.
///
/// # Errors
/// The transport failing, or a final status that is not [`STATUS_DONE`].
pub(crate) fn read_register<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    resid: u8,
    cmd: u8,
    payload: &mut [u8],
    label: &str,
) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    match control_read(transport, policy, resid, cmd, payload) {
        Ok((STATUS_DONE, _)) => Ok(()),
        Ok((status, attempts)) => Err(format!(
            "{label} read returned status 0x{status:02x} after {attempts} transaction(s)"
        )),
        Err(e) => Err(format!("{label} read failed: {e}")),
    }
}

/// One register written under `policy`, or the one line that says why it was not
/// taken. The read's contract, in the other direction.
///
/// # Errors
/// The transport failing, or a final status that is not [`STATUS_DONE`].
pub(crate) fn write_register<T: ControlTransport>(
    transport: &mut T,
    policy: RetryPolicy,
    resid: u8,
    cmd: u8,
    payload: &[u8],
    label: &str,
) -> Result<(), String>
where
    T::Error: fmt::Display,
{
    match control_write(transport, policy, resid, cmd, payload) {
        Ok((STATUS_DONE, _)) => Ok(()),
        Ok((status, attempts)) => Err(format!(
            "{label} write returned status 0x{status:02x} after {attempts} transaction(s)"
        )),
        Err(e) => Err(format!("{label} write failed: {e}")),
    }
}
