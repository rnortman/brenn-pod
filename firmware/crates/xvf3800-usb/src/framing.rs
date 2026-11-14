//! The XVF3800's control protocol framed as USB vendor control transfers.
//!
//! The protocol is the one the chip speaks over I2C — a resource id, a command id
//! with bit 7 set for reads, and a status byte ahead of a read's payload. On USB
//! the resid and command travel in the setup packet (`bRequest` 0, `wValue` =
//! command, `wIndex` = resid) instead of in three header bytes, and a read's
//! `wLength` covers the status byte on top of the payload.
//!
//! Transfers go to the device recipient on endpoint 0, which needs no claimed
//! interface: the audio interfaces stay bound to the kernel's `snd_usb_audio`
//! while the host talks control.
//!
//! This module is data only, both directions: it builds setup packets and splits
//! answers, and sends nothing.

use std::fmt;
use std::time::Duration;

use xvf3800_ctrl::READ_BIT;

/// Per-transfer timeout. One second is far tighter than the vendor tooling's 100 s:
/// a control transfer that has not completed by then is a board that has stopped
/// answering, and the caller's answer to that is to stop rather than to block a
/// thread for a minute and a half.
pub const CONTROL_TIMEOUT: Duration = Duration::from_secs(1);

/// `bRequest` for every XVF3800 control transfer, read or write. The chip
/// multiplexes on `wValue`/`wIndex` alone.
pub const CONTROL_REQUEST: u8 = 0;

/// `bmRequestType` of a read: device-to-host, vendor, device recipient.
pub const REQUEST_TYPE_VENDOR_IN: u8 = 0xC0;

/// `bmRequestType` of a write: host-to-device, vendor, device recipient.
pub const REQUEST_TYPE_VENDOR_OUT: u8 = 0x40;

/// The direction bit of `bmRequestType`: set on a device-to-host transfer.
pub const DIRECTION_IN: u8 = 0x80;

/// The setup packet for one control transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setup {
    pub request_type: u8,
    pub request: u8,
    pub value: u16,
    pub index: u16,
    /// Bytes on the wire: the payload, plus the status byte on a read.
    pub length: usize,
}

impl Setup {
    /// Whether this packet asks the device to send data to the host.
    pub fn is_device_to_host(&self) -> bool {
        self.request_type & DIRECTION_IN != 0
    }
}

impl fmt::Display for Setup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bmRequestType={:#04x} bRequest={} wValue={:#06x} wIndex={} wLength={}",
            self.request_type, self.request, self.value, self.index, self.length
        )
    }
}

/// The setup packet for a control read of `payload_len` payload bytes.
///
/// `wLength` is one byte longer than the payload: the chip answers with its status
/// byte first. The read bit goes in `wValue`, never in the payload.
pub fn read_setup(resid: u8, cmd: u8, payload_len: usize) -> Setup {
    Setup {
        request_type: REQUEST_TYPE_VENDOR_IN,
        request: CONTROL_REQUEST,
        value: u16::from(cmd | READ_BIT),
        index: u16::from(resid),
        length: payload_len + 1,
    }
}

/// The setup packet for a control write of `payload_len` payload bytes.
///
/// No status byte: an OUT transfer carries the payload and nothing else.
pub fn write_setup(resid: u8, cmd: u8, payload_len: usize) -> Setup {
    Setup {
        request_type: REQUEST_TYPE_VENDOR_OUT,
        request: CONTROL_REQUEST,
        value: u16::from(cmd),
        index: u16::from(resid),
        length: payload_len,
    }
}

/// A transfer that completed with the wrong number of bytes. Reported rather than
/// zero-filled: a short read would otherwise decode as a plausible value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShortTransfer {
    pub expected: usize,
    pub got: usize,
}

impl fmt::Display for ShortTransfer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transfer moved {} of {} bytes", self.got, self.expected)
    }
}

impl std::error::Error for ShortTransfer {}

/// Split a completed read into its status byte and payload.
///
/// `buf` is the staging buffer the transfer filled and `transferred` is what the
/// transfer says it moved; anything short of the whole setup length is an error,
/// so a truncated answer cannot reach a decoder.
pub fn split_response(
    buf: &[u8],
    transferred: usize,
    payload: &mut [u8],
) -> Result<u8, ShortTransfer> {
    let expected = payload.len() + 1;
    if transferred != expected || buf.len() < expected {
        return Err(ShortTransfer {
            expected,
            got: transferred,
        });
    }
    payload.copy_from_slice(&buf[1..expected]);
    Ok(buf[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use xvf3800_ctrl::{
        AEC_AECCONVERGED_CMD, AEC_ASROUTGAIN_CMD, AEC_ASROUTONOFF_CMD, AEC_AZIMUTH_READ_LEN,
        AEC_AZIMUTH_VALUES_CMD, AEC_RESID, AEC_SPENERGY_READ_LEN, AEC_SPENERGY_VALUES_CMD,
        APPLICATION_SERVICER_RESID, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_READ_LEN, AUDIO_MGR_RESID,
        BLD_MSG_CMD, BLD_MSG_READ_LEN, CTRL_BUF_CAPACITY, GPO_CMD, GPO_RESID, GPO_VECTOR_LEN,
        PP_AGCGAIN_CMD, PP_AGCONOFF_CMD, PP_DTSENSITIVE_CMD, PP_ECHOONOFF_CMD, PP_MIN_NN_CMD,
        PP_MIN_NS_CMD, PP_RESID, REBOOT_CMD, REBOOT_WRITE_LEN, SCALAR_READ_LEN, STATUS_DONE,
        STATUS_RETRY, VERSION_CMD, VERSION_READ_LEN,
    };

    // ── Framing ───────────────────────────────────────────────────────────────

    #[test]
    fn a_spenergy_read_is_framed_as_the_vendor_tooling_frames_it() {
        let setup = read_setup(AEC_RESID, AEC_SPENERGY_VALUES_CMD, AEC_SPENERGY_READ_LEN);
        assert_eq!(setup.request_type, 0xC0);
        assert_eq!(setup.request, 0);
        // 80 | 0x80.
        assert_eq!(setup.value, 0xD0);
        assert_eq!(setup.index, 33);
        // 4 × f32 plus the status byte.
        assert_eq!(setup.length, 17);
    }

    #[test]
    fn an_azimuth_read_and_a_version_read_carry_their_own_resid_and_length() {
        let doa = read_setup(AEC_RESID, AEC_AZIMUTH_VALUES_CMD, AEC_AZIMUTH_READ_LEN);
        assert_eq!((doa.value, doa.index, doa.length), (0xCB, 33, 17));
        let version = read_setup(APPLICATION_SERVICER_RESID, VERSION_CMD, VERSION_READ_LEN);
        // Command 0 with the read bit is 0x80, and the servicer is 48.
        assert_eq!(
            (version.value, version.index, version.length),
            (0x80, 48, 4)
        );
        assert_eq!(version.request_type, 0xC0);
    }

    #[test]
    fn a_write_carries_no_status_byte_and_no_read_bit() {
        let setup = write_setup(GPO_RESID, GPO_CMD, GPO_VECTOR_LEN);
        assert_eq!(setup.request_type, 0x40);
        assert_eq!(setup.request, 0);
        assert_eq!(setup.value, 0);
        assert_eq!(setup.index, 20);
        assert_eq!(setup.length, GPO_VECTOR_LEN);
    }

    #[test]
    fn the_routing_registers_frame_as_the_user_guide_numbers_them() {
        // AUDIO_MGR_OP_R: two bytes, category and source, on the audio manager.
        let route = write_setup(AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_READ_LEN);
        assert_eq!(
            (route.request_type, route.value, route.index),
            (0x40, 19, 35)
        );
        assert_eq!(route.length, 2);
        let read_back = read_setup(AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, AUDIO_MGR_OP_READ_LEN);
        assert_eq!((read_back.value, read_back.length), (19 | 0x80, 3));
        // AEC_ASROUTONOFF: one int32 on the AEC servicer, written then read back.
        let enable = write_setup(AEC_RESID, AEC_ASROUTONOFF_CMD, SCALAR_READ_LEN);
        assert_eq!((enable.value, enable.index, enable.length), (35, 33, 4));
        let confirm = read_setup(AEC_RESID, AEC_ASROUTONOFF_CMD, SCALAR_READ_LEN);
        assert_eq!(
            (confirm.value, confirm.index, confirm.length),
            (0xA3, 33, 5)
        );
    }

    #[test]
    fn the_state_line_registers_frame_as_five_byte_reads() {
        for (resid, cmd) in [
            (AEC_RESID, AEC_ASROUTGAIN_CMD),
            (AEC_RESID, AEC_AECCONVERGED_CMD),
            (PP_RESID, PP_AGCONOFF_CMD),
            (PP_RESID, PP_AGCGAIN_CMD),
            (PP_RESID, PP_MIN_NS_CMD),
            (PP_RESID, PP_MIN_NN_CMD),
            (PP_RESID, PP_ECHOONOFF_CMD),
            (PP_RESID, PP_DTSENSITIVE_CMD),
        ] {
            let setup = read_setup(resid, cmd, SCALAR_READ_LEN);
            assert_eq!(setup.request_type, 0xC0, "resid {resid} cmd {cmd}");
            assert_eq!(setup.index, u16::from(resid));
            assert_eq!(setup.value, u16::from(cmd | READ_BIT));
            assert_eq!(setup.length, 5);
        }
    }

    #[test]
    fn reboot_is_a_one_byte_write_and_the_build_message_the_longest_read() {
        let reboot = write_setup(APPLICATION_SERVICER_RESID, REBOOT_CMD, REBOOT_WRITE_LEN);
        assert_eq!(reboot.request_type, 0x40);
        assert_eq!((reboot.value, reboot.index, reboot.length), (7, 48, 1));

        let build = read_setup(APPLICATION_SERVICER_RESID, BLD_MSG_CMD, BLD_MSG_READ_LEN);
        assert_eq!((build.value, build.index, build.length), (0x81, 48, 51));
        // A transport stages a read in `CTRL_BUF_CAPACITY + 1` bytes and asserts
        // on anything longer, so the longest register must fit.
        assert!(build.length <= CTRL_BUF_CAPACITY + 1);
    }

    #[test]
    fn the_read_bit_is_the_only_difference_between_the_two_directions() {
        let cmd = 75;
        let read = read_setup(AEC_RESID, cmd, 16);
        let write = write_setup(AEC_RESID, cmd, 16);
        assert_eq!(read.value, write.value | u16::from(READ_BIT));
        assert_eq!(read.index, write.index);
        assert_eq!(read.length, write.length + 1);
        assert_ne!(read.request_type, write.request_type);
        assert!(read.is_device_to_host());
        assert!(!write.is_device_to_host());
    }

    #[test]
    fn a_setup_renders_every_field_of_the_packet() {
        let setup = read_setup(APPLICATION_SERVICER_RESID, VERSION_CMD, VERSION_READ_LEN);
        assert_eq!(
            setup.to_string(),
            "bmRequestType=0xc0 bRequest=0 wValue=0x0080 wIndex=48 wLength=4"
        );
    }

    // ── Response splitting ────────────────────────────────────────────────────

    #[test]
    fn a_complete_response_yields_the_status_and_the_payload_behind_it() {
        let buf = [STATUS_DONE, 1, 2, 3];
        let mut payload = [0u8; 3];
        assert_eq!(split_response(&buf, 4, &mut payload).expect("split"), 0);
        assert_eq!(payload, [1, 2, 3]);
    }

    #[test]
    fn a_transient_status_still_delivers_the_bytes_that_arrived() {
        // The retry driver logs raw payloads next to failing statuses, so the split
        // must not withhold them.
        let buf = [STATUS_RETRY, 9, 9, 9];
        let mut payload = [0u8; 3];
        assert_eq!(split_response(&buf, 4, &mut payload).expect("split"), 0x40);
        assert_eq!(payload, [9, 9, 9]);
    }

    #[test]
    fn a_short_transfer_is_an_error_rather_than_a_zero_filled_payload() {
        let buf = [STATUS_DONE, 1, 2, 3];
        let mut payload = [0xAAu8; 3];
        let err = split_response(&buf, 3, &mut payload).expect_err("short");
        assert_eq!(
            err,
            ShortTransfer {
                expected: 4,
                got: 3
            }
        );
        // Untouched, so a caller that logs the buffer sees its own fill and not a
        // decodable value.
        assert_eq!(payload, [0xAA; 3]);
        assert_eq!(err.to_string(), "transfer moved 3 of 4 bytes");
    }

    #[test]
    fn a_status_only_answer_to_a_payload_read_is_short() {
        let buf = [STATUS_DONE; 1];
        let mut payload = [0u8; 3];
        assert_eq!(
            split_response(&buf, 1, &mut payload),
            Err(ShortTransfer {
                expected: 4,
                got: 1
            })
        );
    }
}
