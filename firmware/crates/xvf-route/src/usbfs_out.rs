//! One host-to-device control transfer on endpoint 0 of a usbfs node, through
//! the kernel's `USBDEVFS_CONTROL` ioctl.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::time::Duration;

use xvf3800_usb::framing::Setup;

/// The kernel's `struct usbdevfs_ctrltransfer` (`linux/usbdevice_fs.h`): one
/// control transfer, the setup packet's fields in host byte order.
#[repr(C)]
struct CtrlTransfer {
    request_type: u8,
    request: u8,
    value: u16,
    index: u16,
    length: u16,
    /// Milliseconds.
    timeout: u32,
    data: *mut libc::c_void,
}

/// `_IOWR('U', 0, struct usbdevfs_ctrltransfer)` in the generic Linux ioctl
/// encoding, which is the encoding of x86_64 and aarch64.
const USBDEVFS_CONTROL: u32 = {
    const IOC_READ_WRITE: u32 = 3;
    let size = std::mem::size_of::<CtrlTransfer>();
    assert!(size < (1 << 14), "ioctl argument too large to encode");
    (IOC_READ_WRITE << 30) | ((size as u32) << 16) | ((b'U' as u32) << 8)
};

/// Send `data` to the device in one host-to-device control transfer on `file`,
/// an open usbfs node. Returns the number of bytes the kernel reports sent.
///
/// # Panics
/// On a setup packet with the device-to-host bit, or whose length is not
/// `data`'s, before anything is sent.
// TODO(pod-capture-reachy-pod-one-capture-path): this transfer is replaced by the
// shared USB crate's host-to-device transfer once that crate has one.
pub(crate) fn control_out(
    file: &File,
    setup: &Setup,
    data: &[u8],
    timeout: Duration,
) -> io::Result<usize> {
    assert!(
        !setup.is_device_to_host(),
        "usbfs: control_out given a device-to-host setup packet ({setup})"
    );
    assert_eq!(
        setup.length,
        data.len(),
        "usbfs: a {}-byte payload for a {}-byte transfer",
        data.len(),
        setup.length
    );
    let length = u16::try_from(setup.length).expect("usbfs: wLength exceeds 16 bits");
    // The kernel copies from `data` and never writes to it on an OUT transfer;
    // the struct's pointer is mutable only because the struct serves both
    // directions.
    let mut staging = data.to_vec();
    let mut transfer = CtrlTransfer {
        request_type: setup.request_type,
        request: setup.request,
        value: setup.value,
        index: setup.index,
        length,
        timeout: u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX),
        data: staging.as_mut_ptr().cast(),
    };
    // SAFETY: the fd is open for the duration of the call; `transfer` is the
    // kernel's struct by layout and outlives the call; `data` points at
    // `staging`, which is `length` bytes and not otherwise borrowed.
    let ret = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            USBDEVFS_CONTROL as _,
            &mut transfer as *mut CtrlTransfer,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(usize::try_from(ret).expect("a non-negative byte count"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xvf3800_usb::framing::{read_setup, write_setup};

    /// The kernel's documented layout on a 64-bit target and the ioctl number
    /// every 64-bit build of libusb issues.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn the_control_ioctl_is_the_kernels_on_a_64_bit_target() {
        assert_eq!(std::mem::size_of::<CtrlTransfer>(), 24);
        assert_eq!(std::mem::offset_of!(CtrlTransfer, timeout), 8);
        assert_eq!(std::mem::offset_of!(CtrlTransfer, data), 16);
        assert_eq!(USBDEVFS_CONTROL, 0xC018_5500);
    }

    #[test]
    #[should_panic(expected = "control_out given a device-to-host setup packet")]
    fn a_read_setup_panics_before_any_ioctl() {
        let file = tempfile::tempfile().expect("file");
        let _ = control_out(
            &file,
            &read_setup(35, 23, 12),
            &[0; 13],
            Duration::from_secs(1),
        );
    }

    #[test]
    fn a_write_on_a_file_that_is_no_usb_node_is_an_ioctl_error() {
        let file = tempfile::tempfile().expect("file");
        let err = control_out(
            &file,
            &write_setup(35, 23, 12),
            &[0; 12],
            Duration::from_secs(1),
        )
        .expect_err("a regular file takes no usbfs ioctl");
        assert_eq!(err.raw_os_error(), Some(libc::ENOTTY));
    }
}
