//! One USB device node under `/dev/bus/usb`, driven through the kernel's usbfs
//! interface with no USB library.
//!
//! Two things are done with a node, and nothing else: its device descriptor is
//! read (a plain `read` of the node returns it), and device-to-host control
//! transfers are made on endpoint 0 with the `USBDEVFS_CONTROL` ioctl. There is
//! no host-to-device transfer here.
//!
//! Whether a node may be opened is decided by its mode and owner, which udev sets;
//! an account that may not open it gets the kernel's "Permission denied".

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::framing::Setup;

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

/// `_IOC(dir, type, nr, size)` as the generic Linux ioctl encoding lays it out,
/// which is the encoding of x86_64 and aarch64.
const fn ioc(dir: u32, ty: u8, nr: u8, size: usize) -> u32 {
    const NRSHIFT: u32 = 0;
    const TYPESHIFT: u32 = 8;
    const SIZESHIFT: u32 = 16;
    const DIRSHIFT: u32 = 30;
    assert!(size < (1 << 14), "ioctl argument too large to encode");
    (dir << DIRSHIFT)
        | ((size as u32) << SIZESHIFT)
        | ((ty as u32) << TYPESHIFT)
        | ((nr as u32) << NRSHIFT)
}

/// `_IOC_READ | _IOC_WRITE`.
const IOC_READ_WRITE: u32 = 3;

/// `USBDEVFS_CONTROL`, `_IOWR('U', 0, struct usbdevfs_ctrltransfer)`, computed
/// from this target's layout of the struct.
const USBDEVFS_CONTROL: u32 = ioc(IOC_READ_WRITE, b'U', 0, std::mem::size_of::<CtrlTransfer>());

/// Length of a USB device descriptor.
const DEVICE_DESCRIPTOR_LEN: usize = 18;

/// `bDescriptorType` of a device descriptor.
const DEVICE_DESCRIPTOR_TYPE: u8 = 1;

/// An open usbfs device node.
#[derive(Debug)]
pub struct Node {
    file: File,
    path: PathBuf,
}

impl Node {
    /// Open the node at `path` for control transfers.
    ///
    /// Read-write, as libusb opens it: usbfs refuses the control ioctl on a node
    /// opened read-only. Close-on-exec, as every file std opens is.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// The path this node was opened at.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The vendor and product id in the device descriptor the node itself
    /// returns, in host byte order.
    ///
    /// Read from the node rather than from sysfs, so a caller that resolved the
    /// node's bus and address through sysfs can check that the node is still that
    /// device.
    pub fn descriptor_ids(&self) -> io::Result<(u16, u16)> {
        let mut desc = [0u8; DEVICE_DESCRIPTOR_LEN];
        self.file.read_exact_at(&mut desc, 0)?;
        if usize::from(desc[0]) != DEVICE_DESCRIPTOR_LEN || desc[1] != DEVICE_DESCRIPTOR_TYPE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} does not begin with a device descriptor (bLength {}, bDescriptorType {})",
                    self.path.display(),
                    desc[0],
                    desc[1]
                ),
            ));
        }
        let vendor = u16::from_le_bytes([desc[8], desc[9]]);
        let product = u16::from_le_bytes([desc[10], desc[11]]);
        Ok((vendor, product))
    }

    /// One device-to-host control transfer on endpoint 0, into `buf`. Returns the
    /// number of bytes the device sent.
    ///
    /// # Panics
    /// On a setup packet whose request type lacks the device-to-host bit, before
    /// anything is sent: a write handed to this function is a programming error.
    /// Also when `buf` is shorter than the setup's length or the length does not
    /// fit `wLength`.
    pub fn control_in(
        &self,
        setup: &Setup,
        buf: &mut [u8],
        timeout: Duration,
    ) -> io::Result<usize> {
        assert!(
            setup.is_device_to_host(),
            "usbfs: control_in given a host-to-device setup packet ({setup})"
        );
        assert!(
            buf.len() >= setup.length,
            "usbfs: a {}-byte buffer for a {}-byte transfer",
            buf.len(),
            setup.length
        );
        let length = u16::try_from(setup.length).expect("usbfs: wLength exceeds 16 bits");
        let mut transfer = CtrlTransfer {
            request_type: setup.request_type,
            request: setup.request,
            value: setup.value,
            index: setup.index,
            length,
            timeout: u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX),
            data: buf.as_mut_ptr().cast(),
        };
        // SAFETY: the fd is this node's, open for the life of `self`; `transfer`
        // is the kernel's struct by layout and outlives the call; `data` points at
        // `buf`, which is at least `length` bytes and is not otherwise borrowed
        // while the ioctl writes into it.
        let ret = unsafe {
            libc::ioctl(
                self.file.as_raw_fd(),
                USBDEVFS_CONTROL as _,
                &mut transfer as *mut CtrlTransfer,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(usize::try_from(ret).expect("a non-negative byte count"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{read_setup, write_setup};

    /// The kernel's documented layout on a 64-bit target: eight bytes of setup
    /// packet, a four-byte timeout, padding, an eight-byte pointer; and the ioctl
    /// number every 64-bit build of libusb issues.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn the_control_ioctl_is_the_kernels_on_a_64_bit_target() {
        assert_eq!(std::mem::size_of::<CtrlTransfer>(), 24);
        assert_eq!(std::mem::offset_of!(CtrlTransfer, timeout), 8);
        assert_eq!(std::mem::offset_of!(CtrlTransfer, data), 16);
        assert_eq!(USBDEVFS_CONTROL, 0xC018_5500);
    }

    fn descriptor(vendor: u16, product: u16) -> [u8; DEVICE_DESCRIPTOR_LEN] {
        let mut d = [0u8; DEVICE_DESCRIPTOR_LEN];
        d[0] = 18;
        d[1] = 1;
        d[8..10].copy_from_slice(&vendor.to_le_bytes());
        d[10..12].copy_from_slice(&product.to_le_bytes());
        d
    }

    #[test]
    fn the_descriptor_ids_are_read_little_endian_from_the_node() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("028");
        let mut bytes = descriptor(0x2886, 0x0022).to_vec();
        // A node returns its configuration descriptors after the device one.
        bytes.extend_from_slice(&[9, 2, 0, 0]);
        std::fs::write(&path, bytes).expect("node");
        let node = Node::open(&path).expect("open");
        assert_eq!(node.descriptor_ids().expect("ids"), (0x2886, 0x0022));
        assert_eq!(node.path(), path);
    }

    #[test]
    fn a_node_that_does_not_begin_with_a_device_descriptor_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("node");
        let mut bytes = descriptor(0x2886, 0x0022);
        bytes[1] = 2;
        std::fs::write(&path, bytes).expect("node");
        let err = Node::open(&path)
            .expect("open")
            .descriptor_ids()
            .expect_err("not a device descriptor");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("bDescriptorType 2"), "{err}");

        std::fs::write(&path, [18u8, 1, 0]).expect("short node");
        let err = Node::open(&path)
            .expect("open")
            .descriptor_ids()
            .expect_err("short");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn a_missing_node_fails_to_open_with_the_systems_words() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = Node::open(&dir.path().join("absent")).expect_err("absent");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    #[should_panic(expected = "control_in given a host-to-device setup packet")]
    fn a_write_setup_panics_before_any_ioctl() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("node");
        std::fs::write(&path, descriptor(0x2886, 0x0022)).expect("node");
        let node = Node::open(&path).expect("open");
        let mut buf = [0u8; 8];
        let _ = node.control_in(&write_setup(20, 0, 6), &mut buf, Duration::from_secs(1));
    }

    #[test]
    fn a_read_on_a_file_that_is_no_usb_node_is_an_ioctl_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("node");
        std::fs::write(&path, descriptor(0x2886, 0x0022)).expect("node");
        let node = Node::open(&path).expect("open");
        let mut buf = [0u8; 4];
        let err = node
            .control_in(&read_setup(48, 0, 3), &mut buf, Duration::from_secs(1))
            .expect_err("a regular file takes no usbfs ioctl");
        assert_eq!(err.raw_os_error(), Some(libc::ENOTTY));
    }
}
