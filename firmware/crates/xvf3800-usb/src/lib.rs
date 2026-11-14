//! The XVF3800's control protocol on USB, for a Linux host, with no USB library.
//!
//! - [`framing`]: the setup packets and the answer split, both directions. Data
//!   only; it sends nothing.
//! - [`usbfs`]: a device node, its descriptor, and one device-to-host control
//!   transfer through the kernel's ioctl.
//! - [`sysfs`]: a USB device's attributes, read from its sysfs directory.
//! - [`ChipReader`]: the transport over a node. It implements
//!   [`xvf3800_ctrl::ControlRead`] and nothing else, so it can be handed to the
//!   read driver and not to the write driver, and nothing in this crate can send a
//!   payload to a device.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use xvf3800_ctrl::{CTRL_BUF_CAPACITY, ControlRead};

pub mod framing;
pub mod sysfs;
pub mod usbfs;

use framing::{CONTROL_TIMEOUT, ShortTransfer, read_setup, split_response};
use sysfs::UsbDevice;
use usbfs::Node;

/// Where the kernel presents usbfs device nodes.
pub const DEV_BUS_USB: &str = "/dev/bus/usb";

/// A USB id under which an XVF3800 answers the control protocol this crate reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownId {
    pub vendor: u16,
    pub product: u16,
    /// What enumerates under this id, in one line.
    pub name: &'static str,
}

impl fmt::Display for KnownId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04x}:{:04x}", self.vendor, self.product)
    }
}

/// The ids a chip-state read is attempted for. This is what the reader knows how
/// to read; it says nothing about what any program drives.
pub const XVF3800_IDS: [KnownId; 3] = [
    KnownId {
        vendor: 0x38fb,
        product: 0x1001,
        name: "XVF3800 on the reachy firmware",
    },
    KnownId {
        vendor: 0x2886,
        product: 0x001a,
        name: "XVF3800 module under its own id, before a firmware update",
    },
    KnownId {
        vendor: 0x2886,
        product: 0x0022,
        name: "reSpeaker Flex XVF3800 on Seeed's six-channel USB audio image",
    },
];

/// The entry of [`XVF3800_IDS`] for `vendor:product`, if there is one.
pub fn known_id(vendor: u16, product: u16) -> Option<&'static KnownId> {
    XVF3800_IDS
        .iter()
        .find(|id| id.vendor == vendor && id.product == product)
}

/// A control read that did not deliver an answer.
#[derive(Debug)]
pub enum ReadError {
    /// The device stalled the transfer (`EPIPE`): it declined the request.
    Declined(io::Error),
    /// The ioctl failed for any other reason: timeout, device gone, no permission.
    Transfer(io::Error),
    /// The transfer completed with the wrong number of bytes.
    Short(ShortTransfer),
}

impl ReadError {
    /// Classify a failed control ioctl: `EPIPE` is how the kernel reports a
    /// stalled control transfer, and everything else is a transfer that did not
    /// complete.
    pub fn from_ioctl(e: io::Error) -> Self {
        if e.raw_os_error() == Some(libc::EPIPE) {
            Self::Declined(e)
        } else {
            Self::Transfer(e)
        }
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Declined(e) => write!(f, "the device stalled the request: {e}"),
            Self::Transfer(e) => write!(f, "{e}"),
            Self::Short(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Declined(e) | Self::Transfer(e) => Some(e),
            Self::Short(s) => Some(s),
        }
    }
}

/// Why [`open_reader`] produced no reader.
#[derive(Debug)]
pub enum OpenError {
    /// The node would not open, or its descriptor could not be read; the system's
    /// own error, with the node's path.
    Open { path: PathBuf, source: io::Error },
    /// The node's own descriptor names a different device than sysfs did: the
    /// address now belongs to another device.
    DeviceChanged {
        path: PathBuf,
        sysfs: (u16, u16),
        node: (u16, u16),
    },
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { path, source } => write!(f, "{}: {source}", path.display()),
            Self::DeviceChanged { path, sysfs, node } => write!(
                f,
                "{} is {:04x}:{:04x}, but sysfs named {:04x}:{:04x} at that address",
                path.display(),
                node.0,
                node.1,
                sysfs.0,
                sysfs.1
            ),
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open { source, .. } => Some(source),
            Self::DeviceChanged { .. } => None,
        }
    }
}

/// Open the usbfs node of `device` under `dev_bus_usb` ([`DEV_BUS_USB`] outside
/// tests) as a [`ChipReader`].
///
/// The node is named by the bus and address sysfs gave, and is refused unless the
/// ids in its own device descriptor equal the ones sysfs gave: an address reused
/// by another device since sysfs was read is caught here.
pub fn open_reader(device: &UsbDevice, dev_bus_usb: &Path) -> Result<ChipReader, OpenError> {
    let path = device.node_path(dev_bus_usb);
    let node = Node::open(&path).map_err(|source| OpenError::Open {
        path: path.clone(),
        source,
    })?;
    let ids = node.descriptor_ids().map_err(|source| OpenError::Open {
        path: path.clone(),
        source,
    })?;
    let sysfs = (device.vendor, device.product);
    if ids != sysfs {
        return Err(OpenError::DeviceChanged {
            path,
            sysfs,
            node: ids,
        });
    }
    Ok(ChipReader { node })
}

// TODO(xvf-control-on-usb-audio-image): this transport can only read. A routing
// write to the chip of a 2886:0022 board needs a transport that can write, and
// whether one exists beside this reader is decided here.
/// The read-only XVF3800 control transport over one usbfs node.
#[derive(Debug)]
pub struct ChipReader {
    node: Node,
}

impl ChipReader {
    /// The node this reader transfers on.
    pub fn node_path(&self) -> &Path {
        self.node.path()
    }
}

impl ControlRead for ChipReader {
    type Error = ReadError;

    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        let setup = read_setup(resid, cmd, payload.len());
        // Every known register fits; a longer read is a programming error, not a
        // runtime condition.
        let mut staging = [0u8; CTRL_BUF_CAPACITY + 1];
        assert!(
            setup.length <= staging.len(),
            "usb ctrl: read of {} payload bytes exceeds the {CTRL_BUF_CAPACITY}-byte staging buffer",
            payload.len()
        );
        let staging = &mut staging[..setup.length];
        let transferred = self.node.control_in(&setup, staging, CONTROL_TIMEOUT);
        answer(transferred, staging, payload).inspect_err(|e| {
            log::warn!("usb ctrl: attempt {attempt} read resid={resid} cmd={cmd}: {e}");
        })
    }

    fn delay_ms(&mut self, ms: u32) {
        std::thread::sleep(Duration::from_millis(u64::from(ms)));
    }

    fn refused(err: &Self::Error) -> bool {
        matches!(err, ReadError::Declined(_))
    }
}

/// The status byte of one control read: `transferred` is the ioctl's outcome
/// and `staging` the buffer it filled. `payload` is written only when the
/// transfer moved the whole answer.
fn answer(
    transferred: io::Result<usize>,
    staging: &[u8],
    payload: &mut [u8],
) -> Result<u8, ReadError> {
    let transferred = transferred.map_err(ReadError::from_ioctl)?;
    split_response(staging, transferred, payload).map_err(ReadError::Short)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sysfs::tests::{FLEX, device_dir};

    #[test]
    fn the_three_known_ids_include_the_flex_image() {
        let ids: Vec<String> = XVF3800_IDS.iter().map(|id| id.to_string()).collect();
        assert_eq!(ids, ["38fb:1001", "2886:001a", "2886:0022"]);
        assert_eq!(known_id(0x2886, 0x0022), Some(&XVF3800_IDS[2]));
        assert_eq!(known_id(0x2886, 0x0023), None);
    }

    #[test]
    fn a_stall_is_a_refusal_and_anything_else_a_failed_transfer() {
        let stalled = ReadError::from_ioctl(io::Error::from_raw_os_error(libc::EPIPE));
        assert!(matches!(stalled, ReadError::Declined(_)), "{stalled:?}");
        assert!(ChipReader::refused(&stalled));
        assert!(
            stalled
                .to_string()
                .starts_with("the device stalled the request: ")
        );

        for errno in [libc::ETIMEDOUT, libc::ENODEV, libc::EACCES, libc::EPROTO] {
            let e = ReadError::from_ioctl(io::Error::from_raw_os_error(errno));
            assert!(matches!(e, ReadError::Transfer(_)), "{e:?}");
            assert!(!ChipReader::refused(&e));
        }
        let short = ReadError::Short(ShortTransfer {
            expected: 4,
            got: 1,
        });
        assert!(!ChipReader::refused(&short));
        assert_eq!(short.to_string(), "transfer moved 1 of 4 bytes");
    }

    #[test]
    fn a_whole_answer_is_its_status_and_payload() {
        let mut payload = [0u8; 3];
        let status = answer(Ok(4), &[0x00, 1, 0, 2], &mut payload).expect("a whole answer");
        assert_eq!(status, 0x00);
        assert_eq!(payload, [1, 0, 2]);
    }

    #[test]
    fn a_short_answer_is_a_short_transfer_and_reaches_no_payload() {
        let mut payload = [0xaa; 3];
        let err = answer(Ok(2), &[0x00, 1, 0, 0], &mut payload).expect_err("short");
        match err {
            ReadError::Short(ShortTransfer { expected, got }) => {
                assert_eq!((expected, got), (4, 2));
            }
            other => panic!("expected Short, got {other:?}"),
        }
        assert_eq!(payload, [0xaa; 3], "the payload is untouched");
        assert!(!ChipReader::refused(&err));
    }

    #[test]
    fn a_failed_ioctl_is_classified_and_reaches_no_payload() {
        let mut payload = [0xaa; 3];
        let stalled = answer(
            Err(io::Error::from_raw_os_error(libc::EPIPE)),
            &[0; 4],
            &mut payload,
        )
        .expect_err("stalled");
        assert!(matches!(stalled, ReadError::Declined(_)), "{stalled:?}");
        let timed_out = answer(
            Err(io::Error::from_raw_os_error(libc::ETIMEDOUT)),
            &[0; 4],
            &mut payload,
        )
        .expect_err("timed out");
        assert!(matches!(timed_out, ReadError::Transfer(_)), "{timed_out:?}");
        assert_eq!(payload, [0xaa; 3]);
    }

    fn node_file(dev: &Path, bus: &str, address: &str, vendor: u16, product: u16) {
        let mut d = [0u8; 18];
        d[0] = 18;
        d[1] = 1;
        d[8..10].copy_from_slice(&vendor.to_le_bytes());
        d[10..12].copy_from_slice(&product.to_le_bytes());
        std::fs::create_dir_all(dev.join(bus)).expect("bus dir");
        std::fs::write(dev.join(bus).join(address), d).expect("node");
    }

    #[test]
    fn a_node_whose_descriptor_matches_sysfs_opens_as_a_reader() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        let device = UsbDevice::read(&device_dir(sys.path(), "3-2.2", &FLEX)).expect("device");
        node_file(dev.path(), "003", "028", 0x2886, 0x0022);
        let reader = open_reader(&device, dev.path()).expect("reader");
        assert_eq!(reader.node_path(), dev.path().join("003/028"));
    }

    #[test]
    fn a_node_whose_descriptor_names_another_device_is_refused() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        let device = UsbDevice::read(&device_dir(sys.path(), "3-2.2", &FLEX)).expect("device");
        node_file(dev.path(), "003", "028", 0x046d, 0xc52b);
        let err = open_reader(&device, dev.path()).expect_err("another device");
        match &err {
            OpenError::DeviceChanged { sysfs, node, .. } => {
                assert_eq!(*sysfs, (0x2886, 0x0022));
                assert_eq!(*node, (0x046d, 0xc52b));
            }
            other => panic!("expected DeviceChanged, got {other:?}"),
        }
        assert!(
            err.to_string()
                .contains("is 046d:c52b, but sysfs named 2886:0022"),
            "{err}"
        );
    }

    #[test]
    fn a_node_that_will_not_open_is_reported_with_its_path_and_the_systems_words() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        let device = UsbDevice::read(&device_dir(sys.path(), "3-2.2", &FLEX)).expect("device");
        let err = open_reader(&device, dev.path()).expect_err("no node");
        match &err {
            OpenError::Open { path, source } => {
                assert_eq!(path, &dev.path().join("003/028"));
                assert_eq!(source.kind(), io::ErrorKind::NotFound);
            }
            other => panic!("expected Open, got {other:?}"),
        }
        assert!(err.to_string().contains("003/028: "), "{err}");
    }
}
