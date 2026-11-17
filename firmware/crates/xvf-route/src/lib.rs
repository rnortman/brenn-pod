//! Writing the output routing of the XVF3800 on a reSpeaker Flex running
//! Seeed's six-channel USB audio image (`2886:0022`), from a Linux host with no
//! USB library.
//!
//! - [`flex_device`]: the USB device behind a sound card named on the command
//!   line, refused unless it is `2886:0022`.
//! - [`ChipWriter`]: a [`ControlTransport`] that reads through
//!   [`xvf3800_usb::ChipReader`] and writes through a usbfs file of its own. It
//!   sends one write only: twelve bytes to `AUDIO_MGR_OP_ALL`.
//! - [`read_op_all`], [`write_op_all`]: the routing register read, and written
//!   then read back.
//! - [`Routed`]: a routing in force, holding what the board held before it, put
//!   back by [`Routed::restore`] or on drop.
//!
//! Every write here is volatile: nothing sends `SAVE_CONFIGURATION`,
//! `CLEAR_CONFIGURATION` or `REBOOT`, so a power cycle returns the board to its
//! stored routing.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use xvf3800_ctrl::{
    AUDIO_MGR_OP_ALL_CMD, AUDIO_MGR_OP_ALL_LABEL, AUDIO_MGR_OP_ALL_READ_LEN, AUDIO_MGR_OP_READ_LEN,
    AUDIO_MGR_RESID, ControlRead, ControlTransport, OP_ALL_SLOTS, OpSlot, STATUS_DONE, USB_RETRY,
    control_read, control_write,
};
use xvf3800_usb::framing::{CONTROL_TIMEOUT, write_setup};
use xvf3800_usb::sysfs::UsbDevice;
use xvf3800_usb::{ChipReader, ReadError, open_reader};

mod usbfs_out;

/// The one USB id this crate writes to: the reSpeaker Flex on Seeed's
/// six-channel USB audio image.
pub const FLEX_IMAGE_ID: (u16, u16) = (0x2886, 0x0022);

/// The twelve bytes of `AUDIO_MGR_OP_ALL`: six (category, source) pairs in the
/// order of [`OP_ALL_SLOTS`].
pub type OpAll = [u8; AUDIO_MGR_OP_ALL_READ_LEN];

/// The highest output category the vendor's User Guide lists (Table 10.4).
pub const MAX_CATEGORY: u8 = 12;

/// The highest source within a category the vendor's User Guide lists
/// (Table 10.4).
pub const MAX_SOURCE: u8 = 5;

/// The USB device behind the one sound card whose name contains `card`.
///
/// No card, several cards, a card not on USB and a device whose id is not
/// [`FLEX_IMAGE_ID`] are all refusals.
pub fn flex_device(card: &str) -> Result<UsbDevice, String> {
    let cards = alsa_pcm::enumerate_cards().map_err(|e| format!("the card list: {e}"))?;
    let card = alsa_pcm::select_one(&cards, card).map_err(|e| e.to_string())?;
    let dir = alsa_pcm::usb_device_dir(card.index, Path::new("/sys"))
        .map_err(|e| format!("{card}: sysfs: {e}"))?
        .ok_or_else(|| format!("{card} is not on USB"))?;
    let device = UsbDevice::read(&dir).map_err(|e| format!("{card}: sysfs: {e}"))?;
    require_flex_image(&device)?;
    Ok(device)
}

/// Refuse a device whose id is not [`FLEX_IMAGE_ID`]: the slots this crate
/// writes are those of that image alone.
pub fn require_flex_image(device: &UsbDevice) -> Result<(), String> {
    if (device.vendor, device.product) == FLEX_IMAGE_ID {
        Ok(())
    } else {
        Err(format!(
            "{device} is not the reSpeaker Flex six-channel USB audio image ({:04x}:{:04x}); \
             nothing is written to it",
            FLEX_IMAGE_ID.0, FLEX_IMAGE_ID.1
        ))
    }
}

/// Why [`open_writer`] produced no writer.
#[derive(Debug)]
pub enum OpenError {
    /// The device is not [`FLEX_IMAGE_ID`].
    NotTheImage(String),
    /// The reader would not open.
    Reader(xvf3800_usb::OpenError),
    /// The writer's own file would not open, or its descriptor would not read.
    Open { path: PathBuf, source: io::Error },
    /// The writer's own file names a different device than sysfs did.
    DeviceChanged {
        path: PathBuf,
        sysfs: (u16, u16),
        node: (u16, u16),
    },
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotTheImage(why) => f.write_str(why),
            Self::Reader(e) => write!(f, "{e}"),
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

impl std::error::Error for OpenError {}

/// Open `device`'s usbfs node under `dev_bus_usb` (`/dev/bus/usb` outside tests)
/// as a [`ChipWriter`].
///
/// The device must be [`FLEX_IMAGE_ID`]. The reader is opened as
/// [`open_reader`] opens it; the writer's own file is opened read-write on the
/// same node, and refused unless the ids in the device descriptor it returns
/// are the device's.
pub fn open_writer(device: &UsbDevice, dev_bus_usb: &Path) -> Result<ChipWriter, OpenError> {
    require_flex_image(device).map_err(OpenError::NotTheImage)?;
    let reader = open_reader(device, dev_bus_usb).map_err(OpenError::Reader)?;
    let path = device.node_path(dev_bus_usb);
    let open = |source| OpenError::Open {
        path: path.clone(),
        source,
    };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(open)?;
    let node = descriptor_ids(&file).map_err(open)?;
    let sysfs = (device.vendor, device.product);
    if node != sysfs {
        return Err(OpenError::DeviceChanged { path, sysfs, node });
    }
    Ok(ChipWriter { reader, file })
}

/// The vendor and product id in the device descriptor a usbfs node returns.
fn descriptor_ids(file: &File) -> io::Result<(u16, u16)> {
    let mut desc = [0u8; 18];
    file.read_exact_at(&mut desc, 0)?;
    if desc[0] != 18 || desc[1] != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "not a device descriptor (bLength {}, bDescriptorType {})",
                desc[0], desc[1]
            ),
        ));
    }
    Ok((
        u16::from_le_bytes([desc[8], desc[9]]),
        u16::from_le_bytes([desc[10], desc[11]]),
    ))
}

/// A failed transfer of [`ChipWriter`].
#[derive(Debug)]
pub enum ChipError {
    /// A read failed, as the reader reports it.
    Read(ReadError),
    /// A write this transport does not send; nothing was sent.
    NotAllowed { resid: u8, cmd: u8, len: usize },
    /// The device stalled the write (`EPIPE`): it declined the request.
    Declined(io::Error),
    /// The write's ioctl failed for any other reason.
    Transfer(io::Error),
    /// The write moved fewer bytes than its payload.
    Short { expected: usize, got: usize },
}

impl fmt::Display for ChipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(e) => write!(f, "{e}"),
            Self::NotAllowed { resid, cmd, len } => write!(
                f,
                "a write of {len} bytes to resid {resid} cmd {cmd} is not one this transport \
                 sends; only {AUDIO_MGR_OP_ALL_READ_LEN} bytes to {AUDIO_MGR_OP_ALL_LABEL}"
            ),
            Self::Declined(e) => write!(f, "the device stalled the write: {e}"),
            Self::Transfer(e) => write!(f, "{e}"),
            Self::Short { expected, got } => write!(f, "write moved {got} of {expected} bytes"),
        }
    }
}

impl std::error::Error for ChipError {}

impl ChipError {
    /// Classify a failed write ioctl: `EPIPE` is a stall, anything else a
    /// transfer that did not complete.
    fn from_ioctl(e: io::Error) -> Self {
        if e.raw_os_error() == Some(libc::EPIPE) {
            Self::Declined(e)
        } else {
            Self::Transfer(e)
        }
    }
}

/// The XVF3800 control transport that can write, over one usbfs node of a
/// [`FLEX_IMAGE_ID`] device.
#[derive(Debug)]
pub struct ChipWriter {
    reader: ChipReader,
    file: File,
}

impl ChipWriter {
    /// The node this writer transfers on.
    pub fn node_path(&self) -> &Path {
        self.reader.node_path()
    }
}

/// Whether a write is the one [`ChipWriter`] sends.
fn allowed(resid: u8, cmd: u8, len: usize) -> bool {
    resid == AUDIO_MGR_RESID && cmd == AUDIO_MGR_OP_ALL_CMD && len == AUDIO_MGR_OP_ALL_READ_LEN
}

impl ControlTransport for ChipWriter {
    type Error = ChipError;

    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        ControlRead::control_read_once(&mut self.reader, resid, cmd, payload, attempt)
            .map_err(ChipError::Read)
    }

    /// Send one write. Refuses, before anything is sent, every write other than
    /// [`AUDIO_MGR_OP_ALL_READ_LEN`] bytes to `AUDIO_MGR_OP_ALL`.
    ///
    /// Returns [`STATUS_DONE`] when the kernel reports the whole payload sent:
    /// that is this transport's claim that the bytes reached the chip, not a
    /// status the chip sent, since the chip returns none for a write. Only a
    /// read-back says whether the chip took them.
    fn control_write_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &[u8],
        _attempt: u32,
    ) -> Result<u8, Self::Error> {
        if !allowed(resid, cmd, payload.len()) {
            return Err(ChipError::NotAllowed {
                resid,
                cmd,
                len: payload.len(),
            });
        }
        let setup = write_setup(resid, cmd, payload.len());
        let sent = usbfs_out::control_out(&self.file, &setup, payload, CONTROL_TIMEOUT)
            .map_err(ChipError::from_ioctl)?;
        if sent != payload.len() {
            return Err(ChipError::Short {
                expected: payload.len(),
                got: sent,
            });
        }
        Ok(STATUS_DONE)
    }

    fn delay_ms(&mut self, ms: u32) {
        std::thread::sleep(Duration::from_millis(u64::from(ms)));
    }

    fn refused(err: &Self::Error) -> bool {
        match err {
            ChipError::Read(e) => <ChipReader as ControlRead>::refused(e),
            ChipError::Declined(_) => true,
            ChipError::NotAllowed { .. } | ChipError::Transfer(_) | ChipError::Short { .. } => {
                false
            }
        }
    }
}

/// A routing read or write that did not end with the board holding what was
/// asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// The routing is outside the range the vendor's guide states; nothing was
    /// sent.
    Invalid(String),
    /// A read got no answer.
    Read {
        register: &'static str,
        error: String,
        refused: bool,
    },
    /// A read was answered with a status other than done.
    Status {
        register: &'static str,
        status: u8,
        attempts: u32,
    },
    /// The read-back after a write is not what was written. `write` is the
    /// write's own failure, if it reported one.
    NotWritten {
        wrote: OpAll,
        read: OpAll,
        write: Option<String>,
    },
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(why) => write!(f, "invalid routing: {why}"),
            Self::Read {
                register,
                error,
                refused,
            } => write!(
                f,
                "{register}: no answer: {error} (declined by the device: {refused})"
            ),
            Self::Status {
                register,
                status,
                attempts,
            } => write!(
                f,
                "{register}: status {status:#04x} after {attempts} attempt(s)"
            ),
            Self::NotWritten { wrote, read, write } => {
                write!(
                    f,
                    "{AUDIO_MGR_OP_ALL_LABEL} reads {} after a write of {}",
                    bytes(read),
                    bytes(wrote)
                )?;
                match write {
                    Some(e) => write!(f, "; the write reported: {e}"),
                    None => write!(f, "; the write reported no error"),
                }
            }
        }
    }
}

impl std::error::Error for RouteError {}

/// Bytes as the command line takes them: decimal, comma-separated.
pub fn bytes(b: &[u8]) -> String {
    b.iter().map(u8::to_string).collect::<Vec<_>>().join(",")
}

/// The command that writes `op_all` to the board behind `card`.
pub fn set_command(card: &str, op_all: &OpAll) -> String {
    format!("xvf-route set --card '{card}' --op-all {}", bytes(op_all))
}

/// Refuse a routing with a category above [`MAX_CATEGORY`] or a source above
/// [`MAX_SOURCE`] in any slot.
pub fn validate(op_all: &OpAll) -> Result<(), RouteError> {
    for (slot, pair) in OP_ALL_SLOTS.iter().zip(op_all.chunks_exact(2)) {
        let (category, source) = (pair[0], pair[1]);
        if category > MAX_CATEGORY {
            return Err(RouteError::Invalid(format!(
                "{}: category {category} is above {MAX_CATEGORY}",
                slot.name
            )));
        }
        if source > MAX_SOURCE {
            return Err(RouteError::Invalid(format!(
                "{}: source {source} is above {MAX_SOURCE}",
                slot.name
            )));
        }
    }
    Ok(())
}

fn read_register<T: ControlRead>(
    transport: &mut T,
    register: &'static str,
    cmd: u8,
    payload: &mut [u8],
) -> Result<(), RouteError>
where
    T::Error: fmt::Display,
{
    match control_read(transport, USB_RETRY, AUDIO_MGR_RESID, cmd, payload) {
        Err(e) => Err(RouteError::Read {
            register,
            error: e.to_string(),
            refused: T::refused(&e),
        }),
        Ok((STATUS_DONE, _)) => Ok(()),
        Ok((status, attempts)) => Err(RouteError::Status {
            register,
            status,
            attempts,
        }),
    }
}

/// Read `AUDIO_MGR_OP_ALL`.
pub fn read_op_all<T: ControlRead>(transport: &mut T) -> Result<OpAll, RouteError>
where
    T::Error: fmt::Display,
{
    let mut op_all = [0u8; AUDIO_MGR_OP_ALL_READ_LEN];
    read_register(
        transport,
        AUDIO_MGR_OP_ALL_LABEL,
        AUDIO_MGR_OP_ALL_CMD,
        &mut op_all,
    )?;
    Ok(op_all)
}

/// Read one slot's own register.
pub fn read_slot<T: ControlRead>(
    transport: &mut T,
    slot: &OpSlot,
) -> Result<[u8; AUDIO_MGR_OP_READ_LEN], RouteError>
where
    T::Error: fmt::Display,
{
    let mut pair = [0u8; AUDIO_MGR_OP_READ_LEN];
    read_register(transport, slot.label, slot.cmd, &mut pair)?;
    Ok(pair)
}

/// What [`write_op_all`] found when the read-back matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    /// The write's own failure, reported although the board reads what was
    /// written.
    pub write_error: Option<String>,
}

/// Validate `op_all`, write it to `AUDIO_MGR_OP_ALL`, read the register back,
/// and fail unless it reads `op_all`.
///
/// The read-back decides: a write whose transfer reported an error but whose
/// read-back matches succeeds, with the error in [`Written::write_error`]; a
/// write the transfer reported done but whose read-back differs fails.
pub fn write_op_all<T: ControlTransport>(
    transport: &mut T,
    op_all: &OpAll,
) -> Result<Written, RouteError>
where
    T::Error: fmt::Display,
{
    validate(op_all)?;
    let write_error = match control_write(
        transport,
        USB_RETRY,
        AUDIO_MGR_RESID,
        AUDIO_MGR_OP_ALL_CMD,
        op_all,
    ) {
        Ok((STATUS_DONE, _)) => None,
        Ok((status, attempts)) => Some(format!("status {status:#04x} after {attempts} attempt(s)")),
        Err(e) => Some(format!(
            "{e} (declined by the device: {})",
            <T as ControlTransport>::refused(&e)
        )),
    };
    let read = read_op_all(transport)?;
    if read == *op_all {
        Ok(Written { write_error })
    } else {
        Err(RouteError::NotWritten {
            wrote: *op_all,
            read,
            write: write_error,
        })
    }
}

/// A routing written to the board, and what it held before.
///
/// [`Routed::restore`] writes the earlier routing back and reports the result;
/// a guard dropped without it does the same. Either way a restore that fails
/// is tried once more, and if that fails too a line beginning
/// `ROUTING NOT RESTORED` is printed to stderr.
pub struct Routed<'a, T: ControlTransport>
where
    T::Error: fmt::Display,
{
    transport: &'a mut T,
    card: String,
    before: OpAll,
    restored: bool,
    warn: fn(&str),
}

fn warn_stderr(line: &str) {
    eprintln!("{line}");
}

impl<'a, T: ControlTransport> Routed<'a, T>
where
    T::Error: fmt::Display,
{
    /// Read the routing in force, then write `op_all` as [`write_op_all`] does.
    /// `card` names the board in the restore command a failure prints.
    ///
    /// A write that fails after the earlier routing was read is restored as the
    /// guard's drop restores it.
    pub fn apply(
        transport: &'a mut T,
        card: &str,
        op_all: &OpAll,
    ) -> Result<(Self, Written), RouteError> {
        validate(op_all)?;
        let before = read_op_all(transport)?;
        let guard = Self {
            transport,
            card: card.to_owned(),
            before,
            restored: false,
            warn: warn_stderr,
        };
        let written = write_op_all(&mut *guard.transport, op_all)?;
        Ok((guard, written))
    }

    /// The routing the board held before [`Routed::apply`] wrote.
    pub fn before(&self) -> &OpAll {
        &self.before
    }

    /// Write the earlier routing back as [`write_op_all`] does, once more if
    /// that fails, and report the last attempt's result. A failure is also
    /// printed as the drop prints it.
    pub fn restore(mut self) -> Result<Written, RouteError> {
        self.put_back()
    }

    /// Up to two attempts at writing the earlier routing back; the line
    /// beginning `ROUTING NOT RESTORED` is printed if both fail.
    fn put_back(&mut self) -> Result<Written, RouteError> {
        self.restored = true;
        let result = write_op_all(&mut *self.transport, &self.before)
            .or_else(|_| write_op_all(&mut *self.transport, &self.before));
        if let Err(e) = &result {
            self.report_not_restored(e);
        }
        result
    }

    fn report_not_restored(&mut self, error: &RouteError) {
        let holds = match read_op_all(&mut *self.transport) {
            Ok(now) => bytes(&now),
            Err(e) => format!("unknown ({e})"),
        };
        (self.warn)(&format!(
            "ROUTING NOT RESTORED: {AUDIO_MGR_OP_ALL_LABEL} holds {holds}; it held {} before; \
             the restore failed: {error}; put it back with: {}",
            bytes(&self.before),
            set_command(&self.card, &self.before)
        ));
    }
}

impl<T: ControlTransport> Drop for Routed<'_, T>
where
    T::Error: fmt::Display,
{
    fn drop(&mut self) {
        if !self.restored {
            let _ = self.put_back();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use xvf3800_ctrl::{
        APPLICATION_SERVICER_RESID, AUDIO_MGR_OP_R_CMD, REBOOT_CMD, REBOOT_WRITE_LEN,
    };

    fn device_dir(root: &Path, vendor: &str, product: &str) -> PathBuf {
        let dir = root.join("3-2.2");
        std::fs::create_dir_all(&dir).expect("device dir");
        for (name, value) in [
            ("busnum", "3"),
            ("devnum", "28"),
            ("idVendor", vendor),
            ("idProduct", product),
            ("bcdDevice", "0100"),
        ] {
            std::fs::write(dir.join(name), format!("{value}\n")).expect("attribute");
        }
        dir
    }

    fn node_file(dev: &Path, vendor: u16, product: u16) {
        let mut d = [0u8; 18];
        d[0] = 18;
        d[1] = 1;
        d[8..10].copy_from_slice(&vendor.to_le_bytes());
        d[10..12].copy_from_slice(&product.to_le_bytes());
        std::fs::create_dir_all(dev.join("003")).expect("bus dir");
        std::fs::write(dev.join("003/028"), d).expect("node");
    }

    fn flex(sys: &Path) -> UsbDevice {
        UsbDevice::read(&device_dir(sys, "2886", "0022")).expect("device")
    }

    #[test]
    fn a_device_other_than_the_flex_image_is_refused() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        assert_eq!(require_flex_image(&flex(sys.path())), Ok(()));
        for (vendor, product) in [("2886", "001a"), ("38fb", "1001"), ("2886", "0023")] {
            let sys = tempfile::tempdir().expect("sys");
            let device = UsbDevice::read(&device_dir(sys.path(), vendor, product)).expect("dev");
            let why = require_flex_image(&device).expect_err("another id");
            assert!(why.contains("nothing is written to it"), "{why}");
            let v = u16::from_str_radix(vendor, 16).unwrap();
            let p = u16::from_str_radix(product, 16).unwrap();
            node_file(dev.path(), v, p);
            let err = open_writer(&device, dev.path()).expect_err("another id");
            assert!(matches!(err, OpenError::NotTheImage(_)), "{err:?}");
        }
    }

    #[test]
    fn a_node_whose_descriptor_names_another_device_is_refused() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        node_file(dev.path(), 0x046d, 0xc52b);
        let err = open_writer(&flex(sys.path()), dev.path()).expect_err("another device");
        // The reader's own check fires first; either check refuses.
        assert!(
            err.to_string()
                .contains("is 046d:c52b, but sysfs named 2886:0022"),
            "{err}"
        );
    }

    fn writer_on_a_regular_file(dev: &Path, sys: &Path) -> ChipWriter {
        node_file(dev, 0x2886, 0x0022);
        open_writer(&flex(sys), dev).expect("writer")
    }

    #[test]
    fn every_write_but_op_all_is_refused_before_any_ioctl() {
        let sys = tempfile::tempdir().expect("sys");
        let dev = tempfile::tempdir().expect("dev");
        let mut writer = writer_on_a_regular_file(dev.path(), sys.path());
        // SAVE_CONFIGURATION and CLEAR_CONFIGURATION, as the vendor's host tool
        // numbers them on the application servicer.
        let refused: [(u8, u8, usize); 6] = [
            (APPLICATION_SERVICER_RESID, 9, 1),
            (APPLICATION_SERVICER_RESID, 10, 1),
            (APPLICATION_SERVICER_RESID, REBOOT_CMD, REBOOT_WRITE_LEN),
            (AUDIO_MGR_RESID, AUDIO_MGR_OP_R_CMD, 2),
            (AUDIO_MGR_RESID, AUDIO_MGR_OP_ALL_CMD, 11),
            (APPLICATION_SERVICER_RESID, AUDIO_MGR_OP_ALL_CMD, 12),
        ];
        for (resid, cmd, len) in refused {
            let payload = vec![0u8; len];
            let err = writer
                .control_write_once(resid, cmd, &payload, 1)
                .expect_err("not allowed");
            // A file that is no USB node fails an ioctl with ENOTTY, so a
            // refusal that reached the ioctl would read as a failed transfer.
            assert!(
                matches!(err, ChipError::NotAllowed { .. }),
                "{resid}/{cmd}/{len}: {err:?}"
            );
            assert!(!<ChipWriter as ControlTransport>::refused(&err));
        }
        let err = writer
            .control_write_once(AUDIO_MGR_RESID, AUDIO_MGR_OP_ALL_CMD, &[0; 12], 1)
            .expect_err("a regular file takes no ioctl");
        match err {
            ChipError::Transfer(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOTTY)),
            other => panic!("expected Transfer, got {other:?}"),
        }
    }

    #[test]
    fn a_stalled_write_is_a_refusal_and_any_other_error_a_failed_transfer() {
        let stalled = ChipError::from_ioctl(io::Error::from_raw_os_error(libc::EPIPE));
        assert!(matches!(stalled, ChipError::Declined(_)), "{stalled:?}");
        assert!(<ChipWriter as ControlTransport>::refused(&stalled));
        for errno in [libc::ETIMEDOUT, libc::ENODEV, libc::EACCES, libc::EPROTO] {
            let e = ChipError::from_ioctl(io::Error::from_raw_os_error(errno));
            assert!(matches!(e, ChipError::Transfer(_)), "{e:?}");
            assert!(!<ChipWriter as ControlTransport>::refused(&e));
        }
        assert!(!<ChipWriter as ControlTransport>::refused(
            &ChipError::Short {
                expected: 12,
                got: 8
            }
        ));
        let read_stall = ChipError::Read(ReadError::from_ioctl(io::Error::from_raw_os_error(
            libc::EPIPE,
        )));
        assert!(<ChipWriter as ControlTransport>::refused(&read_stall));
    }

    const BASELINE: OpAll = [8, 0, 1, 0, 1, 2, 7, 3, 1, 1, 1, 3];

    #[test]
    fn a_category_above_12_or_a_source_above_5_is_refused() {
        assert_eq!(validate(&BASELINE), Ok(()));
        assert_eq!(validate(&[12, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]), Ok(()));
        let mut high_category = BASELINE;
        high_category[6] = 13;
        let err = validate(&high_category).expect_err("category 13");
        assert_eq!(
            err.to_string(),
            "invalid routing: AUDIO_MGR_OP_R_PK0: category 13 is above 12"
        );
        let mut high_source = BASELINE;
        high_source[11] = 6;
        let err = validate(&high_source).expect_err("source 6");
        assert_eq!(
            err.to_string(),
            "invalid routing: AUDIO_MGR_OP_R_PK2: source 6 is above 5"
        );
    }

    #[derive(Debug)]
    struct BoardError(&'static str);

    impl fmt::Display for BoardError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    /// A board's `AUDIO_MGR_OP_ALL`, with scripted misbehaviour.
    struct Board {
        op_all: OpAll,
        /// Writes are reported and not applied.
        ignores_writes: bool,
        /// The next write alone is reported and not applied.
        ignores_next_write: bool,
        /// Every write reports this error, whether or not it is applied.
        write_error: Option<&'static str>,
        /// The status every write is answered with, when it reports no error.
        write_status: u8,
        /// Every read fails with this error.
        read_error: Option<&'static str>,
        /// The status every read is answered with.
        read_status: u8,
        writes: Vec<OpAll>,
    }

    impl Board {
        fn new() -> Self {
            Self {
                op_all: BASELINE,
                ignores_writes: false,
                ignores_next_write: false,
                write_error: None,
                write_status: STATUS_DONE,
                read_error: None,
                read_status: STATUS_DONE,
                writes: Vec::new(),
            }
        }
    }

    impl ControlTransport for Board {
        type Error = BoardError;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, BoardError> {
            assert_eq!((resid, cmd), (AUDIO_MGR_RESID, AUDIO_MGR_OP_ALL_CMD));
            if let Some(e) = self.read_error {
                return Err(BoardError(e));
            }
            payload.copy_from_slice(&self.op_all);
            Ok(self.read_status)
        }

        fn control_write_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &[u8],
            _attempt: u32,
        ) -> Result<u8, BoardError> {
            assert!(allowed(resid, cmd, payload.len()));
            let op_all: OpAll = payload.try_into().unwrap();
            self.writes.push(op_all);
            if !self.ignores_writes && !self.ignores_next_write {
                self.op_all = op_all;
            }
            self.ignores_next_write = false;
            match self.write_error {
                Some(e) => Err(BoardError(e)),
                None => Ok(self.write_status),
            }
        }

        fn delay_ms(&mut self, _ms: u32) {}
    }

    const ROUTED: OpAll = [8, 0, 12, 0, 1, 2, 7, 3, 1, 1, 1, 3];

    #[test]
    fn a_write_whose_read_back_differs_fails_with_both_values() {
        let mut board = Board::new();
        board.ignores_writes = true;
        let err = write_op_all(&mut board, &ROUTED).expect_err("ignored");
        assert_eq!(
            err,
            RouteError::NotWritten {
                wrote: ROUTED,
                read: BASELINE,
                write: None
            }
        );
        assert_eq!(
            err.to_string(),
            "AUDIO_MGR_OP_ALL (resid 35 cmd 23) reads 8,0,1,0,1,2,7,3,1,1,1,3 after a write of \
             8,0,12,0,1,2,7,3,1,1,1,3; the write reported no error"
        );
    }

    #[test]
    fn a_write_whose_read_back_matches_passes_under_a_reported_transfer_error() {
        let mut board = Board::new();
        board.write_error = Some("timed out");
        let written = write_op_all(&mut board, &ROUTED).expect("the board reads what was written");
        assert_eq!(
            written.write_error.as_deref(),
            Some("timed out (declined by the device: false)")
        );
        assert_eq!(board.op_all, ROUTED);
    }

    #[test]
    fn a_write_answered_with_a_status_other_than_done_is_judged_by_its_read_back() {
        let mut board = Board::new();
        board.write_status = 0x03;
        let written = write_op_all(&mut board, &ROUTED).expect("the board reads what was written");
        assert_eq!(
            written.write_error.as_deref(),
            Some("status 0x03 after 1 attempt(s)")
        );

        let mut board = Board::new();
        board.write_status = 0x03;
        board.ignores_writes = true;
        let err = write_op_all(&mut board, &ROUTED).expect_err("ignored");
        assert_eq!(
            err,
            RouteError::NotWritten {
                wrote: ROUTED,
                read: BASELINE,
                write: Some("status 0x03 after 1 attempt(s)".into())
            }
        );
        assert!(
            err.to_string()
                .ends_with("; the write reported: status 0x03 after 1 attempt(s)"),
            "{err}"
        );
    }

    #[test]
    fn a_failed_read_refuses_the_routing_before_anything_is_written() {
        let mut board = Board::new();
        board.read_status = 0x03;
        let err = Routed::apply(&mut board, "Flex", &ROUTED)
            .err()
            .expect("unanswered");
        assert_eq!(
            err,
            RouteError::Status {
                register: AUDIO_MGR_OP_ALL_LABEL,
                status: 0x03,
                attempts: 1
            }
        );
        assert!(board.writes.is_empty());

        let mut board = Board::new();
        board.read_error = Some("timed out");
        let err = Routed::apply(&mut board, "Flex", &ROUTED)
            .err()
            .expect("no answer");
        assert!(
            matches!(err, RouteError::Read { register, refused: false, .. }
                if register == AUDIO_MGR_OP_ALL_LABEL),
            "{err:?}"
        );
        assert!(board.writes.is_empty());
    }

    #[test]
    fn an_invalid_routing_is_never_sent() {
        let mut board = Board::new();
        let mut bad = ROUTED;
        bad[0] = 200;
        assert!(matches!(
            write_op_all(&mut board, &bad),
            Err(RouteError::Invalid(_))
        ));
        assert!(Routed::apply(&mut board, "Flex", &bad).is_err());
        assert!(board.writes.is_empty());
    }

    thread_local! {
        static WARNED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    fn record(line: &str) {
        WARNED.with(|w| w.borrow_mut().push(line.to_owned()));
    }

    fn warned() -> Vec<String> {
        WARNED.with(|w| w.borrow_mut().drain(..).collect())
    }

    #[test]
    fn restore_writes_back_what_the_board_held() {
        let mut board = Board::new();
        let (routed, written) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
        assert_eq!(written.write_error, None);
        assert_eq!(routed.before(), &BASELINE);
        routed.restore().expect("restored");
        assert_eq!(board.op_all, BASELINE);
        assert_eq!(board.writes, [ROUTED, BASELINE]);
    }

    #[test]
    fn a_dropped_guard_restores() {
        let mut board = Board::new();
        {
            let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
            routed.warn = record;
        }
        assert_eq!(board.op_all, BASELINE);
        assert_eq!(board.writes, [ROUTED, BASELINE]);
        assert!(warned().is_empty());
    }

    #[test]
    fn a_restore_whose_read_back_differs_is_reported_with_the_command_that_puts_it_back() {
        let mut board = Board::new();
        let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
        routed.warn = record;
        routed.transport.ignores_writes = true;
        let err = routed.restore().expect_err("ignored");
        assert!(matches!(err, RouteError::NotWritten { .. }), "{err:?}");
        assert_eq!(board.writes, [ROUTED, BASELINE, BASELINE]);
        let lines = warned();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with(
                "ROUTING NOT RESTORED: AUDIO_MGR_OP_ALL (resid 35 cmd 23) holds \
                 8,0,12,0,1,2,7,3,1,1,1,3; it held 8,0,1,0,1,2,7,3,1,1,1,3 before;"
            ),
            "{}",
            lines[0]
        );
        assert!(
            lines[0].ends_with(
                "put it back with: xvf-route set --card 'Flex' --op-all 8,0,1,0,1,2,7,3,1,1,1,3"
            ),
            "{}",
            lines[0]
        );

        let mut board = Board::new();
        {
            let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
            routed.warn = record;
            routed.transport.ignores_writes = true;
        }
        assert_eq!(board.writes, [ROUTED, BASELINE, BASELINE]);
        let lines = warned();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("ROUTING NOT RESTORED: "),
            "{}",
            lines[0]
        );
    }

    #[test]
    fn a_restore_that_fails_once_is_tried_again() {
        let mut board = Board::new();
        let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
        routed.warn = record;
        routed.transport.ignores_next_write = true;
        routed.restore().expect("restored on the second attempt");
        assert_eq!(board.op_all, BASELINE);
        assert_eq!(board.writes, [ROUTED, BASELINE, BASELINE]);
        assert!(warned().is_empty());

        let mut board = Board::new();
        {
            let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
            routed.warn = record;
            routed.transport.ignores_next_write = true;
        }
        assert_eq!(board.op_all, BASELINE);
        assert_eq!(board.writes, [ROUTED, BASELINE, BASELINE]);
        assert!(warned().is_empty());
    }

    #[test]
    fn a_restore_whose_reads_fail_reports_the_routing_held_as_unknown() {
        let mut board = Board::new();
        let (mut routed, _) = Routed::apply(&mut board, "Flex", &ROUTED).expect("applied");
        routed.warn = record;
        routed.transport.read_error = Some("timed out");
        let err = routed.restore().expect_err("no read-back");
        assert!(matches!(err, RouteError::Read { .. }), "{err:?}");
        let lines = warned();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with(
                "ROUTING NOT RESTORED: AUDIO_MGR_OP_ALL (resid 35 cmd 23) holds unknown (\
                 AUDIO_MGR_OP_ALL (resid 35 cmd 23): no answer: timed out"
            ),
            "{}",
            lines[0]
        );
    }

    #[test]
    fn a_failed_apply_restores_what_the_board_held() {
        let mut board = Board::new();
        // The board takes the write but mangles it: the read-back differs, and
        // the guard puts the earlier routing back.
        struct Mangling(Board);
        impl ControlTransport for Mangling {
            type Error = BoardError;
            fn control_read_once(
                &mut self,
                resid: u8,
                cmd: u8,
                payload: &mut [u8],
                attempt: u32,
            ) -> Result<u8, BoardError> {
                ControlTransport::control_read_once(&mut self.0, resid, cmd, payload, attempt)
            }
            fn control_write_once(
                &mut self,
                resid: u8,
                cmd: u8,
                payload: &[u8],
                attempt: u32,
            ) -> Result<u8, BoardError> {
                let status = self.0.control_write_once(resid, cmd, payload, attempt)?;
                if payload != BASELINE {
                    self.0.op_all[0] = 0;
                }
                Ok(status)
            }
            fn delay_ms(&mut self, _ms: u32) {}
        }
        board.op_all = BASELINE;
        let mut mangling = Mangling(board);
        let err = Routed::apply(&mut mangling, "Flex", &ROUTED)
            .err()
            .expect("mangled");
        assert!(matches!(err, RouteError::NotWritten { .. }), "{err:?}");
        assert_eq!(mangling.0.op_all, BASELINE);
        assert_eq!(mangling.0.writes, [ROUTED, BASELINE]);
    }

    #[test]
    fn the_set_command_names_the_card_and_every_byte() {
        assert_eq!(
            set_command("Flex XVF3800 L16K6Ch", &BASELINE),
            "xvf-route set --card 'Flex XVF3800 L16K6Ch' --op-all 8,0,1,0,1,2,7,3,1,1,1,3"
        );
    }
}
