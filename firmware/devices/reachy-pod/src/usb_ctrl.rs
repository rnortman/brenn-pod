//! XVF3800 control plane over USB vendor control transfers, moved by libusb.
//!
//! The chip's control protocol is the same one the ESP pod speaks over I2C, so
//! everything above the wire lives in `xvf3800_ctrl`, and the setup-packet framing
//! lives in `xvf3800_usb::framing`, shared with the recorder's reader. What is
//! here is the board search and the transport that hands those packets to libusb.
//!
//! Transfers go to the device recipient on EP0, which needs no claimed interface:
//! the audio interfaces stay bound to the kernel's `snd_usb_audio` while we talk
//! control, which is what lets telemetry poll during capture.

use std::fmt;
use std::time::Duration;

use rusb::{DeviceHandle, GlobalContext};
use xvf3800_ctrl::{CTRL_BUF_CAPACITY, ControlTransport, STATUS_DONE};
use xvf3800_usb::framing::{
    CONTROL_TIMEOUT, ShortTransfer, read_setup, split_response, write_setup,
};

// ── Which board, and which firmware generation ────────────────────────────────

/// Which firmware the board is running, as told by the id it enumerates under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Generation {
    /// `38fb:1001` — the reachy firmware, which is what a current board reports.
    ReachyFirmware,
    /// `2886:001a` — the id of the module the board is built on, reported before
    /// its firmware has been updated. The commands this pipeline issues exist in
    /// both tables, so it is a loud log line rather than a refusal.
    LegacyModule,
}

/// A USB id the mic-array board is known to enumerate under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceId {
    pub vendor: u16,
    pub product: u16,
    pub generation: Generation,
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04x}:{:04x}", self.vendor, self.product)
    }
}

/// The two ids, in the order the factory software searches them: the updated
/// firmware first, the module's own id second.
pub const DEVICE_IDS: [DeviceId; 2] = [
    DeviceId {
        vendor: 0x38fb,
        product: 0x1001,
        generation: Generation::ReachyFirmware,
    },
    DeviceId {
        vendor: 0x2886,
        product: 0x001a,
        generation: Generation::LegacyModule,
    },
];

/// A board found on the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Board {
    pub id: DeviceId,
    pub bus: u8,
    pub address: u8,
}

impl Board {
    /// The device node udev owns for this board — the file whose group and mode
    /// decide whether the application account may open it at all.
    pub fn node_path(&self) -> String {
        format!("/dev/bus/usb/{:03}/{:03}", self.bus, self.address)
    }
}

impl fmt::Display for Board {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.id, self.node_path())
    }
}

/// Every board on the bus matching one of [`DEVICE_IDS`].
///
/// Enumeration reads descriptors only, so it needs no permission on the device
/// node: a board this reports and cannot be opened is a permissions finding, which
/// is exactly the distinction the presence self-test draws.
pub fn find_boards() -> Result<Vec<Board>, rusb::Error> {
    let mut found = Vec::new();
    for device in rusb::devices()?.iter() {
        let desc = match device.device_descriptor() {
            Ok(d) => d,
            // A device that went away between the listing and the descriptor read
            // is not this board's problem; anything else about it is not either.
            Err(e) => {
                log::debug!(
                    "usb: no descriptor from bus {} address {}: {e}",
                    device.bus_number(),
                    device.address()
                );
                continue;
            }
        };
        if let Some(id) = DEVICE_IDS
            .iter()
            .find(|id| id.vendor == desc.vendor_id() && id.product == desc.product_id())
        {
            found.push(Board {
                id: *id,
                bus: device.bus_number(),
                address: device.address(),
            });
        }
    }
    Ok(found)
}

/// Say which firmware generation a board enumerated as.
///
/// The pre-update id is loud: every command this pipeline issues exists in both the
/// vendor and the reachy tables, so it proceeds, but a board that has not been
/// updated is worth seeing in the journal without going looking for it.
pub fn log_generation(board: Board) {
    match board.id.generation {
        Generation::ReachyFirmware => log::info!("xvf3800: {board}, reachy firmware"),
        Generation::LegacyModule => log::warn!(
            "xvf3800: {board} — the pre-update module id, so this board's firmware is old; \
             proceeding, because every command used here exists in both command tables"
        ),
    }
}

/// The one board, or why there is not one.
///
/// The two ids are two firmware generations of the same hardware and never two
/// devices, so exactly one match is the expectation in both directions: none means
/// the board is absent, and more than one means something is on the bus that no
/// revision of this hardware explains. Both are reported with what was actually
/// seen, because "not found" and "found twice" call for opposite investigations.
pub fn select_board(found: &[Board]) -> Result<Board, String> {
    match found {
        [one] => Ok(*one),
        [] => Err(format!(
            "no XVF3800 board on the bus; looked for {}",
            DEVICE_IDS
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(" and ")
        )),
        many => Err(format!(
            "{} XVF3800 boards on the bus, expected one: {}",
            many.len(),
            many.iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// A control transfer that did not deliver an answer.
#[derive(Debug)]
pub enum ControlError {
    /// libusb refused or failed the transfer.
    Transfer(rusb::Error),
    /// The transfer completed with the wrong number of bytes. Reported rather than
    /// zero-filled: a short read would otherwise decode as plausible telemetry.
    ShortTransfer { expected: usize, got: usize },
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transfer(e) => write!(f, "{e}"),
            Self::ShortTransfer { expected, got } => {
                write!(f, "transfer moved {got} of {expected} bytes")
            }
        }
    }
}

impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transfer(e) => Some(e),
            Self::ShortTransfer { .. } => None,
        }
    }
}

impl From<rusb::Error> for ControlError {
    fn from(e: rusb::Error) -> Self {
        Self::Transfer(e)
    }
}

impl From<ShortTransfer> for ControlError {
    fn from(ShortTransfer { expected, got }: ShortTransfer) -> Self {
        Self::ShortTransfer { expected, got }
    }
}

// ── The transport ─────────────────────────────────────────────────────────────

// TODO(pod-capture-reachy-pod-one-capture-path): this is the second code that moves
// control bytes for the one framing in `xvf3800_usb::framing`; it moves into that
// crate, with a host-to-device transfer the recorder cannot reach, once a run of
// `reachy-selftest` on a Reachy can carry it.
/// One open board, ready for control transfers.
///
/// Long-lived by design: the telemetry thread owns it for the life of the process
/// and polls through it while ALSA streams from the same device. The factory
/// software opens a handle per request instead, which this deliberately does not —
/// re-opening 20 times a second is a syscall bill for nothing.
///
/// `xvf3800_usb::ChipReader` moves the same packets with the kernel's ioctl and
/// can only read.
pub struct UsbControl {
    handle: DeviceHandle<GlobalContext>,
    board: Board,
}

impl UsbControl {
    /// Open `board` for control transfers.
    ///
    /// Opening is the permission gate: the udev rule hands the node to the `audio`
    /// group, and an account outside it fails here with `Access` while enumeration
    /// succeeded.
    ///
    /// The board is re-found by bus, address *and* id rather than by id alone, so a
    /// replug that reused the address under a different device cannot be opened as
    /// this one, and a board that went away since enumeration reports `NoDevice`.
    pub fn open(board: Board) -> Result<Self, rusb::Error> {
        let device = rusb::devices()?
            .iter()
            .find(|d| {
                d.bus_number() == board.bus
                    && d.address() == board.address
                    && d.device_descriptor().is_ok_and(|desc| {
                        desc.vendor_id() == board.id.vendor && desc.product_id() == board.id.product
                    })
            })
            .ok_or(rusb::Error::NoDevice)?;
        let handle = device.open()?;
        Ok(Self { handle, board })
    }

    /// The board this transport speaks to.
    pub fn board(&self) -> Board {
        self.board
    }

    /// Which firmware generation the board enumerated as.
    pub fn generation(&self) -> Generation {
        self.board.id.generation
    }
}

impl ControlTransport for UsbControl {
    type Error = ControlError;

    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        let setup = read_setup(resid, cmd, payload.len());
        // Every known register fits; a longer read is a programming error, not a
        // runtime condition, and truncating it would report a plausible answer to
        // a question nobody asked.
        let mut staging = [0u8; CTRL_BUF_CAPACITY + 1];
        assert!(
            setup.length <= staging.len(),
            "usb ctrl: read of {} payload bytes exceeds the {CTRL_BUF_CAPACITY}-byte staging buffer",
            payload.len()
        );
        let staging = &mut staging[..setup.length];
        let transferred = self
            .handle
            .read_control(
                setup.request_type,
                setup.request,
                setup.value,
                setup.index,
                staging,
                CONTROL_TIMEOUT,
            )
            .map_err(|e| {
                log::warn!("usb ctrl: attempt {attempt} read resid={resid} cmd={cmd}: {e}");
                ControlError::Transfer(e)
            })?;
        split_response(staging, transferred, payload).map_err(|e| {
            log::warn!("usb ctrl: attempt {attempt} read resid={resid} cmd={cmd}: {e}");
            ControlError::from(e)
        })
    }

    fn control_write_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &[u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        let setup = write_setup(resid, cmd, payload.len());
        let transferred = self
            .handle
            .write_control(
                setup.request_type,
                setup.request,
                setup.value,
                setup.index,
                payload,
                CONTROL_TIMEOUT,
            )
            .map_err(|e| {
                log::warn!("usb ctrl: attempt {attempt} write resid={resid} cmd={cmd}: {e}");
                ControlError::Transfer(e)
            })?;
        if transferred != payload.len() {
            let e = ControlError::ShortTransfer {
                expected: payload.len(),
                got: transferred,
            };
            log::warn!("usb ctrl: attempt {attempt} write resid={resid} cmd={cmd}: {e}");
            return Err(e);
        }
        // A completed OUT transfer is the whole answer available on this path: the
        // chip returns no status for a write, and neither the vendor tooling nor the
        // factory software reads one back. Reporting DONE is this transport's claim
        // that the bytes reached the chip, not a status the chip sent.
        Ok(STATUS_DONE)
    }

    fn delay_ms(&mut self, ms: u32) {
        std::thread::sleep(Duration::from_millis(u64::from(ms)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn board(vendor: u16, product: u16, generation: Generation) -> Board {
        Board {
            id: DeviceId {
                vendor,
                product,
                generation,
            },
            bus: 1,
            address: 4,
        }
    }

    // ── Errors ────────────────────────────────────────────────────────────────

    #[test]
    fn a_short_transfer_renders_in_the_words_it_always_had() {
        let e = ControlError::from(ShortTransfer {
            expected: 4,
            got: 3,
        });
        assert!(matches!(
            e,
            ControlError::ShortTransfer {
                expected: 4,
                got: 3
            }
        ));
        assert_eq!(e.to_string(), "transfer moved 3 of 4 bytes");
    }

    // ── Board selection ───────────────────────────────────────────────────────

    #[test]
    fn exactly_one_board_is_the_expectation_in_both_directions() {
        let reachy = board(0x38fb, 0x1001, Generation::ReachyFirmware);
        assert_eq!(select_board(&[reachy]).expect("one"), reachy);

        let absent = select_board(&[]).expect_err("none");
        assert!(
            absent.contains("38fb:1001") && absent.contains("2886:001a"),
            "{absent}"
        );

        let legacy = Board {
            address: 5,
            ..board(0x2886, 0x001a, Generation::LegacyModule)
        };
        let both = select_board(&[reachy, legacy]).expect_err("two");
        assert!(
            both.contains('2') && both.contains("expected one"),
            "{both}"
        );
        // Both readings are named: which two were seen is the whole finding.
        assert!(
            both.contains("/dev/bus/usb/001/004") && both.contains("/dev/bus/usb/001/005"),
            "{both}"
        );
    }

    #[test]
    fn a_board_renders_as_its_id_and_the_node_udev_owns() {
        let b = board(0x38fb, 0x1001, Generation::ReachyFirmware);
        assert_eq!(b.node_path(), "/dev/bus/usb/001/004");
        assert_eq!(b.to_string(), "38fb:1001 at /dev/bus/usb/001/004");
        // Three digits, zero-padded, which is how the kernel names them.
        let low = Board {
            bus: 1,
            address: 2,
            ..b
        };
        assert_eq!(low.node_path(), "/dev/bus/usb/001/002");
    }

    #[test]
    fn the_two_known_ids_are_searched_updated_firmware_first() {
        assert_eq!(DEVICE_IDS[0].generation, Generation::ReachyFirmware);
        assert_eq!(DEVICE_IDS[0].to_string(), "38fb:1001");
        assert_eq!(DEVICE_IDS[1].generation, Generation::LegacyModule);
        assert_eq!(DEVICE_IDS[1].to_string(), "2886:001a");
    }
}
