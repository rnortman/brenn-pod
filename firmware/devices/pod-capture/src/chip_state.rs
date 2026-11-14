//! What the capture's device holds, read once before the stream starts: the USB
//! device behind the card and, for an XVF3800, its chip state.
//!
//! The device is found through the card, never by an id search: the card's
//! sysfs `device` link resolves to its USB interface, whose parent is the USB
//! device. The caller holds the card's PCM open throughout, so the card's index
//! cannot pass to another device in between. The node's own descriptor is
//! checked against sysfs at open, and sysfs is read again after the last
//! transfer, so a board replaced during the read is recorded as changed and
//! not as read.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use frame_log::{
    DeviceState, NotAttemptedWhy, RegisterEntry, RegisterReading, RegisterRefusal, UnavailableWhy,
    UsbLocation,
};
use xvf3800_ctrl::state::{ChipState, KIND, Kind, Reading, Refusal, read_state};
use xvf3800_ctrl::{ControlRead, decode_f32, decode_i32};
use xvf3800_usb::OpenError;
use xvf3800_usb::sysfs::UsbDevice;

use crate::seams::Clock;

/// The card's USB device, if sysfs could describe it, and what was read of its
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceReading {
    pub usb: Option<UsbLocation>,
    pub state: DeviceState,
}

/// Read the state of the device behind card `card_index`, resolving it under
/// `sys_root` (`/sys` outside tests) and opening its chip with `open`
/// ([`xvf3800_usb::open_reader`] outside tests).
///
/// Every outcome is a reading: nothing here refuses the capture.
pub fn read_device<R, O, C>(card_index: i32, sys_root: &Path, open: O, clock: &C) -> DeviceReading
where
    R: ControlRead,
    R::Error: std::fmt::Display,
    O: FnOnce(&UsbDevice) -> Result<R, OpenError>,
    C: Clock,
{
    let device = match resolve(card_index, sys_root) {
        Ok(Some(device)) => device,
        Ok(None) => {
            return DeviceReading {
                usb: None,
                state: DeviceState::NotAttempted {
                    why: NotAttemptedWhy::NotUsb,
                    detail: format!("card {card_index}'s device is not on USB"),
                },
            };
        }
        Err(e) => {
            return DeviceReading {
                usb: None,
                state: unavailable(
                    UnavailableWhy::Sysfs,
                    format!("describing card {card_index}'s USB device failed: {e}"),
                    None,
                    None,
                ),
            };
        }
    };
    let usb = Some(location(&device));
    let Some(known) = xvf3800_usb::known_id(device.vendor, device.product) else {
        return DeviceReading {
            usb,
            state: DeviceState::NotAttempted {
                why: NotAttemptedWhy::UnknownUsbId,
                detail: format!("{} is not a device whose state this writer reads", device),
            },
        };
    };
    let kind = Some(KIND);

    let mut reader = match open(&device) {
        Ok(reader) => reader,
        Err(e) => {
            let why = match e {
                OpenError::Open { .. } => UnavailableWhy::Open,
                OpenError::DeviceChanged { .. } => UnavailableWhy::DeviceChanged,
            };
            return DeviceReading {
                usb,
                state: unavailable(why, format!("{known}: {e}"), kind, None),
            };
        }
    };

    let begun = clock.pair();
    // TODO(correctness-chip-read-worst-case-exceeds-brenn-lab-start-timeout): no
    // bound on the whole read and no look at the stop flag between registers; a
    // chip answering retry to every register holds the start about half a minute.
    let read = read_state(&mut reader);
    let ended = clock.pair();
    let began = Some((begun.mono_us, begun.wall_us));
    let chip = match read {
        Ok(chip) => chip,
        Err(failure) => {
            let r = failure.register;
            return DeviceReading {
                usb,
                state: unavailable(
                    UnavailableWhy::Transfer,
                    format!(
                        "reading {} (resid {}, cmd {}) failed: {}",
                        r.name, r.resid, r.cmd, failure.error
                    ),
                    kind,
                    began,
                ),
            };
        }
    };

    if let Some(change) = changed_since(&device, card_index, sys_root) {
        return DeviceReading {
            usb,
            state: unavailable(UnavailableWhy::DeviceChanged, change, kind, began),
        };
    }

    DeviceReading {
        usb,
        state: DeviceState::Read {
            kind: KIND.to_owned(),
            begun_mono_us: begun.mono_us,
            ended_mono_us: ended.mono_us,
            wall_us: begun.wall_us,
            registers: registers(&chip),
        },
    }
}

/// The USB device behind the card, or `None` for a card not on USB.
fn resolve(card_index: i32, sys_root: &Path) -> io::Result<Option<UsbDevice>> {
    alsa_pcm::usb_device_dir(card_index, sys_root)?
        .map(|dir| UsbDevice::read(&dir))
        .transpose()
}

/// Why the device behind the card is no longer `before`, or `None` when it
/// still is.
fn changed_since(before: &UsbDevice, card_index: i32, sys_root: &Path) -> Option<String> {
    let after = match resolve(card_index, sys_root) {
        Ok(Some(after)) => after,
        Ok(None) => return Some(format!("card {card_index}'s device is no longer on USB")),
        Err(e) => return Some(format!("{before} is gone: {e}")),
    };
    (after.dir != before.dir || after.bus != before.bus || after.address != before.address).then(
        || {
            format!(
                "card {card_index} was {before} before the read and {after} after it ({} then {})",
                before.dir.display(),
                after.dir.display()
            )
        },
    )
}

fn location(device: &UsbDevice) -> UsbLocation {
    UsbLocation {
        port: device.port.clone(),
        bus: device.bus,
        address: device.address,
        serial: device.serial.clone(),
        bcd_device: format!("{:04x}", device.bcd_device),
    }
}

fn unavailable(
    why: UnavailableWhy,
    detail: String,
    kind: Option<&str>,
    began: Option<(u64, u64)>,
) -> DeviceState {
    DeviceState::Unavailable {
        why,
        detail,
        kind: kind.map(str::to_owned),
        begun_mono_us: began.map(|(mono, _)| mono),
        wall_us: began.map(|(_, wall)| wall),
    }
}

/// The record's registers: each payload as lowercase hex beside its decoded
/// value, or the chip's refusal.
pub fn registers(chip: &ChipState) -> BTreeMap<String, RegisterEntry> {
    chip.entries()
        .map(|(register, reading)| {
            let reading = match reading {
                Reading::Payload { buf, len } => {
                    let payload = &buf[..*len];
                    RegisterReading::Read {
                        bytes: payload.iter().map(|b| format!("{b:02x}")).collect(),
                        value: decode(register.kind, payload),
                    }
                }
                Reading::Refused(Refusal::Status { status, attempts }) => {
                    RegisterReading::Refused {
                        refused: RegisterRefusal::Status {
                            status: *status,
                            attempts: *attempts,
                        },
                    }
                }
                Reading::Refused(Refusal::Declined) => RegisterReading::Refused {
                    refused: RegisterRefusal::Declined,
                },
            };
            (
                register.name.to_owned(),
                RegisterEntry {
                    resid: register.resid,
                    cmd: register.cmd,
                    reading,
                },
            )
        })
        .collect()
}

/// `payload` decoded by `kind`, as the record's `value` holds it. A scalar of the
/// wrong length is a defect in the register table, whose test holds every scalar
/// to four bytes.
pub fn decode(kind: Kind, payload: &[u8]) -> serde_json::Value {
    let four = || -> &[u8; 4] { payload.try_into().expect("a scalar register is four bytes") };
    match kind {
        Kind::U8s => serde_json::Value::from(payload.to_vec()),
        Kind::Ascii => {
            let end = payload
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(payload.len());
            serde_json::Value::from(String::from_utf8_lossy(&payload[..end]).into_owned())
        }
        Kind::I32 => serde_json::Value::from(decode_i32(four())),
        Kind::F32 => {
            let x = decode_f32(four());
            // The shortest decimal that reads back as this `f32`, so 1.9 is
            // written 1.9 and not as the nearest `f64` to the `f32`.
            format!("{x}")
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::path::PathBuf;
    use std::rc::Rc;

    use alsa_pcm::ClockPair;
    use xvf3800_ctrl::state::{STATE_REGISTER_COUNT, STATE_REGISTERS};
    use xvf3800_ctrl::{STATUS_DONE, STATUS_RETRY, USB_RETRY};

    use super::*;

    /// A clock that advances by one microsecond on each reading.
    #[derive(Clone)]
    struct Ticks(std::sync::Arc<std::sync::atomic::AtomicU64>);

    impl Ticks {
        fn new() -> Self {
            Ticks(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(100)))
        }
    }

    impl Clock for Ticks {
        fn pair(&self) -> ClockPair {
            let t = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ClockPair {
                mono_us: t,
                wall_us: 1_794_000_000_000_000 + t,
            }
        }
    }

    /// One answer of the stand-in chip.
    #[derive(Clone, Copy)]
    enum Answer {
        /// Status done, the payload filled with the register's index.
        Done,
        Status(u8),
        Stall,
        Fail,
    }

    #[derive(Debug)]
    enum FakeError {
        Stall,
        TimedOut,
    }

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                FakeError::Stall => write!(f, "stalled"),
                FakeError::TimedOut => write!(f, "timed out"),
            }
        }
    }

    /// A read-only chip answering each register by its row, running `on_read`
    /// before every transfer.
    struct Chip {
        answers: [Answer; STATE_REGISTER_COUNT],
        on_read: Box<dyn FnMut()>,
        transfers: Rc<Cell<u32>>,
    }

    impl Chip {
        fn answering(answers: [Answer; STATE_REGISTER_COUNT]) -> Self {
            Chip {
                answers,
                on_read: Box::new(|| {}),
                transfers: Rc::new(Cell::new(0)),
            }
        }
    }

    impl ControlRead for Chip {
        type Error = FakeError;

        fn control_read_once(
            &mut self,
            resid: u8,
            cmd: u8,
            payload: &mut [u8],
            _attempt: u32,
        ) -> Result<u8, FakeError> {
            (self.on_read)();
            self.transfers.set(self.transfers.get() + 1);
            let row = STATE_REGISTERS
                .iter()
                .position(|r| r.resid == resid && r.cmd == cmd)
                .expect("a register of the table");
            match self.answers[row] {
                Answer::Done => {
                    payload.fill(row as u8);
                    Ok(STATUS_DONE)
                }
                Answer::Status(s) => Ok(s),
                Answer::Stall => Err(FakeError::Stall),
                Answer::Fail => Err(FakeError::TimedOut),
            }
        }

        fn delay_ms(&mut self, _: u32) {}

        fn refused(err: &FakeError) -> bool {
            matches!(err, FakeError::Stall)
        }
    }

    /// A sysfs tree under `root` with card `index` whose device is interface
    /// `<port>:1.0` of the USB device `port`, carrying `attrs`.
    fn usb_card(root: &Path, index: u32, port: &str, attrs: &[(&str, &str)]) -> PathBuf {
        let device = root.join("devices/usb3").join(port);
        let interface = device.join(format!("{port}:1.0"));
        fs::create_dir_all(&interface).expect("interface");
        for (name, value) in attrs {
            fs::write(device.join(name), format!("{value}\n")).expect("attribute");
        }
        link_card(root, index, &interface);
        device
    }

    fn link_card(root: &Path, index: u32, target: &Path) {
        let card = root.join(format!("class/sound/card{index}"));
        fs::create_dir_all(&card).expect("card");
        let link = card.join("device");
        let _ = fs::remove_file(&link);
        std::os::unix::fs::symlink(target, link).expect("device link");
    }

    const FLEX: [(&str, &str); 6] = [
        ("busnum", "3"),
        ("devnum", "28"),
        ("idVendor", "2886"),
        ("idProduct", "0022"),
        ("bcdDevice", "0100"),
        ("serial", "EXAMPLE-SERIAL"),
    ];

    fn flex_location() -> UsbLocation {
        UsbLocation {
            port: "3-2.2".to_owned(),
            bus: 3,
            address: 28,
            serial: Some("EXAMPLE-SERIAL".to_owned()),
            bcd_device: "0100".to_owned(),
        }
    }

    fn never_opened(_: &UsbDevice) -> Result<Chip, OpenError> {
        panic!("nothing may be opened for this device")
    }

    fn all_done() -> [Answer; STATE_REGISTER_COUNT] {
        [Answer::Done; STATE_REGISTER_COUNT]
    }

    #[test]
    fn a_flex_whose_chip_answers_is_read_whole() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let chip = Chip::answering(all_done());
        let transfers = Rc::clone(&chip.transfers);
        let opened = Cell::new(None);
        let got = read_device(
            2,
            sys.path(),
            |d| {
                opened.set(Some((d.bus, d.address)));
                Ok(chip)
            },
            &Ticks::new(),
        );
        assert_eq!(opened.get(), Some((3, 28)));
        assert_eq!(got.usb, Some(flex_location()));
        assert_eq!(transfers.get(), STATE_REGISTER_COUNT as u32);
        let DeviceState::Read {
            kind,
            begun_mono_us,
            ended_mono_us,
            wall_us,
            registers,
        } = got.state
        else {
            panic!("expected read, got {:?}", got.state);
        };
        assert_eq!(kind, "xvf3800");
        assert_eq!((begun_mono_us, ended_mono_us), (100, 101));
        assert_eq!(wall_us, 1_794_000_000_000_100);
        assert_eq!(registers.len(), STATE_REGISTER_COUNT);
        assert_eq!(
            registers["version"],
            RegisterEntry {
                resid: 48,
                cmd: 0,
                reading: RegisterReading::Read {
                    bytes: "000000".to_owned(),
                    value: serde_json::json!([0, 0, 0]),
                },
            }
        );
        assert_eq!(registers["audio_mgr_op_r"].cmd, 19);
    }

    #[test]
    fn refusals_are_recorded_and_the_read_goes_on() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let mut answers = all_done();
        answers[2] = Answer::Status(3);
        answers[11] = Answer::Stall;
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(Chip::answering(answers)),
            &Ticks::new(),
        );
        let DeviceState::Read { registers, .. } = got.state else {
            panic!("expected read, got {:?}", got.state);
        };
        assert_eq!(
            registers["bld_repo_hash"].reading,
            RegisterReading::Refused {
                refused: RegisterRefusal::Status {
                    status: 3,
                    attempts: 1
                }
            }
        );
        assert_eq!(
            registers["aec_asroutgain"].reading,
            RegisterReading::Refused {
                refused: RegisterRefusal::Declined
            }
        );
        assert!(matches!(
            registers["i2s_dac_dsp_enable"].reading,
            RegisterReading::Read { .. }
        ));
    }

    #[test]
    fn a_retry_status_after_the_budget_is_a_refusal_with_its_attempts() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let mut answers = all_done();
        answers[0] = Answer::Status(STATUS_RETRY);
        let chip = Chip::answering(answers);
        let transfers = Rc::clone(&chip.transfers);
        let got = read_device(2, sys.path(), |_| Ok(chip), &Ticks::new());
        let DeviceState::Read { registers, .. } = got.state else {
            panic!("expected read, got {:?}", got.state);
        };
        let RegisterReading::Refused {
            refused: RegisterRefusal::Status { status, attempts },
        } = registers["version"].reading
        else {
            panic!("expected a status refusal");
        };
        assert_eq!(status, STATUS_RETRY);
        let budget = USB_RETRY.max_retries + 1;
        assert_eq!(attempts, budget);
        assert_eq!(
            transfers.get(),
            budget + STATE_REGISTER_COUNT as u32 - 1,
            "every attempt at version, then one transfer per other register"
        );
    }

    #[test]
    fn a_card_not_on_usb_is_not_attempted() {
        let sys = tempfile::tempdir().expect("sys");
        let pci = sys.path().join("devices/host/0000:00:1f.3");
        fs::create_dir_all(&pci).expect("pci");
        link_card(sys.path(), 0, &pci);
        let got = read_device(0, sys.path(), never_opened, &Ticks::new());
        assert_eq!(got.usb, None);
        assert_eq!(
            got.state,
            DeviceState::NotAttempted {
                why: NotAttemptedWhy::NotUsb,
                detail: "card 0's device is not on USB".to_owned(),
            }
        );
    }

    #[test]
    fn a_usb_card_of_another_id_is_not_attempted_and_keeps_its_location() {
        let sys = tempfile::tempdir().expect("sys");
        let mut attrs = FLEX;
        attrs[2] = ("idVendor", "046d");
        attrs[3] = ("idProduct", "0825");
        usb_card(sys.path(), 1, "1-4", &attrs[..5]);
        let got = read_device(1, sys.path(), never_opened, &Ticks::new());
        let usb = got.usb.expect("location");
        assert_eq!((usb.port.as_str(), usb.serial), ("1-4", None));
        let DeviceState::NotAttempted { why, detail } = got.state else {
            panic!("expected not_attempted, got {:?}", got.state);
        };
        assert_eq!(why, NotAttemptedWhy::UnknownUsbId);
        assert!(detail.starts_with("046d:0825 at port 1-4"), "{detail}");
    }

    #[test]
    fn a_device_sysfs_cannot_describe_is_unavailable_with_no_location() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(
            sys.path(),
            2,
            "3-2.2",
            &[FLEX[0], FLEX[2], FLEX[3], FLEX[4]],
        );
        let got = read_device(2, sys.path(), never_opened, &Ticks::new());
        assert_eq!(got.usb, None);
        let DeviceState::Unavailable {
            why,
            detail,
            kind,
            begun_mono_us,
            ..
        } = got.state
        else {
            panic!("expected unavailable, got {:?}", got.state);
        };
        assert_eq!(why, UnavailableWhy::Sysfs);
        assert!(detail.contains("devnum"), "{detail}");
        assert_eq!((kind, begun_mono_us), (None, None));

        let missing = read_device(5, sys.path(), never_opened, &Ticks::new());
        assert!(matches!(
            missing.state,
            DeviceState::Unavailable {
                why: UnavailableWhy::Sysfs,
                ..
            }
        ));
    }

    #[test]
    fn a_node_that_will_not_open_is_unavailable_with_the_systems_words() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let got = read_device(
            2,
            sys.path(),
            |d| -> Result<Chip, OpenError> {
                Err(OpenError::Open {
                    path: d.node_path(Path::new("/dev/bus/usb")),
                    source: io::Error::from(io::ErrorKind::PermissionDenied),
                })
            },
            &Ticks::new(),
        );
        assert_eq!(got.usb, Some(flex_location()));
        let DeviceState::Unavailable {
            why,
            detail,
            kind,
            begun_mono_us,
            wall_us,
        } = got.state
        else {
            panic!("expected unavailable, got {:?}", got.state);
        };
        assert_eq!(why, UnavailableWhy::Open);
        assert!(detail.contains("/dev/bus/usb/003/028"), "{detail}");
        assert!(detail.contains("permission denied"), "{detail}");
        assert_eq!(kind.as_deref(), Some("xvf3800"));
        assert_eq!((begun_mono_us, wall_us), (None, None));
    }

    #[test]
    fn a_node_naming_another_device_is_a_changed_device() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let got = read_device(
            2,
            sys.path(),
            |d| -> Result<Chip, OpenError> {
                Err(OpenError::DeviceChanged {
                    path: d.node_path(Path::new("/dev/bus/usb")),
                    sysfs: (d.vendor, d.product),
                    node: (0x046d, 0xc52b),
                })
            },
            &Ticks::new(),
        );
        assert!(matches!(
            got.state,
            DeviceState::Unavailable {
                why: UnavailableWhy::DeviceChanged,
                begun_mono_us: None,
                ..
            }
        ));
    }

    #[test]
    fn a_failed_transfer_ends_the_read_and_names_its_register() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let mut answers = all_done();
        answers[4] = Answer::Fail;
        let chip = Chip::answering(answers);
        let transfers = Rc::clone(&chip.transfers);
        let got = read_device(2, sys.path(), |_| Ok(chip), &Ticks::new());
        assert_eq!(transfers.get(), 5);
        assert_eq!(
            got.state,
            DeviceState::Unavailable {
                why: UnavailableWhy::Transfer,
                detail: "reading audio_mgr_op_l (resid 35, cmd 15) failed: timed out".to_owned(),
                kind: Some("xvf3800".to_owned()),
                begun_mono_us: Some(100),
                wall_us: Some(1_794_000_000_000_100),
            }
        );
    }

    #[test]
    fn a_board_replaced_during_the_read_is_a_changed_device() {
        let sys = tempfile::tempdir().expect("sys");
        let device = usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let mut chip = Chip::answering(all_done());
        chip.on_read = Box::new(move || {
            fs::write(device.join("devnum"), "29\n").expect("re-enumerated");
        });
        let got = read_device(2, sys.path(), |_| Ok(chip), &Ticks::new());
        assert_eq!(got.usb, Some(flex_location()));
        let DeviceState::Unavailable {
            why,
            detail,
            begun_mono_us,
            ..
        } = got.state
        else {
            panic!("expected unavailable, got {:?}", got.state);
        };
        assert_eq!(why, UnavailableWhy::DeviceChanged);
        assert!(detail.contains("bus 3 address 29"), "{detail}");
        assert_eq!(begun_mono_us, Some(100));
    }

    #[test]
    fn a_board_gone_after_the_read_is_a_changed_device() {
        let sys = tempfile::tempdir().expect("sys");
        let device = usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let mut chip = Chip::answering(all_done());
        chip.on_read = Box::new(move || {
            let _ = fs::remove_dir_all(&device);
        });
        let got = read_device(2, sys.path(), |_| Ok(chip), &Ticks::new());
        assert!(matches!(
            got.state,
            DeviceState::Unavailable {
                why: UnavailableWhy::DeviceChanged,
                ..
            }
        ));
    }

    #[test]
    fn every_kind_decodes_and_a_non_finite_float_is_null() {
        assert_eq!(decode(Kind::U8s, &[8, 0]), serde_json::json!([8, 0]));
        assert_eq!(
            decode(Kind::Ascii, b"ua-io16-6ch-lin\0\0\0"),
            serde_json::json!("ua-io16-6ch-lin")
        );
        assert_eq!(decode(Kind::Ascii, b"FALSE"), serde_json::json!("FALSE"));
        assert_eq!(
            decode(Kind::I32, &(-32i32).to_le_bytes()),
            serde_json::json!(-32)
        );
        assert_eq!(
            decode(Kind::F32, &[0x33, 0x33, 0xf3, 0x3f]),
            serde_json::json!(1.9)
        );
        assert_eq!(
            decode(Kind::F32, &10.0f32.to_le_bytes()),
            serde_json::json!(10.0)
        );
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(decode(Kind::F32, &bad.to_le_bytes()).is_null(), "{bad}");
        }
    }

    #[test]
    fn a_non_finite_reading_keeps_its_bytes() {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        struct Nan;
        impl ControlRead for Nan {
            type Error = FakeError;
            fn control_read_once(
                &mut self,
                _: u8,
                _: u8,
                payload: &mut [u8],
                _: u32,
            ) -> Result<u8, FakeError> {
                for chunk in payload.chunks_mut(4) {
                    let n = chunk.len();
                    chunk.copy_from_slice(&f32::NAN.to_le_bytes()[..n]);
                }
                Ok(STATUS_DONE)
            }
            fn delay_ms(&mut self, _: u32) {}
            fn refused(_: &FakeError) -> bool {
                false
            }
        }
        let got = read_device(2, sys.path(), |_| Ok(Nan), &Ticks::new());
        let DeviceState::Read { registers, .. } = got.state else {
            panic!("expected read, got {:?}", got.state);
        };
        assert_eq!(
            registers["audio_mgr_ref_gain"].reading,
            RegisterReading::Read {
                bytes: "0000c07f".to_owned(),
                value: serde_json::Value::Null,
            }
        );
    }
}
