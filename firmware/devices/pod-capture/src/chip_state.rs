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
//!
//! The read is bounded: no transfer begins once [`STATE_READ_BUDGET`] has
//! passed since the reading taken directly before the first, and none begins
//! once the stop flag is set. A transfer in flight is not cut; the transport's
//! own timeout bounds it.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use frame_log::{
    CutShortWhy, DeviceState, NotAttemptedWhy, RegisterEntry, RegisterReading, RegisterRefusal,
    UnavailableWhy, UsbLocation,
};
use xvf3800_ctrl::state::{KIND, Kind, Reading, Refusal, Register, read_state};
use xvf3800_ctrl::{ControlRead, decode_f32, decode_i32};
use xvf3800_usb::sysfs::UsbDevice;
use xvf3800_usb::{OpenError, ReadError};

use crate::seams::Clock;

/// How long transfers of the state read may keep beginning, on
/// `CLOCK_MONOTONIC`, from the reading taken directly before the first. With
/// the transport's one-second timeout on the last transfer begun and one retry
/// delay, the read ends within 3.01 s of that reading.
pub const STATE_READ_BUDGET: Duration = Duration::from_secs(2);

/// Whether a reader's error is a transfer the system reports as interrupted by
/// a signal, as against a transfer that failed for its own reason.
pub trait Interrupted {
    fn interrupted(&self) -> bool;
}

impl Interrupted for ReadError {
    fn interrupted(&self) -> bool {
        matches!(self, ReadError::Transfer(e) if e.raw_os_error() == Some(libc::EINTR))
    }
}

/// Why the gated reader made no transfer, or why a transfer it forwarded is
/// not the device's failure.
#[derive(Debug)]
enum Cut<E> {
    /// The budget had passed.
    Deadline,
    /// The stop flag was set; `interrupted` holds the error of a transfer the
    /// stop's signal interrupted in flight.
    Stopped { interrupted: Option<E> },
}

/// The gated reader's error: the reader's own, or a cut.
#[derive(Debug)]
enum Gated<E> {
    Reader(E),
    Cut(Cut<E>),
}

/// The last transfer forwarded for the register in progress: its attempt
/// number and the status it answered.
#[derive(Debug, Clone, Copy)]
struct Forwarded {
    register: (u8, u8),
    attempt: u32,
    status: u8,
}

/// The opened reader behind the stop flag and the budget: each transfer is
/// asked of both before it is forwarded.
struct Gate<'a, R, C> {
    reader: R,
    stop: &'a AtomicBool,
    clock: &'a C,
    begun_mono_us: u64,
    budget_us: u64,
    last: Option<Forwarded>,
}

impl<R, C> ControlRead for Gate<'_, R, C>
where
    R: ControlRead,
    R::Error: Interrupted,
    C: Clock,
{
    type Error = Gated<R::Error>;

    fn control_read_once(
        &mut self,
        resid: u8,
        cmd: u8,
        payload: &mut [u8],
        attempt: u32,
    ) -> Result<u8, Self::Error> {
        if self.last.is_some_and(|f| f.register != (resid, cmd)) {
            self.last = None;
        }
        if self.stop.load(Ordering::SeqCst) {
            return Err(Gated::Cut(Cut::Stopped { interrupted: None }));
        }
        let elapsed = self.clock.pair().mono_us.saturating_sub(self.begun_mono_us);
        if elapsed >= self.budget_us {
            return Err(Gated::Cut(Cut::Deadline));
        }
        match self.reader.control_read_once(resid, cmd, payload, attempt) {
            Ok(status) => {
                self.last = Some(Forwarded {
                    register: (resid, cmd),
                    attempt,
                    status,
                });
                Ok(status)
            }
            Err(e) if e.interrupted() && self.stop.load(Ordering::SeqCst) => {
                Err(Gated::Cut(Cut::Stopped {
                    interrupted: Some(e),
                }))
            }
            Err(e) => Err(Gated::Reader(e)),
        }
    }

    fn delay_ms(&mut self, ms: u32) {
        self.reader.delay_ms(ms);
    }

    fn refused(err: &Self::Error) -> bool {
        match err {
            Gated::Reader(e) => R::refused(e),
            Gated::Cut(_) => false,
        }
    }
}

/// The record's `detail` for a read cut at `register`.
fn cut_detail<E: fmt::Display>(
    cut: &Cut<E>,
    register: &Register,
    last: Option<Forwarded>,
) -> String {
    let budget = STATE_READ_BUDGET.as_secs_f64();
    let reason = match cut {
        Cut::Deadline => format!("the state read's {budget} s had passed"),
        Cut::Stopped { interrupted: None } => "the recorder was told to stop".to_owned(),
        Cut::Stopped {
            interrupted: Some(e),
        } => format!("the recorder was told to stop, and the signal interrupted a transfer: {e}"),
    };
    let transfers = match last {
        Some(f) if f.register == (register.resid, register.cmd) => format!(
            "after {} transfer{} answering status {}",
            f.attempt,
            if f.attempt == 1 { "" } else { "s" },
            f.status
        ),
        _ => "with no transfer answered for it".to_owned(),
    };
    format!(
        "{reason}; cut at {} (resid {}, cmd {}) {transfers}",
        register.name, register.resid, register.cmd
    )
}

/// The card's USB device, if sysfs could describe it, and what was read of its
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceReading {
    pub usb: Option<UsbLocation>,
    pub state: DeviceState,
}

/// Read the state of the device behind card `card_index`, resolving it under
/// `sys_root` (`/sys` outside tests) and opening its chip with `open`
/// ([`xvf3800_usb::open_reader`] outside tests). No transfer begins once `stop`
/// is set or [`STATE_READ_BUDGET`] has passed; a read cut either way is
/// `cut_short` with the registers it completed.
///
/// Every outcome is a reading: nothing here refuses the capture.
pub fn read_device<R, O, C>(
    card_index: i32,
    sys_root: &Path,
    open: O,
    clock: &C,
    stop: &AtomicBool,
) -> DeviceReading
where
    R: ControlRead,
    R::Error: fmt::Display + Interrupted,
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

    let reader = match open(&device) {
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
    let mut gate = Gate {
        reader,
        stop,
        clock,
        begun_mono_us: begun.mono_us,
        budget_us: u64::try_from(STATE_READ_BUDGET.as_micros()).expect("the budget fits u64"),
        last: None,
    };
    let read = read_state(&mut gate);
    let ended = clock.pair();
    let began = Some((begun.mono_us, begun.wall_us));
    let state = match read {
        Ok(chip) => DeviceState::Read {
            kind: KIND.to_owned(),
            begun_mono_us: begun.mono_us,
            ended_mono_us: ended.mono_us,
            wall_us: begun.wall_us,
            registers: registers(chip.entries()),
        },
        Err(failure) => {
            let r = failure.register;
            match failure.error {
                Gated::Reader(error) => {
                    return DeviceReading {
                        usb,
                        state: unavailable(
                            UnavailableWhy::Transfer,
                            format!(
                                "reading {} (resid {}, cmd {}) failed: {error}",
                                r.name, r.resid, r.cmd
                            ),
                            kind,
                            began,
                        ),
                    };
                }
                Gated::Cut(cut) => DeviceState::CutShort {
                    why: match cut {
                        Cut::Deadline => CutShortWhy::Deadline,
                        Cut::Stopped { .. } => CutShortWhy::Stopped,
                    },
                    at: r.name.to_owned(),
                    detail: cut_detail(&cut, r, gate.last),
                    kind: KIND.to_owned(),
                    begun_mono_us: begun.mono_us,
                    ended_mono_us: ended.mono_us,
                    wall_us: begun.wall_us,
                    registers: registers(failure.completed.entries()),
                },
            }
        }
    };

    if let Some(change) = changed_since(&device, card_index, sys_root) {
        return DeviceReading {
            usb,
            state: unavailable(UnavailableWhy::DeviceChanged, change, kind, began),
        };
    }

    DeviceReading { usb, state }
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
fn registers<'a>(
    entries: impl Iterator<Item = (&'static Register, &'a Reading)>,
) -> BTreeMap<String, RegisterEntry> {
    entries
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
    use std::cell::{Cell, RefCell};
    use std::fs;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

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

    /// A clock read without advancing it, moved only by whoever holds it: the
    /// stand-in chip, by each transfer's cost and each retry delay.
    #[derive(Clone)]
    struct FakeTime(Arc<AtomicU64>);

    /// Where fake time starts, in microseconds.
    const T0: u64 = 1_000;

    impl FakeTime {
        fn new() -> Self {
            FakeTime(Arc::new(AtomicU64::new(T0)))
        }

        fn now(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }

        fn advance(&self, us: u64) {
            self.0.fetch_add(us, Ordering::SeqCst);
        }
    }

    impl Clock for FakeTime {
        fn pair(&self) -> ClockPair {
            let t = self.now();
            ClockPair {
                mono_us: t,
                wall_us: 1_794_000_000_000_000 + t,
            }
        }
    }

    const BUDGET_US: u64 = 2_000_000;

    /// One answer of the stand-in chip.
    #[derive(Clone, Copy)]
    enum Answer {
        /// Status done, the payload filled with the register's index.
        Done,
        Status(u8),
        Stall,
        Fail,
        /// A transfer the system reports as interrupted by a signal.
        Interrupted,
    }

    #[derive(Debug)]
    enum FakeError {
        Stall,
        TimedOut,
        Interrupted,
    }

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                FakeError::Stall => write!(f, "stalled"),
                FakeError::TimedOut => write!(f, "timed out"),
                FakeError::Interrupted => write!(f, "Interrupted system call"),
            }
        }
    }

    impl Interrupted for FakeError {
        fn interrupted(&self) -> bool {
            matches!(self, FakeError::Interrupted)
        }
    }

    /// A read-only chip answering each register by its row, running `on_read`
    /// before every transfer. With `time`, each transfer advances it by `cost_us`
    /// and each retry delay by the delay.
    struct Chip {
        answers: [Answer; STATE_REGISTER_COUNT],
        on_read: Box<dyn FnMut()>,
        transfers: Rc<Cell<u32>>,
        time: Option<(FakeTime, u64)>,
        /// The fake time at which each transfer began.
        began: Rc<RefCell<Vec<u64>>>,
    }

    impl Chip {
        fn answering(answers: [Answer; STATE_REGISTER_COUNT]) -> Self {
            Chip {
                answers,
                on_read: Box::new(|| {}),
                transfers: Rc::new(Cell::new(0)),
                time: None,
                began: Rc::new(RefCell::new(Vec::new())),
            }
        }

        fn timed(answers: [Answer; STATE_REGISTER_COUNT], time: &FakeTime, cost_us: u64) -> Self {
            Chip {
                time: Some((time.clone(), cost_us)),
                ..Chip::answering(answers)
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
            if let Some((time, cost_us)) = &self.time {
                self.began.borrow_mut().push(time.now());
                time.advance(*cost_us);
            }
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
                Answer::Interrupted => Err(FakeError::Interrupted),
            }
        }

        fn delay_ms(&mut self, ms: u32) {
            if let Some((time, _)) = &self.time {
                time.advance(u64::from(ms) * 1_000);
            }
        }

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
            &AtomicBool::new(false),
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
        assert_eq!(
            (begun_mono_us, ended_mono_us),
            (100, 101 + STATE_REGISTER_COUNT as u64),
            "one clock reading before the read, one at each transfer's gate, one after"
        );
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
            &AtomicBool::new(false),
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
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(chip),
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
        let got = read_device(
            0,
            sys.path(),
            never_opened,
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
        let got = read_device(
            1,
            sys.path(),
            never_opened,
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
        let got = read_device(
            2,
            sys.path(),
            never_opened,
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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

        let missing = read_device(
            5,
            sys.path(),
            never_opened,
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
            &AtomicBool::new(false),
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
            &AtomicBool::new(false),
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
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(chip),
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(chip),
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(chip),
            &Ticks::new(),
            &AtomicBool::new(false),
        );
        assert!(matches!(
            got.state,
            DeviceState::Unavailable {
                why: UnavailableWhy::DeviceChanged,
                ..
            }
        ));
    }

    /// The fields of a `cut_short` state, or a panic naming what came instead.
    struct CutShortState {
        why: CutShortWhy,
        at: String,
        detail: String,
        begun_mono_us: u64,
        ended_mono_us: u64,
        registers: BTreeMap<String, RegisterEntry>,
    }

    fn cut_short(state: DeviceState) -> CutShortState {
        let DeviceState::CutShort {
            why,
            at,
            detail,
            kind,
            begun_mono_us,
            ended_mono_us,
            wall_us,
            registers,
        } = state
        else {
            panic!("expected cut_short, got {state:?}");
        };
        assert_eq!(kind, "xvf3800");
        assert_eq!(wall_us, 1_794_000_000_000_000 + begun_mono_us);
        assert!(!registers.contains_key(&at), "{at} is in registers");
        CutShortState {
            why,
            at,
            detail,
            begun_mono_us,
            ended_mono_us,
            registers,
        }
    }

    /// The registers' names in table order; every key is a register of the
    /// table.
    fn names(registers: &BTreeMap<String, RegisterEntry>) -> Vec<&'static str> {
        let names: Vec<_> = STATE_REGISTERS
            .iter()
            .map(|r| r.name)
            .filter(|n| registers.contains_key(*n))
            .collect();
        assert_eq!(names.len(), registers.len(), "a key outside the table");
        names
    }

    fn first(n: usize) -> Vec<&'static str> {
        STATE_REGISTERS[..n].iter().map(|r| r.name).collect()
    }

    /// Read card 2, a Flex, from `chip` with `time` as the clock.
    fn read_timed(chip: Chip, time: &FakeTime, stop: &AtomicBool) -> DeviceReading {
        let sys = tempfile::tempdir().expect("sys");
        usb_card(sys.path(), 2, "3-2.2", &FLEX);
        read_device(2, sys.path(), |_| Ok(chip), time, stop)
    }

    #[test]
    fn a_chip_answering_retry_throughout_is_cut_by_the_deadline() {
        let time = FakeTime::new();
        let chip = Chip::timed(
            [Answer::Status(STATUS_RETRY); STATE_REGISTER_COUNT],
            &time,
            0,
        );
        let began = Rc::clone(&chip.began);
        let got = cut_short(read_timed(chip, &time, &AtomicBool::new(false)).state);
        assert_eq!(got.why, CutShortWhy::Deadline);
        assert_eq!(got.at, "bld_msg");
        assert_eq!(names(&got.registers), ["version"]);
        assert_eq!(
            got.registers["version"].reading,
            RegisterReading::Refused {
                refused: RegisterRefusal::Status {
                    status: STATUS_RETRY,
                    attempts: USB_RETRY.max_retries + 1
                }
            }
        );
        let began = began.borrow();
        assert!(
            began.iter().all(|&t| t - T0 < BUDGET_US),
            "a transfer began at or after the budget"
        );
        assert_eq!(began.len(), 101 + 100, "version's 101, then bld_msg's 100");
        assert_eq!(
            got.detail,
            format!(
                "the state read's 2 s had passed; cut at bld_msg (resid 48, cmd 1) after 100 \
                 transfers answering status {STATUS_RETRY}"
            )
        );
    }

    #[test]
    fn slow_answers_are_cut_within_one_transfer_of_the_budget() {
        let time = FakeTime::new();
        let cost = 400_000;
        let chip = Chip::timed(all_done(), &time, cost);
        let transfers = Rc::clone(&chip.transfers);
        let got = cut_short(read_timed(chip, &time, &AtomicBool::new(false)).state);
        assert_eq!(got.why, CutShortWhy::Deadline);
        assert_eq!(names(&got.registers), first(5));
        assert_eq!(got.at, STATE_REGISTERS[5].name);
        assert_eq!(transfers.get(), 5);
        assert_eq!(got.begun_mono_us, T0);
        assert!(got.ended_mono_us - got.begun_mono_us <= BUDGET_US + cost);
        assert!(
            got.detail.ends_with(
                "cut at audio_mgr_op_r (resid 35, cmd 19) with no transfer answered for it"
            ),
            "{}",
            got.detail
        );
    }

    #[test]
    fn retry_answers_each_taking_the_timeout_end_within_the_stated_bound() {
        let time = FakeTime::new();
        let chip = Chip::timed(
            [Answer::Status(STATUS_RETRY); STATE_REGISTER_COUNT],
            &time,
            1_000_000,
        );
        let transfers = Rc::clone(&chip.transfers);
        let got = cut_short(read_timed(chip, &time, &AtomicBool::new(false)).state);
        assert_eq!(got.why, CutShortWhy::Deadline);
        assert_eq!(got.at, "version");
        assert!(got.registers.is_empty());
        assert_eq!(transfers.get(), 2);
        assert!(got.ended_mono_us - got.begun_mono_us <= 3_010_000);
        assert!(time.now() - T0 <= 3_010_000);
    }

    #[test]
    fn a_stop_after_some_transfers_keeps_what_was_read_and_asks_no_more() {
        for k in [1u32, 4, 16] {
            let stop = Arc::new(AtomicBool::new(false));
            let time = FakeTime::new();
            let mut chip = Chip::timed(all_done(), &time, 9_000);
            let transfers = Rc::clone(&chip.transfers);
            let seen = Rc::clone(&chip.transfers);
            let flag = Arc::clone(&stop);
            chip.on_read = Box::new(move || {
                if seen.get() + 1 == k {
                    flag.store(true, Ordering::SeqCst);
                }
            });
            let got = cut_short(read_timed(chip, &time, &stop).state);
            assert_eq!(got.why, CutShortWhy::Stopped, "k={k}");
            assert_eq!(names(&got.registers), first(k as usize), "k={k}");
            assert_eq!(got.at, STATE_REGISTERS[k as usize].name, "k={k}");
            assert_eq!(transfers.get(), k, "k={k}");
            assert!(
                got.detail
                    .starts_with("the recorder was told to stop; cut at"),
                "{}",
                got.detail
            );
        }
    }

    #[test]
    fn a_stop_before_the_read_makes_no_transfer() {
        let time = FakeTime::new();
        let chip = Chip::timed(all_done(), &time, 9_000);
        let transfers = Rc::clone(&chip.transfers);
        let got = cut_short(read_timed(chip, &time, &AtomicBool::new(true)).state);
        assert_eq!(got.why, CutShortWhy::Stopped);
        assert_eq!(got.at, "version");
        assert!(got.registers.is_empty());
        assert_eq!(transfers.get(), 0);
        assert_eq!(got.begun_mono_us, got.ended_mono_us);
    }

    #[test]
    fn the_stop_is_reported_when_the_deadline_has_also_passed() {
        let stop = Arc::new(AtomicBool::new(false));
        let time = FakeTime::new();
        let mut chip = Chip::timed(all_done(), &time, BUDGET_US);
        let flag = Arc::clone(&stop);
        chip.on_read = Box::new(move || flag.store(true, Ordering::SeqCst));
        let got = cut_short(read_timed(chip, &time, &stop).state);
        assert_eq!(got.why, CutShortWhy::Stopped);
        assert_eq!(names(&got.registers), ["version"]);
    }

    /// A chip whose register `row` answers `answer`, and whose hook sets the
    /// stop flag at that register's transfer when `set_stop`.
    fn failing_at(row: usize, answer: Answer, set_stop: bool) -> (DeviceState, u32) {
        let stop = Arc::new(AtomicBool::new(false));
        let time = FakeTime::new();
        let mut answers = all_done();
        answers[row] = answer;
        let mut chip = Chip::timed(answers, &time, 9_000);
        let transfers = Rc::clone(&chip.transfers);
        let seen = Rc::clone(&chip.transfers);
        let flag = Arc::clone(&stop);
        chip.on_read = Box::new(move || {
            if set_stop && seen.get() == row as u32 {
                flag.store(true, Ordering::SeqCst);
            }
        });
        let state = read_timed(chip, &time, &stop).state;
        (state, transfers.get())
    }

    #[test]
    fn a_transfer_interrupted_by_the_stop_is_a_stop() {
        let (state, transfers) = failing_at(3, Answer::Interrupted, true);
        assert_eq!(transfers, 4);
        let got = cut_short(state);
        assert_eq!(got.why, CutShortWhy::Stopped);
        assert_eq!(got.at, "bld_modified");
        assert_eq!(names(&got.registers), first(3));
        assert_eq!(
            got.detail,
            "the recorder was told to stop, and the signal interrupted a transfer: Interrupted \
             system call; cut at bld_modified (resid 48, cmd 4) with no transfer answered for it"
        );
    }

    #[test]
    fn an_interrupted_transfer_without_a_stop_or_a_failure_during_one_is_a_transfer_failure() {
        for (answer, set_stop, words) in [
            (Answer::Interrupted, false, "Interrupted system call"),
            (Answer::Fail, true, "timed out"),
        ] {
            let (state, _) = failing_at(3, answer, set_stop);
            let DeviceState::Unavailable { why, detail, .. } = state else {
                panic!("expected unavailable, got {state:?}");
            };
            assert_eq!(why, UnavailableWhy::Transfer);
            assert_eq!(
                detail,
                format!("reading bld_modified (resid 48, cmd 4) failed: {words}")
            );
        }
    }

    #[test]
    fn a_read_whose_last_transfer_ends_after_the_budget_is_read() {
        let time = FakeTime::new();
        // Seventeen transfers of 120 ms: the last begins at 1.92 s and ends at
        // 2.04 s.
        let cost = 120_000;
        let chip = Chip::timed(all_done(), &time, cost);
        let got = read_timed(chip, &time, &AtomicBool::new(false));
        let DeviceState::Read { registers, .. } = got.state else {
            panic!("expected read, got {:?}", got.state);
        };
        assert_eq!(registers.len(), STATE_REGISTER_COUNT);
        let elapsed = time.now() - T0;
        assert!(elapsed > BUDGET_US, "{elapsed}");
        assert!(
            elapsed - cost < BUDGET_US,
            "the last transfer began at {}",
            elapsed - cost
        );
    }

    #[test]
    fn only_a_transfer_failing_with_eintr_is_interrupted() {
        let eintr = || io::Error::from_raw_os_error(libc::EINTR);
        assert!(ReadError::Transfer(eintr()).interrupted());
        assert!(!ReadError::Transfer(io::Error::from_raw_os_error(libc::ETIMEDOUT)).interrupted());
        assert!(!ReadError::Declined(eintr()).interrupted());
    }

    #[test]
    fn a_board_replaced_during_a_cut_read_is_a_changed_device() {
        let sys = tempfile::tempdir().expect("sys");
        let device = usb_card(sys.path(), 2, "3-2.2", &FLEX);
        let stop = Arc::new(AtomicBool::new(false));
        let mut chip = Chip::answering(all_done());
        let flag = Arc::clone(&stop);
        chip.on_read = Box::new(move || {
            fs::write(device.join("devnum"), "29\n").expect("re-enumerated");
            flag.store(true, Ordering::SeqCst);
        });
        let got = read_device(2, sys.path(), |_| Ok(chip), &Ticks::new(), &stop);
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
        let got = read_device(
            2,
            sys.path(),
            |_| Ok(Nan),
            &Ticks::new(),
            &AtomicBool::new(false),
        );
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
