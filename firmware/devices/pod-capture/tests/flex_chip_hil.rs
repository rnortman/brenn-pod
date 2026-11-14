//! Hardware: the XVF3800 of the reSpeaker Flex (USB id `2886:0022`, the
//! USB-audio image) read over USB control transfers, found as the recorder finds
//! its device: the sound card by name, then the USB device behind that card.
//! Read-only and silent: nothing is written to the chip and nothing is played.
//! Not in `make check` or CI; run by `make -C firmware chip-hil` with the Flex
//! attached and, for the capture test, its capture device free.
//!
//! Two Flexes attached make the card selection ambiguous, and the test fails
//! saying so rather than picking one.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use alsa_pcm::{enumerate_cards, select_one, usb_device_dir};
use frame_log::summarize;
use pod_capture::chip_state::decode;
use xvf3800_ctrl::state::{KIND, Reading, Refusal, read_state};
use xvf3800_ctrl::{
    APPLICATION_SERVICER_RESID, ControlRead, STATUS_DONE, USB_RETRY, VERSION_CMD, VERSION_READ_LEN,
    control_read,
};
use xvf3800_usb::framing::read_setup;
use xvf3800_usb::sysfs::UsbDevice;
use xvf3800_usb::{ChipReader, DEV_BUS_USB, known_id, open_reader};

const CARD: &str = "Flex XVF3800 L16K6Ch";

/// The USB device behind the Flex's sound card, resolved through sysfs.
fn flex_device() -> UsbDevice {
    let cards = enumerate_cards().expect("the card list reads");
    let card = select_one(&cards, CARD).unwrap_or_else(|e| panic!("{e}"));
    let dir = usb_device_dir(card.index, Path::new("/sys"))
        .unwrap_or_else(|e| panic!("{card}: sysfs: {e}"))
        .unwrap_or_else(|| panic!("{card} is not on USB"));
    let device = UsbDevice::read(&dir).unwrap_or_else(|e| panic!("{card}: sysfs: {e}"));
    println!("{card}");
    println!(
        "{device}, bcdDevice {:04x}, serial {:?}",
        device.bcd_device, device.serial
    );
    device
}

/// Presence: the chip answers a read of its version register with a completed
/// transfer and a done status. Nothing about the value.
#[test]
#[ignore = "needs the reSpeaker Flex attached; run by `make -C firmware chip-hil`"]
fn the_chip_answers_a_version_read() {
    let device = flex_device();
    let id = known_id(device.vendor, device.product)
        .unwrap_or_else(|| panic!("{device} is not an id the reader reads"));
    println!("{id}: {}", id.name);

    let mut reader = open_reader(&device, Path::new(DEV_BUS_USB)).unwrap_or_else(|e| panic!("{e}"));
    let setup = read_setup(APPLICATION_SERVICER_RESID, VERSION_CMD, VERSION_READ_LEN);
    println!("{}: {setup}", reader.node_path().display());

    let mut payload = [0u8; VERSION_READ_LEN];
    match control_read(
        &mut reader,
        USB_RETRY,
        APPLICATION_SERVICER_RESID,
        VERSION_CMD,
        &mut payload,
    ) {
        Err(e) => panic!(
            "VERSION (resid 48 cmd 0) got no answer: {setup}: {e} (declined by the device: {})",
            ChipReader::refused(&e)
        ),
        Ok((status, attempts)) => {
            println!(
                "answer: status {status:#04x} after {attempts} attempt(s), payload {payload:02x?}"
            );
            assert_eq!(
                status, STATUS_DONE,
                "VERSION (resid 48 cmd 0) answered status {status:#04x} after {attempts} attempt(s), \
                 payload {payload:02x?}"
            );
        }
    }
}

// ── Identity ─────────────────────────────────────────────────────────────────

/// What one register of the bench board's chip is expected to hold, and where
/// that expectation comes from.
enum Expect {
    /// These payload bytes.
    Bytes(&'static [u8], &'static str),
    /// A little-endian `int32`.
    I32(i32, &'static str),
    /// A little-endian `float`.
    F32(f32, &'static str),
    /// No source states a value; the reading fails the test, printed.
    Unsourced(&'static str),
}

impl Expect {
    fn bytes(&self) -> Option<Vec<u8>> {
        match self {
            Expect::Bytes(b, _) => Some(b.to_vec()),
            Expect::I32(v, _) => Some(v.to_le_bytes().to_vec()),
            Expect::F32(v, _) => Some(v.to_le_bytes().to_vec()),
            Expect::Unsourced(_) => None,
        }
    }

    fn source(&self) -> &'static str {
        match self {
            Expect::Bytes(_, s) | Expect::I32(_, s) | Expect::F32(_, s) | Expect::Unsourced(s) => s,
        }
    }
}

/// The firmware's identity. No document states what this image reports for any
/// of the four.
// TODO(flex-usb-image-chip-readings-unconfirmed): all four rows here have no confirmed expectation.
const IDENTITY: [(&str, Expect); 4] = [
    (
        "version",
        Expect::Unsourced("no document states this image's version triple"),
    ),
    (
        "bld_msg",
        Expect::Unsourced("no document states this image's build string"),
    ),
    (
        "bld_repo_hash",
        Expect::Unsourced("no document states the source revision of this image"),
    ),
    (
        "bld_modified",
        Expect::Unsourced("no document states whether this image's source was modified"),
    ),
];

/// The output routing and the reference path. Sources are the vendor's XVF3800
/// User Guide v3.2.1 (in the tree under `vendor/xvf3800/docs`), by printed page;
/// the guide describes XMOS's development kit, not Seeed's image.
// TODO(flex-usb-image-chip-readings-unconfirmed): audio_mgr_op_r, audio_mgr_op_all, audio_mgr_op_upsample, aec_asroutonoff, audio_mgr_ref_gain and audio_mgr_sys_delay here.
const ROUTING: [(&str, Expect); 13] = [
    (
        "audio_mgr_op_l",
        Expect::Bytes(
            &[8, 0],
            "User Guide p. 23, reset example `AUDIO_MGR_OP_L 8 0`; Table 10.4, p. 81",
        ),
    ),
    // The guide states two defaults for the right output: p. 21's prose says
    // "the raw input from one of the microphones after amplification", a
    // microphone category with no source named; Table 3.2 (p. 21), the reset
    // example on p. 23 and Table 10.4 (p. 81) say (0, 0), silence.
    (
        "audio_mgr_op_r",
        Expect::Unsourced(
            "the User Guide states two defaults: an amplified microphone (p. 21 prose) and \
             (0, 0) (Table 3.2, p. 21; p. 23; Table 10.4, p. 81)",
        ),
    ),
    // Table 10.4 gives no default for OP_ALL itself; its right-hand first slot is
    // OP_R, whose default the guide states two ways.
    (
        "audio_mgr_op_all",
        Expect::Unsourced("Table 10.4 (p. 82) states no default"),
    ),
    (
        "audio_mgr_op_packed",
        Expect::Bytes(&[0, 0], "User Guide Table 10.4, p. 81"),
    ),
    // The guide states upsampling on by default for a 48 kHz interface only
    // (section 4.1.3.3); this image's interface is 16 kHz.
    (
        "audio_mgr_op_upsample",
        Expect::Unsourced("Table 10.4 (p. 81) states no default; none for a 16 kHz interface"),
    ),
    (
        "audio_mgr_selected_channels",
        Expect::Bytes(&[3, 3], "User Guide Table 10.4, p. 81"),
    ),
    (
        "aec_asroutonoff",
        Expect::I32(0, "User Guide Table 10.1, p. 75"),
    ),
    (
        "aec_asroutgain",
        Expect::F32(1.0, "User Guide Table 10.1, p. 75"),
    ),
    (
        "audio_mgr_mic_gain",
        Expect::F32(10.0, "User Guide Table 10.4, p. 80"),
    ),
    (
        "audio_mgr_ref_gain",
        Expect::F32(1.5, "User Guide Table 10.4, p. 80"),
    ),
    (
        "audio_mgr_sys_delay",
        Expect::I32(-32, "User Guide Table 10.4, p. 82"),
    ),
    (
        "audio_mgr_far_end_dsp_enable",
        Expect::Bytes(&[0], "User Guide Table 10.4, p. 82"),
    ),
    (
        "i2s_dac_dsp_enable",
        Expect::Bytes(&[0], "User Guide Table 10.4, p. 82"),
    ),
];

/// Read the chip's state from the Flex, print every register of `expected` with
/// its reading and its expectation, and fail naming every register whose reading
/// is not the expected one.
fn assert_registers(expected: &[(&str, Expect)]) {
    let device = flex_device();
    let mut reader = open_reader(&device, Path::new(DEV_BUS_USB)).unwrap_or_else(|e| panic!("{e}"));
    let state = read_state(&mut reader).unwrap_or_else(|f| {
        panic!(
            "{}: {}: {} (declined by the device: {})",
            f.register.label,
            read_setup(f.register.resid, f.register.cmd, f.register.len),
            f.error,
            ChipReader::refused(&f.error)
        )
    });

    let mut wrong = Vec::new();
    for (name, expect) in expected {
        let (register, reading) = state
            .entries()
            .find(|(r, _)| r.name == *name)
            .unwrap_or_else(|| panic!("{name} is not in the state table"));
        let setup = read_setup(register.resid, register.cmd, register.len);
        let got = match reading {
            Reading::Payload { .. } => {
                let bytes = reading.payload().expect("a payload");
                format!("bytes {bytes:02x?} = {}", decode(register.kind, bytes))
            }
            Reading::Refused(Refusal::Status { status, attempts }) => {
                format!("refused: status {status:#04x} after {attempts} attempt(s)")
            }
            Reading::Refused(Refusal::Declined) => "refused: declined (stall)".to_owned(),
        };
        let want = expect.bytes();
        let shown = want.as_deref().map_or_else(
            || "none".to_owned(),
            |b| decode(register.kind, b).to_string(),
        );
        println!(
            "{name}: {setup}\n  read:     {got}\n  expected: {shown} ({})",
            expect.source()
        );
        // An unsourced row fails whatever was read, a refusal included; a sourced
        // row fails unless its payload was read and equals the expectation.
        if want.is_none() || want.as_deref() != reading.payload() {
            wrong.push(*name);
        }
    }
    assert!(
        wrong.is_empty(),
        "readings not the expected ones (printed above): {}",
        wrong.join(", ")
    );
}

/// Identity: the firmware's version, build string, source revision and modified
/// flag are the expected ones.
#[test]
#[ignore = "needs the reSpeaker Flex attached; run by `make -C firmware chip-hil`"]
fn the_chip_is_the_expected_firmware() {
    assert_registers(&IDENTITY);
}

/// Identity: the output routing and reference-path registers are the expected
/// ones.
#[test]
#[ignore = "needs the reSpeaker Flex attached; run by `make -C firmware chip-hil`"]
fn the_chip_routing_is_the_expected_routing() {
    assert_registers(&ROUTING);
}

// ── The recorder's record ────────────────────────────────────────────────────

const BIN: &str = env!("CARGO_BIN_EXE_pod-capture");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The recorder reads the same chip state this test reads directly, and states it
/// identically on `capture_started` and in the log's header, beside the USB
/// device sysfs names. Records two seconds; silent.
#[test]
#[ignore = "needs the reSpeaker Flex attached and free; run by `make -C firmware chip-hil`"]
fn a_capture_carries_the_reading() {
    let device = flex_device();
    let direct = {
        let mut reader =
            open_reader(&device, Path::new(DEV_BUS_USB)).unwrap_or_else(|e| panic!("{e}"));
        read_state(&mut reader)
            .unwrap_or_else(|f| panic!("{}: {} (direct read)", f.register.label, f.error))
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flex-chip.framelog");
    let mut child = Command::new(BIN)
        .args(["--card", CARD, "--rate", "16000", "--format", "s16_le"])
        .args(["--layout", "a,b,c,d,e,f", "--pod-id", "chip-hil", "--out"])
        .arg(&path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("the pod-capture binary runs");
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut first = String::new();
    stdout.read_line(&mut first).unwrap();
    println!("{}", first.trim_end());
    let line: serde_json::Value = serde_json::from_str(first.trim_end())
        .unwrap_or_else(|e| panic!("first line is not JSON ({e}): {first:?}"));
    if line["event"] != "capture_started" {
        let _ = child.wait();
        panic!("the capture did not start:\n{first}");
    }
    thread::sleep(Duration::from_secs(2));
    let pid = libc::pid_t::try_from(child.id()).unwrap();
    // SAFETY: kill(2) on a child this test spawned and has not yet reaped.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0, "kill({pid})");
    for rest in stdout.lines() {
        println!("{}", rest.unwrap());
    }
    assert_eq!(child.wait().unwrap().code(), Some(0), "pod-capture's exit");

    let state = &line["device_state"];
    assert_eq!(state["outcome"], "read", "{state}");
    assert_eq!(state["kind"], KIND, "{state}");
    let registers = state["registers"].as_object().expect("registers is a map");
    assert_eq!(registers.len(), direct.entries().count(), "{state}");
    for (register, reading) in direct.entries() {
        let entry = &registers[register.name];
        assert_eq!(entry["resid"], register.resid, "{}: {entry}", register.name);
        assert_eq!(entry["cmd"], register.cmd, "{}: {entry}", register.name);
        match reading.payload() {
            Some(bytes) => assert_eq!(
                entry["bytes"],
                hex(bytes),
                "{}: the recorder's reading against the direct one",
                register.name
            ),
            None => assert!(
                entry.get("refused").is_some(),
                "{}: refused when read directly, not in the record: {entry}",
                register.name
            ),
        }
    }

    let usb = &line["device"]["usb"];
    assert_eq!(usb["port"], device.port.as_str(), "{usb}");
    assert_eq!(usb["serial"].as_str(), device.serial.as_deref(), "{usb}");
    assert_eq!(usb["bus"], device.bus, "{usb}");
    assert_eq!(usb["address"], device.address, "{usb}");

    let ended = state["ended_mono_us"].as_u64().expect("ended_mono_us");
    let started = line["mono_us"].as_u64().expect("mono_us");
    assert!(
        ended < started,
        "read ended at {ended}, the capture started at {started}"
    );

    let summary = summarize(&path).expect("the log reads back");
    let capture = &summary.header["capture"];
    assert_eq!(
        &capture["device_state"], state,
        "header against capture_started"
    );
    assert_eq!(
        &capture["device"]["usb"], usb,
        "header against capture_started"
    );
}
