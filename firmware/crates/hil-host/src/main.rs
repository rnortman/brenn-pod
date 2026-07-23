//! HIL host harness binary.
//!
//! Enumerates the ESP32-S3 by VID:PID, performs four independent checks (build-ID,
//! schema, test-registry, behavioral), runs the self-test suite, and exits zero iff
//! all pass.
//!
//! Invoked only via `make hil-test`; never from `make check`, `check-host`, or CI.

use build_id::build_id;
use device_protocol::log_tokens::{
    CAPTURE_OBS_LINE, CAPTURE_TX_LINE, CHUNKS, CORE, NO_NVS_CREDENTIALS, NONEMPTY_POLLS,
    POLL_EMPTY, PREROLL_REARMS, PREROLL_WAITS, PRIO, RX_DEFICIT, RX_WIN_OK, RX_WINDOW_US,
    WIFI_CONNECTED, WIFI_CONSECUTIVE_FAILURES, WIFI_DHCP_LEASE, WIFI_DISCONNECTED,
    WIFI_PARKED_NO_CREDS, WIFI_REASSOC_ATTEMPT_FAILED, WIFI_REASSOC_ATTEMPT_START,
    WIFI_REASSOCIATED, WIFI_SUPERVISOR_STARTED,
};
use device_protocol::{
    Command, MallocProbe, Payload, REGISTERED_TESTS, Response, SSID_TRUNC_BYTES, Status, TestData,
    TestName, TestReport, doa_azimuth_ok, sp_energy_ok, test_name_discriminant,
    truncate_utf8_prefix,
};
use pod_transport::{
    ESP32S3_APP_PID, ESP32S3_DFU_PID, ESP32S3_VID, Harness, HarnessError, PodMode,
    RESPONSE_TIMEOUT, enumerate_pods, escape_device_str, open_port,
};
use std::{
    collections::BTreeSet,
    io::{BufRead, Read as _},
    net::{Ipv4Addr, TcpListener, UdpSocket},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

fn main() {
    // --print-ports mode: print resolved ports and exit (no sockets, no device I/O).
    if std::env::args().nth(1).as_deref() == Some("--print-ports") {
        std::process::exit(print_ports());
    }
    std::process::exit(run());
}

/// Print the resolved echo ports to stdout in machine-readable form and exit.
///
/// Each line is `<name>_port=<value>/<proto>`, where `<proto>` (`udp` or `tcp`)
/// is emitted so the firewall helper opens each port with the correct protocol
/// without encoding a port-name-to-proto table of its own:
/// ```text
/// udp_port=17380/udp
/// inbound_frames_port=17382/tcp
/// backpressure_port=17383/tcp
/// poll_readiness_port=17384/tcp
/// rtd_port=17385/tcp
/// tls_psk_port=17386/tcp
/// tls_psk_bad_port=17387/tcp
/// ```
///
/// Values reflect overrides from `RESPEAKER_HIL_UDP_PORT` /
/// `RESPEAKER_HIL_INBOUND_FRAMES_PORT` / `RESPEAKER_HIL_BACKPRESSURE_PORT` /
/// `RESPEAKER_HIL_POLL_READINESS_PORT` (env or `.hil-secrets`), falling back to the
/// compiled-in defaults.  No device enumeration, no socket binds, no side effects.
/// Format the machine-readable port lines emitted by `--print-ports`.
///
/// Extracted from `print_ports()` so tests can call the same formatter and
/// detect any drift between the Rust output and what the firewall helper parses.
#[allow(clippy::too_many_arguments)]
fn format_port_lines(
    udp: u16,
    inbound: u16,
    backpressure: u16,
    poll_readiness: u16,
    rtd: u16,
    tls_psk: u16,
    tls_psk_bad: u16,
) -> String {
    format!(
        "udp_port={udp}/udp\ninbound_frames_port={inbound}/tcp\nbackpressure_port={backpressure}/tcp\npoll_readiness_port={poll_readiness}/tcp\nrtd_port={rtd}/tcp\ntls_psk_port={tls_psk}/tcp\ntls_psk_bad_port={tls_psk_bad}/tcp\n"
    )
}

fn print_ports() -> i32 {
    match load_hil_secrets() {
        Ok(secrets) => {
            print!(
                "{}",
                format_port_lines(
                    secrets.udp_echo_port,
                    secrets.inbound_frames_port,
                    secrets.backpressure_port,
                    secrets.poll_readiness_port,
                    secrets.rtd_port,
                    secrets.tls_psk_port,
                    secrets.tls_psk_bad_port,
                )
            );
            0
        }
        Err(e) => {
            eprintln!("ERROR: failed to load HIL secrets for --print-ports: {e}");
            1
        }
    }
}

/// Parse the `RESPEAKER_HIL_ONLY` single-test selector.
///
/// - unset (`NotPresent`) or set-to-blank → `Ok(None)`: run the full suite. Blank is the
///   shell "clear the variable" idiom (`RESPEAKER_HIL_ONLY=`), and full coverage is the safe
///   default, so it is deliberately not an error.
/// - a non-blank name matching a registered test → `Ok(Some(t))`.
/// - a non-blank name matching nothing, or a non-UTF-8 value (`NotUnicode`) → `Err(message)`.
///   These are set-but-unusable selectors: rejecting them loudly beats silently running the
///   full suite, which would mask a typo as a passing single-test run.
fn parse_hil_only(var: Result<String, std::env::VarError>) -> Result<Option<TestName>, String> {
    let raw = match var {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(
                "RESPEAKER_HIL_ONLY is set to a non-UTF-8 value and cannot name a test; \
                 unset it to run the full suite"
                    .to_string(),
            );
        }
    };
    let name = raw.trim();
    if name.is_empty() {
        return Ok(None);
    }
    match REGISTERED_TESTS.iter().find(|t| format!("{t:?}") == name) {
        Some(t) => Ok(Some(*t)),
        None => {
            let names: Vec<String> = REGISTERED_TESTS.iter().map(|t| format!("{t:?}")).collect();
            Err(format!(
                "RESPEAKER_HIL_ONLY={name:?} does not name a registered test; valid names: {}",
                names.join(", ")
            ))
        }
    }
}

/// True when no selector is active, or `t` is the selected test. Every behavioral test block
/// must consult this; a test hosted as a prerequisite of another (see the `SpeakerOutput`
/// carve-out) additionally runs when its dependent is selected.
fn selector_wants(only: Option<TestName>, t: TestName) -> bool {
    match only {
        Some(sel) => sel == t,
        None => true,
    }
}

fn run() -> i32 {
    // Single-test selector: `RESPEAKER_HIL_ONLY=<TestName>` runs only that one behavioral
    // test, while still running the infrastructure chain it depends on (Identify + registry
    // checks, WifiScan, WiFi provisioning + association, and the peer echo/listener servers).
    // Unset → the full suite runs, byte-identical to before. The "any failure fails the run"
    // exit-code contract is unchanged: every gated block keeps its own `return 1` on failure.
    // This is a debug-iteration tool only — the evidence and mission-gate runs are full-suite
    // (`RESPEAKER_HIL_ONLY` unset), because RTD's heap/socket landscape depends on the tests
    // that precede it (notably the parked backpressure connection).
    let only: Option<TestName> = match parse_hil_only(std::env::var("RESPEAKER_HIL_ONLY")) {
        Ok(sel) => sel,
        Err(msg) => {
            eprintln!("ERROR: {msg}");
            return 1;
        }
    };
    if let Some(sel) = only {
        println!(
            "Single-test selector: RESPEAKER_HIL_ONLY={sel:?} — running only this behavioral \
             test (the infrastructure chain still runs)"
        );
    }

    // ── Step 1: enumerate by VID:PID ─────────────────────────────────────────

    let pods = match enumerate_pods() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: failed to enumerate serial ports: {e}");
            return 1;
        }
    };

    // Check for DFU mode first — give a specific message.
    let dfu = pods.iter().any(|p| p.mode == PodMode::Dfu);
    if dfu {
        eprintln!(
            "ERROR: device in bootloader/DFU mode (VID {ESP32S3_VID:#06x} PID {ESP32S3_DFU_PID:#06x}), not running app firmware. \
             Flash the firmware and reset the device."
        );
        return 1;
    }

    let app_pod = pods.iter().find(|p| p.mode == PodMode::App);

    let port_name = match app_pod {
        Some(p) => p.port_name.clone(),
        None => {
            eprintln!(
                "ERROR: no device attached. Expected ESP32-S3 at VID {ESP32S3_VID:#06x} PID {ESP32S3_APP_PID:#06x}. \
                 Is the device connected and powered on?"
            );
            return 1;
        }
    };

    println!("Found ESP32-S3 at {port_name}");

    // ── Step 2: open port ────────────────────────────────────────────────────

    let transport = match open_port(&port_name) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "ERROR: failed to open {port_name}: {e}. \
                 Check udev rules (plugdev/uaccess for VID {ESP32S3_VID:#06x})."
            );
            return 1;
        }
    };

    println!("Opened {port_name}");

    let harness_build = build_id();
    let mut harness = Harness::new(transport);

    // The volatile audio-PSK override goes live inside the suite (step 3b'); the suite
    // records that in this cell so this outer frame can clear it on every exit path.
    let override_live = std::cell::Cell::new(false);
    let code = run_suite(&mut harness, &harness_build, only, &override_live);

    // Clear-on-exit: once the temporary audio PSK went live, drop it on every exit path so
    // NVS-key precedence is restored for future key reads without a power cycle. Best-effort
    // — a killed process or a dead serial link leaves the override armed until the next
    // reboot, which also clears it.
    if override_live.get() {
        println!(
            "  *** NOTICE: clearing the temporary audio-PSK override; this restores NVS-key \
             precedence for future reads (a reboot also clears it). A streamer running from \
             boot-time provisioning holds its boot-time key either way. ***"
        );
        match harness.send_command(Command::ClearTemporaryAudioPsk) {
            Ok(r) if r.status == Status::Ok => {
                println!("  ClearTemporaryAudioPsk: OK");
            }
            Ok(r) => {
                eprintln!(
                    "  *** NOTICE: device refused ClearTemporaryAudioPsk (status {:?}); reboot \
                     the pod to clear the override and restore NVS-key precedence. ***",
                    r.status
                );
            }
            Err(e) => {
                eprintln!(
                    "  *** NOTICE: best-effort ClearTemporaryAudioPsk failed ({e}); reboot the \
                     pod to clear the override and restore NVS-key precedence. ***"
                );
            }
        }
    }

    code
}

/// Run the full HIL check/self-test suite over an already-opened `harness`.
///
/// The caller owns `harness` after this returns and clears the temporary audio-PSK override
/// on every exit path (pass, fail, or early `return 1`). This function records in
/// `override_live` whether the override went live (step 3b'); the caller reads it afterward.
fn run_suite(
    harness: &mut Harness,
    harness_build: &device_protocol::BuildId,
    only: Option<TestName>,
    override_live: &std::cell::Cell<bool>,
) -> i32 {
    let want = |t: TestName| selector_wants(only, t);

    // ── Step 3: four independent checks ──────────────────────────────────────

    // Send Identify command to gather info for checks 1–3.
    // Use a longer timeout (30 s) to accommodate boot-time WiFi association, which
    // can take up to ~10 s before the protocol loop is reached.
    // TODO(hil-first-attempt-after-boot-ac9): the first invocation after a physical
    // power-cycle reliably fails here (AC9) because accumulated boot-console bytes
    // on the serial port desync the frame parser. A drain-and-discard of pending
    // input before this send would likely fix it.
    println!("Sending Identify...");
    let identify_response = match harness.send_command_timeout(
        Command::RunTest(TestName::Identify),
        Duration::from_secs(30),
    ) {
        Ok(r) => r,
        Err(HarnessError::Timeout) => {
            eprintln!(
                "ERROR [AC9]: device present but not responding with protocol frames — \
                 wrong/old firmware?"
            );
            return 1;
        }
        Err(e) => {
            eprintln!("ERROR: identify command failed: {e}");
            return 1;
        }
    };

    // Check 2: Schema match — the successful strict decode of Identify into our
    // compiled-in types IS the schema check. If the types disagreed we'd have
    // gotten a decode error in send_command. Assert the response is well-formed.
    match &identify_response.payload {
        Payload::Identify { .. } => {} // schema match confirmed
        other => {
            eprintln!(
                "ERROR [check 2 — schema]: Identify response has unexpected payload shape: {other:?}"
            );
            return 1;
        }
    }
    println!("Check 2 (schema): PASS");

    let (device_build, device_tests) = match identify_response.payload {
        Payload::Identify { build, tests } => (build, tests),
        _ => unreachable!(),
    };

    // Check 1: Build-ID match.
    if let Err(msg) = compare_build_ids(harness_build, &device_build) {
        eprintln!("ERROR [check 1 — build-ID]: {msg}");
        return 1;
    }
    println!("Check 1 (build-ID): PASS");

    // Check 3: Test-registry set-equality — runtime confirmation against the flashed
    // firmware; membership invariant enforced at cargo-test time in device-protocol.
    let expected_tests: BTreeSet<u8> = REGISTERED_TESTS
        .iter()
        .map(test_name_discriminant)
        .collect();
    let reported_tests: BTreeSet<u8> = device_tests.iter().map(test_name_discriminant).collect();

    if expected_tests != reported_tests {
        let missing: Vec<_> = REGISTERED_TESTS
            .iter()
            .filter(|t| !device_tests.contains(t))
            .collect();
        let extra: Vec<_> = device_tests
            .iter()
            .filter(|t| !REGISTERED_TESTS.contains(t))
            .collect();
        eprintln!(
            "ERROR [check 3 — test-registry]: set mismatch.\n  missing from device: {missing:?}\n  extra on device: {extra:?}"
        );
        return 1;
    }
    println!("Check 3 (test-registry): PASS");

    // Corrupt-frame recovery: always-on host-driven check (needs no network peers or
    // provisioning). Runs between the Step-3 identity checks and the self-test suite.
    // It only mutates the device's in-memory `deser_error_count`, harmless to every
    // other check and reset on reboot.
    if run_deser_error_check(harness).is_err() {
        return 1;
    }
    println!("deser-error recovery check: PASS");

    // Check 4: Behavioral — run each registered self-test via RunTest.
    // Only `Generic`-phase tests run in this uniform loop (see `test_meta`). `Network`- and
    // `DedicatedLocal`-phase tests are dispatched in their own blocks after this loop: network
    // tests need captured pod IP / bound server handles / per-test timeouts, and the
    // dedicated-local tests each need a log-collecting send so the host can assert the periodic
    // summary lines the generic loop's plain `send_command` does not collect (two of them also
    // run a ~5 s feed needing the longer per-test timeout, which the generic loop cannot pass).
    println!("Check 4 (behavioral): running self-tests...");
    for test_name in device_tests
        .iter()
        .filter(|t| test_meta(t).phase == TestPhase::Generic)
    {
        // Single-test selector: skip non-selected tests, but keep SpeakerOutput when a test
        // that depends on its warm capture-loop state (PlaybackDrainRate / FullDuplexRxIntegrity,
        // hosted in the SpeakerOutput iteration below) is the selected target.
        let speaker_prereq = *test_name == TestName::SpeakerOutput
            && (want(TestName::PlaybackDrainRate) || want(TestName::FullDuplexRxIntegrity));
        if !want(*test_name) && !speaker_prereq {
            continue;
        }
        let resp = match harness.send_command(Command::RunTest(*test_name)) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest({test_name:?}) — \
                     wrong/old firmware?"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest({test_name:?}) failed: {e}");
                return 1;
            }
        };

        if check_status(&resp, &format!("self-test {test_name:?}")).is_err() {
            return 1;
        }

        // For Identify self-test: assert payload is the Identify variant (values
        // already checked by checks 1 and 3).
        // For Ping self-test: assert payload is the Pong variant.
        // All other tests return TestReport; check4_test_report validates shape.
        // For I2cBusScan: additionally assert the found-list contains both required
        // addresses, catching device regressions where Status is wrongly Ok but the
        // criterion addresses are absent.
        // Convention: Status is authoritative for pass/fail; report detail is purely
        // human-readable narrative.
        match test_name {
            TestName::Identify => match &resp.payload {
                Payload::Identify { .. } => {}
                other => {
                    eprintln!(
                        "FAIL [check 4 — behavioral]: Identify self-test returned unexpected payload: {other:?}"
                    );
                    return 1;
                }
            },
            TestName::Ping => match &resp.payload {
                Payload::Pong(_) => {}
                other => {
                    eprintln!(
                        "FAIL [check 4 — behavioral]: Ping self-test returned unexpected payload: {other:?}"
                    );
                    return 1;
                }
            },
            // Second enforcement point over typed data: re-derive the pass criterion from
            // the report's `TestData`, independent of the Status field. Catches a device
            // returning Status::Ok with stale or absent data. (SpeakerOutput does NOT assert
            // sound — the acoustic result has no programmatic observable and is the
            // operator's ear; and it carries no amp-enable field, the amp being always-on
            // hardware.)
            TestName::I2sWaveformSanity
            | TestName::SpeakerOutput
            | TestName::I2cBusScan
            | TestName::PsramIdentity
            | TestName::Xvf3800RegRead
            | TestName::Xvf3800DoAPlausibility
            | TestName::Xvf3800SpEnergy
            | TestName::AmpAlwaysOnGpoInert => {
                let (pred, criterion) = check4_typed_gate_for(test_name)
                    .expect("these test names always have a typed check-4 gate");
                if gate_check4_typed(test_name, &resp, pred, &criterion).is_err() {
                    return 1;
                }
            }
            _ => match check4_test_report(&resp) {
                Ok(report) => print_report(test_name, &report),
                Err(e) => {
                    eprintln!("FAIL [check 4 — behavioral]: {test_name:?} {e}");
                    return 1;
                }
            },
        }

        println!("  self-test {test_name:?}: PASS");

        // ── PlaybackDrainRate: raw-drain rate under a saturating feed ──
        // Run immediately after SpeakerOutput passes (its codec/DAC/speaker bring-up is
        // PlaybackDrainRate's required setup) and BEFORE the loop advances to the next
        // registered test, AmpAlwaysOnGpoInert. AmpAlwaysOnGpoInert currently FAILs on
        // hardware and the runner is fail-fast, so leaving PlaybackDrainRate after the
        // loop meant the suite aborted before our drain-rate measurement ran. Gating on
        // the SpeakerOutput iteration places it after its setup but ahead of the amp test
        // without relocating any other test.
        //
        // Drives a steady, at-least-real-time inbound feed through the production playback
        // path (device-side `run_playback_drain_rate`) and asserts the production capture
        // thread sustains real-time drain: over the saturated windows the inbound ring drains
        // raw bytes at ≥ 1.0× the raw real-time rate (32 B/ms), read from its periodic
        // `capture: playback tx …` lines (design §5). Same
        // `send_command_timeout_collect_logs` + log-eval shape as CapturePeriodicLine below;
        // the host owns the numeric bounds.
        //
        // Selector gating: this hosted block runs PlaybackDrainRate (and, gated separately
        // below, FullDuplexRxIntegrity). Run it when either is wanted — FullDuplexRxIntegrity
        // depends on PlaybackDrainRate's warm capture-loop state, so selecting FDRI still runs
        // PDR first as its prerequisite. Under the full suite (`want` always true) behavior is
        // unchanged. Without this gate a `RESPEAKER_HIL_ONLY=PlaybackDrainRate` run would also
        // run — and could be failed by — FullDuplexRxIntegrity, which it does not depend on.
        if *test_name == TestName::SpeakerOutput
            && (want(TestName::PlaybackDrainRate) || want(TestName::FullDuplexRxIntegrity))
        {
            // Settle past the tone test's RX-dead window before driving the drain feed. The tone
            // test does not drain the RX DMA ring, so the first periodic window after it would
            // otherwise straddle that RX-dead span; let the device's capture loop resume RX
            // draining and roll past it so the first steady window reflects normal servicing.
            // Print the settle so its wall-clock time is accounted for in the runner's output (it
            // precedes the PlaybackDrainRate command timeout and is NOT inside the test_timeout
            // budget).
            println!(
                "  Settling {} ms past the tone test's RX-dead window before PlaybackDrainRate...",
                PLAYBACK_DRAIN_PRETEST_SETTLE_MS
            );
            thread::sleep(Duration::from_millis(PLAYBACK_DRAIN_PRETEST_SETTLE_MS));
            println!("  Running PlaybackDrainRate (raw-drain rate under saturating feed)...");
            let mut drain_logs: Vec<String> = Vec::new();
            let drain_resp = match harness.send_command_timeout_collect_logs(
                Command::RunTest(TestName::PlaybackDrainRate),
                test_timeout(&TestName::PlaybackDrainRate),
                &mut drain_logs,
            ) {
                Ok(r) => r,
                Err(HarnessError::Timeout) => {
                    eprintln!(
                        "ERROR [AC9]: device not responding to RunTest(PlaybackDrainRate) — \
                         capture thread may be wedged or the feed window exceeded the budget"
                    );
                    return 1;
                }
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: RunTest(PlaybackDrainRate) failed: {e}"
                    );
                    return 1;
                }
            };
            if check_status(&drain_resp, "PlaybackDrainRate").is_err() {
                return 1;
            }
            let report = match check4_test_report(&drain_resp) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("FAIL [check 4 — behavioral]: PlaybackDrainRate {e}");
                    return 1;
                }
            };
            print_report(&TestName::PlaybackDrainRate, &report);
            if let Err(e) = eval_playback_drain_rate(&report.data, &drain_logs) {
                eprintln!("FAIL [check 4 — behavioral]: PlaybackDrainRate {e}");
                return 1;
            }
            println!("  self-test PlaybackDrainRate: PASS");

            // Selector gating: FullDuplexRxIntegrity is a distinct behavioral test. Skip it when
            // a selector names only PlaybackDrainRate (which does not depend on it); it still
            // runs under the full suite and when it is itself the selected target (its warm-state
            // dependency on PlaybackDrainRate is satisfied by the block above having run first).
            if want(TestName::FullDuplexRxIntegrity) {
                // ── FullDuplexRxIntegrity: mic-RX integrity under playback ──
                // Runs immediately after PlaybackDrainRate (same warm capture-loop state, RX already
                // draining at cadence — no separate settle needed) and BEFORE the loop advances to the
                // fail-fast AmpAlwaysOnGpoInert. Drives the same saturating inbound feed (device-side
                // `run_full_duplex_rx_integrity`) so the capture thread is TX-drain-bound and must
                // service mic RX concurrently — the exact condition the pre-fix blocking-TX pass
                // starved RX under. Asserts the device-computed, dead-banded `rx_deficit` telemetry is
                // zero across the saturated windows: the direct proof the ~48 % mic-sample loss is
                // gone, read from the periodic `capture: playback obs …` lines (design §5).
                println!(
                    "  Running FullDuplexRxIntegrity (mic-RX integrity under saturating feed)..."
                );
                let mut rx_logs: Vec<String> = Vec::new();
                let rx_resp = match harness.send_command_timeout_collect_logs(
                    Command::RunTest(TestName::FullDuplexRxIntegrity),
                    test_timeout(&TestName::FullDuplexRxIntegrity),
                    &mut rx_logs,
                ) {
                    Ok(r) => r,
                    Err(HarnessError::Timeout) => {
                        eprintln!(
                            "ERROR [AC9]: device not responding to RunTest(FullDuplexRxIntegrity) — \
                         capture thread may be wedged or the feed window exceeded the budget"
                        );
                        return 1;
                    }
                    Err(e) => {
                        eprintln!(
                            "ERROR [check 4 — behavioral]: RunTest(FullDuplexRxIntegrity) failed: {e}"
                        );
                        return 1;
                    }
                };
                if check_status(&rx_resp, "FullDuplexRxIntegrity").is_err() {
                    return 1;
                }
                let rx_report = match check4_test_report(&rx_resp) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("FAIL [check 4 — behavioral]: FullDuplexRxIntegrity {e}");
                        return 1;
                    }
                };
                print_report(&TestName::FullDuplexRxIntegrity, &rx_report);
                if let Err(e) = eval_full_duplex_rx_integrity(&rx_report.data, &rx_logs) {
                    eprintln!("FAIL [check 4 — behavioral]: FullDuplexRxIntegrity {e}");
                    return 1;
                }
                println!("  self-test FullDuplexRxIntegrity: PASS");
            }
        }
    }

    // ── WifiScan: credential-less radio + AP scan (before provisioning) ──────
    // Runs here (before any ProvisionWifi) to prove the radio inits and can scan
    // on a factory-fresh device with no credentials.
    // WifiScan stays ungated even under a single-test selector: it starts and proves
    // the credential-less radio the later network-phase blocks build on.
    if run_dedicated_test(
        harness,
        TestName::WifiScan,
        "credential-less radio + scan",
        eval_wifi_scan,
    )
    .is_err()
    {
        return 1;
    }

    // ── WifiPowerSaveCheck: modem power save must be off (before provisioning) ─
    // Runs on the same credential-less started radio as WifiScan to prove the
    // start path forces WIFI_PS_NONE — the exact path ensure_wifi_started guards.
    // Gated on the single-test selector like the other dedicated blocks: this is a
    // pure assertion nothing else depends on, so a surprising first-hardware reading
    // must not fail-fast-abort unrelated RESPEAKER_HIL_ONLY iterations.
    if want(TestName::WifiPowerSaveCheck)
        && run_dedicated_test(
            harness,
            TestName::WifiPowerSaveCheck,
            "modem power save off",
            eval_wifi_power_save,
        )
        .is_err()
    {
        return 1;
    }

    // ── Pre-network setup block (design §3.4 step 3) ─────────────────────────
    // Load optional HIL secrets (never aborts here — absent config means NVS
    // fallback for WiFi and skip for TLS, both announced loudly below).
    let secrets = match load_hil_secrets() {
        Ok(s) => s,
        Err(msg) => {
            eprintln!("ERROR [check 4 — network]: {msg}");
            return 1;
        }
    };

    // 3a. Provision WiFi credentials — or fall back to NVS-stored credentials.
    match &secrets.wifi {
        Some(creds) => {
            println!("  Provisioning WiFi credentials...");
            let mut ssid: heapless::String<32> = heapless::String::new();
            let mut pass: heapless::String<64> = heapless::String::new();
            if ssid.push_str(&creds.ssid).is_err() {
                eprintln!("ERROR [check 4 — network]: SSID too long (max 32 bytes)");
                return 1;
            }
            if pass.push_str(&creds.pass).is_err() {
                eprintln!("ERROR [check 4 — network]: passphrase too long (max 64 bytes)");
                return 1;
            }
            let prov_resp = match harness.send_command(Command::ProvisionWifi {
                ssid,
                passphrase: pass,
            }) {
                Ok(r) => r,
                Err(HarnessError::Timeout) => {
                    eprintln!(
                        "ERROR [AC9]: device not responding to ProvisionWifi — wrong/old firmware?"
                    );
                    return 1;
                }
                Err(e) => {
                    eprintln!("ERROR [check 4 — network]: ProvisionWifi failed: {e}");
                    return 1;
                }
            };
            if check_status(&prov_resp, "ProvisionWifi").is_err() {
                return 1;
            }
            println!("  ProvisionWifi: OK");
        }
        None => {
            // LOUD explicit fallback announcement — AC-B1.3 spirit: never silent.
            println!("  *** NOTICE: RESPEAKER_WIFI_SSID / RESPEAKER_WIFI_PASS are not set. ***");
            println!(
                "  *** Skipping ProvisionWifi and relying on credentials already stored in device NVS. ***"
            );
            println!(
                "  *** If WifiAssociate fails below, the device has no NVS credentials — run with RESPEAKER_WIFI_SSID/RESPEAKER_WIFI_PASS set to provision them. ***"
            );
        }
    }

    // 3b. Run WifiAssociate, assert link bounds, capture pod IP.
    println!("  Running WifiAssociate...");
    let pod_ip: [u8; 4];
    {
        let assoc_resp = match harness.send_command_timeout(
            Command::RunTest(TestName::WifiAssociate),
            test_timeout(&TestName::WifiAssociate),
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest(WifiAssociate) — \
                     check device-side timeout / stuck WiFi?"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — network]: RunTest(WifiAssociate) failed: {e}");
                return 1;
            }
        };
        if check_status(&assoc_resp, "WifiAssociate").is_err() {
            return 1;
        }
        let assoc_report = match check4_test_report(&assoc_resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: WifiAssociate {e}");
                return 1;
            }
        };
        // Host-side second enforcement of link bounds (AC-B2.1–B2.3); returns the
        // validated pod IP.
        pod_ip = match eval_wifi_info(&assoc_report.data) {
            Ok(ip) => ip,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: WifiAssociate link assertion: {e}");
                return 1;
            }
        };
        println!(
            "  WifiAssociate: ip={} ({:?})",
            Ipv4Addr::from(pod_ip),
            assoc_report.data
        );
        println!("  self-test WifiAssociate: PASS");
    }

    // 3b'. Install a fresh audio-link PSK as a RAM-only override — never persisted to NVS —
    // and learn the pod identity from the device's own answer. That identity is what the
    // TLS-PSK listeners will authenticate, and it is authoritative only on the device. The
    // override shadows the pod's stored production key for the run and is cleared on exit
    // (or the next reboot), so a HIL run never overwrites the production key in flash.
    let pod_psk = {
        let key = match generate_audio_psk() {
            Ok(k) => k,
            Err(e) => {
                eprintln!("ERROR [check 4 — network]: {e}");
                return 1;
            }
        };
        // Arm clear-on-exit *before* the send: a timeout or transport error leaves the
        // outcome unknown, and the device may well have stored the override. A clear with
        // no override active is a documented Ok no-op, so arming early only ever costs one
        // harmless command.
        override_live.set(true);
        let psk_resp = match harness.send_command(Command::SetTemporaryAudioPsk { key }) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to SetTemporaryAudioPsk — wrong/old firmware?"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — network]: SetTemporaryAudioPsk failed: {e}");
                return 1;
            }
        };
        if check_status(&psk_resp, "SetTemporaryAudioPsk").is_err() {
            return 1;
        }
        let Payload::PodId(pod_id) = &psk_resp.payload else {
            eprintln!(
                "FAIL [check 4 — behavioral]: SetTemporaryAudioPsk did not answer with a PodId \
                 payload: {:?}",
                psk_resp.payload
            );
            return 1;
        };
        println!("  SetTemporaryAudioPsk: OK pod_id={}", pod_id.as_str());
        PodPsk {
            identity: pod_id.as_str().to_string(),
            key,
        }
    };

    // 3c–3d. Derive host self-IP, bind echo servers, provision peer.
    // PeerServers RAII guard lives to end of network block; Drop joins threads.
    let peer_servers = match PeerServers::start(pod_ip, &secrets, &pod_psk) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ERROR [check 4 — network]: failed to start peer servers: {e}");
            return 1;
        }
    };
    {
        let peer_host_arr: [u8; 4] = peer_servers.host_ip;
        // Use TLS config from secrets if present; otherwise push zeros so the device still
        // has a session peer record (TLS test will be skipped host-side).
        let (tls_host, tls_port) = secrets
            .tls
            .as_ref()
            .map(|t| (t.host, t.port))
            .unwrap_or(([0u8; 4], 0u16));
        let peer_resp = match harness.send_command(Command::SetTemporaryPeerConfig {
            host: peer_host_arr,
            udp_port: peer_servers.udp_port,
            tls_host,
            tls_port,
            inbound_frames_port: peer_servers.inbound_frames_port,
            backpressure_port: peer_servers.backpressure_port,
            poll_readiness_port: peer_servers.poll_readiness_port,
            rtd_port: peer_servers.rtd_port,
            tls_psk_port: peer_servers.tls_psk_port,
            tls_psk_bad_port: peer_servers.tls_psk_bad_port,
        }) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to SetTemporaryPeerConfig — wrong/old firmware?"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — network]: SetTemporaryPeerConfig failed: {e}");
                return 1;
            }
        };
        if check_status(&peer_resp, "SetTemporaryPeerConfig").is_err() {
            return 1;
        }
        println!("  SetTemporaryPeerConfig: OK");
    }

    // Announce TLS skip before the loop so the operator knows before any test runs.
    if secrets.tls.is_none() {
        println!("  *** NOTICE: RESPEAKER_TLS_HOST not set — skipping TlsReachability test. ***");
        println!(
            "  *** Set RESPEAKER_TLS_HOST and RESPEAKER_TLS_PORT in the environment or in .hil-secrets to enable it. ***"
        );
    }

    // ── Network reachability loop ──────────────────────────────────────────────
    let reachability_tests = [
        TestName::UdpRoundtrip,
        TestName::TlsReachability,
        TestName::TlsInboundFrames,
        TestName::TlsSendBackpressure,
        TestName::TlsInboundBackpressure,
        TestName::PollReadinessBidir,
        TestName::TlsPskHandshake,
        TestName::TlsPskWrongKeyRejected,
    ];
    for test_name in &reachability_tests {
        // Single-test selector: skip non-selected reachability tests.
        if !want(*test_name) {
            continue;
        }
        // Skip TlsReachability if TLS host is not configured.
        if *test_name == TestName::TlsReachability && secrets.tls.is_none() {
            println!("  self-test TlsReachability: SKIPPED (RESPEAKER_TLS_HOST not set)");
            continue;
        }
        println!("  Running {test_name:?}...");
        let resp = match harness
            .send_command_timeout(Command::RunTest(*test_name), test_timeout(test_name))
        {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest({test_name:?}) — \
                     check peer server reachability and device-side network state"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest({test_name:?}) failed: {e}");
                return 1;
            }
        };
        // A non-PASS device result aborts the entire HIL run, preserving the "any
        // failure fails the run" exit-code contract.
        if check_status(&resp, &format!("{test_name:?}")).is_err() {
            return 1;
        }
        // Host-side second enforcement (typed verdict data).
        let report = match check4_test_report(&resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: {test_name:?} {e}");
                return 1;
            }
        };
        print_report(test_name, &report);
        let data = &report.data;
        let outcome = match test_name {
            TestName::UdpRoundtrip => eval_udp_roundtrip(data),
            TestName::TlsReachability => eval_tls_reachability(data),
            TestName::TlsInboundFrames => eval_tls_inbound_frames(data),
            TestName::TlsSendBackpressure => eval_tls_send_backpressure(data),
            TestName::TlsInboundBackpressure => eval_tls_inbound_backpressure(data),
            TestName::PollReadinessBidir => eval_poll_readiness_bidir(data),
            TestName::TlsPskHandshake => eval_tls_psk_handshake(data),
            TestName::TlsPskWrongKeyRejected => eval_tls_psk_wrong_key_rejected(data),
            _ => unreachable!("unhandled reachability test variant: {test_name:?}"),
        };
        if let Err(e) = outcome {
            eprintln!(
                "FAIL [check 4 — behavioral]: {test_name:?} Status::Ok but host-side assertion failed: {e}"
            );
            return 1;
        }
        println!("  self-test {test_name:?}: PASS");
    }

    // ── StreamRealtimeDuplex: streamer keeps up with real time (Scenario A) ───
    // The device drives the extracted `run_segment` drain loop against a synthetic
    // real-time producer and streams a full segment to the rtd listener; the host owns the
    // burst-drain + catch-up + integrity assertions (device→host observation). Per CLAUDE.md
    // bring-up doctrine this ASSERTS the expected keep-up behavior and is allowed to FAIL
    // first against the current one-action-per-wake loop.
    // The generic check-4 loop runs before this block; a pre-existing failure there (e.g.
    // AmpAlwaysOnGpoInert) is fail-fast and aborts the run. To reach RTD on hardware without
    // hoisting, iterate with RESPEAKER_HIL_ONLY=StreamRealtimeDuplex (the single-test selector),
    // which skips the other behavioral tests while keeping the WiFi/provision/peer-server chain.
    if want(TestName::StreamRealtimeDuplex) {
        let rtd = TestName::StreamRealtimeDuplex;
        // On any RTD failure, dump both observation slots so a connection that never reached the
        // listener — or reached the wrong one — is impossible to miss in the transcript.
        let dump_rtd_obs = || {
            let a = peer_servers.rtd_observation.lock().unwrap().clone();
            let b = peer_servers.rtd_observation_b.lock().unwrap().clone();
            dump_rtd_observations(&a, &b);
        };
        println!("  Running {rtd:?}...");
        let resp = match harness.send_command_timeout(Command::RunTest(rtd), test_timeout(&rtd)) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest({rtd:?}) — \
                     check rtd listener reachability and device-side network state"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest({rtd:?}) failed: {e}");
                return 1;
            }
        };
        if check_status(&resp, &format!("{rtd:?}")).is_err() {
            dump_rtd_obs();
            return 1;
        }
        let report = match check4_test_report(&resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: {rtd:?} {e}");
                dump_rtd_obs();
                return 1;
            }
        };
        print_report(&rtd, &report);
        // The device runs Scenario A then B; the B (second) connection's SegmentEnd is the
        // last thing decoded. Give the listener a brief window to finish before reading both
        // observations.
        let (obs_a, obs_b) = {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                let a = peer_servers.rtd_observation.lock().unwrap().clone();
                let b = peer_servers.rtd_observation_b.lock().unwrap().clone();
                if b.end_reason.is_some()
                    || b.error.is_some()
                    || a.error.is_some()
                    || std::time::Instant::now() >= deadline
                {
                    break (a, b);
                }
                thread::sleep(Duration::from_millis(20));
            }
        };
        // Scenario A: outbound catch-up with no inbound co-traffic.
        if let Err(e) = eval_stream_realtime_duplex(&report.data, &obs_a) {
            eprintln!("FAIL [check 4 — behavioral]: {rtd:?} Scenario A assertion failed: {e}");
            dump_rtd_observations(&obs_a, &obs_b);
            return 1;
        }
        // Scenario B: the same outbound assertions under duplex load, plus zero fake-DAC
        // underruns and an exact consumed-frame count.
        match eval_stream_realtime_duplex_b(&report.data, &obs_b) {
            Ok(()) => println!("  self-test {rtd:?}: PASS"),
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: {rtd:?} Scenario B assertion failed: {e}");
                dump_rtd_observations(&obs_a, &obs_b);
                return 1;
            }
        }
    }

    // PeerServers drops here, joining echo threads.
    drop(peer_servers);

    // ── WifiReassociation: supervisor re-associates after forced drop ─────────
    // Asserts the full WifiEvent/IpEvent subscription
    // line set (`wifi: disconnected`, `wifi: connected`, `wifi: dhcp lease`) plus
    // the supervisor's `wifi-supervisor: re-associated` line, within a bounded
    // window after a forced disconnect. See design §4 "Re-association after forced drop".
    if want(TestName::WifiReassociation) {
        println!("  Running WifiReassociation (re-associate after forced drop)...");
        let mut reassoc_logs: Vec<String> = Vec::new();
        let reassoc_resp = match harness.send_command_timeout_collect_logs(
            Command::RunTest(TestName::WifiReassociation),
            test_timeout(&TestName::WifiReassociation),
            &mut reassoc_logs,
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest(WifiReassociation) — \
                     supervisor may be stuck or test timed out"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest(WifiReassociation) failed: {e}");
                return 1;
            }
        };
        if check_status(&reassoc_resp, "WifiReassociation").is_err() {
            return 1;
        }
        let reassoc_report = match check4_test_report(&reassoc_resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: WifiReassociation {e}");
                return 1;
            }
        };
        print_report(&TestName::WifiReassociation, &reassoc_report);
        if let Err(e) = eval_wifi_reassociation_pass(&reassoc_report.data, &reassoc_logs) {
            eprintln!("FAIL [check 4 — behavioral]: WifiReassociation {e}");
            return 1;
        }
        println!("  self-test WifiReassociation: PASS");

        // Confirm WifiAssociate still returns `up` after the re-association cycle.
        println!("  Running WifiAssociate (post-reassociation check)...");
        let post_assoc_resp = match harness.send_command_timeout(
            Command::RunTest(TestName::WifiAssociate),
            test_timeout(&TestName::WifiAssociate),
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to post-reassociation WifiAssociate — \
                     WiFi may not have fully recovered"
                );
                return 1;
            }
            Err(e) => {
                eprintln!(
                    "ERROR [check 4 — behavioral]: post-reassociation WifiAssociate failed: {e}"
                );
                return 1;
            }
        };
        if check_status(&post_assoc_resp, "post-reassociation WifiAssociate").is_err() {
            return 1;
        }
        match check4_test_report(&post_assoc_resp).and_then(|r| eval_wifi_info(&r.data)) {
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "FAIL [check 4 — behavioral]: post-reassociation WifiAssociate link assertion: {e}"
                );
                return 1;
            }
        }
        println!("  post-reassociation WifiAssociate: PASS");
    }

    // ── GatewayProbeGate: reachable target → no re-associate; unreachable → bounce ──
    // Asserts both halves of the gateway-reachability gate (§4 of the
    // gateway-reachability-gate ADR):
    //   Half 1 — peer_ip (host address, always reachable) → probe=reachable, no bounce.
    //   Half 2 — blackhole IP on device's subnet → probe=unreachable, force-reassociate.
    // The test uses the target-parameterised ping_reachable inner probe so neither the
    // real router nor AP control is required.
    if want(TestName::GatewayProbeGate) {
        println!(
            "  Running GatewayProbeGate (gateway probe gate — reachable + unreachable halves)..."
        );
        let mut gate_logs: Vec<String> = Vec::new();
        let gate_resp = match harness.send_command_timeout_collect_logs(
            Command::RunTest(TestName::GatewayProbeGate),
            test_timeout(&TestName::GatewayProbeGate),
            &mut gate_logs,
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest(GatewayProbeGate) — \
                     probe or reassociate may be stuck"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest(GatewayProbeGate) failed: {e}");
                return 1;
            }
        };
        if check_status(&gate_resp, "GatewayProbeGate").is_err() {
            return 1;
        }
        let gate_report = match check4_test_report(&gate_resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: GatewayProbeGate {e}");
                return 1;
            }
        };
        print_report(&TestName::GatewayProbeGate, &gate_report);
        // Assert reachable half: find the half-1 PASS log line in gate_logs and check its
        // tokens.  The device emits "gateway-probe-gate: half-1 PASS probe=reachable
        // reassociated=false" as a log::info; if that line is absent the device returned
        // early FAIL (caught above), but we confirm the tokens here as an independent
        // host-side guard for the core "peer-down must not bounce" assertion (§4).
        {
            let half1_line = gate_logs
                .iter()
                .find(|l| l.contains("half-1") && l.contains("probe=reachable"));
            match half1_line {
                Some(line) => {
                    // Extract the "PASS ..." suffix so eval_gateway_probe_gate_reachable's
                    // starts_with("PASS") check works.
                    let pass_idx = match line.find("PASS") {
                        Some(i) => i,
                        None => {
                            eprintln!(
                                "FAIL [check 4 — behavioral]: GatewayProbeGate: \
                                 half-1 log line matched filter but contains no 'PASS' token \
                                 (log format changed?): {line}"
                            );
                            return 1;
                        }
                    };
                    if let Err(e) = eval_gateway_probe_gate_reachable(&line[pass_idx..]) {
                        eprintln!(
                            "FAIL [check 4 — behavioral]: GatewayProbeGate reachable half: {e}"
                        );
                        return 1;
                    }
                }
                None => {
                    eprintln!(
                        "FAIL [check 4 — behavioral]: GatewayProbeGate: \
                         no half-1 probe=reachable log line seen in device output \
                         (reachable-half assertion not confirmed host-side)"
                    );
                    return 1;
                }
            }
        }
        // Assert unreachable half and log markers.
        if let Err(e) = eval_gateway_probe_gate_unreachable(&gate_report.data, &gate_logs) {
            eprintln!("FAIL [check 4 — behavioral]: GatewayProbeGate {e}");
            return 1;
        }
        println!("  self-test GatewayProbeGate: PASS");

        // Confirm WifiAssociate still returns `up` after the gate test's re-association cycle.
        println!("  Running WifiAssociate (post-gateway-probe-gate check)...");
        let post_gate_resp = match harness.send_command_timeout(
            Command::RunTest(TestName::WifiAssociate),
            test_timeout(&TestName::WifiAssociate),
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to post-GatewayProbeGate WifiAssociate — \
                     WiFi may not have fully recovered"
                );
                return 1;
            }
            Err(e) => {
                eprintln!(
                    "ERROR [check 4 — behavioral]: post-GatewayProbeGate WifiAssociate failed: {e}"
                );
                return 1;
            }
        };
        if check_status(&post_gate_resp, "post-GatewayProbeGate WifiAssociate").is_err() {
            return 1;
        }
        match check4_test_report(&post_gate_resp).and_then(|r| eval_wifi_info(&r.data)) {
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "FAIL [check 4 — behavioral]: post-GatewayProbeGate WifiAssociate link assertion: {e}"
                );
                return 1;
            }
        }
        println!("  post-GatewayProbeGate WifiAssociate: PASS");
    }

    // ── CapturePeriodicLine: production capture thread emits its summary line ──
    // Drives inbound audio through the production playback path (device-side
    // `run_capture_periodic_line`) and asserts the production capture thread's
    // periodic `capture: playback tx …` summary `log::info!` appeared at cadence
    // (≥2 lines over the feed window). Follows the WifiReassociation log-line
    // pattern (`send_command_timeout_collect_logs` + a log-substring eval). This is
    // the audio-pipeline-observability §5 HIL regression guard; only the
    // periodic-line presence at cadence is asserted (induced-anomaly warns are out
    // of scope per design §5 / §6 resolved-decision 1).
    if want(TestName::CapturePeriodicLine) {
        println!("  Running CapturePeriodicLine (production capture-thread summary line)...");
        let mut capture_logs: Vec<String> = Vec::new();
        let capture_resp = match harness.send_command_timeout_collect_logs(
            Command::RunTest(TestName::CapturePeriodicLine),
            test_timeout(&TestName::CapturePeriodicLine),
            &mut capture_logs,
        ) {
            Ok(r) => r,
            Err(HarnessError::Timeout) => {
                eprintln!(
                    "ERROR [AC9]: device not responding to RunTest(CapturePeriodicLine) — \
                     capture thread may be wedged or the feed window exceeded the budget"
                );
                return 1;
            }
            Err(e) => {
                eprintln!("ERROR [check 4 — behavioral]: RunTest(CapturePeriodicLine) failed: {e}");
                return 1;
            }
        };
        if check_status(&capture_resp, "CapturePeriodicLine").is_err() {
            return 1;
        }
        let report = match check4_test_report(&capture_resp) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("FAIL [check 4 — behavioral]: CapturePeriodicLine {e}");
                return 1;
            }
        };
        print_report(&TestName::CapturePeriodicLine, &report);
        if let Err(e) = eval_capture_periodic_line(&report.data, &capture_logs) {
            eprintln!("FAIL [check 4 — behavioral]: CapturePeriodicLine {e}");
            return 1;
        }
        println!("  self-test CapturePeriodicLine: PASS");
    }

    // ── BootAssociationRetry: supervisor retries a bogus config with backoff, then
    // recovers without a reboot ────────────────────────────────────────────────
    // Harness-only behavioral step (no `TestName`): the sequence spans minutes of
    // supervisor wall-clock behavior and must observe unsolicited log frames across
    // multiple commands — a blocking device-side test would freeze the protocol loop for
    // the duration and could not observe its own supervisor's logs. Having no `TestName`
    // means it can never be targeted by RESPEAKER_HIL_ONLY, so skip-under-selector is the
    // only coherent gating; it therefore only runs on a full-suite invocation. Must run
    // before NoCredentialsPark, which deprovisions and must stay last.
    //
    // Not exercised: a true cold boot with the AP absent, and the backoff timer firing
    // into a newly-reachable AP with unchanged credentials — both require harness AP
    // control, which does not exist. Accepted residual.
    match only {
        Some(sel) => {
            println!(
                "  *** NOTICE: BootAssociationRetry SKIPPED — selector active (RESPEAKER_HIL_ONLY={sel:?}); this harness-only step has no TestName and only runs full-suite. ***"
            );
        }
        None => {
            println!(
                "  Running BootAssociationRetry (temporary bogus config, retry-with-backoff, revert-and-recover)..."
            );

            // Precondition: device associated at step start. Guaranteed by construction
            // this far into a full-suite run (WifiAssociate is unconditional earlier in
            // this function, and every preceding network step re-confirms association
            // before returning); this cheap probe defends against future reordering
            // rather than assuming it silently.
            let precheck = harness.send_command_timeout(
                Command::RunTest(TestName::WifiAssociate),
                test_timeout(&TestName::WifiAssociate),
            );
            match precheck {
                Ok(resp) if resp.status == Status::Ok => {
                    let mut guard = TempConfigGuard::arm(harness, "BootAssociationRetry");

                    // 2. Apply a fixed, improbable bogus SSID.
                    let set_resp = match guard
                        .harness
                        .send_command_timeout(bogus_temp_wifi_command(), TEMP_WIFI_COMMAND_TIMEOUT)
                    {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!(
                                "ERROR [check 4 — behavioral]: BootAssociationRetry SetTemporaryWifiConfig failed: {e}"
                            );
                            return 1;
                        }
                    };
                    if check_status(&set_resp, "BootAssociationRetry SetTemporaryWifiConfig")
                        .is_err()
                    {
                        return 1;
                    }

                    // 3. Observe retry-with-backoff for 100 s.
                    let mut failure_logs: Vec<(Instant, String)> = Vec::new();
                    if let Err(e) = guard
                        .harness
                        .drain_logs_for(Duration::from_secs(100), &mut failure_logs)
                    {
                        eprintln!(
                            "ERROR [check 4 — behavioral]: BootAssociationRetry retry-observation drain failed: {e}"
                        );
                        return 1;
                    }
                    if let Err(e) = eval_boot_association_retry_failures(&failure_logs) {
                        eprintln!("FAIL [check 4 — behavioral]: BootAssociationRetry {e}");
                        return 1;
                    }

                    // 4. Revert.
                    let clear_resp = match guard.harness.send_command_timeout(
                        Command::ClearTemporaryWifiConfig,
                        TEMP_WIFI_COMMAND_TIMEOUT,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!(
                                "ERROR [check 4 — behavioral]: BootAssociationRetry ClearTemporaryWifiConfig failed: {e}"
                            );
                            return 1;
                        }
                    };
                    if check_status(&clear_resp, "BootAssociationRetry ClearTemporaryWifiConfig")
                        .is_err()
                    {
                        return 1;
                    }
                    guard.disarm();
                    drop(guard);

                    // 5. Observe recovery without reboot, up to 45 s — exits as soon as
                    // WIFI_REASSOCIATED is seen rather than always burning the full window.
                    let mut recovery_logs: Vec<(Instant, String)> = Vec::new();
                    if let Err(e) = harness.drain_logs_until(
                        WIFI_REASSOCIATED,
                        Duration::from_secs(45),
                        &mut recovery_logs,
                    ) {
                        eprintln!(
                            "ERROR [check 4 — behavioral]: BootAssociationRetry recovery-observation drain failed: {e}"
                        );
                        return 1;
                    }
                    if let Err(e) = eval_boot_association_retry_recovery(&recovery_logs) {
                        eprintln!("FAIL [check 4 — behavioral]: BootAssociationRetry {e}");
                        return 1;
                    }

                    // 6. Confirm final link state.
                    let final_resp = match harness.send_command_timeout(
                        Command::RunTest(TestName::WifiAssociate),
                        test_timeout(&TestName::WifiAssociate),
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!(
                                "ERROR [check 4 — behavioral]: BootAssociationRetry post-recovery WifiAssociate failed: {e}"
                            );
                            return 1;
                        }
                    };
                    if check_status(
                        &final_resp,
                        "BootAssociationRetry post-recovery WifiAssociate",
                    )
                    .is_err()
                    {
                        return 1;
                    }
                    match check4_test_report(&final_resp).and_then(|r| eval_wifi_info(&r.data)) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!(
                                "FAIL [check 4 — behavioral]: BootAssociationRetry post-recovery WifiAssociate link assertion: {e}"
                            );
                            return 1;
                        }
                    }
                    println!("  BootAssociationRetry: PASS");
                }
                Ok(resp) => {
                    println!(
                        "  *** NOTICE: BootAssociationRetry SKIPPED — device not associated at step start (status={:?}). ***",
                        resp.status
                    );
                }
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: BootAssociationRetry precondition probe failed: {e}"
                    );
                    return 1;
                }
            }
        }
    }

    // ── NoCredentialsPark: supervisor parks calmly with no NVS credentials ────
    // Deliberately deprovisions the device and drops WiFi, so it must stay the LAST
    // behavioral step; it restores the provisioned/associated state on the way out.
    // Gated on holding harness credentials (otherwise clearing would destroy the only
    // copy) and on a full-suite run (a selector run must not deprovision as a side effect).
    match (&secrets.wifi, &only) {
        (None, _) => {
            println!(
                "  *** NOTICE: NoCredentialsPark SKIPPED — RESPEAKER_WIFI_SSID / RESPEAKER_WIFI_PASS are not set. ***"
            );
            println!(
                "  *** Clearing credentials without a harness copy would leave the device unprovisionable. ***"
            );
        }
        (_, Some(sel)) => {
            println!(
                "  *** NOTICE: NoCredentialsPark SKIPPED — selector active (RESPEAKER_HIL_ONLY={sel:?}); a selector run must not deprovision the device. ***"
            );
        }
        (Some(creds), None) => {
            println!(
                "  Running NoCredentialsPark (clear credentials, assert calm park, reprovision)..."
            );
            let mut logs: Vec<String> = Vec::new();
            // Armed before the clear is even sent: a failed clear may still have removed
            // one key, which already leaves the device unprovisioned.
            let mut unprovisioned = UnprovisionedNotice::arm();

            // 0. Apply a temporary override first, so the clear below has to prove it
            // clears the override too, not just NVS: if `ClearWifiCredentials` stopped
            // clearing the override, the device would keep retrying this bogus SSID
            // instead of parking, and the calm-park assertion in step 2 (no retry-spam
            // tokens after the park line) would catch it. Armed the same way
            // BootAssociationRetry arms it: any early exit between the override landing
            // and `ClearWifiCredentials` succeeding (which itself clears the override
            // device-side) leaves the device chasing the bogus SSID otherwise.
            let mut override_guard = TempConfigGuard::arm(harness, "NoCredentialsPark");
            let override_resp = match override_guard
                .harness
                .send_command_timeout(bogus_temp_wifi_command(), TEMP_WIFI_COMMAND_TIMEOUT)
            {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: NoCredentialsPark SetTemporaryWifiConfig failed: {e}"
                    );
                    return 1;
                }
            };
            if check_status(&override_resp, "NoCredentialsPark SetTemporaryWifiConfig").is_err() {
                return 1;
            }

            // 1. Clear.
            let clear_resp = match override_guard.harness.send_command_timeout_collect_logs(
                Command::ClearWifiCredentials,
                test_timeout(&TestName::WifiAssociate),
                &mut logs,
            ) {
                Ok(r) => r,
                Err(HarnessError::Timeout) => {
                    eprintln!(
                        "ERROR [AC9]: device not responding to ClearWifiCredentials — wrong/old firmware?"
                    );
                    return 1;
                }
                Err(e) => {
                    eprintln!("ERROR [check 4 — behavioral]: ClearWifiCredentials failed: {e}");
                    return 1;
                }
            };
            if check_status(&clear_resp, "ClearWifiCredentials").is_err() {
                return 1;
            }
            // The device-side clear already removed the override; the guard's own
            // best-effort clear is no longer needed.
            override_guard.disarm();
            drop(override_guard);

            // 2. Assert the cleared state and a calm park.
            let park_resp = match harness.send_command_timeout_collect_logs(
                Command::RunTest(TestName::WifiAssociate),
                test_timeout(&TestName::WifiAssociate),
                &mut logs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: NoCredentialsPark post-clear WifiAssociate failed: {e}"
                    );
                    return 1;
                }
            };
            if let Err(e) = eval_no_credentials_park(&park_resp, &logs) {
                eprintln!("FAIL [check 4 — behavioral]: NoCredentialsPark {e}");
                return 1;
            }

            // 3. Re-provision without a reboot.
            let mut ssid: heapless::String<32> = heapless::String::new();
            let mut pass: heapless::String<64> = heapless::String::new();
            if ssid.push_str(&creds.ssid).is_err() || pass.push_str(&creds.pass).is_err() {
                eprintln!("ERROR [check 4 — behavioral]: NoCredentialsPark credentials too long");
                return 1;
            }
            let reprov_resp = match harness.send_command(Command::ProvisionWifi {
                ssid,
                passphrase: pass,
            }) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: NoCredentialsPark re-ProvisionWifi failed: {e}"
                    );
                    return 1;
                }
            };
            if check_status(&reprov_resp, "NoCredentialsPark re-ProvisionWifi").is_err() {
                return 1;
            }
            unprovisioned.disarm();

            // 4. Assert the doorbell wake brings the link back with no reboot.
            let mut wake_logs: Vec<String> = Vec::new();
            let wake_resp = match harness.send_command_timeout_collect_logs(
                Command::RunTest(TestName::WifiAssociate),
                test_timeout(&TestName::WifiAssociate),
                &mut wake_logs,
            ) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "ERROR [check 4 — behavioral]: NoCredentialsPark post-provision WifiAssociate failed: {e}"
                    );
                    return 1;
                }
            };
            if check_status(&wake_resp, "NoCredentialsPark post-provision WifiAssociate").is_err() {
                return 1;
            }
            let wake_report = match check4_test_report(&wake_resp) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("FAIL [check 4 — behavioral]: NoCredentialsPark {e}");
                    return 1;
                }
            };
            if let Err(e) = eval_wifi_info(&wake_report.data) {
                eprintln!("FAIL [check 4 — behavioral]: NoCredentialsPark post-provision {e}");
                return 1;
            }
            println!("  NoCredentialsPark: PASS");
        }
    }

    println!("Check 4 (behavioral): PASS");

    // ── Step 4: full suite (AC4) ─────────────────────────────────────────────
    // All four checks passed — suite is the same set of tests already run above;
    // no duplicate run needed. AC4 is satisfied by the behavioral check above
    // running all registered tests. Exit zero.

    // Make reduced coverage unmistakable in the final summary, not just the startup line: an
    // evidence/mission-gate transcript must self-document that a selector shrank the suite, so a
    // one-test PASS can never be mistaken for a full-suite PASS (e.g. if RESPEAKER_HIL_ONLY
    // leaked in from the ambient environment). Mission-gate runs must show "FULL SUITE".
    match only {
        Some(sel) => println!(
            "All checks passed. HIL test suite: PASS (SELECTOR ACTIVE: RESPEAKER_HIL_ONLY={sel:?} \
             — only this behavioral test ran; NOT a full-suite pass)"
        ),
        None => println!("All checks passed. HIL test suite: PASS (FULL SUITE)"),
    }
    0
}

// ── Check-1 compare logic ─────────────────────────────────────────────────────

/// Compare two `BuildId` values for check-1 equality.
///
/// Returns `Ok(())` on match; `Err(message)` with a human-readable mismatch
/// description on failure. Extracted so the check-1 e2e test can exercise the
/// compare logic without opening a real serial port.
fn compare_build_ids(
    expected: &device_protocol::BuildId,
    device: &device_protocol::BuildId,
) -> Result<(), String> {
    if device != expected {
        Err(format!(
            "build-ID mismatch.\n  expected: commit={} dirty={}\n  device:   commit={} dirty={}",
            escape_device_str(&expected.commit),
            expected.dirty,
            escape_device_str(&device.commit),
            device.dirty,
        ))
    } else {
        Ok(())
    }
}

// ── Check-4 TestReport dispatch logic ────────────────────────────────────────

/// A check-4 pass predicate over typed report data, paired with the criterion phrase that
/// completes the failure line "`<Name>` Status::Ok but …".
type Check4TypedGate = (fn(&TestData) -> bool, String);

/// The behavioral check-4 gate for a test name, or `None` if the test has no data
/// criterion beyond a well-formed report.
///
/// Sole binding site of test name → pass predicate. Kept apart from the dispatch so a
/// test can assert each pairing directly; inline in the `match` arms the pairing was
/// unobservable and a swapped predicate compiled clean.
fn check4_typed_gate_for(test_name: &TestName) -> Option<Check4TypedGate> {
    match test_name {
        TestName::I2sWaveformSanity => Some((
            eval_i2s_waveform_pass as fn(&TestData) -> bool,
            format!(
                "waveform must be live (ac1 > {I2S_HOST_AUTOCORR_FLOOR}, \
                 max_abs > {I2S_HOST_ZERO_ABS_THRESHOLD}, spread > {I2S_HOST_SPREAD_FLOOR})"
            ),
        )),
        TestName::SpeakerOutput => Some((
            eval_speaker_pass as fn(&TestData) -> bool,
            "must report codec_ok".to_string(),
        )),
        TestName::I2cBusScan => Some((
            eval_i2c_scan_pass as fn(&TestData) -> bool,
            format!(
                "found-list must contain 0x{XVF3800_ADDR:02x} and 0x{AIC3104_ADDR:02x} \
                 on a bus-error-free scan"
            ),
        )),
        TestName::Xvf3800RegRead => Some((
            eval_xvf3800_reg_read_pass as fn(&TestData) -> bool,
            format!(
                "must report status=0x00 and the pinned version {XVF3800_EXPECTED_VERSION:02x?}"
            ),
        )),
        TestName::Xvf3800DoAPlausibility => Some((
            eval_doa_plausibility_pass as fn(&TestData) -> bool,
            "azimuths must be NaN-or-plausible with a finite scanner (index 2)".to_string(),
        )),
        TestName::Xvf3800SpEnergy => Some((
            eval_sp_energy_pass as fn(&TestData) -> bool,
            "SPENERGY values must all be finite and non-negative".to_string(),
        )),
        TestName::AmpAlwaysOnGpoInert => Some((
            eval_amp_gpo_inert_pass as fn(&TestData) -> bool,
            "must report the inert GPO write accepted-DONE".to_string(),
        )),
        TestName::PsramIdentity => Some((
            eval_psram_identity_pass as fn(&TestData) -> bool,
            format!(
                "fails presence/identity criterion (init size={PSRAM_EXPECTED_SIZE_BYTES} \
                 malloc probe not external)"
            ),
        )),
        _ => None,
    }
}

/// Print a typed report's operator line: the machine-checked data plus any human detail.
///
/// `detail` is device-authored text and goes through [`escape_device_str`]; the typed
/// fields are rendered host-side and need no escaping.
fn print_report(test_name: &TestName, report: &TestReport) {
    let detail = escape_device_str(&report.detail);
    println!("  {test_name:?} result: {:?} {detail}", report.data);
}

/// Extract the check-4 typed report, print it, and gate its data on `pred`.
///
/// The typed counterpart of [`gate_check4`]; `criterion` completes the sentence
/// "`<Name>` Status::Ok but …" in the failure line.
fn gate_check4_typed(
    test_name: &TestName,
    resp: &Response,
    pred: fn(&TestData) -> bool,
    criterion: &str,
) -> Result<(), ()> {
    match check4_test_report(resp) {
        Ok(report) => {
            print_report(test_name, &report);
            if !pred(&report.data) {
                eprintln!(
                    "FAIL [check 4 — behavioral]: {test_name:?} Status::Ok but {criterion}: \
                     {:?}",
                    report.data
                );
                return Err(());
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("FAIL [check 4 — behavioral]: {test_name:?} {e}");
            Err(())
        }
    }
}

/// Validate the check-4 payload for a typed-report self-test.
///
/// Accepts `Payload::TestReport`. A `Status::Fail` report must carry non-empty
/// `detail` — a silent failure is a protocol violation. A `Status::Ok` report must say
/// something: typed `data`, or human `detail` for the tests that carry no machine-checked
/// data. An Ok report with neither is information-free and is rejected.
fn check4_test_report(resp: &Response) -> Result<TestReport, String> {
    match &resp.payload {
        Payload::TestReport(report) => {
            if resp.status == Status::Fail && report.detail.is_empty() {
                return Err("TestReport detail is empty on Status::Fail".to_string());
            }
            if resp.status == Status::Ok
                && report.data == TestData::None
                && report.detail.is_empty()
            {
                return Err("TestReport carries neither data nor detail on Status::Ok".to_string());
            }
            Ok(report.clone())
        }
        other => Err(format!(
            "unexpected payload (expected TestReport): {other:?}"
        )),
    }
}

// ── DeserError recovery check (corrupt-frame fault injection) ─────────────────

/// Substring identifying the device's rate-limited `DeserError` warn line as it
/// arrives through `format_log` (`[device <Level>] <target>: <message>`).
const DESER_WARN_SUBSTR: &str = "[device Warn] protocol: COBS DeserError";

/// Build a well-framed COBS packet whose postcard payload fails to deserialize as
/// `Request`, so the device read loop takes the `DeserError` arm.
///
/// The bytes mimic `Request`'s postcard layout `[id-varint, discriminant-varint]`:
/// a `0` id varint followed by discriminant varint `99`. `Command` has 6 variants
/// (discriminants 0–5), so 99 is out of range and postcard's enum decode fails,
/// landing the device in `DeserError` rather than `OverFull`/`Consumed`. The frame
/// is a few bytes, far under the device's `CobsAccumulator<512>` cap.
///
/// Invariant: the discriminant (99) must exceed `Command`'s variant count. If
/// `Command` ever grows to ≥100 variants, 99 becomes legal and this frame decodes
/// as a valid `Request` — bump the discriminant. `corrupt_frame_fails_to_decode`
/// guards this in CI.
fn craft_corrupt_frame() -> Vec<u8> {
    postcard::to_allocvec_cobs(&(0u32, 99u32)).expect("crafting corrupt frame must not fail")
}

/// Run one dedicated post-loop self-test block: send `RunTest(name)`, check status,
/// extract the check-4 report, print it, and run the behavioral `eval`. `description`
/// is the human banner suffix. Returns `Err(())` on any failure so the caller can
/// `return 1`. Factors the shape shared by the credential-less network-phase blocks
/// (WifiScan, WifiPowerSaveCheck) into one place.
fn run_dedicated_test(
    harness: &mut Harness,
    name: TestName,
    description: &str,
    eval: fn(&TestData) -> Result<(), String>,
) -> Result<(), ()> {
    println!("  Running {name:?} ({description})...");
    let resp = match harness.send_command_timeout(Command::RunTest(name), RESPONSE_TIMEOUT) {
        Ok(r) => r,
        Err(HarnessError::Timeout) => {
            eprintln!(
                "ERROR [AC9]: device not responding to RunTest({name:?}) — \
                 wrong/old firmware?"
            );
            return Err(());
        }
        Err(e) => {
            eprintln!("ERROR [check 4 — behavioral]: RunTest({name:?}) failed: {e}");
            return Err(());
        }
    };
    if check_status(&resp, &format!("{name:?}")).is_err() {
        return Err(());
    }
    match check4_test_report(&resp) {
        Ok(report) => {
            print_report(&name, &report);
            if let Err(e) = eval(&report.data) {
                eprintln!("FAIL [check 4 — behavioral]: {name:?} {e}");
                return Err(());
            }
        }
        Err(e) => {
            eprintln!("FAIL [check 4 — behavioral]: {name:?} {e}");
            return Err(());
        }
    }
    println!("  self-test {name:?}: PASS");
    Ok(())
}

/// Fault-injection check for the device's `FeedResult::DeserError` recovery path.
///
/// Phase A (fixed 64-frame burst): inject 64 corrupt frames, then send `Ping`. Asserts
/// the Ping still answers `Status::Ok` (accumulator not wedged, next valid frame decodes)
/// and that a `COBS DeserError` warn line reached the host. The burst is 64 rather than 1
/// so the warn assertion holds regardless of the device's session-cumulative
/// `deser_error_count`: any window of 64 consecutive counts contains exactly one multiple
/// of 64, so exactly one warn fires even on a re-run against an un-rebooted device.
///
/// Phase B (flood): inject 200 corrupt frames back-to-back, then `Ping`. Asserts the
/// Ping answers within the timeout (loop made forward progress, did not stall) and
/// that the number of warn lines stays within the rate-limit bound `1..=N/64 + 2` —
/// a regression to warn-per-frame would yield ~200 lines and fail loudly. The bound
/// is a range, not an exact count, because `deser_error_count` is session-cumulative.
fn run_deser_error_check(harness: &mut Harness) -> Result<(), ()> {
    println!("Running deser-error recovery check (corrupt-frame fault injection)...");
    let frame = craft_corrupt_frame();

    // ── Phase A: fixed 64-frame burst ────────────────────────────────────────
    // 64 frames guarantee exactly one warn regardless of the device's cumulative
    // count, so the check is idempotent across re-runs against an un-rebooted device.
    const PHASE_A_N: usize = 64;
    for _ in 0..PHASE_A_N {
        if let Err(e) = harness.write_raw(&frame) {
            eprintln!("FAIL [deser-error]: write_raw (phase A) failed: {e}");
            return Err(());
        }
    }
    let mut logs_a: Vec<String> = Vec::new();
    let resp_a = match harness.send_command_timeout_collect_logs(
        Command::RunTest(TestName::Ping),
        RESPONSE_TIMEOUT,
        &mut logs_a,
    ) {
        Ok(r) => r,
        Err(HarnessError::Timeout) => {
            eprintln!(
                "FAIL [deser-error]: Ping timed out after a {PHASE_A_N}-frame corrupt burst — \
                 accumulator may be wedged"
            );
            return Err(());
        }
        Err(e) => {
            eprintln!("FAIL [deser-error]: Ping after phase A corrupt burst failed: {e}");
            return Err(());
        }
    };
    if resp_a.status != Status::Ok {
        if let Payload::TestReport(report) = &resp_a.payload {
            eprintln!(
                "  deser-error phase A Ping failure detail: {}",
                escape_device_str(&report.detail)
            );
        }
        eprintln!(
            "FAIL [deser-error]: phase A Ping returned {:?} (expected Ok)",
            resp_a.status
        );
        return Err(());
    }
    let warns_a = logs_a
        .iter()
        .filter(|l| l.contains(DESER_WARN_SUBSTR))
        .count();
    if warns_a < 1 {
        eprintln!(
            "FAIL [deser-error]: no 'COBS DeserError' warn line reached the host after a \
             {PHASE_A_N}-frame corrupt burst; collected logs: {logs_a:?}"
        );
        return Err(());
    }
    println!("  deser-error phase A (warn delivered + next frame decodes): PASS");

    // ── Phase B: corrupt flood ───────────────────────────────────────────────
    const FLOOD_N: usize = 200;
    for _ in 0..FLOOD_N {
        if let Err(e) = harness.write_raw(&frame) {
            eprintln!("FAIL [deser-error]: write_raw (phase B flood) failed: {e}");
            return Err(());
        }
    }
    let mut logs_b: Vec<String> = Vec::new();
    let resp_b = match harness.send_command_timeout_collect_logs(
        Command::RunTest(TestName::Ping),
        RESPONSE_TIMEOUT,
        &mut logs_b,
    ) {
        Ok(r) => r,
        Err(HarnessError::Timeout) => {
            eprintln!(
                "FAIL [deser-error]: Ping timed out after a {FLOOD_N}-frame corrupt flood — \
                 the read loop stalled"
            );
            return Err(());
        }
        Err(e) => {
            eprintln!("FAIL [deser-error]: Ping after corrupt flood failed: {e}");
            return Err(());
        }
    };
    if resp_b.status != Status::Ok {
        if let Payload::TestReport(report) = &resp_b.payload {
            eprintln!(
                "  deser-error phase B Ping failure detail: {}",
                escape_device_str(&report.detail)
            );
        }
        eprintln!(
            "FAIL [deser-error]: phase B Ping returned {:?} (expected Ok)",
            resp_b.status
        );
        return Err(());
    }
    let warns_b = logs_b
        .iter()
        .filter(|l| l.contains(DESER_WARN_SUBSTR))
        .count();
    let max_expected = FLOOD_N / 64 + 2;
    if warns_b < 1 || warns_b > max_expected {
        eprintln!(
            "FAIL [deser-error]: rate-limit bound violated: {warns_b} 'COBS DeserError' warn \
             lines over a {FLOOD_N}-frame flood (expected 1..={max_expected}); the rate limit \
             may have regressed to warn-per-frame"
        );
        return Err(());
    }
    println!(
        "  deser-error phase B (flood non-stall + rate-limit bound): PASS ({warns_b} warn lines)"
    );
    Ok(())
}

// ── I2C scan pass-criterion mirror ───────────────────────────────────────────

/// XVF3800 I2C address expected in the found-list (7-bit, matches device firmware).
const XVF3800_ADDR: u8 = 0x2C;
/// AIC3104 codec I2C address expected in the found-list (7-bit, matches device firmware).
const AIC3104_ADDR: u8 = 0x18;

/// Mirror of the device-side I2C scan pass criterion.
///
/// Returns `true` iff the scan found both required addresses on a bus-error-free scan.
/// Mirrors `run_i2c_bus_scan`'s criterion, providing a host-side enforcement point
/// independent of `Status`, so a device regression returning `Status::Ok` with a wrong
/// or empty found-list — or a dirty bus — is caught here.
///
/// Any other `TestData` variant is rejected: a report carrying another test's data (or
/// `None` from a fail path) can never satisfy the scan criterion.
fn eval_i2c_scan_pass(data: &TestData) -> bool {
    match data {
        TestData::I2cScan { found, bus_errors } => {
            found.contains(&XVF3800_ADDR) && found.contains(&AIC3104_ADDR) && *bus_errors == 0
        }
        _ => false,
    }
}

// ── I2C address-list formatter (mirrors device-side addr_list loop) ───────────

/// Format a slice of I2C addresses as `"0xNN,0xNN,..."` using `{:#04x}` per address.
// Used only in tests; suppress dead_code lint.
#[allow(dead_code)]
///
/// Mirrors the `addr_list` construction loop in `run_i2c_bus_scan`
/// (`firmware/devices/respeaker-pod/src/i2c.rs`): addresses are emitted
/// lower-case with `0x` prefix and exactly 2 hex digits (e.g. `0x08`, `0x2c`),
/// comma-separated, no spaces. An empty slice produces an empty string.
///
/// This function is the host-side reference for the format contract — any change
/// to the device-side loop must be mirrored here so the host unit tests catch it.
fn format_addr_list(addrs: &[u8]) -> String {
    addrs
        .iter()
        .enumerate()
        .fold(String::new(), |mut acc, (i, &addr)| {
            if i > 0 {
                acc.push(',');
            }
            acc.push_str(&format!("{addr:#04x}"));
            acc
        })
}

// ── I2C bus-error detail formatter (mirrors device-side error_detail loop) ─────

/// Format bus-error entries as `"[(0xNN,EEEE),(0xNN,EEEE),...]"`, or `"[]"` when empty.
///
/// Mirrors the `error_detail` construction loop in `run_i2c_bus_scan`
/// (`firmware/devices/respeaker-pod/src/i2c.rs`): push `'['`, then per entry
/// `({eaddr:#04x},{ecode})` — address lower-case with `0x` prefix and exactly 2
/// hex digits, error code a plain decimal `i32` — comma-separated with no spaces,
/// then `']'`.
///
/// This function is the host-side reference for both the format contract *and* the
/// `<80>` capacity proof: it builds into the same `heapless::String<80>` fixed
/// capacity as the device, so the identical overflow-vs-fit arithmetic runs on the
/// host. Capacity proof: each entry `"(0xNN,EEEEEEEEEE)"` is at most 18 chars
/// (`'('` + 4 for `0xNN` + `','` + 11 for `-2147483648` + `')'`); 4 entries give
/// `'[' + 4*18 + 3 separators + ']'` = 77 <= 80. Any change to the device-side loop
/// must be mirrored here so the host unit tests catch it.
// Used only in tests; suppress dead_code lint.
#[allow(dead_code)]
fn format_error_detail(entries: &[(u8, i32)]) -> heapless::String<80> {
    let mut s = heapless::String::<80>::new();
    s.push('[').expect("error_detail capacity");
    for (i, &(eaddr, ecode)) in entries.iter().enumerate() {
        if i > 0 {
            s.push(',').expect("error_detail capacity");
        }
        core::fmt::write(&mut s, format_args!("({eaddr:#04x},{ecode})"))
            .expect("error_detail capacity");
    }
    s.push(']').expect("error_detail capacity");
    s
}

// ── XVF3800 reg-read pass-criterion mirror ────────────────────────────────────

/// Expected XVF3800 VERSION payload bytes (major, minor, patch) as an identity
/// regression pin.
///
/// Observed value: `[0x01, 0x00, 0x00]` = v1.0.0 — confirmed stable across 3
/// consecutive HIL runs on 2026-06-05, stock l16k2ch firmware. Baked as expected
/// identity pending human confirmation (cycle-04-reg-read.md).
const XVF3800_EXPECTED_VERSION: [u8; 3] = [0x01, 0x00, 0x00];

/// Mirror of the device-side XVF3800 register-read pass criterion, with identity pin.
///
/// Returns `true` iff the data represents transport success (`status == 0x00`, CTRL_DONE)
/// AND the pinned firmware version (regression guard — update if the image changes).
///
/// The device's implausible-payload guard needs no host counterpart: a fail path carries
/// `TestData::None`, and no all-zero / all-0xFF payload matches the pinned version.
fn eval_xvf3800_reg_read_pass(data: &TestData) -> bool {
    match data {
        TestData::Xvf3800RegRead { status, version } => {
            *status == 0x00 && *version == XVF3800_EXPECTED_VERSION
        }
        _ => false,
    }
}

// ── PsramIdentity pass-criterion mirror ───────────────────────────────────────

/// Vendor-documented PSRAM size for the XIAO ESP32-S3 R8 module (8 MiB octal SPI PSRAM),
/// in bytes. Mirrors the device-side `PSRAM_EXPECTED_SIZE_BYTES` in respeaker-pod's
/// health.rs — keep in sync. Asserted, not measured: a differing observed size is a
/// hardware discovery for human review, not a silent constant change on either side.
const PSRAM_EXPECTED_SIZE_BYTES: usize = 8 * 1024 * 1024;

/// Host-side second enforcement point for the `PsramIdentity` self-test.
///
/// Returns `true` iff the data asserts presence, identity, and allocator-stays-internal:
/// - `init` — PSRAM initialized.
/// - `size == PSRAM_EXPECTED_SIZE_BYTES` — the part is the expected 8 MiB.
/// - `malloc_probe` is not `External` — a plain-malloc that spilled to PSRAM is a
///   `SPIRAM_USE_CAPS_ALLOC` violation, caught here in case the device ever returns
///   `Status::Ok` alongside the spill.
///
/// `spiram_free` is observability only and is not asserted. Any other `TestData` variant
/// is rejected — including the `None` a fail path carries.
fn eval_psram_identity_pass(data: &TestData) -> bool {
    match data {
        TestData::PsramIdentity {
            init,
            size,
            spiram_free: _,
            malloc_probe,
        } => {
            *init
                && usize::try_from(*size) == Ok(PSRAM_EXPECTED_SIZE_BYTES)
                && *malloc_probe != MallocProbe::External
        }
        _ => false,
    }
}

// ── SpeakerOutput pass-criterion mirror ───────────────────────────────────────

/// Host-side second enforcement point for the `SpeakerOutput` self-test (design §2.7).
///
/// Returns `true` iff the report carries the `SpeakerOutput` variant with `codec_ok` set —
/// the device-side *programmatic* contract that the playback pipeline initialized and ran
/// without I2C/I2S fault: `aic3104_init` issued the §5 register sequence without an I2C
/// fault and every persistent config register read back at its written value. Every
/// fail path carries `TestData::None`, so a wrong variant is never a pass. `freq`/`amp`/
/// `dur_ms` are the requested tone parameters, reported for observability, not graded.
///
/// There is no amp-enable field: the TPA3139D2 amp is always-on hardware on this board
/// and the cmd-0 GPO write is read-only, so there is no software amp-enable to assert
/// (AmpAlwaysOnGpoInert self-test; design realfix §2.3/§2.5). The DAC soft-mute is the
/// click-safe lever; codec init is the programmatic token PASS gates on.
///
/// This deliberately does NOT assert sound: there is no programmatic loopback, so the
/// acoustic result (audible 440 Hz at correct pitch) is confirmed by the operator's ear.
/// It exists to catch the same class of regression the other hardware tests' second
/// enforcement points catch — a device returning `Status::Ok` with a stale/absent token.
///
fn eval_speaker_pass(data: &TestData) -> bool {
    match data {
        TestData::SpeakerOutput {
            freq: _,
            amp: _,
            dur_ms: _,
            codec_ok,
        } => *codec_ok,
        _ => false,
    }
}

// ── Amp always-on GPO-inert pass-criterion mirror ─────────────────────────────

/// Mirror of the device-side `AmpAlwaysOnGpoInert` pass criterion.
///
/// The test asserts that writing the amp-enable GPO has no effect (the amp is always-on
/// hardware). Reaching the `AmpGpoInert` variant at all *is* the inert verdict: the device
/// emits it only after confirming the write was accepted-DONE and X0D31 did not move, and
/// every rejection path carries `TestData::None`. The `x0d31` / `write_status` fields are
/// informational, so only `write_status` DONE is re-asserted here.
fn eval_amp_gpo_inert_pass(data: &TestData) -> bool {
    match data {
        TestData::AmpGpoInert {
            x0d31: _,
            write_status,
        } => *write_status == 0x00,
        _ => false,
    }
}

// ── DoA plausibility pass-criterion mirror ────────────────────────────────────

/// Mirror of the device-side DoA plausibility pass criterion.
///
/// Returns `true` iff `status == 0x00` (CTRL_DONE) and the four azimuth values satisfy:
/// every non-NaN value is finite and |x| ≤ π (`doa_azimuth_ok`); index 2 (the free-running
/// scanner) must additionally be finite, while indices 0/1/3 may legitimately be NaN in a
/// quiet room. NaN and Inf cross the wire as real `f32` bit patterns.
fn eval_doa_plausibility_pass(data: &TestData) -> bool {
    match data {
        TestData::Xvf3800Doa { status, az } => {
            *status == 0x00 && az.iter().all(|&v| doa_azimuth_ok(v)) && az[2].is_finite()
        }
        _ => false,
    }
}

// ── SPENERGY plausibility pass-criterion mirror ───────────────────────────────

/// Host-side plausibility check for the `Xvf3800SpEnergy` self-test.
///
/// Returns `true` iff `status == 0x00` and all four values are finite, non-NaN and ≥ 0
/// (`sp_energy_ok`, the shared device predicate).
///
/// All-zero is a valid PASS — SPENERGY is per-beam speech energy; 0.0 = no speech present.
/// An unattended HIL run cannot guarantee speech, so all-zero is expected and correct.
/// Magnitude/threshold proving is done via interactive full-system testing, not HIL.
///
/// Used by check-4 as a second enforcement point (re-derives PASS from the report data,
/// independent of the `Status` field, mirroring the device-side criterion).
fn eval_sp_energy_pass(data: &TestData) -> bool {
    match data {
        TestData::Xvf3800SpEnergy { status, sp } => {
            *status == 0x00 && sp.iter().all(|&v| sp_energy_ok(v))
        }
        _ => false,
    }
}

// ── I2S waveform sanity pass-criterion mirror ─────────────────────────────────

/// Host-side mirror of the dead-line guard (mirrors device `ZERO_ABS_THRESHOLD`).
/// Minimum absolute peak `max(|min|, |max|)`; NOT a loudness floor. Keep in sync.
const I2S_HOST_ZERO_ABS_THRESHOLD: i32 = 16;

/// Host-side mirror of the frozen-line guard (mirrors device `STUCK_SPREAD_FLOOR`).
/// Minimum spread (`max − min`) separating quiet-real audio (≥ 76) from a frozen /
/// 1-bit line (≈ 0). Keep in sync with the device constant.
const I2S_HOST_SPREAD_FLOOR: i32 = 32;

/// Host-side mirror of the lag-1 autocorrelation floor (mirrors device `AUTOCORR_FLOOR` × 1000)
/// — the PRIMARY health gate. The device emits `ac1=` as r1 × 1000 (integer milli-units);
/// this threshold is 200 (= 0.2). RNG noise has ac1 ≈ 0; confirmed quiet-room acoustic audio
/// has ac1 0.41–0.97 (ADR 2026-06-17). Keep in sync.
const I2S_HOST_AUTOCORR_FLOOR: i32 = 200;

/// Mirror of the device-side I2S waveform PASS criterion, evaluated from the typed report.
///
/// Returns `true` iff the report carries the `I2sWaveform` variant (every fail path carries
/// `TestData::None`, so a Status/data mismatch can never pass) and:
/// - `ac1` (lag-1 autocorrelation × 1000) exceeds `I2S_HOST_AUTOCORR_FLOOR`
///   (the primary health gate — correlated real audio vs. RNG noise).
/// - `max_abs = max(|min|, |max|)` exceeds `I2S_HOST_ZERO_ABS_THRESHOLD` (not a dead line).
/// - `spread = max − min` exceeds `I2S_HOST_SPREAD_FLOOR` (not a frozen / constant line).
///
/// There is deliberately no minimum-`rms` (loudness) gate: a healthy mic in a quiet room
/// produces quiet correlated audio that must PASS (ADR 2026-06-17). `rms`, `sat_pct` and
/// `samples` are reported for observability but not graded here.
fn eval_i2s_waveform_pass(data: &TestData) -> bool {
    let TestData::I2sWaveform {
        min,
        max,
        rms: _,
        sat_pct: _,
        samples: _,
        ac1,
    } = data
    else {
        return false;
    };
    // The device reports neither spread nor max_abs directly; derive both from min & max.
    // Widened to i64: a corrupt-but-decodable frame can carry any i32, and i32::MIN would
    // overflow both `abs()` and the subtraction.
    let max_abs = i64::from(min.unsigned_abs().max(max.unsigned_abs()));
    let spread = i64::from(*max) - i64::from(*min);
    let live = *ac1 > I2S_HOST_AUTOCORR_FLOOR
        && max_abs > i64::from(I2S_HOST_ZERO_ABS_THRESHOLD)
        && spread > i64::from(I2S_HOST_SPREAD_FLOOR);
    if !live {
        eprintln!(
            "FAIL [threshold]: I2S waveform data does not meet host criteria \
             (ac1={ac1} max_abs={max_abs} spread={spread})"
        );
        return false;
    }
    true
}

/// Environment variable naming the WiFi network to provision and to look for in a
/// scan. Shared by [`eval_wifi_scan`] and [`resolve_secrets`]'s caller so the two
/// readers cannot drift apart.
const RESPEAKER_WIFI_SSID_VAR: &str = "RESPEAKER_WIFI_SSID";

/// Validate a `WifiScan` result, reading the configured SSID from the environment.
///
/// Supplies `RESPEAKER_WIFI_SSID` (the provisioning variable) to
/// [`eval_wifi_scan_with`], keeping the evaluation logic itself free of I/O. Must
/// match the `eval` callback signature `run_dedicated_test` expects.
fn eval_wifi_scan(data: &TestData) -> Result<(), String> {
    eval_wifi_scan_with(data, std::env::var(RESPEAKER_WIFI_SSID_VAR).ok().as_deref())
}

/// Whether the configured SSID appears in a scanned SSID list.
///
/// `None` means the check was skipped: `configured` is absent or empty. Otherwise
/// the SSID is truncated by the device's own rule before comparing, so a non-ASCII
/// SSID whose UTF-8 form exceeds the device's buffer still matches.
fn configured_ssid_seen(
    configured: Option<&str>,
    ssids: &[heapless::String<SSID_TRUNC_BYTES>],
) -> Option<bool> {
    let target = configured.filter(|ssid| !ssid.is_empty())?;
    let check_prefix = truncate_utf8_prefix(target, SSID_TRUNC_BYTES);
    Some(ssids.iter().any(|s| s.as_str() == check_prefix))
}

/// Pure evaluation logic, separated from the environment read for testability.
///
/// Asserts `aps > 0` (at least one AP found, proving the radio inited and the scan
/// produced output). `configured_ssid` is the optional `RESPEAKER_WIFI_SSID` value:
/// `Some(ssid)` checks that SSID appears in the reported `ssids` list as a non-fatal
/// diagnostic, while `None` or `Some("")` skips the check entirely.
fn eval_wifi_scan_with(data: &TestData, configured_ssid: Option<&str>) -> Result<(), String> {
    let TestData::WifiScan {
        aps,
        best_rssi: _, // observability only — no threshold is asserted on signal strength
        ssids,
    } = data
    else {
        return Err(format!("expected WifiScan result data, got: {data:?}"));
    };
    if *aps == 0 {
        return Err(
            "scan found 0 APs — radio up but nothing heard (antenna/RF issue?)".to_string(),
        );
    }
    // Non-fatal: a missing SSID is logged, not failed. The AP count assertion is the
    // hard gate; SSID presence is a diagnostic hint when the target is in range.
    if configured_ssid_seen(configured_ssid, ssids) == Some(false) {
        let check_prefix = truncate_utf8_prefix(configured_ssid.unwrap_or(""), SSID_TRUNC_BYTES);
        eprintln!(
            "  WifiScan: configured SSID '{check_prefix}' not found in scan list \
                 (device may be out of range or SSID is hidden): {ssids:?}"
        );
    }
    Ok(())
}

/// Validate the `WifiPowerSaveCheck` payload: modem power save must be off.
///
/// Asserts the raw `wifi_ps_type_t` read back is `0` (`WIFI_PS_NONE`). A non-zero
/// value means power save is on — the root-cause mechanism of a host→device
/// playback-dropout regime — and fails loudly with the raw mode.
fn eval_wifi_power_save(data: &TestData) -> Result<(), String> {
    let TestData::WifiPowerSaveCheck { ps_mode } = data else {
        return Err(format!(
            "expected WifiPowerSaveCheck result data, got: {data:?}"
        ));
    };
    if *ps_mode != device_protocol::WIFI_PS_NONE_RAW {
        return Err(format!(
            "modem power save is ON: ps_mode={ps_mode} (expected 0=WIFI_PS_NONE; \
             1=MIN_MODEM 2=MAX_MODEM) — downlink playback dropouts expected"
        ));
    }
    Ok(())
}

/// Check that a `Response` carries `Status::Ok`.
///
/// On `Fail` logs the report detail and a `FAIL [label]` line. On `Unsupported`
/// logs a `FAIL [label]` line. Returns `Ok(())` iff `Status::Ok`.
///
/// Consolidates the 6 structurally-identical `match resp.status` blocks across the
/// network-phase handlers.
fn check_status(resp: &Response, label: &str) -> Result<(), ()> {
    match resp.status {
        Status::Ok => Ok(()),
        Status::Fail => {
            if let Payload::TestReport(report) = &resp.payload {
                eprintln!(
                    "  {label} failure detail: {}",
                    escape_device_str(&report.detail)
                );
            }
            eprintln!("FAIL [check 4 — behavioral]: {label} returned Fail");
            Err(())
        }
        Status::Unsupported => {
            eprintln!(
                "FAIL [check 4 — behavioral]: {label} returned Unsupported \
                 (logic bug — unreachable on happy path after check 3)"
            );
            Err(())
        }
    }
}

/// Where a registered self-test is dispatched during check 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestPhase {
    /// Uniform check-4 loop: plain `send_command`, default timeout. The `timeout`
    /// field on a `Generic` test is ignored (the generic loop does not pass it), so a
    /// custom timeout there is dead configuration — `generic_phase_tests_use_default_timeout`
    /// guards against it.
    Generic,
    /// Dedicated network block after the generic loop (needs captured pod IP, bound
    /// server handles, and per-test timeouts beyond `RESPONSE_TIMEOUT`).
    Network,
    /// Dedicated local block: needs a log-collecting send (so the host can assert the
    /// periodic summary lines the generic loop's plain `send_command` does not collect)
    /// and/or a long feed with a per-test timeout.
    DedicatedLocal,
}

/// Per-test classification: which check-4 phase dispatches it and, for the dedicated
/// blocks, the timeout to use with `send_command_timeout`.
#[derive(Debug, Clone, Copy)]
struct TestMeta {
    phase: TestPhase,
    timeout: Duration,
}

/// The single source of truth for per-test check-4 classification.
///
/// One exhaustive match with no `_` arm: adding a `TestName` variant is a compile error
/// here, forcing a conscious (phase, timeout) decision. `Network` and `DedicatedLocal`
/// tests are dispatched in their own blocks after the generic loop; `Generic` tests run
/// in the uniform loop with plain `send_command` (their `timeout` is ignored, so it is
/// always `RESPONSE_TIMEOUT`).
const fn test_meta(t: &TestName) -> TestMeta {
    match t {
        // ── Network phase ──────────────────────────────────────────────────────
        TestName::WifiAssociate => TestMeta {
            phase: TestPhase::Network,
            // 25 s — accommodates first-connect 5–15 s + DHCP, with margin.
            timeout: Duration::from_secs(25),
        },
        TestName::UdpRoundtrip | TestName::TlsReachability => TestMeta {
            phase: TestPhase::Network,
            // 15 s — network I/O with margin.
            timeout: Duration::from_secs(15),
        },
        TestName::TlsInboundFrames => TestMeta {
            phase: TestPhase::Network,
            // Idle fail-fast budget (3 × 2 s) + connect (5 s) + TLS-PSK handshake (3 s)
            // + serial-round-trip margin. See the budget-invariant test for the exact
            // accounting.
            timeout: Duration::from_secs(20),
        },
        TestName::TlsSendBackpressure => TestMeta {
            phase: TestPhase::Network,
            // Backpressure: two sub-cases run sequentially — A saturate-then-drain plus
            // C's ~1.0 s ceiling + bounded byte-drip grants — plus per-sub-case warm-up,
            // the TLS-PSK handshake (3 s per connection), and generous network/IO margin.
            timeout: Duration::from_secs(35),
        },
        TestName::TlsInboundBackpressure => TestMeta {
            phase: TestPhase::Network,
            // Device deadline (INBOUND_BP_DEADLINE_SECS 20s) + connect + TLS-PSK handshake
            // + one in-flight read overshoot + serial-round-trip margin, so the host always
            // sees a typed TestReport rather than a hang. See the budget-invariant test for
            // the exact accounting.
            timeout: Duration::from_secs(35),
        },
        TestName::WifiReassociation => TestMeta {
            phase: TestPhase::Network,
            // 90 s device-side poll + ~15 s worst-case association + 15 s margin.
            timeout: Duration::from_secs(120),
        },
        TestName::GatewayProbeGate => TestMeta {
            phase: TestPhase::Network,
            // 2 × ping-budget (~8 s) + 90 s device-side poll + ~15 s association + 15 s margin.
            timeout: Duration::from_secs(130),
        },
        TestName::TlsPskHandshake => TestMeta {
            phase: TestPhase::Network,
            // Device worst case: TCP connect (TLS_PSK_CONNECT_TIMEOUT_SECS 10 s) +
            // handshake deadline (TLS_HANDSHAKE_TIMEOUT_SECS 3 s) + echo round-trip
            // (TLS_PSK_ECHO_TIMEOUT_SECS 5 s) = 18 s, plus serial-round-trip margin,
            // so the host always sees the typed TestReport rather than a hang. See
            // the budget-invariant test for the exact accounting.
            timeout: Duration::from_secs(25),
        },
        TestName::TlsPskWrongKeyRejected => TestMeta {
            phase: TestPhase::Network,
            // Device worst case: reachability pre-probe connect + the TLS connect
            // (2 × TLS_PSK_CONNECT_TIMEOUT_SECS = 20 s) + handshake deadline
            // (TLS_HANDSHAKE_TIMEOUT_SECS 3 s) = 23 s, plus serial-round-trip margin.
            // See the budget-invariant test for the exact accounting.
            timeout: Duration::from_secs(30),
        },
        TestName::PollReadinessBidir => TestMeta {
            phase: TestPhase::Network,
            // POLLOUT poll + up to a 5 s POLLIN_WAIT_BUDGET (device-side) + connect +
            // TLS-PSK handshake (3 s) + serial round-trip, with margin so the host always
            // sees the typed TestReport.
            timeout: Duration::from_secs(20),
        },
        TestName::StreamRealtimeDuplex => TestMeta {
            phase: TestPhase::Network,
            // Two sequential ~5 s synthetic segments (Scenario A outbound-only, then Scenario B
            // duplex — each RTD_PRODUCER_FRAMES × 20 ms) + two connects + two TLS-PSK
            // handshakes (3 s each) + drain tails + serial round-trip, with margin so the host
            // always sees the typed TestReport even when the current (slow) loop stretches each
            // catch-up wall well past 5 s.
            timeout: Duration::from_secs(45),
        },
        // WifiScan and WifiPowerSaveCheck are Network-phase but use the default timeout
        // (both are credential-less, manually dispatched before provisioning; the quirk
        // is pinned by `generic_phase_tests_use_default_timeout`'s explicit assertions).
        TestName::WifiScan | TestName::WifiPowerSaveCheck => TestMeta {
            phase: TestPhase::Network,
            timeout: RESPONSE_TIMEOUT,
        },
        // ── DedicatedLocal phase ───────────────────────────────────────────────
        TestName::CapturePeriodicLine => TestMeta {
            phase: TestPhase::DedicatedLocal,
            // ~2.5 s audio feed (CAPTURE_PERIODIC_LINE_FEED_MS) + device pre-work + serial
            // round-trip, with margin so the host always sees the typed TestReport.
            timeout: Duration::from_secs(15),
        },
        TestName::PlaybackDrainRate => TestMeta {
            phase: TestPhase::DedicatedLocal,
            // ~5 s over-delivering feed (PLAYBACK_DRAIN_RATE_FEED_MS) spanning several ~1 s
            // emit windows + device pre-work + serial round-trip, with margin so the host
            // always collects the periodic lines and the typed TestReport (design §5 Q1).
            // NOTE: a separate ~2 s PLAYBACK_DRAIN_PRETEST_SETTLE_MS sleep runs in the runner
            // BEFORE this command times out (it is not counted here), so the worst-case wall
            // time for the block is settle + this timeout ≈ 22 s (errhandling-3).
            timeout: Duration::from_secs(20),
        },
        TestName::FullDuplexRxIntegrity => TestMeta {
            phase: TestPhase::DedicatedLocal,
            // ~5 s over-delivering feed (FULL_DUPLEX_RX_FEED_MS) spanning several ~1 s emit
            // windows + device pre-work + serial round-trip, with margin so the host always
            // collects the periodic `capture: playback obs …` lines and the typed TestReport
            // (design §5). No pretest settle: it runs right after PlaybackDrainRate with the
            // capture loop already warm and RX draining at cadence.
            timeout: Duration::from_secs(20),
        },
        // ── Generic phase (uniform loop; timeout ignored, always default) ──────
        TestName::Ping
        | TestName::Identify
        | TestName::GpioSelfTest
        | TestName::DeviceHealthCheck
        | TestName::I2cBusScan
        | TestName::Xvf3800RegRead
        | TestName::Xvf3800DoAPlausibility
        | TestName::I2sWaveformSanity
        | TestName::Xvf3800SpEnergy
        | TestName::SpeakerOutput
        | TestName::AmpAlwaysOnGpoInert
        | TestName::PsramIdentity => TestMeta {
            phase: TestPhase::Generic,
            timeout: RESPONSE_TIMEOUT,
        },
    }
}

/// Returns the timeout to use with `send_command_timeout` for a given `TestName`.
fn test_timeout(t: &TestName) -> Duration {
    test_meta(t).timeout
}

// ── HIL secrets / credential sourcing (AC-B1.3) ───────────────────────────────

/// WiFi credentials loaded from the environment or a gitignored local file.
///
/// Both SSID and passphrase must be present together; if either is absent both are
/// treated as absent (NVS fallback path).
#[derive(Debug, Clone)]
struct WifiCreds {
    ssid: String,
    pass: String,
}

/// TLS reachability config (host IP + port) loaded from env or secrets file.
#[derive(Debug, Clone)]
struct TlsConfig {
    /// TLS target as a 4-byte IPv4 address (literal IP, no DNS).
    host: [u8; 4],
    port: u16,
}

/// Default fixed port for the UDP echo server (below OS ephemeral range 32768–60999).
const DEFAULT_HIL_UDP_PORT: u16 = 17380;
/// Default fixed port for the TCP audio-frame source server used by `TlsInboundFrames`.
const DEFAULT_HIL_INBOUND_FRAMES_PORT: u16 = 17382;
/// Number of `StreamFrame::Audio` frames the inbound-frames-source server sends per connection.
/// The device must receive exactly this many; `eval_tls_inbound_frames` enforces the count.
const INBOUND_FRAMES_COUNT: u32 = 10;
/// Number of `StreamFrame::Audio` frames the inbound-frames-source server's **flood**
/// profile sends, unpaced, per `TlsInboundBackpressure` connection. Host-only constant:
/// the host both sends and evaluates the count; the device merely reports what it
/// received. Sized to clear the ring (~102
/// frames) + `FrameAccumulator` (~1 frame) + lwIP recv buffer (~8 frames) + in-flight
/// drain (tens of frames) with ≥ 2× margin, guaranteeing a sustained window-closed
/// regime: 300 frames (~197 KB wire, 6 s of audio) against ~115 frames of total
/// elastic capacity.
const INBOUND_BP_FLOOD_FRAMES: u32 = 300;
/// Default fixed port for the TCP backpressure source server used by `TlsSendBackpressure`.
/// The server accepts a connection and then deliberately withholds reads so the device's
/// send buffer fills, driving `send_frame_bp` into its `poll(POLLOUT)` wait.
const DEFAULT_HIL_BACKPRESSURE_PORT: u16 = 17383;
/// Default fixed port for the TCP poll-readiness adversary server used by
/// `PollReadinessBidir`. The server accepts a connection, consumes the device's in-band
/// trigger byte, then immediately queues a fixed payload of inbound bytes back so the device
/// can prove `poll(POLLIN)` readiness on real lwIP.
const DEFAULT_HIL_POLL_READINESS_PORT: u16 = 17384;
/// Default fixed port for the `StreamRealtimeDuplex` listener server. The server accepts the
/// device's outbound streamer connection, decodes the `Hello`/`SegmentStart`/`Audio`/
/// `SegmentEnd` frames, and times the pre-roll burst drain + catch-up wall clock so the host
/// can assert the streamer keeps up with real time.
const DEFAULT_HIL_RTD_PORT: u16 = 17385;
/// Default fixed port for the TLS-PSK listener holding the pod's real audio-link key,
/// used by `TlsPskHandshake`.
const DEFAULT_HIL_TLS_PSK_PORT: u16 = 17386;
/// Default fixed port for the TLS-PSK listener holding a *different* key for the pod's
/// identity, used by `TlsPskWrongKeyRejected`. Separate port rather than a mode byte: the
/// handshake fails before any application byte can select anything.
const DEFAULT_HIL_TLS_PSK_BAD_PORT: u16 = 17387;
/// Number of 20 ms audio frames the device's synthetic producer commits after the pre-roll
/// (250 × 20 ms = 5 s). Manual-sync copy of the device-side `RTD_PRODUCER_FRAMES` (the two
/// crates share no const module); a drift surfaces as an integrity-count mismatch in
/// `eval_stream_realtime_duplex`.
const RTD_PRODUCER_FRAMES: u64 = 250;
/// Burst-drain ceiling (ms): the pre-roll must reach the host within this budget of
/// `SegmentStart`. Slow-loop discrimination scales with the pre-roll frame count. At the
/// product 50-frame pre-roll the paced drain is ~250 ms and a one-frame-per-wake loop needs
/// ≥ ~500 ms, which this 350 ms ceiling cleanly separates.
const RTD_BURST_DRAIN_MAX_MS: u64 = 350;
/// Catch-up ceiling (ms): `SegmentStart`→`SegmentEnd` wall clock — the ~5 s real-time
/// segment plus 500 ms of drain/RTT margin. Initial estimate — tuned on the first hardware run.
const RTD_CATCH_UP_MAX_MS: u64 = 5500;
/// Number of inbound playback frames the host paces to the device during Scenario B (the
/// duplex sub-scenario). The host stops pacing 500 ms (25 × 20 ms) before the device's
/// scripted vad-close so every sent frame has ample time to be consumed before the segment
/// exits — an exact consumed-count assertion would otherwise race the last in-flight frames.
const RTD_PLAYBACK_FRAMES: u64 = RTD_PRODUCER_FRAMES - 25;
/// Real-time cadence for the Scenario B inbound playback pacer (one 20 ms frame per 20 ms).
const RTD_PLAYBACK_FRAME_INTERVAL: Duration = Duration::from_millis(20);
/// Burst lead the Scenario B pacer front-loads before dropping to real-time cadence. The shared
/// source of truth is `audio_pipeline::playback::PLAYBACK_BURST_LEAD_MS`, which also feeds the
/// product host pacer `speech_pipeline::PacerConfig::default().lead_ms` and the surface config
/// guard — a retune there moves this pacer in step. The first `PLAYBACK_BURST_LEAD_MS / 20 ms`
/// frames go out as fast as writes complete (50 frames at the current 1 000 ms value), then the
/// deadline schedule catches up to real time.
const RTD_PLAYBACK_LEAD_MS: u64 = audio_pipeline::playback::PLAYBACK_BURST_LEAD_MS;
/// Number of bytes the poll-readiness server queues back to the device on each connection.
/// Small and fixed: the device only needs to observe `POLLIN` and read ≥1 byte; the exact
/// count is reported on the device PASS line but not asserted against this value.
const POLL_READINESS_PAYLOAD_BYTES: usize = 16;

/// Short saturate-only withhold (ms) for the profiles that must hit a write boundary on a
/// still-alive boundary frame (A saturate-then-drain, C just-over-ceiling).  These
/// profiles must resume (A) or begin dripping (C) while the boundary frame is still alive
/// — i.e. before the device's 1.0 s per-frame `FRAME_WALL_CLOCK_MAX_MS` ceiling fires.
/// A ceiling-length withhold would keep the server silent past the device's ceiling, so
/// the device would always give up *before* the drain resumes: adversary A could never
/// reach `resumed`, and adversary C's drip would never causally drive `c_resume_cycles`.
/// With the receive window clamped to
/// `BACKPRESSURE_RCVBUF_BYTES` the pipe fills within a handful of the device's ~664 B TLS
/// records, so 300 ms is ample, and it stays well under the 750 ms per-wait budget so the
/// resume/first drip lands inside the boundary frame's first wait window.
const BACKPRESSURE_SATURATE_MS: u64 = 300;

/// Requested `SO_RCVBUF` on the backpressure listener, applied before `listen(2)` so
/// accepted sockets inherit it and the window scale advertised at SYN-ACK reflects it
/// (shrinking after accept is unreliable on Linux).
///
/// This is what makes the withhold phase close the TCP window *by construction* rather
/// than by kernel-autotune accident: with no clamp the host kernel keeps ACKing into a
/// 128 KiB autotuned receive buffer, which the device's whole warm-up volume barely
/// reaches, so whether the device ever sees a write boundary depends on the environment.
/// Linux roughly doubles the request and advertises about half of the result as window, so
/// 4096 lands near a 4 KiB window; with the device's 2880 B `CONFIG_LWIP_TCP_SND_BUF_DEFAULT`
/// the end-to-end pipe saturates within ~10 records.  The accepted socket's effective value
/// is logged at accept — kernel rounding is not contractual.
const BACKPRESSURE_RCVBUF_BYTES: usize = 4096;

/// Loose upper bound on the accepted backpressure socket's effective `SO_RCVBUF`.
///
/// Kernel rounding of the [`BACKPRESSURE_RCVBUF_BYTES`] request is not contractual, so this
/// is not an assertion — it is the threshold above which the clamp evidently did not take
/// (an unclamped Linux socket autotunes toward `tcp_rmem[1]`, 128 KiB by default, far above
/// this).  Crossing it means the withhold phase cannot close the TCP window, so the device
/// will run its whole warm-up without ever hitting a write boundary; the accept-time WARN
/// makes that self-diagnosing in the HIL log instead of surfacing as an unexplained
/// `TlsSendBackpressure` exhaustion that reads like a device regression.
const BACKPRESSURE_RCVBUF_CEILING_BYTES: usize = 32 * 1024;

// The device writes a single in-band sub-case selector byte first on a backpressure
// connection (one in-band byte on the raw backpressure socket, not a `Command`/`DeviceFrame`
// message).  Only the `A` saturate-then-drain sub-case remains, so the host no longer
// dispatches on the byte: `consume_backpressure_selector_byte` reads it off the stream and
// discards it, and every connection runs the one remaining `A` profile.  The device's
// `BP_SUBCASE_A` (its `main.rs`) is the source of truth for the byte actually sent.

/// Minimum resume cycles the eval requires on adversary A's boundary frame: ≥1 proves the
/// device hit a genuine write boundary on real lwIP and resumed the frame to `Sent` rather
/// than false-desyncing — the one fact only hardware can establish.
///
/// A resume cycle is *a completed writability wait followed by forward progress*.  Over
/// the TLS link that is `WANT_WRITE` → `poll` → same-bytes retry accepting the whole
/// record; a partial plaintext count never occurs for a single-record frame.  Back-to-back
/// accepting writes with no wait between them count nothing, which is what keeps this floor
/// from passing vacuously.
///
/// Deliberately **not** ≥2: forcing a second distinct device-side boundary on hardware
/// is unreliable (a single host `read` frees far more than one byte of device-side
/// headroom).  The per-wait reset's *repeatability* is proven deterministically off-target
/// by the device's many-cycle slow-drain unit test.
/// Must match the device-side `BACKPRESSURE_A_MIN_RESUME_CYCLES` floor — and its **type**:
/// both are `u32` (the two crates have no shared constant module, so the type is part of
/// the manual-sync contract).  The eval compares it against `parse_token_i32`'s `i32`, so
/// the one comparison site casts locally.
const BACKPRESSURE_A_MIN_RESUME_CYCLES: u32 = 1;

/// All optional HIL config loaded from the environment or a gitignored local file.
///
/// Absent WiFi creds → NVS fallback with a loud announcement (not a run abort).
/// Absent TLS config → TlsReachability test is skipped with a loud announcement.
/// The gitignored file is `firmware/crates/hil-host/.hil-secrets`; each line is
/// `KEY=VALUE`, same shape as an env-var assignment. Environment overrides the file.
#[derive(Debug, Clone)]
struct HilSecrets {
    /// Present iff BOTH RESPEAKER_WIFI_SSID and RESPEAKER_WIFI_PASS are set.
    wifi: Option<WifiCreds>,
    /// Present iff BOTH RESPEAKER_TLS_HOST and RESPEAKER_TLS_PORT are set.
    tls: Option<TlsConfig>,
    /// Resolved UDP echo port (override from RESPEAKER_HIL_UDP_PORT or DEFAULT_HIL_UDP_PORT).
    udp_echo_port: u16,
    /// Resolved TCP audio-frame source port for `TlsInboundFrames`
    /// (override from RESPEAKER_HIL_INBOUND_FRAMES_PORT or DEFAULT_HIL_INBOUND_FRAMES_PORT).
    inbound_frames_port: u16,
    /// Resolved TCP backpressure source port for `TlsSendBackpressure`
    /// (override from RESPEAKER_HIL_BACKPRESSURE_PORT or DEFAULT_HIL_BACKPRESSURE_PORT).
    backpressure_port: u16,
    /// Resolved TCP poll-readiness adversary port for `PollReadinessBidir`
    /// (override from RESPEAKER_HIL_POLL_READINESS_PORT or DEFAULT_HIL_POLL_READINESS_PORT).
    poll_readiness_port: u16,
    /// Resolved TCP `StreamRealtimeDuplex` listener port
    /// (override from RESPEAKER_HIL_RTD_PORT or DEFAULT_HIL_RTD_PORT).
    rtd_port: u16,
    /// Resolved TCP TLS-PSK listener port for `TlsPskHandshake`
    /// (override from RESPEAKER_HIL_TLS_PSK_PORT or DEFAULT_HIL_TLS_PSK_PORT).
    tls_psk_port: u16,
    /// Resolved TCP wrong-key TLS-PSK listener port for `TlsPskWrongKeyRejected`
    /// (override from RESPEAKER_HIL_TLS_PSK_BAD_PORT or DEFAULT_HIL_TLS_PSK_BAD_PORT).
    tls_psk_bad_port: u16,
}

/// Parse a 4-byte IPv4 address from a dotted-decimal string (e.g. "1.2.3.4").
fn parse_ipv4(s: &str) -> Result<[u8; 4], String> {
    let parts: Vec<&str> = s.trim().split('.').collect();
    if parts.len() != 4 {
        return Err(format!("expected a.b.c.d, got: {s:?}"));
    }
    let mut out = [0u8; 4];
    for (i, p) in parts.iter().enumerate() {
        out[i] = p
            .parse::<u8>()
            .map_err(|e| format!("octet {i} not a valid u8 in {s:?}: {e}"))?;
    }
    Ok(out)
}

/// Load HIL secrets from the environment, falling back to a gitignored local file.
///
/// File path: `firmware/crates/hil-host/.hil-secrets`, relative to the repo root.
/// Each non-blank, non-`#` line must be `KEY=VALUE`. Environment wins over file.
/// Never fails: absent/partial config is returned as `None` fields, not an error.
fn load_hil_secrets() -> Result<HilSecrets, String> {
    // ── Collect from file (lower priority) ───────────────────────────────────
    let mut file_vars: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    // Locate the file relative to CARGO_MANIFEST_DIR (set at build time),
    // or fall back to a relative path from cwd when running via `make hil-test`.
    let secrets_path = std::env::var("CARGO_MANIFEST_DIR")
        .map(|d| format!("{d}/.hil-secrets"))
        .unwrap_or_else(|_| "firmware/crates/hil-host/.hil-secrets".to_string());
    if let Ok(f) = std::fs::File::open(&secrets_path) {
        for line in std::io::BufReader::new(f).lines() {
            let line = line.map_err(|e| format!("reading {secrets_path}: {e}"))?;
            let line = line.trim().to_string();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                file_vars.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }

    // ── Resolve a key: env wins over file ────────────────────────────────────
    let get = |key: &str| -> Option<String> {
        std::env::var(key)
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| file_vars.get(key).cloned().filter(|v| !v.is_empty()))
    };

    Ok(resolve_secrets(get, &secrets_path))
}

/// Pure resolution logic for HIL secrets, separated from I/O for testability.
///
/// `get` is a resolver that returns `Some(value)` for a given key name, or `None`
/// if absent. `secrets_path` is used only to construct actionable error messages
/// (printed by the caller, not by this function).
///
/// WiFi creds are `Some` iff BOTH RESPEAKER_WIFI_SSID and RESPEAKER_WIFI_PASS are
/// present. If only one is set that is a configuration mistake — we surface it as
/// a loud warning (printed here to stderr) and treat both as absent. This prevents
/// a silent one-field-set-one-absent NVS fallback that would be confusing.
///
/// TLS config is `Some` iff BOTH RESPEAKER_TLS_HOST and RESPEAKER_TLS_PORT are
/// present and parseable. Absent/unparseable → `None`; parse error → warning.
fn resolve_secrets(get: impl Fn(&str) -> Option<String>, secrets_path: &str) -> HilSecrets {
    // ── WiFi creds ────────────────────────────────────────────────────────────
    let wifi_ssid = get(RESPEAKER_WIFI_SSID_VAR);
    let wifi_pass = get("RESPEAKER_WIFI_PASS");
    let wifi = match (wifi_ssid, wifi_pass) {
        (Some(ssid), Some(pass)) => Some(WifiCreds { ssid, pass }),
        (Some(_), None) => {
            eprintln!(
                "WARN [check 4 — network]: RESPEAKER_WIFI_SSID is set but RESPEAKER_WIFI_PASS is \
                 absent — treating both as unset and using NVS-stored credentials. \
                 Set both in the environment or in {secrets_path} to provision new credentials."
            );
            None
        }
        (None, Some(_)) => {
            eprintln!(
                "WARN [check 4 — network]: RESPEAKER_WIFI_PASS is set but RESPEAKER_WIFI_SSID is \
                 absent — treating both as unset and using NVS-stored credentials. \
                 Set both in the environment or in {secrets_path} to provision new credentials."
            );
            None
        }
        (None, None) => None,
    };

    // ── TLS config ────────────────────────────────────────────────────────────
    let tls_host_str = get("RESPEAKER_TLS_HOST");
    let tls_port_str = get("RESPEAKER_TLS_PORT");
    let tls = match (tls_host_str, tls_port_str) {
        (Some(host_str), Some(port_str)) => {
            let host_result = parse_ipv4(&host_str);
            let port_result = port_str.trim().parse::<u16>();
            match (host_result, port_result) {
                (Ok(host), Ok(port)) => Some(TlsConfig { host, port }),
                (Err(e), _) => {
                    eprintln!(
                        "WARN [check 4 — network]: RESPEAKER_TLS_HOST parse error: {e} — \
                         skipping TlsReachability test"
                    );
                    None
                }
                (_, Err(e)) => {
                    eprintln!(
                        "WARN [check 4 — network]: RESPEAKER_TLS_PORT not a valid u16: {e} — \
                         skipping TlsReachability test"
                    );
                    None
                }
            }
        }
        (Some(_), None) => {
            eprintln!(
                "WARN [check 4 — network]: RESPEAKER_TLS_HOST is set but RESPEAKER_TLS_PORT is \
                 absent — skipping TlsReachability test. Set both in the environment or in {secrets_path}."
            );
            None
        }
        (None, Some(_)) => {
            eprintln!(
                "WARN [check 4 — network]: RESPEAKER_TLS_PORT is set but RESPEAKER_TLS_HOST is \
                 absent — skipping TlsReachability test. Set both in the environment or in {secrets_path}."
            );
            None
        }
        (None, None) => None,
    };

    // ── Echo port resolution ──────────────────────────────────────────────────
    // absent/empty → compiled-in default (no warning; normal case).
    // present but unparseable or == 0 → WARN + fall back to default.
    // Port 0 is rejected: it would mean "OS-assigned ephemeral", the exact behavior
    // this change exists to eliminate.
    let udp_echo_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_UDP_PORT",
        DEFAULT_HIL_UDP_PORT,
        secrets_path,
    );
    let inbound_frames_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_INBOUND_FRAMES_PORT",
        DEFAULT_HIL_INBOUND_FRAMES_PORT,
        secrets_path,
    );
    let backpressure_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_BACKPRESSURE_PORT",
        DEFAULT_HIL_BACKPRESSURE_PORT,
        secrets_path,
    );
    let poll_readiness_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_POLL_READINESS_PORT",
        DEFAULT_HIL_POLL_READINESS_PORT,
        secrets_path,
    );
    let rtd_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_RTD_PORT",
        DEFAULT_HIL_RTD_PORT,
        secrets_path,
    );
    let tls_psk_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_TLS_PSK_PORT",
        DEFAULT_HIL_TLS_PSK_PORT,
        secrets_path,
    );
    let tls_psk_bad_port = resolve_hil_port(
        &get,
        "RESPEAKER_HIL_TLS_PSK_BAD_PORT",
        DEFAULT_HIL_TLS_PSK_BAD_PORT,
        secrets_path,
    );

    HilSecrets {
        wifi,
        tls,
        udp_echo_port,
        inbound_frames_port,
        backpressure_port,
        poll_readiness_port,
        rtd_port,
        tls_psk_port,
        tls_psk_bad_port,
    }
}

/// Resolve a HIL port env/file key to a port number; port 0 and unparseable values fall back to `default_port` with a warning.
fn resolve_hil_port(
    get: &impl Fn(&str) -> Option<String>,
    key: &str,
    default_port: u16,
    secrets_path: &str,
) -> u16 {
    match get(key) {
        None => default_port,
        Some(val) => match val.trim().parse::<u16>() {
            Ok(0) => {
                eprintln!(
                    "WARN [check 4 — network]: {key}=0 is not a valid fixed port (0 means \
                     OS-assigned ephemeral, which this configuration exists to avoid) — \
                     falling back to default {default_port}. \
                     Set a non-zero port in the environment or in {secrets_path}."
                );
                default_port
            }
            Ok(port) => port,
            Err(e) => {
                eprintln!(
                    "WARN [check 4 — network]: {key} value {val:?} is not a valid u16: {e} — \
                     falling back to default {default_port}. \
                     Set a valid port number in the environment or in {secrets_path}."
                );
                default_port
            }
        },
    }
}

// ── TLS-PSK listener fixture (TlsPskHandshake / TlsPskWrongKeyRejected) ───────

/// The one identity/key pair a TLS-PSK listener will accept.
///
/// The identity is the pod id the device itself reported from `ProvisionAudioPsk`,
/// so the fixture authenticates the exact string the device puts in the handshake
/// rather than one the host guessed.
#[derive(Clone)]
struct PodPsk {
    identity: String,
    key: [u8; AUDIO_PSK_LEN],
}

impl PodPsk {
    /// The same identity with a key the device has never seen, for the wrong-key
    /// listener. Derived by inverting every byte so the two keys can never collide,
    /// whatever the random one turned out to be.
    fn with_wrong_key(&self) -> Self {
        let mut key = self.key;
        for b in &mut key {
            *b = !*b;
        }
        PodPsk {
            identity: self.identity.clone(),
            key,
        }
    }
}

/// Redacting `Debug`: the key never reaches a log line or a panic message.
impl std::fmt::Debug for PodPsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodPsk")
            .field("identity", &self.identity)
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Length of the audio-link pre-shared key, in bytes. Manual-sync copy of the
/// device-side `tls_link::PSK_LEN` (the two crates share no const module); a drift
/// surfaces immediately, since `Command::ProvisionAudioPsk`'s field is `[u8; 32]`.
const AUDIO_PSK_LEN: usize = 32;

/// The one ciphersuite this link negotiates, in OpenSSL's spelling. mbedTLS spells
/// the same suite [`EXPECTED_MBEDTLS_SUITE`], which is what the device reports.
const PSK_CIPHERSUITE: &str = "ECDHE-PSK-CHACHA20-POLY1305";

/// The negotiated ciphersuite the device must report, in mbedTLS's spelling.
const EXPECTED_MBEDTLS_SUITE: &str = "TLS-ECDHE-PSK-WITH-CHACHA20-POLY1305-SHA256";

/// The negotiated protocol version the device must report. Both ends are pinned to
/// TLS 1.2 because esp-tls's PSK support is the 1.2 `psk_hint_key` path.
const EXPECTED_TLS_VERSION: &str = "TLSv1.2";

/// Read `AUDIO_PSK_LEN` bytes from the OS CSPRNG.
///
/// A failure is an error, never a silently weak key.
fn generate_audio_psk() -> Result<[u8; AUDIO_PSK_LEN], String> {
    let mut key = [0u8; AUDIO_PSK_LEN];
    getrandom::fill(&mut key)
        .map_err(|e| format!("OS CSPRNG failed generating the audio PSK: {e}"))?;
    Ok(key)
}

/// Build the server-side TLS context a PSK listener accepts with: TLS 1.2 pinned at
/// both ends, the single ECDHE-PSK suite, no certificate, and a callback that hands
/// back `psk.key` only for `psk.identity`.
///
/// A non-matching identity gets a zero-length key, which fails the handshake — so the
/// fixture is an identity test as well as a key test.
fn psk_server_context(psk: &PodPsk) -> Result<openssl::ssl::SslContext, String> {
    use openssl::ssl::{SslContext, SslMethod, SslVersion};

    let psk = psk.clone();
    let mut builder = SslContext::builder(SslMethod::tls_server())
        .map_err(|e| format!("SslContext::builder failed: {e}"))?;
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_2))
        .and_then(|()| builder.set_max_proto_version(Some(SslVersion::TLS1_2)))
        .map_err(|e| format!("pinning TLS 1.2 failed: {e}"))?;
    builder
        .set_cipher_list(PSK_CIPHERSUITE)
        .map_err(|e| format!("set_cipher_list({PSK_CIPHERSUITE}) failed: {e}"))?;
    builder.set_psk_server_callback(move |_ssl, identity, secret| {
        let matches = identity
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .is_some_and(|id| id == psk.identity);
        if matches && secret.len() >= psk.key.len() {
            secret[..psk.key.len()].copy_from_slice(&psk.key);
            Ok(psk.key.len())
        } else {
            Ok(0)
        }
    });
    Ok(builder.build())
}

/// Spawn a TLS-PSK echo listener on `port`, accepting only `psk`'s identity+key.
///
/// After a completed handshake the listener echoes whatever it reads back through the
/// tunnel, which is what `TlsPskHandshake` round-trips. A refused handshake is logged
/// and the connection dropped — that is the expected outcome on the wrong-key
/// listener, so it is a normal event there, not a warning.
fn spawn_tls_psk_listener(
    name: &'static str,
    port: u16,
    port_env_key: &str,
    psk: &PodPsk,
    stop: &Arc<Mutex<bool>>,
) -> Result<thread::JoinHandle<()>, String> {
    let ctx = psk_server_context(psk)?;
    let listener = TcpListener::bind(("0.0.0.0", port)).map_err(|e| {
        format!(
            "failed to bind {name} listener on 0.0.0.0:{port}: {e}\n  \
             (port already in use? set {port_env_key} to a free port)"
        )
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set {name} listener non-blocking: {e}"))?;
    let stop = Arc::clone(stop);
    thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let mut conn: u32 = 0;
            loop {
                if *stop.lock().unwrap() {
                    break;
                }
                match listener.accept() {
                    Ok((stream, peer)) => {
                        println!("[{name}] accepted {peer} conn={conn}");
                        conn += 1;
                        tls_psk_serve(&ctx, stream, &peer, name);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => {
                        if *stop.lock().unwrap() {
                            break;
                        }
                        eprintln!("WARN [{name}]: accept error: {e}");
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        })
        .map_err(|e| format!("failed to spawn {name} thread: {e}"))
}

/// An established server-side TLS-PSK connection over a TCP socket. Every converted HIL
/// fixture runs its byte logic over one of these instead of a raw `TcpStream`; socket
/// options (read/write timeouts, blocking mode) are still reachable via `get_ref()`.
type TlsServerStream = openssl::ssl::SslStream<std::net::TcpStream>;

/// Bind a listening socket on `0.0.0.0:port` whose accepted connections inherit a
/// `SO_RCVBUF` of about `rcvbuf` bytes.
///
/// `std::net::TcpListener` exposes no buffer-size setting, and setting it on an already
/// accepted socket is unreliable on Linux — the receive window scale is advertised in the
/// SYN-ACK, so the clamp has to be in place before `listen(2)`. Hence the raw socket
/// construction. `SO_REUSEADDR` matches what `TcpListener::bind` sets, so a rerun after a
/// closed connection lingers in `TIME_WAIT` still binds.
///
/// The kernel treats the request as a hint (it roughly doubles it and enforces its own
/// floor), so callers must log the effective value rather than assert on it.
fn bind_clamped_rcvbuf_listener(port: u16, rcvbuf: usize) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_recv_buffer_size(rcvbuf)?;
    let addr: std::net::SocketAddr = (std::net::Ipv4Addr::UNSPECIFIED, port).into();
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    Ok(socket.into())
}

/// Effective `SO_RCVBUF` of an accepted socket, for the accept-time diagnostic. Returns
/// `None` if the query fails — a diagnostic must never take a fixture down.
fn effective_recv_buffer_size(stream: &std::net::TcpStream) -> Option<usize> {
    socket2::SockRef::from(stream).recv_buffer_size().ok()
}

/// Complete the server side of a TLS-PSK handshake on one accepted TCP connection.
///
/// Puts the socket in blocking mode with handshake read/write timeouts, negotiates with
/// `ctx`, and returns the established stream. A refused handshake — wrong key, or a
/// plaintext client hitting a TLS fixture — is logged and yields `None`; on the wrong-key
/// listener that is the expected outcome, so it is a normal event, not a warning. The
/// caller sets whatever per-phase socket timeouts its byte logic needs via `get_ref()`.
fn tls_accept(
    ctx: &openssl::ssl::SslContext,
    stream: std::net::TcpStream,
    peer: &std::net::SocketAddr,
    name: &str,
) -> Option<TlsServerStream> {
    let io_timeout = Duration::from_secs(5);
    if let Err(e) = stream
        .set_nonblocking(false)
        .and_then(|()| stream.set_read_timeout(Some(io_timeout)))
        .and_then(|()| stream.set_write_timeout(Some(io_timeout)))
    {
        eprintln!("WARN [{name}]: {peer} socket setup failed: {e}");
        return None;
    }
    let ssl = match openssl::ssl::Ssl::new(ctx) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("WARN [{name}]: Ssl::new failed: {e}");
            return None;
        }
    };
    let mut tls = match openssl::ssl::SslStream::new(ssl, stream) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("WARN [{name}]: SslStream::new failed: {e}");
            return None;
        }
    };
    if let Err(e) = tls.accept() {
        println!("[{name}] {peer} handshake refused: {e}");
        return None;
    }
    println!(
        "[{name}] {peer} handshake OK version={} cipher={}",
        tls.ssl().version_str(),
        tls.ssl()
            .current_cipher()
            .map(|c| c.name())
            .unwrap_or("<none>")
    );
    Some(tls)
}

/// Complete the TLS-PSK handshake on one accepted connection and echo until EOF.
///
/// The socket goes blocking with read/write timeouts for both the handshake and the
/// echo: a device that connects and then stalls must not wedge the listener thread
/// against the next connection.
fn tls_psk_serve(
    ctx: &openssl::ssl::SslContext,
    stream: std::net::TcpStream,
    peer: &std::net::SocketAddr,
    name: &str,
) {
    use std::io::{Read as _, Write as _};

    let mut tls = match tls_accept(ctx, stream, peer, name) {
        Some(t) => t,
        None => return,
    };

    let mut buf = [0u8; 512];
    loop {
        match tls.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if let Err(e) = tls.write_all(&buf[..n]) {
                    eprintln!("WARN [{name}]: {peer} echo write failed: {e}");
                    break;
                }
            }
            Err(e) => {
                // A read timeout is the normal end of this exchange.
                println!("[{name}] {peer} echo read ended: {e}");
                break;
            }
        }
    }
    let _ = tls.shutdown();
}

// ── PeerServers RAII guard ─────────────────────────────────────────────────────

/// RAII guard that owns the UDP echo and TLS-PSK fixture server threads for the duration of
/// the network reachability tests.
///
/// - **UDP echo**: a thread that loops `recv_from` → `send_to` the same bytes back.
/// - **TLS-PSK fixtures**: per-fixture `TcpListener`s that accept a connection, complete the
///   session-key TLS handshake, and run their byte logic inside the tunnel until dropped.
///
/// `Drop` signals every thread to stop (via a shared `Arc<Mutex<bool>>` flag) and joins them
/// so repeated `make hil-test` runs do not collide on bound ports. Fixed ports (configured or
/// the compiled-in defaults) allow static firewall rules; `Drop` joins threads and closes the
/// listeners so the next run can re-bind the same ports without racing a still-live prior process.
struct PeerServers {
    /// Host LAN IP used as the `host` field in `SetTemporaryPeerConfig`.
    host_ip: [u8; 4],
    /// Fixed UDP echo port (configured or default).
    udp_port: u16,
    /// Fixed TCP audio-frame source port for `TlsInboundFrames` (configured or default).
    inbound_frames_port: u16,
    /// Fixed TCP backpressure source port for `TlsSendBackpressure` (configured or default).
    backpressure_port: u16,
    /// Fixed TCP poll-readiness adversary port for `PollReadinessBidir` (configured or default).
    poll_readiness_port: u16,
    /// Fixed TCP `StreamRealtimeDuplex` listener port (configured or default).
    rtd_port: u16,
    /// Fixed TCP TLS-PSK listener port holding the pod's real key (`TlsPskHandshake`).
    tls_psk_port: u16,
    /// Fixed TCP TLS-PSK listener port holding a different key for the same identity
    /// (`TlsPskWrongKeyRejected`).
    tls_psk_bad_port: u16,
    /// Observation recorded by the `StreamRealtimeDuplex` listener for the device's first
    /// (Scenario A, outbound-only) connection: pre-roll burst-drain timing, received sample
    /// count, and end reason. The runner reads this after the device returns its typed result.
    rtd_observation: Arc<Mutex<RtdObservation>>,
    /// Observation for the device's second (Scenario B, duplex) connection — same outbound
    /// fields plus the count of inbound playback frames the host paced to the device.
    rtd_observation_b: Arc<Mutex<RtdObservation>>,
    /// Shared stop flag; set to `true` in `Drop` to signal server threads.
    stop: Arc<Mutex<bool>>,
    /// UDP server thread handle.
    udp_thread: Option<thread::JoinHandle<()>>,
    /// TLS-PSK audio-frame source thread handle for `TlsInboundFrames`.
    inbound_frames_thread: Option<thread::JoinHandle<()>>,
    /// TCP backpressure source thread handle for `TlsSendBackpressure`.
    backpressure_thread: Option<thread::JoinHandle<()>>,
    /// TCP poll-readiness adversary thread handle for `PollReadinessBidir`.
    poll_readiness_thread: Option<thread::JoinHandle<()>>,
    /// TCP `StreamRealtimeDuplex` listener thread handle.
    rtd_thread: Option<thread::JoinHandle<()>>,
    /// TLS-PSK echo listener thread handle (correct key).
    tls_psk_thread: Option<thread::JoinHandle<()>>,
    /// TLS-PSK echo listener thread handle (wrong key for the same identity).
    tls_psk_bad_thread: Option<thread::JoinHandle<()>>,
}

impl PeerServers {
    /// Bind echo servers and the audio-frame source server on the given fixed ports,
    /// derive the host LAN IP from the route to `pod_ip`, and return the guard.
    ///
    /// Ports come from `HilSecrets` (resolved override or compiled-in default).
    /// A failed bind hard-errors — there is no `:0` fallback, by design (AC7).
    fn start(pod_ip: [u8; 4], secrets: &HilSecrets, psk: &PodPsk) -> Result<Self, String> {
        let HilSecrets {
            udp_echo_port: udp_port,
            inbound_frames_port,
            backpressure_port,
            poll_readiness_port,
            rtd_port,
            tls_psk_port,
            tls_psk_bad_port,
            ..
        } = *secrets;
        // Derive host self-IP: connect a UDP socket to pod_ip on an arbitrary port
        // (no packet sent; this selects the egress route/interface), read local_addr().
        let route_sock = UdpSocket::bind("0.0.0.0:0")
            .map_err(|e| format!("failed to bind route-probe UDP socket: {e}"))?;
        route_sock
            .connect(std::net::SocketAddr::from((pod_ip, 9u16)))
            .map_err(|e| format!("failed to connect route-probe socket to pod IP: {e}"))?;
        let host_ip_addr = route_sock
            .local_addr()
            .map_err(|e| format!("failed to read local_addr from route-probe socket: {e}"))?
            .ip();
        let host_ip: [u8; 4] = match host_ip_addr {
            std::net::IpAddr::V4(a) => a.octets(),
            other => {
                return Err(format!(
                    "route to pod is via a non-IPv4 address: {other}; check network config"
                ));
            }
        };

        // Bind UDP echo socket on the fixed port (no :0 fallback — AC7).
        let udp_sock = UdpSocket::bind(("0.0.0.0", udp_port)).map_err(|e| {
            format!(
                "failed to bind UDP echo socket on 0.0.0.0:{udp_port}: {e}\n  \
                 (port already in use? a prior hil-host may still hold it — wait for it to \
                 exit, or set RESPEAKER_HIL_UDP_PORT to a free port)"
            )
        })?;

        let stop = Arc::new(Mutex::new(false));

        // Monotonic reference for accept-time routing forensics: every listener stamps each
        // accepted connection with ms since server start, so which handler served a connection
        // (and when) is a transcript fact instead of an inference. Created here (before any
        // listener thread spawns) so every adversary thread — including inbound-frames-source,
        // spawned below — can capture it.
        let servers_start = std::time::Instant::now();

        // ── UDP echo thread ───────────────────────────────────────────────────
        let stop_udp = Arc::clone(&stop);
        udp_sock
            .set_read_timeout(Some(Duration::from_millis(200)))
            .map_err(|e| format!("failed to set UDP echo socket read timeout: {e}"))?;
        let udp_thread = thread::Builder::new()
            .name("udp-echo".to_string())
            .spawn(move || {
                let mut buf = [0u8; 512];
                loop {
                    if *stop_udp.lock().unwrap() {
                        break;
                    }
                    match udp_sock.recv_from(&mut buf) {
                        Ok((n, peer)) => {
                            // Echo the exact bytes back to the sender.
                            if let Err(e) = udp_sock.send_to(&buf[..n], peer) {
                                eprintln!("WARN [udp-echo]: send_to {peer} failed: {e}");
                            }
                        }
                        Err(ref e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            // No data yet; check stop flag.
                        }
                        Err(e) => {
                            eprintln!("WARN [udp-echo]: recv_from error: {e}");
                        }
                    }
                }
            })
            .map_err(|e| format!("failed to spawn UDP echo thread: {e}"))?;

        // ── TLS-PSK audio-frame source thread (for TlsInboundFrames self-test) ─────
        // Accepts one connection at a time, completes the session-key TLS handshake, then
        // sends INBOUND_FRAMES_COUNT StreamFrame::Audio frames inside the tunnel using the
        // shared encode_frame codec, then closes the connection. The device reads until EOF
        // to know when all frames have been delivered.
        let inbound_ctx = psk_server_context(psk)?;
        let inbound_listener =
            TcpListener::bind(("0.0.0.0", inbound_frames_port)).map_err(|e| {
                format!(
                "failed to bind inbound-frames listener on 0.0.0.0:{inbound_frames_port}: {e}\n  \
                 (port already in use? set RESPEAKER_HIL_INBOUND_FRAMES_PORT to a free port)"
            )
            })?;
        inbound_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set inbound-frames listener non-blocking: {e}"))?;
        let stop_inbound = Arc::clone(&stop);
        let inbound_frames_thread = thread::Builder::new()
            .name("inbound-frames-source".to_string())
            .spawn(move || {
                use audio_pipeline::wire::{
                    AudioFrame, ChannelSource, Hello, StreamFrame,
                    AUDIO_PROTOCOL_VERSION, AUDIO_SAMPLES_PER_FRAME, DEVICE_PLAYBACK_FORMAT,
                };
                // Pre-encode a single zero-PCM Audio frame once; reuse the bytes every
                // connection so we don't allocate/encode per-connection (quality-1).
                // INBOUND_FRAMES_COUNT identical frames are sent per connection.
                let pcm_bytes: heapless::Vec<u8, { audio_pipeline::wire::MAX_AUDIO_PAYLOAD }> =
                    audio_pipeline::wire::pack_pcm_s16le(&[0i16; AUDIO_SAMPLES_PER_FRAME]);
                let frame_template = StreamFrame::Audio(AudioFrame {
                    segment_id: 0,
                    first_sample_index: 0,
                    device_ts_us: 0,
                    pcm: pcm_bytes,
                });
                let mut encode_buf = vec![0u8; audio_pipeline::wire::MAX_FRAME_BYTES + 2];
                let encoded_frame_len = match audio_pipeline::wire::encode_frame(&frame_template, &mut encode_buf) {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("WARN [inbound-frames-source]: pre-encode failed: {e:?}");
                        return;
                    }
                };
                let encoded_frame: Vec<u8> = encode_buf[..encoded_frame_len].to_vec();

                // Pre-encode a leading Hello declaring the device format. The device
                // requires a conforming Hello before any Audio; without it
                // the device drops the connection on the first Audio frame and TlsInboundFrames
                // fails. The Hello is written once per accepted connection, before the
                // INBOUND_FRAMES_COUNT Audio frames; it is not an Audio frame and is not counted
                // by eval_tls_inbound_frames.
                let hello_template = StreamFrame::Hello(Hello {
                    version: AUDIO_PROTOCOL_VERSION,
                    pod_id: heapless::String::new(),
                    sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
                    bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
                    channels: DEVICE_PLAYBACK_FORMAT.channels,
                    codec: DEVICE_PLAYBACK_FORMAT.codec,
                    channel_source: ChannelSource::CommunicationBeam,
                });
                let encoded_hello_len =
                    match audio_pipeline::wire::encode_frame(&hello_template, &mut encode_buf) {
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("WARN [inbound-frames-source]: Hello pre-encode failed: {e:?}");
                            return;
                        }
                    };
                let encoded_hello: Vec<u8> = encode_buf[..encoded_hello_len].to_vec();

                loop {
                    if *stop_inbound.lock().unwrap() {
                        break;
                    }
                    match inbound_listener.accept() {
                        Ok((stream, peer)) => {
                            println!(
                                "[inbound-frames-source] accepted {peer} t={}",
                                servers_start.elapsed().as_millis()
                            );
                            let mut stream = match tls_accept(
                                &inbound_ctx,
                                stream,
                                &peer,
                                "inbound-frames-source",
                            ) {
                                Some(t) => t,
                                None => continue,
                            };
                            if let Err(e) = stream
                                .get_ref()
                                .set_write_timeout(Some(Duration::from_secs(10)))
                            {
                                eprintln!(
                                    "WARN [inbound-frames-source]: set_write_timeout for {peer} failed: {e}"
                                );
                                continue;
                            }
                            inbound_frames_serve(
                                &mut stream,
                                &peer,
                                &stop_inbound,
                                &encoded_hello,
                                &encoded_frame,
                            );
                            // stream drops here → FIN or RST depending on prior errors.
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => {
                            if *stop_inbound.lock().unwrap() {
                                break;
                            }
                            eprintln!("WARN [inbound-frames-source]: accept error: {e}");
                            // Back off on persistent errors to avoid a hot spin loop
                            // that floods stderr and wastes CPU if the FD is invalidated.
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })
            .map_err(|e| format!("failed to spawn inbound-frames-source thread: {e}"))?;

        // ── TCP backpressure source thread (for TlsSendBackpressure self-test) ──
        // Accepts one connection at a time and runs the adversary A saturate-then-drain
        // profile, selected by the single in-band selector byte the device writes first on
        // the connection: withhold reads briefly so the device's next record hits a write
        // boundary (WANT_WRITE), then resume draining at line rate so the frame completes
        // (Sent) under the device's 1.0 s per-frame ceiling. Proves the device's
        // resume-the-tail loop on real lwIP (resume_cycles >= 1). Any stray/no-selector
        // caller runs the same profile. The device never reads on this socket (beyond the
        // selector write, device->host only), so all flow control is one-directional after
        // the selector.
        //
        // This is the one fixture whose listener is not a plain `TcpListener::bind`: its
        // receive buffer is clamped so the withhold closes the TCP window deterministically
        // (see `BACKPRESSURE_RCVBUF_BYTES`).
        let backpressure_listener = bind_clamped_rcvbuf_listener(
            backpressure_port,
            BACKPRESSURE_RCVBUF_BYTES,
        )
        .map_err(|e| {
            format!(
                "failed to bind backpressure listener on 0.0.0.0:{backpressure_port}: {e}\n  \
                 (port already in use? set RESPEAKER_HIL_BACKPRESSURE_PORT to a free port)"
            )
        })?;
        backpressure_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set backpressure listener non-blocking: {e}"))?;
        let backpressure_ctx = psk_server_context(psk)?;
        let stop_bp = Arc::clone(&stop);
        let backpressure_thread = thread::Builder::new()
            .name("backpressure-source".to_string())
            .spawn(move || {
                let mut bp_conn: u32 = 0;
                loop {
                    if *stop_bp.lock().unwrap() {
                        break;
                    }
                    match backpressure_listener.accept() {
                        Ok((stream, peer)) => {
                            // The effective SO_RCVBUF is the saturation mechanism: it
                            // bounds the window the device can fill during the withhold.
                            // Log it so an exhausted warm-up can be told apart from a
                            // clamp the kernel did not honour.
                            let so_rcvbuf = effective_recv_buffer_size(&stream);
                            println!(
                                "[backpressure-source] accepted {peer} conn={bp_conn} t={} \
                                 so_rcvbuf={}",
                                servers_start.elapsed().as_millis(),
                                so_rcvbuf
                                    .map(|v| v.to_string())
                                    .unwrap_or_else(|| "<unknown>".to_string()),
                            );
                            // A socket that did not inherit the clamp cannot be saturated by
                            // the withhold, so the test would exhaust its warm-up for a
                            // fixture-side reason. Say so here rather than let it read as a
                            // device regression.
                            if let Some(v) = so_rcvbuf
                                && v > BACKPRESSURE_RCVBUF_CEILING_BYTES
                            {
                                eprintln!(
                                    "WARN [backpressure-source]: SO_RCVBUF={v} exceeds the \
                                         clamp ceiling {BACKPRESSURE_RCVBUF_CEILING_BYTES} \
                                         (requested {BACKPRESSURE_RCVBUF_BYTES}) — the withhold \
                                         will not close the TCP window, so the device may send \
                                         its whole warm-up without hitting a write boundary"
                                );
                            }
                            bp_conn += 1;
                            let mut stream = match tls_accept(
                                &backpressure_ctx,
                                stream,
                                &peer,
                                "backpressure-source",
                            ) {
                                Some(t) => t,
                                None => continue,
                            };
                            // Take the selector byte off the stream so it is not mistaken
                            // for frame data by the drain. Only the A profile remains, so
                            // the value is not inspected.
                            consume_backpressure_selector_byte(&mut stream, &peer);
                            if backpressure_profile_a(&mut stream, &peer, &stop_bp) {
                                return; // stop requested mid-profile
                            }
                            // stream drops here → FIN to the device.
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => {
                            if *stop_bp.lock().unwrap() {
                                break;
                            }
                            eprintln!("WARN [backpressure-source]: accept error: {e}");
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })
            .map_err(|e| format!("failed to spawn backpressure-source thread: {e}"))?;

        // ── TCP poll-readiness adversary thread (for PollReadinessBidir self-test) ──
        // Accepts one connection at a time, consumes the device's single in-band trigger
        // byte (device→host, no new protocol message type), then immediately queues a small
        // fixed payload of inbound bytes back so the device can observe `poll(POLLIN)`
        // readiness on real lwIP. The device never reads more
        // than once and writes only the trigger byte, so flow control is trivial; the
        // connection is left open briefly (the device polls, reads, then drops its end) and
        // the stream drops here → FIN.
        let poll_readiness_listener =
            TcpListener::bind(("0.0.0.0", poll_readiness_port)).map_err(|e| {
                format!(
                    "failed to bind poll-readiness listener on 0.0.0.0:{poll_readiness_port}: {e}\n  \
                 (port already in use? set RESPEAKER_HIL_POLL_READINESS_PORT to a free port)"
                )
            })?;
        poll_readiness_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set poll-readiness listener non-blocking: {e}"))?;
        let poll_readiness_ctx = psk_server_context(psk)?;
        let stop_poll = Arc::clone(&stop);
        let poll_readiness_thread = thread::Builder::new()
            .name("poll-readiness-source".to_string())
            .spawn(move || {
                let mut pr_conn: u32 = 0;
                loop {
                    if *stop_poll.lock().unwrap() {
                        break;
                    }
                    match poll_readiness_listener.accept() {
                        Ok((stream, peer)) => {
                            println!(
                                "[poll-readiness-source] accepted {peer} conn={pr_conn} t={}",
                                servers_start.elapsed().as_millis()
                            );
                            pr_conn += 1;
                            let mut stream = match tls_accept(
                                &poll_readiness_ctx,
                                stream,
                                &peer,
                                "poll-readiness-source",
                            ) {
                                Some(t) => t,
                                None => continue,
                            };
                            poll_readiness_serve(&mut stream, &peer);
                            // stream drops here → FIN to the device.
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => {
                            if *stop_poll.lock().unwrap() {
                                break;
                            }
                            eprintln!("WARN [poll-readiness-source]: accept error: {e}");
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })
            .map_err(|e| format!("failed to spawn poll-readiness-source thread: {e}"))?;

        // ── TLS-PSK StreamRealtimeDuplex listener thread ───────────────────────
        // Accepts the device's outbound streamer connection, completes the session-key TLS
        // handshake, then decodes the Hello/SegmentStart/Audio/SegmentEnd frames and records
        // burst-drain timing + received sample count + end reason into the shared observation
        // for the runner.
        let rtd_ctx = psk_server_context(psk)?;
        let rtd_listener = TcpListener::bind(("0.0.0.0", rtd_port)).map_err(|e| {
            format!(
                "failed to bind rtd listener on 0.0.0.0:{rtd_port}: {e}\n  \
                 (port already in use? set RESPEAKER_HIL_RTD_PORT to a free port)"
            )
        })?;
        rtd_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set rtd listener non-blocking: {e}"))?;
        // Two observation slots: the device runs Scenario A (outbound-only, read here) then
        // reconnects for Scenario B (duplex — the host paces inbound playback). The accept
        // loop routes the first connection to A and the second to B.
        let rtd_observation = Arc::new(Mutex::new(RtdObservation::default()));
        let rtd_observation_b = Arc::new(Mutex::new(RtdObservation::default()));
        let stop_rtd = Arc::clone(&stop);
        let stop_rtd_playback = Arc::clone(&stop);
        let rtd_obs_thread = Arc::clone(&rtd_observation);
        let rtd_obs_b_thread = Arc::clone(&rtd_observation_b);
        let rtd_thread = thread::Builder::new()
            .name("rtd-listener".to_string())
            .spawn(move || {
                let mut conn_index: u32 = 0;
                loop {
                    if *stop_rtd.lock().unwrap() {
                        break;
                    }
                    match rtd_listener.accept() {
                        Ok((stream, peer)) => {
                            println!(
                                "[rtd-listener] accepted {peer} conn={conn_index} t={}",
                                servers_start.elapsed().as_millis()
                            );
                            let mut stream =
                                match tls_accept(&rtd_ctx, stream, &peer, "rtd-listener") {
                                    Some(t) => t,
                                    None => continue,
                                };
                            let paced = conn_index >= 1;
                            let obs = if paced {
                                &rtd_obs_b_thread
                            } else {
                                &rtd_obs_thread
                            };
                            rtd_serve(
                                &mut stream,
                                &peer,
                                obs,
                                paced,
                                &stop_rtd_playback,
                                conn_index,
                            );
                            conn_index += 1;
                            // stream drops here → FIN to the device.
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => {
                            if *stop_rtd.lock().unwrap() {
                                break;
                            }
                            eprintln!("WARN [rtd-listener]: accept error: {e}");
                            thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            })
            .map_err(|e| format!("failed to spawn rtd-listener thread: {e}"))?;

        // ── TLS-PSK echo listeners (TlsPskHandshake / TlsPskWrongKeyRejected) ──
        // Two listeners, same identity, different keys: one holds the key the device
        // was provisioned with, the other a key it has never seen. Separate ports
        // rather than one listener with a mode byte, because the wrong-key handshake
        // fails before any application byte could select a mode.
        let tls_psk_thread = spawn_tls_psk_listener(
            "tls-psk-source",
            tls_psk_port,
            "RESPEAKER_HIL_TLS_PSK_PORT",
            psk,
            &stop,
        )?;
        let tls_psk_bad_thread = spawn_tls_psk_listener(
            "tls-psk-bad-source",
            tls_psk_bad_port,
            "RESPEAKER_HIL_TLS_PSK_BAD_PORT",
            &psk.with_wrong_key(),
            &stop,
        )?;

        println!(
            "  PeerServers: host_ip={} udp_port={udp_port} inbound_frames_port={inbound_frames_port} backpressure_port={backpressure_port} poll_readiness_port={poll_readiness_port} rtd_port={rtd_port} tls_psk_port={tls_psk_port} tls_psk_bad_port={tls_psk_bad_port}",
            Ipv4Addr::from(host_ip)
        );

        Ok(PeerServers {
            host_ip,
            udp_port,
            inbound_frames_port,
            backpressure_port,
            poll_readiness_port,
            rtd_port,
            tls_psk_port,
            tls_psk_bad_port,
            rtd_observation,
            rtd_observation_b,
            stop,
            udp_thread: Some(udp_thread),
            inbound_frames_thread: Some(inbound_frames_thread),
            backpressure_thread: Some(backpressure_thread),
            poll_readiness_thread: Some(poll_readiness_thread),
            rtd_thread: Some(rtd_thread),
            tls_psk_thread: Some(tls_psk_thread),
            tls_psk_bad_thread: Some(tls_psk_bad_thread),
        })
    }
}

/// Read the single in-band trigger/selector byte the device writes first on a just-accepted
/// connection, with a caller-chosen read timeout. Returns the byte, or `None` on
/// EOF/timeout/error (logged with `prefix` and `on_missing`).
fn read_trigger_byte(
    stream: &mut TlsServerStream,
    peer: &std::net::SocketAddr,
    prefix: &str,
    on_missing: &str,
    timeout: Duration,
) -> Option<u8> {
    use std::io::Read as _;
    if let Err(e) = stream.get_ref().set_read_timeout(Some(timeout)) {
        eprintln!("WARN [{prefix}]: set_read_timeout for {peer} failed: {e}");
        // Fall through: still take the byte / serve below.
    }
    let mut byte = [0u8; 1];
    match stream.read(&mut byte) {
        Ok(1) => Some(byte[0]), // trigger/selector byte consumed
        Ok(_) => {
            eprintln!("WARN [{prefix}]: EOF from {peer} before trigger byte; {on_missing}");
            None
        }
        Err(e) => {
            eprintln!("WARN [{prefix}]: trigger read from {peer} failed ({e}); {on_missing}");
            None
        }
    }
}

/// Serve one poll-readiness connection: consume the device's trigger byte, then write a
/// small fixed payload back so the device's `poll(POLLIN)` reports read-readiness. The
/// payload is queued regardless of EOF/timeout/error on the trigger read, so a stray caller
/// still gets a well-formed response.
fn poll_readiness_serve(stream: &mut TlsServerStream, peer: &std::net::SocketAddr) {
    use std::io::Write as _;

    read_trigger_byte(
        stream,
        peer,
        "poll-readiness-source",
        "queuing payload anyway",
        Duration::from_secs(5),
    );
    // Queue the inbound payload so the device's poll(POLLIN) fires. A non-silent fixed
    // pattern; the device only asserts it can read ≥1 byte, not the content.
    let payload = [0xA5u8; POLL_READINESS_PAYLOAD_BYTES];
    if let Err(e) = stream.write_all(&payload) {
        eprintln!("WARN [poll-readiness-source]: payload write to {peer} failed: {e}");
        return;
    }
    if let Err(e) = stream.flush() {
        eprintln!("WARN [poll-readiness-source]: payload flush to {peer} failed: {e}");
    }
}

/// Serve one accepted inbound-frames connection inside the tunnel: read the in-band
/// selector byte, then write a leading `Hello` followed by the profile's Audio frames.
///
/// Selector byte: `'F'` selects the unpaced flood profile (`TlsInboundBackpressure`);
/// anything else, or a timeout/EOF/error, selects the happy-path profile
/// (`INBOUND_FRAMES_COUNT` frames), covering any straggler firmware that omits the
/// selector write. Returns the number of Audio frames written.
fn inbound_frames_serve(
    stream: &mut TlsServerStream,
    peer: &std::net::SocketAddr,
    stop: &Arc<Mutex<bool>>,
    encoded_hello: &[u8],
    encoded_frame: &[u8],
) -> u32 {
    use std::io::Write as _;
    let selector = read_trigger_byte(
        stream,
        peer,
        "inbound-frames-source",
        "running happy-path profile",
        Duration::from_secs(2),
    );
    let flood = selector == Some(b'F');
    let frame_count = if flood {
        INBOUND_BP_FLOOD_FRAMES
    } else {
        INBOUND_FRAMES_COUNT
    };
    println!(
        "[inbound-frames-source] {peer} profile={}",
        if flood { "flood" } else { "happy-path" }
    );
    let mut written: u32 = 0;
    let mut ok = true;
    // Conforming sender: declare the format with a leading Hello before any Audio. The
    // device drops the connection on Audio-before-Hello.
    if let Err(e) = stream.write_all(encoded_hello) {
        eprintln!("WARN [inbound-frames-source]: Hello write to {peer} failed: {e}");
        ok = false;
    }
    for _ in 0..frame_count {
        if !ok {
            break;
        }
        // Straggler-at-teardown guard (flood profile only reaches this many iterations;
        // the happy-path's 10 writes finish quickly regardless): stop mid-flood rather
        // than blocking runner shutdown behind the write timeout.
        if *stop.lock().unwrap() {
            ok = false;
            break;
        }
        if let Err(e) = stream.write_all(encoded_frame) {
            eprintln!("WARN [inbound-frames-source]: write to {peer} failed: {e}");
            ok = false;
            break;
        }
        written += 1;
    }
    if ok {
        // Flush and close (graceful FIN) to signal EOF to the device.
        let _ = stream.flush();
    }
    written
}

/// Take the selector byte off the stream so it is not mistaken for frame data by the drain.
/// Only the A profile remains, so the value is not inspected. EOF/timeout/error are benign
/// (the same A profile runs regardless).
fn consume_backpressure_selector_byte(stream: &mut TlsServerStream, peer: &std::net::SocketAddr) {
    // Shared one-byte trigger read (reuse-2).  EOF/timeout/error are benign — the same A
    // profile runs regardless — so the on-missing clause names that fallback.
    read_trigger_byte(
        stream,
        peer,
        "backpressure-source",
        "running saturate-then-drain (A) profile",
        Duration::from_secs(5),
    );
}

/// Drain the device's send buffer to EOF (or until the device stops sending), discarding
/// all bytes.  Shared tail of every profile once the adversarial phase is over: it frees
/// the device's send buffer so its post-backpressure "still usable?" send completes, and
/// reads to EOF so the device can close cleanly.  Returns `true` if the stop flag was set
/// (caller should return from the thread).
///
/// The 15 s read timeout comfortably exceeds the device's maximum post-backpressure
/// reusability window (`MAX_REUSE_RETRIES` 10 × `WRITE_TIMEOUT_MS` 750 ms = 7.5 s): if the
/// first post-resume frame blocks for close to the full per-frame budget before the
/// server's first read, the drain loop must not time out and close mid-retry (that would
/// surface as `reusable=false` and flake the test).  15 s leaves ~2× margin.
fn backpressure_drain_to_eof(
    stream: &mut TlsServerStream,
    peer: &std::net::SocketAddr,
    stop_bp: &Arc<Mutex<bool>>,
) -> bool {
    if let Err(e) = stream
        .get_ref()
        .set_read_timeout(Some(Duration::from_secs(15)))
    {
        eprintln!("WARN [backpressure-source]: set_read_timeout (drain) for {peer} failed: {e}");
        return false;
    }
    let mut drain_buf = [0u8; 4096];
    // Total plaintext discarded, reported at drain end. It separates "the device sent
    // everything freely" (a volume near the whole warm-up) from "the device stalled
    // somewhere else" (a short count) when the warm-up loop exhausts with no boundary.
    let mut drained: u64 = 0;
    loop {
        if *stop_bp.lock().unwrap() {
            println!("[backpressure-source] {peer}: drain stopped, {drained} bytes discarded");
            return true;
        }
        match stream.read(&mut drain_buf) {
            Ok(0) => {
                // Device closed (EOF).
                println!("[backpressure-source] {peer}: drain saw EOF, {drained} bytes discarded");
                return false;
            }
            Ok(m) => drained += m as u64, // discard drained bytes
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                // No more bytes within the read timeout; the device has stopped sending.
                println!(
                    "[backpressure-source] {peer}: drain read timeout, {drained} bytes discarded"
                );
                return false;
            }
            Err(e) => {
                eprintln!(
                    "WARN [backpressure-source]: read from {peer} failed after {drained} bytes: {e}"
                );
                return false;
            }
        }
    }
}

/// Sleep `dur`, honouring the stop flag in ~20 ms slices so a `Drop` during the wait does
/// not stall the thread join.  Returns `true` if the stop flag was set.
fn backpressure_stoppable_sleep(dur: Duration, stop_bp: &Arc<Mutex<bool>>) -> bool {
    let until = std::time::Instant::now() + dur;
    while std::time::Instant::now() < until {
        if *stop_bp.lock().unwrap() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Adversary A — adversarial-but-ALIVE: saturate-then-drain to prove the
/// device's resume-the-tail loop on real lwIP.
///
/// First fill the end-to-end pipe: the clamped receive buffer
/// (`BACKPRESSURE_RCVBUF_BYTES`) plus the withhold closes the TCP window, the device's
/// 2880 B lwIP send buffer fills behind it, and mbedTLS returns `WANT_WRITE` on the
/// boundary frame.  Then resume draining at line rate so the device's same-bytes retry
/// after `poll(POLLOUT)` accepts the record and the frame reaches `Sent` well inside the
/// device's 1.0 s per-frame ceiling (→ device `bp_outcome=resumed`, `resume_cycles ≥ 1`).
///
/// The observable is `WANT_WRITE` → poll → same-bytes retry, not a partial byte count:
/// a single-record TLS write is all-or-nothing, so the raw-TCP `0 < written < n` shape
/// this fixture once chased cannot occur on this link.
///
/// The bar this adversary proves is the one **only hardware can establish**: that the
/// device hits a real write boundary on real lwIP and resumes rather than false-desyncing.
/// It asserts only `resume_cycles ≥ 1` — it does **not** chase a second distinct boundary.
/// Forcing ≥2 on hardware is not reliably producible: a single host `read` frees far more
/// than one record of device-side send-buffer headroom, so the device typically completes
/// everything queued in one accepting write.  The per-wait reset's *repeatability* is
/// proven deterministically off-target by the device's many-cycle slow-drain unit test.
///
/// Returns `true` if the stop flag was set (caller should return from the thread).
fn backpressure_profile_a(
    stream: &mut TlsServerStream,
    peer: &std::net::SocketAddr,
    stop_bp: &Arc<Mutex<bool>>,
) -> bool {
    let entry = std::time::Instant::now();
    // Phase 1: saturate-only withhold so the clamped window closes, the device send buffer
    // fills, and its next record hits a write boundary (the resume case).  This MUST be
    // short (well under the device's 1.0 s per-frame ceiling) so the drain in Phase 2
    // begins while the boundary frame is still alive — a ceiling-length withhold would
    // kill the frame before the drain resumes and A could never reach `resumed`.
    if backpressure_stoppable_sleep(Duration::from_millis(BACKPRESSURE_SATURATE_MS), stop_bp) {
        return true;
    }

    // Diagnostic for the "drain started too early" failure mode: this saturate-then-drain
    // choreography relies on BACKPRESSURE_SATURATE_MS being long enough for the clamped
    // window plus the device send buffer to fill *before* the drain starts.  If it is not
    // — e.g. a config change grows either buffer — every device write is accepted
    // outright, the device reports resume_cycles=0, and the warmup loop exhausts with no
    // adversary-A verdict.  Logging the saturate window and the elapsed-at-drain-start
    // lets on-call distinguish that mode from a genuine device-side resume failure
    // without source-code archaeology; the accept-time so_rcvbuf line and the drain byte
    // count bracket it.
    eprintln!(
        "[backpressure-source A] {peer}: saturate window {BACKPRESSURE_SATURATE_MS} ms elapsed \
         ({} ms since entry); beginning drain-to-EOF (if the device never hits WANT_WRITE \
         here, the window/send-buffer sizing or BACKPRESSURE_SATURATE_MS is off)",
        entry.elapsed().as_millis(),
    );

    // Phase 2: resume reading at line rate so the device's same-bytes retry accepts the
    // blocked record and the boundary frame completes (`Sent`) well inside the device's
    // 1.0 s per-frame ceiling, then read to EOF.
    backpressure_drain_to_eof(stream, peer, stop_bp)
}

/// Host-side observation recorded by the `StreamRealtimeDuplex` listener for one device
/// connection. All timing is measured relative to `SegmentStart` receipt (the segment clock
/// starts when the host first sees the segment open, per design Scenario A).
#[derive(Debug, Clone, Default)]
struct RtdObservation {
    /// A device connection was accepted. Set as soon as the serve loop starts; frame
    /// counters below may still be zero if this observation is snapshotted mid-flight
    /// before the first frame is decoded.
    connected: bool,
    /// `SegmentStart` was received — marks t0 for all timing below.
    segment_start_seen: bool,
    /// `preroll_samples` declared in the `SegmentStart` frame (the burst-drain target).
    declared_preroll: u32,
    /// ms from `SegmentStart` receipt to the moment cumulative received audio first reached
    /// `declared_preroll` samples. `None` if the pre-roll never fully drained.
    burst_drain_ms: Option<u64>,
    /// Total audio samples received across all `Audio` frames.
    total_samples: u64,
    /// Total `Audio` frames received.
    audio_frames: u32,
    /// `SegmentEnd` reason (`"VadRelease"` / `"Overrun"` / …). `None` if no `SegmentEnd`.
    end_reason: Option<String>,
    /// ms from `SegmentStart` receipt to `SegmentEnd` receipt. `None` if no `SegmentEnd`.
    catch_up_ms: Option<u64>,
    /// Inbound playback frames the host paced to the device on this connection (Scenario B
    /// only; `0` for the read-only Scenario A connection). The device reports how many it
    /// consumed; the two must match exactly (500 ms send-stop margin makes it deterministic).
    playback_frames_sent: u32,
    /// A decode/protocol/read error, if the stream ended abnormally.
    error: Option<String>,
}

/// Dump both RTD observation slots to stderr on any `StreamRealtimeDuplex` failure, so a
/// connection that never reached the listener (or reached the wrong one) is impossible to miss.
fn dump_rtd_observations(a: &RtdObservation, b: &RtdObservation) {
    eprintln!("[rtd] observation A (outbound-only): {a:?}");
    eprintln!("[rtd] observation B (duplex):        {b:?}");
}

/// Outcome of one non-blocking pull from [`RtdFrameReader::poll_frame`].
enum RtdRead {
    /// A whole length-prefixed frame occupies `RtdFrameReader::buf[..n]`.
    Frame(usize),
    /// No complete frame yet; the caller should pace, yield, and retry.
    WouldBlock,
    /// The device closed the connection at a clean frame boundary.
    Eof,
}

/// Non-blocking, resumable reader for length-prefixed `StreamFrame`s over a TLS stream.
///
/// The RTD listener reads the device's outbound frames while, in Scenario B, pacing inbound
/// playback on the *same* TLS connection. One `SslStream` cannot be read and written from two
/// threads, so both directions run in a single thread over a non-blocking socket; this reader
/// accumulates a length prefix or payload split across several reads so partial frames are
/// reassembled without blocking the interleaved writer.
struct RtdFrameReader {
    buf: Vec<u8>,
    filled: usize,
    need: usize,
    have_len: bool,
}

impl RtdFrameReader {
    fn new(cap: usize) -> Self {
        Self {
            buf: vec![0u8; cap],
            filled: 0,
            need: 2,
            have_len: false,
        }
    }

    fn reset(&mut self) {
        self.filled = 0;
        self.need = 2;
        self.have_len = false;
    }

    /// Pull whatever bytes are available toward the current frame. Returns `Frame(n)` once a
    /// full frame occupies `self.buf[..n]`, `WouldBlock` when more bytes are still needed, or
    /// `Eof` on a clean close; a mid-frame EOF or a read error is an `Err`.
    fn poll_frame(&mut self, stream: &mut TlsServerStream) -> std::io::Result<RtdRead> {
        use std::io::Read as _;
        loop {
            if self.have_len && self.filled >= self.need {
                let n = self.need;
                self.reset();
                return Ok(RtdRead::Frame(n));
            }
            if !self.have_len && self.filled >= 2 {
                let len = u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize;
                if 2 + len > self.buf.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("rtd frame length {len} exceeds buffer"),
                    ));
                }
                self.need = 2 + len;
                self.have_len = true;
                continue;
            }
            match stream.read(&mut self.buf[self.filled..self.need]) {
                Ok(0) => {
                    if self.filled == 0 {
                        return Ok(RtdRead::Eof);
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "rtd stream closed mid-frame",
                    ));
                }
                Ok(k) => self.filled += k,
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Ok(RtdRead::WouldBlock);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Serve one `StreamRealtimeDuplex` connection over TLS: decode the device's outbound
/// Hello/SegmentStart/Audio/SegmentEnd frames and record burst-drain timing, received sample
/// count, and end reason into the shared observation. Returns when `SegmentEnd` arrives, on
/// EOF, or on a read/decode error (all captured in the observation).
///
/// When `paced` is set (the Scenario B duplex connection), an inline [`RtdPacer`] writes
/// inbound playback `Audio` frames to the device at real-time rate on the *same* TLS
/// connection, a leading inbound `Hello` first. A single `SslStream` cannot be split across a
/// reader and a writer thread, so the socket goes non-blocking and this loop interleaves the
/// pacer's writes with the frame reads. The pacer stops 500 ms before the device's scripted
/// vad-close; the count it sent is recorded so the host can assert the device consumed exactly
/// that many.
fn rtd_serve(
    stream: &mut TlsServerStream,
    peer: &std::net::SocketAddr,
    obs: &Arc<Mutex<RtdObservation>>,
    paced: bool,
    stop: &Arc<Mutex<bool>>,
    conn: u32,
) {
    use audio_pipeline::wire::{MAX_FRAME_BYTES, StreamFrame, decode_frame};
    use std::sync::atomic::{AtomicU32, Ordering};

    // Both directions share this one TLS connection in this thread, so the socket goes
    // non-blocking and the loop interleaves reads with the Scenario-B playback pacer.
    if let Err(e) = stream.get_ref().set_nonblocking(true) {
        eprintln!("WARN [rtd-listener]: set_nonblocking for {peer} failed: {e}");
    }

    // Counts inbound playback frames handed to the device (Scenario B; stays 0 for A).
    let sent = AtomicU32::new(0);
    let mut pacer = if paced { RtdPacer::new(peer) } else { None };

    let mut o = RtdObservation {
        connected: true,
        ..Default::default()
    };
    // Publish the in-progress observation to the shared slot so an evaluator that snapshots
    // this connection mid-flight never reads it as empty/disconnected. Refreshed after each
    // decoded frame; the post-loop store below carries the final playback/error fields.
    //
    // Guard: never overwrite an already-terminal slot with in-progress state. Every
    // conn_index >= 1 routes to the same B slot, so a spurious extra connection accepted
    // after the real B completed would otherwise blank B's completion markers at accept —
    // exactly while the runner is polling for them — losing the real result. The final
    // store below is unguarded; it only runs when this connection's own serve returns.
    let publish = |o: &RtdObservation| {
        let mut slot = obs.lock().unwrap();
        if slot.end_reason.is_none() && slot.error.is_none() {
            *slot = o.clone();
        }
    };
    publish(&o);
    let mut reader = RtdFrameReader::new(MAX_FRAME_BYTES + 2);
    let mut t0: Option<std::time::Instant> = None;
    // Read-cadence forensics: turns "the host read continuously" from an assumption into
    // evidence in every recorded run — a device 750 ms no-progress stall paired with a small
    // host max_read_gap exonerates the host conclusively.
    let serve_start = std::time::Instant::now();
    let mut total_bytes: u64 = 0;
    let mut frame_count: u64 = 0;
    let mut max_read_gap = Duration::ZERO;
    let mut last_read: Option<std::time::Instant> = None;
    // Inactivity guard: with a non-blocking socket the loop can no longer lean on a blocking
    // read timeout, so the wait for the next frame is bounded explicitly. 8 s comfortably
    // outlasts the ~5 s synthetic segment plus the current slow loop's catch-up stretch.
    let mut last_activity = std::time::Instant::now();
    let inactivity_limit = Duration::from_secs(8);
    loop {
        if *stop.lock().unwrap() {
            break;
        }
        if let Some(p) = pacer.as_mut() {
            p.pump(stream, peer, &sent);
        }
        let framed = match reader.poll_frame(stream) {
            Ok(RtdRead::Frame(n)) => n,
            Ok(RtdRead::WouldBlock) => {
                if last_activity.elapsed() > inactivity_limit {
                    o.error = Some(format!(
                        "no frame for {} s (device stalled or gone)",
                        inactivity_limit.as_secs()
                    ));
                    break;
                }
                // Nothing to read yet; yield briefly so the pacer paces and the CPU idles.
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Ok(RtdRead::Eof) => break,
            Err(e) => {
                o.error = Some(format!("read: {e}"));
                break;
            }
        };
        let read_at = std::time::Instant::now();
        last_activity = read_at;
        if let Some(prev) = last_read {
            max_read_gap = max_read_gap.max(read_at.duration_since(prev));
        }
        last_read = Some(read_at);
        total_bytes += framed as u64;
        frame_count += 1;
        match decode_frame(&reader.buf[..framed]) {
            Ok(StreamFrame::SegmentStart(s)) => {
                t0 = Some(std::time::Instant::now());
                o.segment_start_seen = true;
                o.declared_preroll = s.preroll_samples;
            }
            Ok(StreamFrame::Audio(a)) => {
                o.total_samples += (a.pcm.len() / 2) as u64;
                o.audio_frames += 1;
                if o.burst_drain_ms.is_none()
                    && o.declared_preroll > 0
                    && o.total_samples >= o.declared_preroll as u64
                    && let Some(start) = t0
                {
                    o.burst_drain_ms = Some(start.elapsed().as_millis() as u64);
                }
            }
            Ok(StreamFrame::SegmentEnd(e)) => {
                o.end_reason = Some(format!("{:?}", e.reason));
                if let Some(start) = t0 {
                    o.catch_up_ms = Some(start.elapsed().as_millis() as u64);
                }
                break;
            }
            Ok(_) => {} // Hello / Telemetry / control — not timed here.
            Err(e) => {
                o.error = Some(format!("decode: {e:?}"));
                break;
            }
        }
        publish(&o);
    }
    o.playback_frames_sent = sent.load(Ordering::Relaxed);
    // Report the drain/catch-up observations even here (sentinel `-` when not yet
    // measured) so a run that panics before end-of-test evaluation still surfaces them.
    let fmt_opt = |v: Option<u64>| v.map_or_else(|| "-".to_string(), |n| n.to_string());
    println!(
        "[rtd] conn={conn} paced={paced} bytes={total_bytes} frames={frame_count} \
         max_read_gap_ms={} dur_ms={} burst_drain_ms={} catch_up_ms={}",
        max_read_gap.as_millis(),
        serve_start.elapsed().as_millis(),
        fmt_opt(o.burst_drain_ms),
        fmt_opt(o.catch_up_ms),
    );
    // Terminal fields (end_reason / error from the break arms, playback_frames_sent) reach the
    // slot only here, in one atomic whole-struct swap. The runner's wait predicate keys on
    // end_reason/error, so this ordering guarantees they never become visible before
    // playback_frames_sent is final — the B evaluator asserts consumed == playback_frames_sent
    // and would spuriously fail on a torn read. Any future terminal-state mutation lands here.
    *obs.lock().unwrap() = o;
}

/// Inline Scenario-B playback pacer, driven from the RTD serve loop between reads on the same
/// non-blocking TLS connection.
///
/// Mirrors the product host pacer: a leading inbound `Hello` (the device rejects `Audio`
/// before a conforming Hello), then `RTD_PLAYBACK_FRAMES` zero-PCM `Audio` frames, front-loaded
/// up to `RTD_PLAYBACK_LEAD_MS` before pacing the remainder one per `RTD_PLAYBACK_FRAME_INTERVAL`.
/// The count stops 500 ms before the device's scripted vad-close so all sent frames are consumed
/// before the segment exits. A `WouldBlock` (TLS `WANT_WRITE`) leaves the current frame pending
/// and retries it on the next pump with the identical buffer, as `SSL_write` requires.
struct RtdPacer {
    hello: Vec<u8>,
    audio: Vec<u8>,
    hello_sent: bool,
    frames_queued: u64,
    pending: Option<Vec<u8>>,
    pending_is_audio: bool,
    start: std::time::Instant,
    lead: Duration,
    failed: bool,
}

impl RtdPacer {
    fn new(peer: &std::net::SocketAddr) -> Option<Self> {
        use audio_pipeline::wire::{
            AUDIO_PROTOCOL_VERSION, AUDIO_SAMPLES_PER_FRAME, AudioFrame, ChannelSource,
            DEVICE_PLAYBACK_FORMAT, Hello, MAX_AUDIO_PAYLOAD, MAX_FRAME_BYTES, StreamFrame,
            encode_frame,
        };
        let mut pcm: heapless::Vec<u8, MAX_AUDIO_PAYLOAD> = heapless::Vec::new();
        for _ in 0..AUDIO_SAMPLES_PER_FRAME * 2 {
            pcm.push(0u8).expect("zero PCM fits MAX_AUDIO_PAYLOAD");
        }
        let hello_frame = StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::new(),
            sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
            bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
            channels: DEVICE_PLAYBACK_FORMAT.channels,
            codec: DEVICE_PLAYBACK_FORMAT.codec,
            channel_source: ChannelSource::CommunicationBeam,
        });
        let audio_frame = StreamFrame::Audio(AudioFrame {
            segment_id: 0,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm,
        });
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let hello = match encode_frame(&hello_frame, &mut buf) {
            Ok(n) => buf[..n].to_vec(),
            Err(e) => {
                eprintln!("WARN [rtd-playback {peer}]: Hello encode failed: {e:?}");
                return None;
            }
        };
        let audio = match encode_frame(&audio_frame, &mut buf) {
            Ok(n) => buf[..n].to_vec(),
            Err(e) => {
                eprintln!("WARN [rtd-playback {peer}]: Audio encode failed: {e:?}");
                return None;
            }
        };
        Some(Self {
            hello,
            audio,
            hello_sent: false,
            frames_queued: 0,
            pending: None,
            pending_is_audio: false,
            start: std::time::Instant::now(),
            lead: Duration::from_millis(RTD_PLAYBACK_LEAD_MS),
            failed: false,
        })
    }

    /// Advance the pacer: flush any pending frame, then queue the next due frame. On a write
    /// error (connection closed) the pacer latches failed and stops.
    fn pump(
        &mut self,
        stream: &mut TlsServerStream,
        peer: &std::net::SocketAddr,
        sent: &std::sync::atomic::AtomicU32,
    ) {
        use std::io::Write as _;
        use std::sync::atomic::Ordering;
        if self.failed {
            return;
        }
        // Flush a pending (possibly WANT_WRITE-resumed) frame first, retrying the identical
        // buffer as SSL_write requires.
        if let Some(bytes) = self.pending.take() {
            match stream.write(&bytes) {
                Ok(n) if n != bytes.len() => {
                    // The server context runs OpenSSL's default write mode (no
                    // SSL_MODE_ENABLE_PARTIAL_WRITE), so SSL_write is all-or-WANT_WRITE.
                    // That is an environmental invariant, not a local one: a short write
                    // would splice a truncated frame into the length-prefixed stream and
                    // desync the device, so name it here instead of counting the frame.
                    eprintln!(
                        "WARN [rtd-playback {peer}]: partial TLS write ({n} of {} bytes) — \
                         aborting the pacer rather than splicing a truncated frame",
                        bytes.len()
                    );
                    self.failed = true;
                    return;
                }
                Ok(_) => {
                    if self.pending_is_audio {
                        self.frames_queued += 1;
                        sent.store(self.frames_queued as u32, Ordering::Relaxed);
                    } else {
                        self.hello_sent = true;
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    self.pending = Some(bytes);
                    return;
                }
                Err(e) => {
                    eprintln!("WARN [rtd-playback {peer}]: write failed (connection closed?): {e}");
                    self.failed = true;
                    return;
                }
            }
        }
        // Queue the next frame if one is due.
        if !self.hello_sent {
            self.pending = Some(self.hello.clone());
            self.pending_is_audio = false;
        } else if self.frames_queued < RTD_PLAYBACK_FRAMES {
            // Absolute-deadline pacing with a front-loaded burst: frame k targets
            // start + (k+1)*interval - lead, floored at start. While the target is within the
            // lead the deadline is already past, so frames go out as fast as writes complete;
            // past the lead the schedule tracks real-time cadence.
            let target = RTD_PLAYBACK_FRAME_INTERVAL * (self.frames_queued as u32 + 1);
            let deadline = self.start + target.saturating_sub(self.lead);
            if std::time::Instant::now() >= deadline {
                self.pending = Some(self.audio.clone());
                self.pending_is_audio = true;
            }
        }
    }
}

impl Drop for PeerServers {
    fn drop(&mut self) {
        *self.stop.lock().unwrap() = true;
        if let Some(t) = self.udp_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: UDP echo thread panicked: {e:?}");
        }
        if let Some(t) = self.inbound_frames_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: inbound-frames-source thread panicked: {e:?}");
        }
        if let Some(t) = self.backpressure_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: backpressure-source thread panicked: {e:?}");
        }
        if let Some(t) = self.poll_readiness_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: poll-readiness-source thread panicked: {e:?}");
        }
        if let Some(t) = self.rtd_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: rtd-listener thread panicked: {e:?}");
        }
        if let Some(t) = self.tls_psk_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: tls-psk-source thread panicked: {e:?}");
        }
        if let Some(t) = self.tls_psk_bad_thread.take()
            && let Err(e) = t.join()
        {
            eprintln!("WARN [peer-servers]: tls-psk-bad-source thread panicked: {e:?}");
        }
    }
}

/// Prints an operator notice on drop unless [`disarm`](Self::disarm) was called.
///
/// Armed across the `NoCredentialsPark` window in which the device holds no WiFi
/// credentials, so every exit from that window — including future failure arms nobody
/// remembered to annotate — tells the operator the device was left bare.
struct UnprovisionedNotice {
    armed: bool,
}

impl UnprovisionedNotice {
    fn arm() -> Self {
        Self { armed: true }
    }

    /// Credentials are back on the device; suppress the notice.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnprovisionedNotice {
    fn drop(&mut self) {
        if self.armed {
            eprintln!(
                "  NOTICE: device is left UNPROVISIONED; the next full HIL run re-provisions it."
            );
        }
    }
}

/// Best-effort restores the device off a `BootAssociationRetry` bogus temporary WiFi
/// config on any early exit, so a failed run never leaves the device chasing a
/// nonexistent SSID. Disarmed once the step's own `ClearTemporaryWifiConfig` succeeds.
///
/// Even without this guard the device self-heals (the override is RAM-only and the
/// serial protocol stays fully alive under bounded backoff) — the guard exists to keep
/// subsequent steps and re-runs deterministic rather than to prevent stranding.
struct TempConfigGuard<'a> {
    harness: &'a mut Harness,
    step: &'static str,
    armed: bool,
}

impl<'a> TempConfigGuard<'a> {
    fn arm(harness: &'a mut Harness, step: &'static str) -> Self {
        Self {
            harness,
            step,
            armed: true,
        }
    }

    /// The step's own clear succeeded; suppress the best-effort restore.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempConfigGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            eprintln!(
                "  NOTICE: {} exiting early; best-effort clearing the \
                 temporary WiFi override so the device stops chasing the bogus SSID.",
                self.step
            );
            let _ = self
                .harness
                .send_command_timeout(Command::ClearTemporaryWifiConfig, TEMP_WIFI_COMMAND_TIMEOUT);
        }
    }
}

// ── Network eval predicates ───────────────────────────────────────────────────

/// Retry-spam tokens that must NOT appear while the supervisor is parked: the park arm
/// charges no backoff and blocks on the doorbell, so either line here is a regression.
const PARK_RETRY_SPAM_TOKENS: [&str; 2] = [WIFI_REASSOC_ATTEMPT_FAILED, WIFI_CONSECUTIVE_FAILURES];

/// Fixed, improbable bogus SSID/passphrase applied as a temporary override by any
/// behavioral step that needs the device chasing a nonexistent AP (`BootAssociationRetry`,
/// and `NoCredentialsPark`'s override-clear coverage step). Determinism (not secrecy) is
/// the point: the name only needs to be absent from the airwaves, and a fixed name aids
/// debugging if it ever collides.
const HIL_BOGUS_WIFI_SSID: &str = "respeaker-hil-noexist";
const HIL_BOGUS_WIFI_PASS: &str = "hil-bogus-passphrase-0000";

/// Response timeout for `SetTemporaryWifiConfig`/`ClearTemporaryWifiConfig` wherever a
/// step may send one while an association attempt is in flight: the device handler's
/// `force_disconnect_wifi()` serializes behind `WIFI_STACK`, so the command blocks for the
/// rest of that attempt before responding. The default `RESPONSE_TIMEOUT` (10 s) is
/// marginal against that wait; a bogus-SSID override makes the race routine, not rare, so
/// a generous timeout avoids a spurious harness timeout being misreported as a command
/// failure.
const TEMP_WIFI_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Build the `SetTemporaryWifiConfig` command applying [`HIL_BOGUS_WIFI_SSID`] /
/// [`HIL_BOGUS_WIFI_PASS`]. Both constants are compile-time literals well under the
/// protocol's 32/64-byte limits, so construction cannot fail; panicking on that
/// impossibility (rather than threading a dead error branch through every call site) is
/// the correct response to a violated invariant.
fn bogus_temp_wifi_command() -> Command {
    let mut ssid: heapless::String<32> = heapless::String::new();
    let mut passphrase: heapless::String<64> = heapless::String::new();
    ssid.push_str(HIL_BOGUS_WIFI_SSID)
        .expect("HIL_BOGUS_WIFI_SSID fits the protocol's 32-byte ssid limit");
    passphrase
        .push_str(HIL_BOGUS_WIFI_PASS)
        .expect("HIL_BOGUS_WIFI_PASS fits the protocol's 64-byte passphrase limit");
    Command::SetTemporaryWifiConfig { ssid, passphrase }
}

/// Parse the `(N)` attempt counter out of a line carrying `token` immediately followed by
/// `" (N)"` (e.g. `"wifi-supervisor: re-association attempt failed (N): <msg>"` or
/// `"wifi-supervisor: re-association attempt starting (N)"`). Returns `None` if the line
/// doesn't contain the token or the counter isn't parseable.
fn parse_attempt_counter(line: &str, token: &str) -> Option<u32> {
    let after = line.find(token)?;
    let rest = &line[after + token.len()..];
    let open = rest.find('(')?;
    let close = rest[open..].find(')')?;
    rest[open + 1..open + close].trim().parse().ok()
}

/// Assert no supervisor-start line appears in `logs` — the reboot detector shared by both
/// `BootAssociationRetry` windows (a reboot always re-emits [`WIFI_SUPERVISOR_STARTED`]).
fn check_no_reboot(logs: &[(Instant, String)]) -> Result<(), String> {
    if let Some((_, line)) = logs
        .iter()
        .find(|(_, l)| l.contains(WIFI_SUPERVISOR_STARTED))
    {
        return Err(format!(
            "supervisor-start line seen in the observation window — unexpected reboot: {line:?}"
        ));
    }
    Ok(())
}

/// Collect every `logs` line carrying `token` with a parseable `(N)` attempt counter,
/// alongside its harness-side receipt `Instant`.
fn parsed_counters(logs: &[(Instant, String)], token: &str) -> Vec<(Instant, u32)> {
    logs.iter()
        .filter(|(_, l)| l.contains(token))
        .filter_map(|(t, l)| parse_attempt_counter(l, token).map(|n| (*t, n)))
        .collect()
}

/// Assert `counters` (as collected by [`parsed_counters`] for `token`) has at least two
/// entries and that consecutive counters are strictly increasing — proof of genuine,
/// non-rebooted attempts. `what` names the line class in the regression error.
fn check_strictly_increasing(
    counters: &[(Instant, u32)],
    token: &str,
    what: &str,
) -> Result<(), String> {
    if counters.len() < 2 {
        return Err(format!(
            "only {} {token:?} line(s) with a parseable attempt counter seen in the \
             observation window, expected >= 2 — retry-with-backoff not observed",
            counters.len(),
        ));
    }
    for pair in counters.windows(2) {
        let (_, n0) = pair[0];
        let (_, n1) = pair[1];
        if n1 <= n0 {
            return Err(format!(
                "{what} not strictly increasing ({n0} then {n1}) — reboot or counter regression"
            ));
        }
    }
    Ok(())
}

/// Assert the `BootAssociationRetry` retry-with-backoff window: at least two failed
/// re-association attempts with strictly increasing attempt counters (proves genuine
/// RF/AP failures occurred, backoff charged), each pair of consecutive attempt-*start*
/// lines spaced at least 20 s apart (the jitter-immune lower bound derived from the 30 s
/// supervisor tick), and no reboot in the window.
///
/// The spacing check is deliberately measured on `WIFI_REASSOC_ATTEMPT_START` lines, not
/// `WIFI_REASSOC_ATTEMPT_FAILED` lines: the supervisor's wait-spacing guarantee is anchored
/// at attempt *start*, but the failed-attempt line is logged at attempt *end*, after a
/// blocking connect call whose duration varies attempt to attempt. Consecutive
/// attempt-*end* lines can legitimately be spaced less than the guaranteed interval when a
/// slow attempt is followed by a fast one, even though the wait was honored perfectly —
/// measuring attempt-start lines removes that variance from the assertion.
fn eval_boot_association_retry_failures(logs: &[(Instant, String)]) -> Result<(), String> {
    check_no_reboot(logs)?;

    let failures = parsed_counters(logs, WIFI_REASSOC_ATTEMPT_FAILED);
    check_strictly_increasing(&failures, WIFI_REASSOC_ATTEMPT_FAILED, "attempt counters")?;

    let starts = parsed_counters(logs, WIFI_REASSOC_ATTEMPT_START);
    check_strictly_increasing(
        &starts,
        WIFI_REASSOC_ATTEMPT_START,
        "attempt-start counters",
    )?;

    for pair in starts.windows(2) {
        let (t0, n0) = pair[0];
        let (t1, n1) = pair[1];
        let gap = t1.duration_since(t0);
        if gap < Duration::from_secs(20) {
            return Err(format!(
                "consecutive {:?} lines only {:.1}s apart (attempts {n0} -> {n1}), expected \
                 >= 20s — backoff not honored (busy-spin?)",
                WIFI_REASSOC_ATTEMPT_START,
                gap.as_secs_f64()
            ));
        }
    }
    Ok(())
}

/// Assert the `BootAssociationRetry` revert-and-recover window: the supervisor
/// autonomously re-associated (`WIFI_REASSOCIATED`) with no reboot in between.
fn eval_boot_association_retry_recovery(logs: &[(Instant, String)]) -> Result<(), String> {
    check_no_reboot(logs)?;
    if !logs.iter().any(|(_, l)| l.contains(WIFI_REASSOCIATED)) {
        return Err(format!(
            "no {:?} line in the {} collected log lines — supervisor did not recover autonomously",
            WIFI_REASSOCIATED,
            logs.len()
        ));
    }
    Ok(())
}

/// Assert the no-credentials park: the post-clear `WifiAssociate` must fail with the
/// distinct "no NVS credentials" detail (response-payload proof the keys are gone), the
/// collected logs must carry the supervisor park line, and no retry spam may follow it.
///
/// `logs` is the union of the frames collected across the clear and the post-clear probe —
/// the park line is emitted asynchronously after the clear's doorbell and lands in whichever
/// command window is open when it arrives.
fn eval_no_credentials_park(resp: &Response, logs: &[String]) -> Result<(), String> {
    if resp.status != Status::Fail {
        return Err(format!(
            "post-clear WifiAssociate returned {:?}, expected Fail — credentials appear to survive ClearWifiCredentials",
            resp.status
        ));
    }
    let detail = match &resp.payload {
        Payload::TestReport(report) => report.detail.as_str(),
        other => {
            return Err(format!(
                "post-clear WifiAssociate carried {other:?}, expected a TestReport detail"
            ));
        }
    };
    if !detail.contains(NO_NVS_CREDENTIALS) {
        return Err(format!(
            "post-clear WifiAssociate failed with {detail:?}, expected the distinct {NO_NVS_CREDENTIALS:?} detail"
        ));
    }
    let park_at = match logs.iter().position(|l| l.contains(WIFI_PARKED_NO_CREDS)) {
        Some(i) => i,
        None => {
            return Err(format!(
                "no {:?} line in the {} collected log lines — supervisor did not announce the park",
                WIFI_PARKED_NO_CREDS,
                logs.len()
            ));
        }
    };
    // Only lines at/after the park announcement prove anything about the parked state.
    // Earlier lines can carry an ordinary RF failure from an association attempt that was
    // already in flight when the clear was sent — scanning those would flake on a weak AP.
    for token in PARK_RETRY_SPAM_TOKENS {
        if let Some(line) = logs[park_at..].iter().find(|l| l.contains(token)) {
            return Err(format!(
                "parked supervisor emitted retry spam ({token:?}): {line:?}"
            ));
        }
    }
    Ok(())
}

/// Assert `WifiAssociate` IP/gateway/RSSI bounds (AC-B2.1–B2.3).
///
/// Returns `Ok(())` on pass; `Err(description)` on any violation. Mirrors the
/// device-side assertions in `run_wifi_associate` (double-enforcement pattern).
fn eval_wifi_info(data: &TestData) -> Result<[u8; 4], String> {
    let TestData::WifiAssociate { ip, gateway, rssi } = data else {
        return Err(format!(
            "WifiAssociate returned unexpected result data (expected WifiAssociate): {data:?}"
        ));
    };
    // AC-B2.1: IP non-zero and non-loopback.
    if ip == &[0u8; 4] {
        return Err(format!("WifiAssociate returned zero IP: {ip:?}"));
    }
    if ip[0] == 127 {
        return Err(format!("WifiAssociate returned loopback IP: {ip:?}"));
    }
    // AC-B2.2: Gateway non-zero.
    if gateway == &[0u8; 4] {
        return Err(format!("WifiAssociate returned zero gateway: {gateway:?}"));
    }
    // AC-B2.3: RSSI > -80 dBm and != 0.
    if *rssi == 0 {
        return Err(
            "WifiAssociate returned RSSI=0 (bogus value); device-side assertion should have caught this".to_string()
        );
    }
    if *rssi <= -80 {
        return Err(format!(
            "WifiAssociate RSSI {rssi} dBm is below -80 dBm floor (weak signal or antenna issue)"
        ));
    }
    Ok(*ip)
}

/// Assert a `UdpRoundtrip` result (AC-B3.2 host-side enforcement).
///
/// Returns `Ok(())` if the device reported a `UdpEcho` verdict — reaching that variant
/// *is* the echo-match assertion (every mismatch path carries `TestData::None`).
fn eval_udp_roundtrip(data: &TestData) -> Result<(), String> {
    match data {
        TestData::UdpEcho {
            bytes,
            peer_ip,
            peer_port,
        } => {
            println!(
                "  UdpRoundtrip: echoed {bytes} bytes with {}:{peer_port}",
                Ipv4Addr::from(*peer_ip)
            );
            Ok(())
        }
        other => Err(format!(
            "UdpRoundtrip did not report a UdpEcho verdict — echo assertion not confirmed: \
             {other:?}"
        )),
    }
}

/// Assert a `TlsReachability` result (AC-B5.1/B5.2 host-side enforcement).
///
/// Returns `Ok(())` if the device reported a `TlsHandshake` verdict — the handshake
/// completed (every failure path carries `TestData::None`).
fn eval_tls_reachability(data: &TestData) -> Result<(), String> {
    match data {
        TestData::TlsHandshake { peer_ip, peer_port } => {
            println!(
                "  TlsReachability: handshake completed with {}:{peer_port}",
                Ipv4Addr::from(*peer_ip)
            );
            Ok(())
        }
        other => Err(format!(
            "TlsReachability did not report a TlsHandshake verdict — \
             TLS handshake not confirmed: {other:?}"
        )),
    }
}

/// Assert a `TlsInboundFrames` result.
///
/// Returns `Ok(())` if the device reported `inbound_frames == INBOUND_FRAMES_COUNT`
/// (the server sends exactly that many frames inside the TLS tunnel; the device must
/// receive all of them).
fn eval_tls_inbound_frames(data: &TestData) -> Result<(), String> {
    let TestData::TlsInboundFrames {
        inbound_frames,
        peer_ip,
        peer_port,
    } = data
    else {
        return Err(format!(
            "TlsInboundFrames did not report a TlsInboundFrames verdict: {data:?}"
        ));
    };
    if *inbound_frames != INBOUND_FRAMES_COUNT {
        return Err(format!(
            "TlsInboundFrames: device received {inbound_frames} frames, expected \
             {INBOUND_FRAMES_COUNT} — reassembly bug or partial delivery"
        ));
    }
    println!(
        "  TlsInboundFrames: device received {inbound_frames}/{INBOUND_FRAMES_COUNT} inbound \
         frames from {}:{peer_port}",
        Ipv4Addr::from(*peer_ip)
    );
    Ok(())
}

/// Assert a `TlsSendBackpressure` result (blocked-write resume path).
///
/// Only the adversary A saturate-then-drain profile remains. Requires `a_resumed`,
/// `a_rc >= 1` (at least one blocked write waited on `poll(POLLOUT)` and then completed),
/// and `a_ru`. A `ceiling_dead`/dead-mid-tail `Err` on the device means the resume path
/// regressed; the device reports `TestData::None` and this eval rejects it.
///
/// Every cross-wired A verdict (an `aligned`/`ceiling_dead` where `resumed` is required,
/// etc.) reaches the host as `TestData::None` and is rejected here, so a mis-wired
/// profile cannot pass.
fn eval_tls_send_backpressure(data: &TestData) -> Result<(), String> {
    let TestData::TlsSendBackpressure {
        a_resumed,
        a_rc,
        a_ru,
    } = data
    else {
        return Err(format!(
            "TlsSendBackpressure did not report a TlsSendBackpressure verdict — \
             a dead-mid-tail/ceiling outcome here means the blocked boundary frame never \
             resumed: {data:?}"
        ));
    };
    if !*a_resumed {
        return Err(format!(
            "TlsSendBackpressure A (saturate-then-drain): a_resumed must be true — \
             the boundary frame did not resume to Sent: {data:?}"
        ));
    }
    if *a_rc < BACKPRESSURE_A_MIN_RESUME_CYCLES {
        return Err(format!(
            "TlsSendBackpressure A: a_rc (resume_cycles)={a_rc} < \
             {BACKPRESSURE_A_MIN_RESUME_CYCLES} — the boundary frame never blocked and \
             resumed through poll(POLLOUT) on real lwIP (adversary A)"
        ));
    }
    if !*a_ru {
        return Err(
            "TlsSendBackpressure A: connection not reusable after a resumed frame \
             (expected a_ru=true)"
                .to_string(),
        );
    }

    println!("  TlsSendBackpressure: A resumed (cycles={a_rc})");
    Ok(())
}

/// Assert a `TlsInboundBackpressure` result — the socket-path counterpart to
/// `TlsInboundFrames`'s inbound-frames assertion.
///
/// Returns `Ok(())` iff:
/// 1. the device reported a `TlsInboundBackpressure` verdict (shape check);
/// 2. `inbound_frames == INBOUND_BP_FLOOD_FRAMES` — exact, both directions: a shortfall
///    is a fullness drop or truncation, an excess is a codec/accounting bug (same
///    doctrine as `eval_tls_inbound_frames`);
/// 3. `sink_full_events > 0` — the ring actually backpressured. This is also the guard
///    against the unwired-producer silent-drop mode:
///    a `None`-producer `I2sStreamSink` returns `Enqueued` while dropping everything,
///    which would satisfy assertion 2 alone — a dead channel can never return `Full`, so
///    this assertion catches it.
fn eval_tls_inbound_backpressure(data: &TestData) -> Result<(), String> {
    let TestData::TlsInboundBackpressure {
        inbound_frames,
        sink_full_events,
        peer_ip,
        peer_port,
    } = data
    else {
        return Err(format!(
            "TlsInboundBackpressure did not report a TlsInboundBackpressure verdict: {data:?}"
        ));
    };
    if *inbound_frames != INBOUND_BP_FLOOD_FRAMES {
        return Err(format!(
            "TlsInboundBackpressure: device received {inbound_frames} frames, expected \
             {INBOUND_BP_FLOOD_FRAMES} — reassembly bug, fullness drop, or partial delivery"
        ));
    }
    if *sink_full_events == 0 {
        return Err(format!(
            "TlsInboundBackpressure: sink_full_events=0 — the ring never backpressured. \
             Either the inbound PCM ring producer is unwired (I2sStreamSink drops instead of \
             stalling) or the delivery rate averaged under real time (~32 KB/s), neither of \
             which exercises the accumulator-full read-skip / TCP-window-close path this test \
             exists to prove: {data:?}"
        ));
    }
    println!(
        "  TlsInboundBackpressure: device received {inbound_frames}/{INBOUND_BP_FLOOD_FRAMES} \
         frames, sink_full_events={sink_full_events}, from {}:{peer_port}",
        Ipv4Addr::from(*peer_ip)
    );
    Ok(())
}

/// Assert a `PollReadinessBidir` result (event-loop design §4 test #1).
///
/// This is the host-side enforcement of the gating failing-assert-first proof that
/// `poll(fd, POLLIN|POLLOUT, timeout)` reports per-direction readiness on *this* lwIP/VFS
/// build — the single platform fact the whole audio I/O event-loop architecture rests on
/// (design §5 risk #1). This eval enforces `pollout` (the already-proven write-readiness
/// path, re-confirmed on the same fd), `pollin` (the **never-before-exercised**
/// read-readiness path — the load-bearing assertion), `both` (POLLIN and POLLOUT reported
/// together in one syscall, so the event loop can multiplex one fd), and `read_bytes ≥ 1`
/// (the POLLIN readiness backed real readable data, not a false signal).
///
/// A device-side assertion failure arrives as `TestData::None` and is rejected here, so a
/// mis-reported result cannot slip through. Per CLAUDE.md bring-up doctrine the first
/// observed run gets human review before this is relied on as a regression guard.
fn eval_poll_readiness_bidir(data: &TestData) -> Result<(), String> {
    let TestData::PollReadiness {
        pollin,
        pollout,
        both,
        read_bytes,
    } = data
    else {
        return Err(format!(
            "PollReadinessBidir did not report a PollReadiness verdict: {data:?}"
        ));
    };
    if !*pollout {
        return Err(format!(
            "PollReadinessBidir: pollout must be true — poll(POLLOUT) did not report \
             write-readiness on a fresh empty TX buffer: {data:?}"
        ));
    }
    if !*pollin {
        return Err(format!(
            "PollReadinessBidir: pollin must be true — poll(POLLIN) did not report \
             read-readiness on this lwIP/VFS build (the event-loop design's central \
             feasibility risk, design §5 risk #1): {data:?}"
        ));
    }
    if !*both {
        return Err(format!(
            "PollReadinessBidir: both must be true — POLLIN and POLLOUT were not reported \
             together in one poll() syscall, so one fd cannot be multiplexed (design §2.1): \
             {data:?}"
        ));
    }
    if *read_bytes < 1 {
        return Err(format!(
            "PollReadinessBidir: read_bytes={read_bytes} < 1 — POLLIN reported ready but the \
             non-blocking read returned no data, so the readiness signal was false"
        ));
    }

    println!("  PollReadinessBidir: POLLIN+POLLOUT proven on real lwIP (read_bytes={read_bytes})");
    Ok(())
}

/// Assert a `TlsPskHandshake` result: the production TLS-PSK client completed a
/// handshake against a listener holding this pod's key, negotiated the pinned
/// version and suite, and round-tripped a payload through the tunnel.
///
/// The version and suite are asserted, not merely reported: a downgrade to a
/// non-forward-secret suite, or to a protocol version neither end was pinned to,
/// would still handshake and still echo. Per CLAUDE.md bring-up doctrine an
/// unexpected negotiated value is a discovery for human review, not something to
/// make green by relaxing this assertion.
fn eval_tls_psk_handshake(data: &TestData) -> Result<(), String> {
    let TestData::TlsPskHandshake {
        peer_ip,
        peer_port,
        handshake_ms,
        version,
        ciphersuite,
        echo_bytes,
    } = data
    else {
        return Err(format!(
            "TlsPskHandshake did not report a TlsPskHandshake verdict: {data:?}"
        ));
    };
    if version.as_str() != EXPECTED_TLS_VERSION {
        return Err(format!(
            "TlsPskHandshake: negotiated version {:?}, expected {EXPECTED_TLS_VERSION:?} — \
             both ends are pinned to TLS 1.2 because esp-tls's PSK support is the 1.2 \
             psk_hint_key path",
            escape_device_str(version)
        ));
    }
    if ciphersuite.as_str() != EXPECTED_MBEDTLS_SUITE {
        return Err(format!(
            "TlsPskHandshake: negotiated suite {:?}, expected {EXPECTED_MBEDTLS_SUITE:?} — \
             a different suite means the ECDHE-PSK forward-secrecy requirement was not met",
            escape_device_str(ciphersuite)
        ));
    }
    if *echo_bytes == 0 {
        return Err(
            "TlsPskHandshake: echo_bytes=0 — the handshake completed but nothing was proven \
             to flow through the tunnel"
                .to_string(),
        );
    }
    println!(
        "  TlsPskHandshake: {} via {}:{peer_port} in {handshake_ms} ms, {echo_bytes} bytes \
         echoed through the tunnel",
        escape_device_str(ciphersuite),
        Ipv4Addr::from(*peer_ip)
    );
    Ok(())
}

/// Assert a `TlsPskWrongKeyRejected` result: a listener holding a different key for
/// this pod's identity refused the handshake, promptly and by an alert.
///
/// The device reports `Fail` if the handshake completed, if the port was
/// unreachable, or if the attempt ended on its own deadline, so reaching this eval
/// already means a real refusal. What is checked here is the measured latency of
/// that refusal: an ECDHE-PSK exchange on this silicon costs 100–300 ms, so a
/// refusal an order of magnitude slower is an unexpected reading for human review
/// (a stalling fixture, a retransmit storm) rather than a pass. `reject_ms` covers
/// the handshake stage alone — the TCP connect that preceded it is excluded — so
/// this bound measures the peer's rejection, not the link's connect latency.
fn eval_tls_psk_wrong_key_rejected(data: &TestData) -> Result<(), String> {
    /// Upper bound on a LAN TLS-PSK refusal, generous against the 100–300 ms the
    /// exchange itself costs and well under the device's 3 s handshake deadline.
    const REJECT_MAX_MS: u32 = 1500;

    let TestData::TlsPskRejected {
        peer_ip,
        peer_port,
        reject_ms,
    } = data
    else {
        return Err(format!(
            "TlsPskWrongKeyRejected did not report a TlsPskRejected verdict: {data:?}"
        ));
    };
    if *reject_ms > REJECT_MAX_MS {
        return Err(format!(
            "TlsPskWrongKeyRejected: refusal took {reject_ms} ms (> {REJECT_MAX_MS} ms) — \
             the peer did not promptly reject the key; review before accepting"
        ));
    }
    println!(
        "  TlsPskWrongKeyRejected: handshake refused by {}:{peer_port} in {reject_ms} ms; \
         no stream, so no application byte could cross",
        Ipv4Addr::from(*peer_ip)
    );
    Ok(())
}

/// Expected total received sample count for `StreamRealtimeDuplex` Scenario A: the
/// pre-roll plus the synthetic producer's 5 s of real-time capture. The synthetic producer
/// commits an exact frame count so this is deterministic (design Scenario A integrity).
fn rtd_expected_samples() -> u64 {
    use audio_pipeline::ring::PREROLL_SAMPLES;
    use audio_pipeline::wire::AUDIO_SAMPLES_PER_FRAME;
    PREROLL_SAMPLES + RTD_PRODUCER_FRAMES * AUDIO_SAMPLES_PER_FRAME as u64
}

/// Assert the `StreamRealtimeDuplex` Scenario A outcome from the device report plus the
/// host listener's observation.
///
/// The device report only confirms the loop ran to `Completed`; the throughput properties
/// are host-observed (the device cannot see its own network drain rate):
/// - the device connected and opened a segment,
/// - the pre-roll burst drained within `RTD_BURST_DRAIN_MAX_MS` of `SegmentStart`,
/// - the segment closed with `VadRelease` (not `Overrun` — the current slow loop laps the
///   ring under real-time load),
/// - the received sample count is exactly `rtd_expected_samples()` (integrity),
/// - the `SegmentStart`→`SegmentEnd` catch-up wall clock is within `RTD_CATCH_UP_MAX_MS`.
fn eval_stream_realtime_duplex(data: &TestData, obs: &RtdObservation) -> Result<(), String> {
    let (burst, wall) = eval_rtd_outbound(data, obs)?;
    println!(
        "  StreamRealtimeDuplex A: burst_drain={burst} ms catch_up={wall} ms samples={} frames={}",
        obs.total_samples, obs.audio_frames
    );
    Ok(())
}

/// Shared outbound keep-up checks for both `StreamRealtimeDuplex` scenarios: connection,
/// segment open, exact received-sample integrity, `VadRelease` end reason, burst-drain and
/// catch-up ceilings. Returns `(burst_drain_ms, catch_up_ms)` on success.
fn eval_rtd_outbound(data: &TestData, obs: &RtdObservation) -> Result<(u64, u64), String> {
    if !matches!(data, TestData::Rtd { .. }) {
        return Err(format!(
            "StreamRealtimeDuplex device report is not an rtd result: {data:?}"
        ));
    }
    if let Some(err) = &obs.error {
        return Err(format!(
            "StreamRealtimeDuplex listener recorded a stream error: {err}"
        ));
    }
    if !obs.connected {
        return Err("StreamRealtimeDuplex: device never connected to the rtd listener".to_string());
    }
    if !obs.segment_start_seen {
        return Err(
            "StreamRealtimeDuplex: no SegmentStart received — segment never opened".to_string(),
        );
    }

    // Exact-count integrity. The device borrows the boot-allocated CAPTURE_RING and
    // quiesces production capture while the synthetic producer commits an exact number of
    // samples; if even one mic chunk leaked into the ring mid-test the total would exceed
    // this expectation, so this assertion doubles as the permanent quiesce-violation guard.
    let expected = rtd_expected_samples();
    if obs.total_samples != expected {
        return Err(format!(
            "StreamRealtimeDuplex: received {} samples, expected exactly {expected} \
             (frames={}) — integrity/overrun failure",
            obs.total_samples, obs.audio_frames
        ));
    }

    match obs.end_reason.as_deref() {
        Some("VadRelease") => {}
        Some(other) => {
            return Err(format!(
                "StreamRealtimeDuplex: SegmentEnd reason={other}, expected VadRelease — \
                 the loop could not keep up with real time (Overrun laps the ring)"
            ));
        }
        None => {
            return Err(
                "StreamRealtimeDuplex: no SegmentEnd received — segment never closed cleanly"
                    .to_string(),
            );
        }
    }

    let burst = obs.burst_drain_ms.ok_or_else(|| {
        "StreamRealtimeDuplex: pre-roll never fully drained (no burst-drain time recorded)"
            .to_string()
    })?;
    if burst > RTD_BURST_DRAIN_MAX_MS {
        return Err(format!(
            "StreamRealtimeDuplex: pre-roll burst drained in {burst} ms > {RTD_BURST_DRAIN_MAX_MS} \
             ms ceiling — the loop services at most one frame per poll wake"
        ));
    }

    let wall = obs
        .catch_up_ms
        .ok_or_else(|| "StreamRealtimeDuplex: no catch-up wall time recorded".to_string())?;
    if wall > RTD_CATCH_UP_MAX_MS {
        return Err(format!(
            "StreamRealtimeDuplex: catch-up wall {wall} ms > {RTD_CATCH_UP_MAX_MS} ms ceiling — \
             the loop fell behind the real-time producer"
        ));
    }
    Ok((burst, wall))
}

/// Assert the `StreamRealtimeDuplex` Scenario B (duplex) outcome: the same outbound keep-up
/// checks under paced-playback load, plus the fake-DAC playback assertions.
///
/// The device's fake-DAC sink reports its underrun accounting and consumed-frame count in
/// `TestData::Rtd`; the host owns the outbound observation
/// and the count of playback frames it paced. Asserts:
/// - the shared outbound checks pass under duplex load,
/// - zero fake-DAC underruns and zero total gap after playout start — the fake DAC models the
///   product playback cushion (preroll + ring depth derived from the product constants), fed by
///   the `RTD_PLAYBACK_LEAD_MS`-lead pacer, so zero underruns is the product claim under this
///   link's jitter,
/// - the device consumed exactly the frames the host sent (count integrity).
fn eval_stream_realtime_duplex_b(data: &TestData, obs: &RtdObservation) -> Result<(), String> {
    let (burst, wall) = eval_rtd_outbound(data, obs)?;

    let TestData::Rtd {
        underruns,
        gap_ms,
        consumed,
    } = *data
    else {
        return Err(format!(
            "StreamRealtimeDuplex B: device report is not an rtd result: {data:?}"
        ));
    };

    if underruns != 0 || gap_ms != 0 {
        use audio_pipeline::playback::{
            INBOUND_PCM_RING_BYTES, INBOUND_PCM_WRITE_UNIT_BYTES, PLAYBACK_PREROLL_TARGET_BYTES,
        };
        let preroll_frames = PLAYBACK_PREROLL_TARGET_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES;
        let queue_frames = INBOUND_PCM_RING_BYTES / INBOUND_PCM_WRITE_UNIT_BYTES;
        return Err(format!(
            "StreamRealtimeDuplex B: fake DAC underran {underruns} times ({gap_ms} ms total gap) \
             under the {RTD_PLAYBACK_LEAD_MS} ms-lead pacer, with a modeled {preroll_frames}-frame \
             preroll and {queue_frames}-frame ring — playout drained dry despite the product cushion"
        ));
    }

    let sent = obs.playback_frames_sent as u64;
    if consumed != sent {
        return Err(format!(
            "StreamRealtimeDuplex B: device consumed {consumed} playback frames, host paced \
             {sent} — inbound count integrity failure"
        ));
    }

    println!(
        "  StreamRealtimeDuplex B: burst_drain={burst} ms catch_up={wall} ms consumed={consumed} \
         underruns=0 (paced {sent} playback frames)"
    );
    Ok(())
}

/// Assert a `WifiReassociation` verdict and its accompanying log lines.
///
/// Returns `Ok(())` when:
/// - `data` is a `WifiReassociation` verdict with `reconnected == true`.
/// - `logs` contains at least one line matching each of:
///   - `WIFI_DISCONNECTED` (event callback fired — `WifiEvent` subscription live).
///   - `WIFI_CONNECTED` (StaConnected event — `WifiEvent` subscription live).
///   - `WIFI_DHCP_LEASE` (IpEvent fired — `IpEvent` subscription live).
///   - `WIFI_REASSOCIATED` (supervisor completed re-association).
///
/// The log-line assertions prove the four subscription lines are all wired and
/// firing, not merely the reconnect result.
fn eval_wifi_reassociation_pass(data: &TestData, logs: &[String]) -> Result<(), String> {
    // Verdict assertion. ip/gateway/rssi are observability only — the post-reassociation
    // WifiAssociate run is what re-asserts the AC-B2 link bounds.
    let TestData::WifiReassociation {
        reconnected,
        ip: _,
        gateway: _,
        rssi: _,
    } = data
    else {
        return Err(format!(
            "expected WifiReassociation result data, got: {data:?}"
        ));
    };
    if !reconnected {
        return Err("WifiReassociation reported reconnected=false".to_string());
    }
    // Log-line assertions.
    let required: &[(&str, &str)] = &[
        (
            WIFI_DISCONNECTED,
            "StaDisconnected event — WifiEvent subscription wiring",
        ),
        (
            WIFI_CONNECTED,
            "StaConnected event — WifiEvent subscription wiring",
        ),
        (
            WIFI_DHCP_LEASE,
            "IpEvent DHCP lease — IpEvent subscription wiring",
        ),
        (
            WIFI_REASSOCIATED,
            "supervisor re-associated log — supervisor completion",
        ),
    ];
    for (token, description) in required {
        if !logs.iter().any(|l| l.contains(token)) {
            return Err(format!(
                "WifiReassociation: expected log line containing {token:?} not seen \
                 ({description}). Logs collected: {} lines.",
                logs.len()
            ));
        }
    }
    Ok(())
}

/// Minimum number of `capture: playback tx …` periodic lines the host must collect for
/// the cadence assertion to pass. The device feeds audio across >2 emit windows
/// (`CAPTURE_PERIODIC_LINE_FEED_MS` ≈ 2.5 s, line cadence ~1 s), so two lines proves the
/// heartbeat is periodic (a single line could be a one-off), not merely present.
const CAPTURE_PERIODIC_LINE_MIN_COUNT: usize = 2;

/// Assert a `CapturePeriodicLine` verdict and its accompanying periodic
/// summary log lines (audio-pipeline-observability §5).
///
/// Returns `Ok(())` when:
/// - `data` is [`TestData::CapturePeriodicLine`] (the device drove inbound audio through the
///   production path; any fail path carries [`TestData::None`], and another test's variant
///   cannot be mistaken for this one).
/// - `logs` contains at least `CAPTURE_PERIODIC_LINE_MIN_COUNT` lines carrying the
///   `capture: playback tx ` token — the production capture thread emitted its periodic
///   summary line at cadence (≥2 emits over the feed window). This is the load-bearing
///   assertion: it proves the §2.2 observability heartbeat exists and fires on hardware,
///   following the `WifiReassociation` log-line-assertion pattern.
fn eval_capture_periodic_line(data: &TestData, logs: &[String]) -> Result<(), String> {
    // `chunks_fed` is observability only: this test's assertion lives entirely in the log-line
    // cadence below. The string predecessor asserted only that the token was present, which the
    // variant match now expresses structurally.
    let TestData::CapturePeriodicLine { chunks_fed: _ } = data else {
        return Err(format!(
            "CapturePeriodicLine report carries {data:?}, expected TestData::CapturePeriodicLine"
        ));
    };
    let count = logs.iter().filter(|l| l.contains(CAPTURE_TX_LINE)).count();
    if count < CAPTURE_PERIODIC_LINE_MIN_COUNT {
        return Err(format!(
            "CapturePeriodicLine: saw {count} periodic '{CAPTURE_TX_LINE}' line(s), \
             expected at least {CAPTURE_PERIODIC_LINE_MIN_COUNT} (the capture-thread summary line \
             did not appear at the expected ~1 s cadence). Logs collected: {} lines.",
            logs.len()
        ));
    }
    println!("  CapturePeriodicLine: saw {count} periodic capture-summary lines (cadence OK)");
    Ok(())
}

/// Parse an integer token of the form `<key>N` from a device log line, returning `N`. Used for counters whose
/// firmware accumulator is `u64` (e.g. the uncapped `rx_frames=` RX-delivery counter), so the
/// host field type matches the device type and a long-window value cannot exceed the parse
/// range and surface as a misleading "missing token" error (quality-1 / errhandling-1 /
/// test-2). Only a leading `-` would force `None`; the counter is unsigned, so that is the
/// correct fail-closed behavior.
///
/// Note (errhandling-2): a token that is PRESENT but unparseable (e.g. a firmware regression
/// emitting a `-`-prefixed value where an unsigned counter is expected) returns `None` here and
/// the call site reports "missing/invalid '<key>='" — it does NOT distinguish "key absent" from
/// "key present but malformed". The eval still halts loudly (fail-closed), so this is a
/// diagnostic-context gap, not a silent pass; the producer is the device firmware in the
/// harness's own trust domain and never emits signed counters, so the distinction is not worth
/// splitting per-call-site. The principal real-world misparse (Defect A's truncated PARTIAL
/// value parsing as a garbage positive) is fixed at the source by the §2.1 two-line split, not
/// by this parser.
fn parse_token_u64(msg: &str, key: &str) -> Option<u64> {
    let start = msg.find(key)? + key.len();
    let rest = &msg[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse::<u64>().ok()
}

// ── PlaybackDrainRate host eval (design §5 "HIL self-tests") ──

/// Raw bytes per drained write-unit — the `chunks=` token on the periodic line counts
/// write-units drained, each `INBOUND_PCM_WRITE_UNIT_BYTES` (640 B) of raw S16 mono (20 ms).
/// Sourced directly from the firmware crate (`hil-host` already depends on `audio-pipeline`), so
/// it cannot drift from the value the device emits.
const PLAYBACK_DRAIN_WRITE_UNIT_BYTES: u64 =
    audio_pipeline::playback::INBOUND_PCM_WRITE_UNIT_BYTES as u64;
/// Raw real-time byte rate of the reservoir: 16 kHz × 2 B/sample raw S16 mono = 32 000 B/s. The
/// consumer expands each raw sample to a 32-bit stereo I2S frame at DMA-write time (design §3.1),
/// so the ring drains at exactly this raw rate when it keeps the codec clock fed. The
/// bytes-per-sample factor is sourced from the firmware crate; only the 16 kHz sample rate
/// (firmware `speaker.rs::I2S_SAMPLE_RATE_HZ`) remains a manual sync.
const PLAYBACK_DRAIN_RAW_REALTIME_BYTES_PER_S: u64 =
    16_000 * audio_pipeline::playback::WIRE_BYTES_PER_SAMPLE as u64;
/// Keep-up floor: the aggregate raw-drain rate over the saturated windows must reach at least
/// this fraction of real-time (design §5: "asserted ≥ 1.0× the raw real-time rate"). The codec
/// clock is a hard ceiling — the NON_BLOCK-TX consumer cannot drain faster than the DMA accepts
/// — so a healthy sustained drain reads ≈ 1.0×; the ~5 % slack absorbs the ±120 ms TX-DMA-lead
/// jitter at the window boundaries against that exactly-1.0× ceiling. A genuine drain regression
/// (the pre-fix blocking-write latch, an I2S wedge, or a starved poll cadence) reads well below
/// 1.0×, so this floor cleanly separates healthy from broken.
const PLAYBACK_DRAIN_MIN_KEEP_UP: f64 = 0.95;
/// Minimum-sample-size gate on the saturation signal: a window is eligible for scoring only if
/// it recorded at least this many first-poll-non-empty outer passes (`nonempty_polls`). The
/// saturation test below (`empty_polls == 0`) has NO floor on the sample, so a cold-start ramp-up
/// window with `nonempty_polls=1, empty_polls=0` passes it on a sample of ONE — scoring a
/// never-saturated window and sinking the verdict. Requiring ≥ this many real drain polls drops
/// the ramp-up window like the warmup drop, so only genuinely-saturated windows reach the rate
/// metric.
///
/// A floor of 10 sits an order of magnitude above the ramp-up value (0–1, the consumer parked in
/// `preroll_waits`) and comfortably below the healthy steady-state count (many tens to hundreds
/// of outer passes per ~1 s window at the 5 ms poll cadence), cleanly separating the two.
/// Critically the gate keys on **saturation activity** (ring-full first-polls), NOT on the
/// measured throughput: a genuine drain regression presents as a still-saturated window (high
/// `nonempty_polls`) that drains too slowly, so it stays above this floor, is still scored, and
/// still fails.
const PLAYBACK_DRAIN_MIN_SATURATED_POLLS: u64 = 10;
/// Minimum number of interpretable (saturated, non-warmup) windows the eval needs before
/// it reads a drain verdict (design §5: fail loudly rather than computing on insufficient data).
const PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS: usize = 1;
/// Settle inserted between the `SpeakerOutput` tone test completing and `RunTest(
/// PlaybackDrainRate)` (design §2.4, OQ-ordering — avoid the Defect-C contamination). During
/// the tone test the device deliberately does NOT drain the RX DMA ring (firmware
/// `run_speaker_output` comment "During playback the RX DMA ring is NOT drained"), parking
/// the capture loop ~1.5 s in an RX-dead window. Without a settle, the first
/// `capture: playback tx …` periodic window after the tone STRADDLES that RX-dead span and
/// reports an uninterpretable RX rate (run-analysis §2b: 11280 frames / 2.52 s ≈ 4476 fps).
/// Sized comfortably above the ~1.5 s RX-dead span so the device's capture loop resumes RX
/// draining and rolls past it — the first window emitted under the saturating feed then
/// reflects normal RX servicing, not the straddle. The §2.3 first-poll-dominance filter is
/// retained as defense in depth (a `chunks=0` straddle window has ~0 non-empty polls and is
/// dropped anyway), but the settle removes the straddle at the source rather than relying on
/// the filter / warmup-drop landing on exactly the contaminated line.
const PLAYBACK_DRAIN_PRETEST_SETTLE_MS: u64 = 2_000;

/// One parsed `capture: playback tx …` periodic window (the fields the raw-drain-rate metric
/// scores, design §5). `write_us`, `max_backlog`, and `rx_frames` are emitted on the production
/// line for observability but not parsed here: under NON_BLOCK TX `write_us` is ~0 (a slow write
/// cannot happen), and RX integrity is asserted by the separate `FullDuplexRxIntegrity` self-test.
#[derive(Clone, Copy, Debug)]
struct DrainWindow {
    /// Write-units drained this window (firmware `chunks=` token = `tx_chunks_written`, one
    /// increment per `INBOUND_PCM_WRITE_UNIT_BYTES` raw run pushed to TX DMA). The raw bytes
    /// drained is `chunks × PLAYBACK_DRAIN_WRITE_UNIT_BYTES`. Typed `u64` to match the firmware
    /// accumulator and to multiply by the write-unit size without overflow.
    chunks: u64,
    /// First-poll-non-empty / first-poll-empty outer-pass counts (firmware `nonempty_polls=` /
    /// `empty_polls=` tokens, both `u64` accumulators). Saturation is `empty_polls == 0` (the ring
    /// was non-empty at every drain-classifying pass start, design §5) gated on
    /// `nonempty_polls >= PLAYBACK_DRAIN_MIN_SATURATED_POLLS` (sample size).
    nonempty_polls: u64,
    empty_polls: u64,
    /// Exact wall-time this window spanned, µs (firmware `rx_window_us=` token,
    /// `tx_summary_window_start.elapsed()` at emit). The emit window is gated at ≥1000 ms; the
    /// raw-drain rate divides the drained bytes by this real duration, not a nominal 1 s. Typed
    /// `u64` to match the firmware `as_micros() as u64` emit.
    rx_window_us: u64,
    /// The paired `capture: playback obs …` line's pre-roll counters, or `None` if that line
    /// was never correlated to this window (a dropped device→host log frame). Grouped in one
    /// `Option` so pairing is atomic — a window either has its line-2 data or it does not.
    obs: Option<DrainObs>,
}

impl DrainWindow {
    /// The line-1-only saturation test: the ring was non-empty at every drain-classifying pass
    /// start, over enough passes to be a meaningful sample. Shared by the scoring filter and the
    /// exclusion-count fold so the printed counts always itemize exactly the windows the filter
    /// would otherwise have kept.
    fn line1_saturated(&self) -> bool {
        self.empty_polls == 0 && self.nonempty_polls >= PLAYBACK_DRAIN_MIN_SATURATED_POLLS
    }
}

/// Counts how many otherwise-saturated windows the pre-roll filter drops, split by reason:
/// `(preroll_excluded, obs_missing)`. Windows that are not line-1-saturated are counted in
/// neither bucket — they were never scoring candidates — and correlated clean windows, which do
/// score, are counted in neither either. Shared by the per-run `println!` and the Step-4 error so
/// both always itemize exactly the windows the scoring filter would otherwise have kept.
fn exclusion_counts(post_warmup: &[DrainWindow]) -> (usize, usize) {
    post_warmup.iter().filter(|w| w.line1_saturated()).fold(
        (0usize, 0usize),
        |(preroll, missing), w| match w.obs {
            Some(o) if o.preroll_waits > 0 || o.preroll_rearms > 0 => (preroll + 1, missing),
            None => (preroll, missing + 1),
            Some(_) => (preroll, missing),
        },
    )
}

/// The pre-roll counters carried on the paired `capture: playback obs …` line. Either being
/// nonzero means the consumer spent part of the window parked on the pre-roll gate rather
/// than draining, so the window's `chunks=` understates the drain capability.
#[derive(Clone, Copy, Debug)]
struct DrainObs {
    preroll_waits: u64,
    preroll_rearms: u64,
}

/// Evaluate the `PlaybackDrainRate` HIL result (design §5 "HIL self-tests"). Asserts the
/// deep-reservoir + NON_BLOCK-TX consumer sustains real-time drain: over the saturated windows
/// the inbound ring must drain raw bytes at ≥ 1.0× the raw real-time rate (32 B/ms). This is the
/// metric redesign the NON_BLOCK TX split forces — the former `write_us`-based per-chunk-drain
/// metric collapses to ~0 once a slow blocking write no longer exists.
///
/// Steps:
/// 1. Confirm the device verdict ran ([`TestData::PlaybackDrainRate`], fed chunks).
/// 2. Parse every collected `capture: playback tx …` line for `chunks`, `rx_window_us`, and the
///    saturation pair (`nonempty_polls`/`empty_polls`).
/// 3. Drop the first (warmup/ring-fill) window, then keep only saturated windows — the ring was
///    non-empty at every drain-classifying pass start (`empty_polls == 0`) with a
///    minimum-sample-size gate (`nonempty_polls >= PLAYBACK_DRAIN_MIN_SATURATED_POLLS`).
/// 4. If no window saturates, split on the feed's own `feed_full`: zero ⇒ the drain kept up
///    (healthy PASS); climbing ⇒ the feed never saturated the ring (uninterpretable, fail).
/// 5. Over the saturated windows compute the aggregate raw-drain rate (Σ chunks × 640 B ÷ Σ
///    window µs) and assert it reaches `PLAYBACK_DRAIN_MIN_KEEP_UP` × real-time; on failure
///    surface every measured value so the shortfall is legible.
fn eval_playback_drain_rate(data: &TestData, logs: &[String]) -> Result<(), String> {
    // ── 1. Device verdict: the handler ran and produced its feed report. `feed_ms` is
    // observability only — the eval derives window durations from the periodic lines' own
    // `rx_window_us`, never from the nominal feed duration. ──
    let TestData::PlaybackDrainRate {
        chunks_fed,
        feed_full,
        feed_ms: _,
        tx_wf,
    } = data
    else {
        return Err(format!(
            "PlaybackDrainRate report carries {data:?}, expected TestData::PlaybackDrainRate"
        ));
    };
    let (chunks_fed, feed_full, tx_wf) = (*chunks_fed, *feed_full, *tx_wf);
    // The feed loop must have accepted at least one chunk for any periodic-line drain figure to
    // be attributable to it. `chunks_fed==0` with periodic windows present is an internal
    // contradiction (the device's own feed reported nothing accepted while the capture thread
    // apparently drained something) — fail loudly rather than burying the contradiction.
    if chunks_fed == 0 {
        return Err(format!(
            "PlaybackDrainRate: device reported chunks_fed={chunks_fed} — the feed loop never \
             accepted a chunk; the drain verdict would be uninterpretable. feed_full={feed_full}, \
             logs collected: {} lines.",
            logs.len()
        ));
    }

    // ── 2. Parse every production periodic line. A line that carries the prefix but is
    // missing a token is a format drift — fail loudly, do not silently skip (design §5). ──
    let mut windows: Vec<DrainWindow> = Vec::new();
    for line in logs
        .iter()
        .filter(|l| l.contains(CAPTURE_TX_LINE) || l.contains(CAPTURE_OBS_LINE))
    {
        // Line 2: the pre-roll counters that classify a window as refill-contaminated. They
        // cannot live on line 1 (it is at its 200-char heapless budget), so they are correlated
        // here by order: the firmware emits the tx/obs pair adjacently from one thread, so each
        // obs line belongs to the most recent window still awaiting its obs. Unrelated lines may
        // interleave, but tx/obs pairs cannot reorder or nest.
        if line.contains(CAPTURE_OBS_LINE) {
            let preroll_waits = parse_token_u64(line, PREROLL_WAITS).ok_or_else(|| {
                format!("PlaybackDrainRate: '{CAPTURE_OBS_LINE}' line missing/invalid '{PREROLL_WAITS}': {line}")
            })?;
            let preroll_rearms = parse_token_u64(line, PREROLL_REARMS).ok_or_else(|| {
                format!("PlaybackDrainRate: '{CAPTURE_OBS_LINE}' line missing/invalid '{PREROLL_REARMS}': {line}")
            })?;
            let target = windows.iter_mut().rev().find(|w| w.obs.is_none());
            match target {
                Some(w) => {
                    w.obs = Some(DrainObs {
                        preroll_waits,
                        preroll_rearms,
                    })
                }
                // No window awaiting an obs line: the emit contract (one adjacent pair per
                // window) has drifted, or the lines reordered. Guessing an attachment would
                // silently mis-classify windows — fail loudly instead.
                None => {
                    return Err(format!(
                        "PlaybackDrainRate: orphaned '{CAPTURE_OBS_LINE}' line with no \
                         un-paired '{CAPTURE_TX_LINE}' window to attach it to — the device \
                         emit order/contract has drifted: {line}"
                    ));
                }
            }
            continue;
        }
        let chunks = parse_token_u64(line, CHUNKS).ok_or_else(|| {
            format!(
                "PlaybackDrainRate: '{CAPTURE_TX_LINE}' line missing/invalid '{CHUNKS}': {line}"
            )
        })?;
        // The saturation pair (design §5): first-poll-non-empty / first-poll-empty outer-pass
        // counts. Same fail-closed discipline as every other token — a prefix-present line missing
        // either is a format drift, not a silent skip. `parse_token_u64` because the firmware
        // accumulators are `u64`.
        let nonempty_polls = parse_token_u64(line, NONEMPTY_POLLS).ok_or_else(|| {
            format!("PlaybackDrainRate: '{CAPTURE_TX_LINE}' line missing/invalid '{NONEMPTY_POLLS}': {line}")
        })?;
        // `POLL_EMPTY` is deliberately not a substring of `NONEMPTY_POLLS` (guarded by a
        // non-containment test in `device_protocol::log_tokens`), so a bare substring search
        // reads the right field regardless of emit order.
        let empty_polls = parse_token_u64(line, POLL_EMPTY).ok_or_else(|| {
            format!(
                "PlaybackDrainRate: '{CAPTURE_TX_LINE}' line missing/invalid '{POLL_EMPTY}': {line}"
            )
        })?;
        // The exact window duration: the emit window is gated at ≥1 s but stretches
        // under load, so the raw-drain rate must divide by this real elapsed time, not a nominal
        // 1 s. Same fail-closed discipline as the other tokens — `parse_token_u64` because the
        // firmware emits `as_micros() as u64`.
        let rx_window_us = parse_token_u64(line, RX_WINDOW_US).ok_or_else(|| {
            format!("PlaybackDrainRate: '{CAPTURE_TX_LINE}' line missing/invalid '{RX_WINDOW_US}': {line}")
        })?;
        windows.push(DrainWindow {
            chunks,
            nonempty_polls,
            empty_polls,
            rx_window_us,
            obs: None,
        });
    }
    if windows.is_empty() {
        return Err(format!(
            "PlaybackDrainRate: no '{CAPTURE_TX_LINE}' periodic lines collected \
             (the capture thread emitted no drain summary during the feed). \
             Device verdict: chunks_fed={chunks_fed} feed_full={feed_full}. \
             Logs collected: {} lines.",
            logs.len()
        ));
    }

    // Observability (not an assertion): report the device's TX-write-failure count alongside the
    // collected window count, so a shortfall of periodic windows can be attributed to device→host
    // log-frame loss (whole frames dropped when the TX ring filled) rather than a real drain
    // deficit. Now a typed field, so it is always present.
    println!(
        "  PlaybackDrainRate: collected {} window(s); device tx_write_failures={tx_wf}",
        windows.len()
    );

    // ── 3. Window selection. Drop the first window as the warmup/ring-fill transient, then keep
    // only saturated windows — the ring was non-empty at every drain-classifying pass start
    // (`empty_polls == 0`, design §5's "ring non-empty at every pass start") gated on a minimum
    // saturated-poll count so a cold-start ramp-up window cannot clear the test on a one-poll
    // sample. ──
    //
    // The warmup drop costs one window, so an interpretable verdict needs at least
    // `MIN_SATURATED_WINDOWS + 1` collected windows. Assert that floor here with its OWN
    // message (distinct from the feed_full split below) — otherwise a one-window run (short
    // feed, cadence jitter, or a host timeout clipping after the first emit) would slice
    // `windows[1..]` to empty, find zero saturated windows, and with feed_full==0 fall through
    // to a false "drain kept up" PASS on data that was entirely the discarded warmup (fail loudly
    // with "collected N, need ≥ K" rather than computing on insufficient data).
    if windows.len() < PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS + 1 {
        return Err(format!(
            "PlaybackDrainRate: collected {} periodic window(s), need ≥ {} (one warmup window is \
             always dropped, then ≥ {} steady-state window(s) are required). Increase feed \
             duration or check the device periodic-line cadence. chunks_fed={chunks_fed}, \
             feed_full={feed_full}.",
            windows.len(),
            PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS + 1,
            PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS,
        ));
    }
    let post_warmup = &windows[1..];
    let saturated: Vec<DrainWindow> = post_warmup
        .iter()
        .copied()
        // Saturation (design §5): the ring was non-empty at every drain-classifying pass start,
        // i.e. no pass found it empty (`empty_polls == 0`). Under NON_BLOCK TX the loop never
        // parks on a write, so a genuinely-saturated over-delivering feed keeps the ring full and
        // no pass starves — the criterion is 100%, not a tolerance. The `nonempty_polls` floor is
        // the minimum sample size: a cold ramp-up window can read `nonempty_polls=1, empty_polls=0`
        // and pass `empty_polls == 0` on a sample of one, so require enough real drain polls to
        // separate ramp-up (0–1) from steady state. The floor keys on saturation ACTIVITY, not the
        // measured drain, so a real drain regression (saturated but slow) still scores and fails.
        // Pre-roll exclusion: while the gate holds, a non-empty first poll still increments
        // `nonempty_polls` but the loop breaks before any chunk is drained, and empty first
        // polls route to `preroll_waits` instead of `empty_polls`. A post-underrun refill
        // window therefore reads exactly like a saturated one with a depressed `chunks` — the
        // misattributed-FAIL this filter exists to prevent. `preroll_rearms` covers the
        // variant where the ring refills before any first poll finds it empty
        // (`preroll_waits == 0` yet part of the window was spent not draining).
        //
        // Residual gap: a re-arm in window N whose fill spans the whole of N+1 without a
        // single empty first poll leaves N+1 reading clean. Closing it needs a device-side
        // "gate pending at window end" flag, which is out of scope (no device behavior change).
        //
        // `obs == None` (a dropped obs log frame) also excludes: an uncorrelated window cannot
        // be proven refill-free. Exclusion is safe both ways — it cannot create a false FAIL
        // (an excluded window contributes nothing to the aggregate) and cannot create a false
        // PASS (a genuine regression presents as many saturated windows, and the all-excluded
        // path routes to the feed_full split below, which fails loudly when feed_full > 0).
        .filter(|w| {
            w.line1_saturated()
                && matches!(w.obs, Some(o) if o.preroll_waits == 0 && o.preroll_rearms == 0)
        })
        .collect();

    // Per-reason exclusion counts over the post-warmup windows that were otherwise saturated —
    // the operator's tell for distinguishing a transient-heavy run from device→host log loss.
    // Reported on every run, PASS included.
    let (preroll_excluded, obs_missing) = exclusion_counts(post_warmup);
    println!(
        "  PlaybackDrainRate: post-warmup exclusions: preroll-excluded={preroll_excluded} \
         obs-missing={obs_missing}"
    );

    // ── 4. No saturated window: split healthy-keep-up vs uninterpretable on feed_full. ──
    // This guard also catches the cold-run case where the minimum-saturated-poll gate
    // (§3) drops every window — e.g. a run that collected only warmup + ramp-up windows. A
    // ramp-up window means the consumer was parked in `preroll_waits` while the feed filled the
    // ring, so `feed_full` climbs (>0) and the eval reports the uninterpretable/insufficient-data
    // failure below rather than silently passing on zero scored windows; the `feed_full == 0`
    // PASS arm stays reserved for the genuine keep-up case (consumer drained as fast as fed, so
    // the ring never filled). The `PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS` floor is the join.
    if saturated.len() < PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS {
        if feed_full == 0 {
            // The over-delivering feed never had to back off — the consumer drained at
            // least as fast as an at-least-real-time feed. A genuine PASS.
            println!(
                "  PlaybackDrainRate: feed never saturated the ring and feed_full=0 — \
                 the drain kept up with an at-least-real-time feed (healthy). \
                 chunks_fed={chunks_fed}, windows={}.",
                windows.len()
            );
            return Ok(());
        }
        // The feed backed off (feed_full climbing) yet no window ran fully saturated — a
        // feed/timing artifact, the drain rate is uninterpretable.
        return Err(format!(
            "PlaybackDrainRate: feed never saturated the ring (no window with empty_polls==0 and \
             ≥ {PLAYBACK_DRAIN_MIN_SATURATED_POLLS} non-empty polls) yet feed_full={feed_full} \
             (>0) — drain rate uninterpretable. Exclusions among otherwise-saturated \
             post-warmup windows: preroll-excluded={preroll_excluded} obs-missing={obs_missing} \
             (preroll-excluded ⇒ a transient-heavy run; obs-missing ⇒ device→host log loss). \
             chunks_fed={chunks_fed}, windows parsed={}, post-warmup={}.",
            windows.len(),
            post_warmup.len()
        ));
    }

    // ── 5. Interpretable: the aggregate raw-drain rate over the saturated windows (design §5).
    // Summing drained bytes and window durations across the saturated windows cancels the
    // ±120 ms TX-DMA-lead jitter at each window boundary, giving a stable sustained-rate reading;
    // a per-window min would be dominated by that boundary jitter against the exactly-1.0×
    // codec-clock ceiling. Assert the aggregate reaches `PLAYBACK_DRAIN_MIN_KEEP_UP` × real-time. ──
    let total_drained_bytes: u64 = saturated
        .iter()
        .map(|w| w.chunks.saturating_mul(PLAYBACK_DRAIN_WRITE_UNIT_BYTES))
        .sum();
    let total_window_us: u64 = saturated.iter().map(|w| w.rx_window_us).sum();
    // `total_window_us == 0` is impossible for a real emit (each window is gated at ≥1 s of
    // elapsed time) but is guarded so a malformed/zero token reports 0.0 rather than dividing by
    // zero.
    let measured_rate_bps = if total_window_us > 0 {
        total_drained_bytes as f64 * 1_000_000.0 / total_window_us as f64
    } else {
        0.0
    };
    let keep_up = measured_rate_bps / PLAYBACK_DRAIN_RAW_REALTIME_BYTES_PER_S as f64;
    // Min chunks/window over the saturated windows, surfaced as observability alongside the
    // aggregate rate. `saturated` is non-empty here: the `< PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS`
    // guard above returns early and that const is ≥1; `expect` makes the coupling explicit so a
    // future relaxation of the floor to 0 fails loudly rather than reading a bogus value.
    let min_chunks =
        saturated.iter().map(|w| w.chunks).min().expect(
            "saturated non-empty: guarded by PLAYBACK_DRAIN_MIN_SATURATED_WINDOWS ≥ 1 above",
        );

    let measured = format!(
        "saturated_windows={} raw_drain_rate={measured_rate_bps:.0}B/s \
         (real-time={PLAYBACK_DRAIN_RAW_REALTIME_BYTES_PER_S}B/s) keep_up={keep_up:.2}x \
         drained_bytes={total_drained_bytes} over {total_window_ms:.0}ms min_chunks={min_chunks} \
         chunks_fed={chunks_fed} feed_full={feed_full}",
        saturated.len(),
        total_window_ms = total_window_us as f64 / 1_000.0,
    );

    if keep_up >= PLAYBACK_DRAIN_MIN_KEEP_UP {
        println!("  PlaybackDrainRate: healthy — {measured}");
        return Ok(());
    }

    Err(format!(
        "PlaybackDrainRate FAIL — the reservoir consumer did not sustain real-time drain: the \
         raw-drain rate over the saturated windows fell below {PLAYBACK_DRAIN_MIN_KEEP_UP:.2}x the \
         real-time rate. Measured: {measured}. A keep_up ≪ 1.0 means the NON_BLOCK-TX consumer is \
         not keeping the DMA fed from the deep ring (an I2S wedge, a starved poll cadence, or a \
         drain regression)."
    ))
}

// ── FullDuplexRxIntegrity host eval (design §5 "HIL self-tests") ──

/// Minimum number of post-warmup `capture: playback obs …` windows the eval needs before it
/// reads an RX-deficit verdict (design §5: fail loudly rather than passing vacuously on
/// insufficient data). The first collected window is always dropped as the warmup/ramp
/// transient, so an interpretable run needs at least `this + 1` collected windows.
const FULL_DUPLEX_RX_MIN_OBS_WINDOWS: usize = 1;

/// Expected capture-thread core affinity and FreeRTOS priority, asserted on every collected
/// `capture: playback obs …` line. Core-1 isolation (not the priority) is the mechanism the
/// TX/RX split relies on to keep mic RX on cadence (design §3.6); a refactor that drops the
/// `pin_to_core: Some(Core::Core1)` or lets the priority regress to the default 5 would silently
/// reintroduce the starvation regime while the deficit could still read 0 by luck of scheduling
/// on a lightly-loaded bench run. Baking the on-hardware-confirmed reading in makes that a test
/// FAIL, not a human-eyeball catch (CLAUDE.md HIL doctrine). Manual sync with the firmware
/// `CAPTURE_THREAD_PRIORITY` (10) and `Core::Core1` in `capture.rs`.
const CAPTURE_EXPECTED_CORE: u64 = 1;
const CAPTURE_EXPECTED_PRIO: u64 = 10;

/// Evaluate the `FullDuplexRxIntegrity` HIL result (design §5 "HIL self-tests"). Asserts the
/// TX/RX split (NON_BLOCK TX + core-1 pinning) eliminated the mic-capture starvation the
/// root-cause analysis found: under a saturating playback feed that holds the capture thread
/// TX-drain-bound, the device-computed, dead-banded per-window `rx_deficit` must be 0 — mic RX
/// kept its 16 kHz cadence under full playback load. This is the direct assertion that the
/// ~48 % mic-sample loss is gone, not merely counted; a nonzero deficit FAILs (never laundered
/// into a pass, per CLAUDE.md HIL doctrine).
///
/// Steps:
/// 1. Confirm the device verdict ran ([`TestData::FullDuplexRxIntegrity`], fed chunks).
/// 2. Require the feed to have saturated the ring (`feed_full>0`) — the exact TX-drain-bound
///    condition the pre-fix code starved RX under. An idle TX never starves RX, so a
///    zero-deficit reading without saturation is vacuous → fail loudly rather than pass.
/// 3. Parse every collected `capture: playback obs …` line for `rx_deficit`, `core`, and `prio`
///    (fail-closed on a prefix-present line missing any token), and assert the capture thread kept
///    its core-1 pin and elevated priority — a dropped pin FAILs distinctly from the deficit.
/// 4. Drop the first (warmup/ramp) window, require ≥ `FULL_DUPLEX_RX_MIN_OBS_WINDOWS` remaining.
/// 5. Assert every remaining window reports `rx_deficit == 0`; any nonzero window FAILs, naming
///    the offending count and the worst deficit.
fn eval_full_duplex_rx_integrity(data: &TestData, logs: &[String]) -> Result<(), String> {
    // ── 1. Device verdict: the handler ran and produced its feed report. `feed_ms` is
    // observability only — the RX-deficit verdict is read from the periodic lines. ──
    let TestData::FullDuplexRxIntegrity {
        chunks_fed,
        feed_full,
        feed_ms: _,
    } = data
    else {
        return Err(format!(
            "FullDuplexRxIntegrity report carries {data:?}, expected \
             TestData::FullDuplexRxIntegrity"
        ));
    };
    let (chunks_fed, feed_full) = (*chunks_fed, *feed_full);
    if chunks_fed == 0 {
        return Err(format!(
            "FullDuplexRxIntegrity: device reported chunks_fed={chunks_fed} — the feed loop never \
             accepted a chunk; the RX-deficit verdict would be uninterpretable. \
             feed_full={feed_full}, logs collected: {} lines.",
            logs.len()
        ));
    }

    // ── 2. Saturation validity. The mic-RX starvation this test guards against only manifests
    // while the capture thread is TX-drain-bound (the pre-fix blocking-TX pass starved RX under
    // exactly that load). If the feed never filled the ring (`feed_full==0`), TX was not
    // drain-bound and a zero deficit proves nothing — fail loudly rather than pass vacuously. A
    // real feed against the 512 ms ring saturates within tens of write-units and reads feed_full
    // in the hundreds, so this is robustly true on a healthy run. ──
    if feed_full == 0 {
        return Err(format!(
            "FullDuplexRxIntegrity: feed_full={feed_full} — the feed never saturated the inbound \
             ring, so the capture thread was not TX-drain-bound and a zero rx_deficit would be \
             vacuous (idle TX cannot starve RX). chunks_fed={chunks_fed}, logs collected: {} lines.",
            logs.len()
        ));
    }

    // ── 3. Parse every production observability line for its RX deficit and its core/priority
    // pin. A line carrying the prefix but missing any of these tokens is a format drift — fail
    // loudly, do not silently skip. The pin is asserted on every window (it cannot change per
    // window): a dropped core-1 pin or a priority regression FAILs distinctly from the deficit
    // metric, so the fix's own mechanism has a regression guard rather than relying on a human
    // reading the log line. ──
    let mut deficits: Vec<u64> = Vec::new();
    let mut suppressed: usize = 0;
    for line in logs.iter().filter(|l| l.contains(CAPTURE_OBS_LINE)) {
        let win_ok = parse_token_u64(line, RX_WIN_OK).ok_or_else(|| {
            format!(
                "FullDuplexRxIntegrity: '{CAPTURE_OBS_LINE}' line missing/invalid \
                 '{RX_WIN_OK}': {line}"
            )
        })?;
        if win_ok > 1 {
            return Err(format!(
                "FullDuplexRxIntegrity: '{CAPTURE_OBS_LINE}' line has invalid '{RX_WIN_OK}{win_ok}' \
                 (expected 0 or 1): {line}"
            ));
        }
        let deficit = parse_token_u64(line, RX_DEFICIT).ok_or_else(|| {
            format!(
                "FullDuplexRxIntegrity: '{CAPTURE_OBS_LINE}' line missing/invalid \
                 '{RX_DEFICIT}': {line}"
            )
        })?;
        let core = parse_token_u64(line, CORE).ok_or_else(|| {
            format!(
                "FullDuplexRxIntegrity: '{CAPTURE_OBS_LINE}' line missing/invalid \
                 '{CORE}': {line}"
            )
        })?;
        let prio = parse_token_u64(line, PRIO).ok_or_else(|| {
            format!(
                "FullDuplexRxIntegrity: '{CAPTURE_OBS_LINE}' line missing/invalid \
                 '{PRIO}': {line}"
            )
        })?;
        if core != CAPTURE_EXPECTED_CORE || prio != CAPTURE_EXPECTED_PRIO {
            return Err(format!(
                "FullDuplexRxIntegrity: capture-thread pin regressed — obs line reports core={core} \
                 prio={prio}, expected core={CAPTURE_EXPECTED_CORE} prio={CAPTURE_EXPECTED_PRIO}. \
                 Core-1 isolation is the mechanism that keeps mic RX on cadence under playback load; \
                 a dropped pin or default-priority regression silently reintroduces the starvation \
                 this step eliminates: {line}"
            ));
        }
        // Exclude tone-test-suppressed windows: their rx_deficit was forced to 0 because mic RX
        // was not drained, so scoring it as a clean pass would launder a
        // discarded measurement. The pin assertion above still runs on every line — the pin cannot
        // change per window, so a suppressed window still guards against a pin regression.
        if win_ok == 1 {
            deficits.push(deficit);
        } else {
            suppressed += 1;
        }
    }
    if deficits.is_empty() {
        if suppressed > 0 {
            return Err(format!(
                "FullDuplexRxIntegrity: all {suppressed} collected '{CAPTURE_OBS_LINE}' window(s) \
                 were tone-test-suppressed (rx_win_ok=0) — no valid mic-RX measurement exists to \
                 score. Device verdict: chunks_fed={chunks_fed} feed_full={feed_full}. \
                 Logs collected: {} lines.",
                logs.len()
            ));
        }
        return Err(format!(
            "FullDuplexRxIntegrity: no '{CAPTURE_OBS_LINE}' periodic lines collected (the \
             capture thread emitted no observability summary during the feed). Device verdict: \
             chunks_fed={chunks_fed} feed_full={feed_full}. Logs collected: {} lines.",
            logs.len()
        ));
    }

    // ── 4. Drop the first window as the warmup/ramp transient, require a steady-state floor. The
    // warmup drop costs one window, so an interpretable verdict needs ≥ MIN + 1 collected windows.
    // (Dropping window 1 cannot hide a genuine regression: the pre-fix defect starved RX in EVERY
    // playback window at a steady ~48 %, so the remaining windows would still show it.) ──
    if deficits.len() < FULL_DUPLEX_RX_MIN_OBS_WINDOWS + 1 {
        return Err(format!(
            "FullDuplexRxIntegrity: collected {} valid '{CAPTURE_OBS_LINE}' window(s) \
             ({suppressed} tone-test-suppressed and excluded), need ≥ {} valid \
             (one warmup window is always dropped, then ≥ {} steady-state window(s) are required). \
             Increase feed duration or check the device periodic-line cadence. \
             chunks_fed={chunks_fed}, feed_full={feed_full}.",
            deficits.len(),
            FULL_DUPLEX_RX_MIN_OBS_WINDOWS + 1,
            FULL_DUPLEX_RX_MIN_OBS_WINDOWS,
        ));
    }
    let post_warmup = &deficits[1..];

    // ── 5. Assert zero deficit on every steady-state window. The device already applies the
    // jitter dead-band (`rx_deficit_frames`, edge case K), so any value reaching the host is a
    // real shortfall — mic RX was starved under playback load. Surface the offending count and the
    // worst deficit; do NOT launder a nonzero reading into a pass. ──
    let offending: Vec<u64> = post_warmup.iter().copied().filter(|&d| d > 0).collect();
    if !offending.is_empty() {
        let worst = offending.iter().copied().max().unwrap_or(0);
        return Err(format!(
            "FullDuplexRxIntegrity FAIL — mic RX was starved under playback: {} of {} steady-state \
             window(s) reported a nonzero rx_deficit (worst {worst} frames past the dead-band). The \
             TX/RX split must keep RX serviced at its 16 kHz cadence under a saturating TX feed; a \
             nonzero deficit means the ~48 % mic-sample loss the root-cause analysis found is not \
             eliminated. chunks_fed={chunks_fed}, feed_full={feed_full}.",
            offending.len(),
            post_warmup.len(),
        ));
    }

    let suppressed_note = if suppressed > 0 {
        format!(" ({suppressed} tone-test-suppressed window(s) excluded)")
    } else {
        String::new()
    };
    println!(
        "  FullDuplexRxIntegrity: healthy — {} steady-state window(s), all rx_deficit=0 under a \
         saturating playback feed{suppressed_note} (feed_full={feed_full}, chunks_fed={chunks_fed}).",
        post_warmup.len(),
    );
    Ok(())
}

/// Evaluate the `GatewayProbeGate` reachable-half result.
///
/// Expects the device log-line suffix to start with "PASS" and contain
/// "probe=reachable" and "reassociated=false".
fn eval_gateway_probe_gate_reachable(msg: &str) -> Result<(), String> {
    if !msg.starts_with("PASS") {
        return Err(format!(
            "GatewayProbeGate reachable half does not start with PASS: {msg}"
        ));
    }
    if !msg.contains("probe=reachable") {
        return Err(format!(
            "GatewayProbeGate half-1 line missing 'probe=reachable' token: {msg}"
        ));
    }
    if !msg.contains("reassociated=false") {
        return Err(format!(
            "GatewayProbeGate half-1 line missing 'reassociated=false' token: {msg}"
        ));
    }
    Ok(())
}

/// Evaluate the `GatewayProbeGate` unreachable-half (full pass) result.
///
/// Expects a `GatewayProbeGate` verdict reporting the blackhole probe unreachable and
/// the link re-associated. Also asserts that the collected logs include the supervisor
/// re-association markers.
fn eval_gateway_probe_gate_unreachable(data: &TestData, logs: &[String]) -> Result<(), String> {
    // ip/gateway/rssi are observability only — the post-gate WifiAssociate run
    // re-asserts the AC-B2 link bounds.
    let TestData::GatewayProbeGate {
        blackhole_reachable,
        reassociated,
        ip: _,
        gateway: _,
        rssi: _,
    } = data
    else {
        return Err(format!(
            "expected GatewayProbeGate result data, got: {data:?}"
        ));
    };
    if *blackhole_reachable {
        return Err(
            "GatewayProbeGate reported blackhole_reachable=true (something answered ICMP on the blackhole IP)"
                .to_string(),
        );
    }
    if !reassociated {
        return Err("GatewayProbeGate reported reassociated=false".to_string());
    }
    // Log-line assertions: supervisor must have gone through disconnect+reconnect.
    let required: &[(&str, &str)] = &[
        (
            WIFI_DISCONNECTED,
            "StaDisconnected event — disconnect was issued",
        ),
        (WIFI_CONNECTED, "StaConnected event — WiFi re-associated"),
        (WIFI_DHCP_LEASE, "IpEvent DHCP lease — netif came back up"),
        (
            WIFI_REASSOCIATED,
            "supervisor re-associated log — supervisor completed the cycle",
        ),
    ];
    for (token, description) in required {
        if !logs.iter().any(|l| l.contains(token)) {
            return Err(format!(
                "GatewayProbeGate: expected log line containing {token:?} not seen \
                 ({description}). Logs collected: {} lines.",
                logs.len()
            ));
        }
    }
    Ok(())
}

// ── Unit tests (hardware-free) ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use device_protocol::{BuildId, DeviceFrame, MAX_TESTS, Payload, Status};

    // ── Corrupt-frame crafting (deser-error recovery check) ──────────────────

    /// The crafted frame must never decode as a valid `Request`, or the device would
    /// process it instead of taking the `DeserError` arm. Guards the ≥100-variant
    /// hazard in CI before HIL: if `Command` grows past discriminant 99, this fails.
    #[test]
    fn corrupt_frame_fails_to_decode() {
        // The frame must be *well-formed COBS* carrying a *bad payload*, so the device
        // lands in `DeserError` — not `OverFull`/`Consumed` (a framing failure). Prove
        // framing is intact by round-tripping the inner payload as its raw tuple: this
        // succeeds only if the COBS layer decodes cleanly, and pins the payload to the
        // crafted varints [id=0, discriminant=99].
        let mut framing_probe = craft_corrupt_frame();
        let payload: (u32, u32) = postcard::from_bytes_cobs(&mut framing_probe).expect(
            "crafted frame must be well-formed COBS (else the device lands in \
             OverFull/Consumed, not DeserError)",
        );
        assert_eq!(
            payload,
            (0, 99),
            "crafted payload must be [id=0, discriminant=99]"
        );

        // Same well-formed frame must NOT decode as a `Request`: discriminant 99 exceeds
        // `Command`'s variant count, so the postcard *enum* decode fails. Guards the
        // ≥100-variant hazard in CI before HIL.
        let mut frame = craft_corrupt_frame();
        let decoded: Result<device_protocol::Request, _> = postcard::from_bytes_cobs(&mut frame);
        assert!(
            decoded.is_err(),
            "crafted frame must NOT decode as a valid Request \
             (discriminant 99 must exceed Command's variant count); decoded: {decoded:?}"
        );
    }

    // ── Single-test selector (RESPEAKER_HIL_ONLY) parsing + gating ────────────

    #[test]
    fn selector_unset_runs_full_suite() {
        let only = parse_hil_only(Err(std::env::VarError::NotPresent)).unwrap();
        assert_eq!(only, None);
        for t in REGISTERED_TESTS {
            assert!(
                selector_wants(only, *t),
                "unset selector must want every test"
            );
        }
    }

    #[test]
    fn selector_blank_or_whitespace_runs_full_suite() {
        // `RESPEAKER_HIL_ONLY=` (clear idiom) and whitespace-only both mean full suite.
        assert_eq!(parse_hil_only(Ok(String::new())).unwrap(), None);
        assert_eq!(parse_hil_only(Ok("   ".to_string())).unwrap(), None);
    }

    #[test]
    fn selector_valid_name_selects_only_that_test() {
        let only = parse_hil_only(Ok("StreamRealtimeDuplex".to_string())).unwrap();
        assert_eq!(only, Some(TestName::StreamRealtimeDuplex));
        assert!(selector_wants(only, TestName::StreamRealtimeDuplex));
        assert!(!selector_wants(only, TestName::SpeakerOutput));
        // Surrounding whitespace is trimmed.
        assert_eq!(
            parse_hil_only(Ok("  SpeakerOutput  ".to_string())).unwrap(),
            Some(TestName::SpeakerOutput)
        );
    }

    #[test]
    fn selector_unknown_name_errors_and_lists_valid_names() {
        let err = parse_hil_only(Ok("NoSuchTest".to_string())).unwrap_err();
        assert!(err.contains("does not name a registered test"));
        // The error enumerates the real vocabulary rather than a hand-maintained example.
        for t in REGISTERED_TESTS {
            assert!(
                err.contains(&format!("{t:?}")),
                "error must list registered name {t:?}; got: {err}"
            );
        }
    }

    #[test]
    fn selector_non_unicode_is_rejected_not_swallowed() {
        // A set-but-unusable selector must not be silently reinterpreted as "unset".
        use std::os::unix::ffi::OsStringExt;
        let bad = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
        let err = parse_hil_only(Err(std::env::VarError::NotUnicode(bad))).unwrap_err();
        assert!(err.contains("non-UTF-8"));
    }

    #[test]
    fn full_duplex_selection_keeps_playback_drain_as_prereq() {
        // The SpeakerOutput-hosted block runs when either PlaybackDrainRate or
        // FullDuplexRxIntegrity is wanted (FDRI needs PDR's warm state); selecting only
        // PlaybackDrainRate must NOT pull in FullDuplexRxIntegrity.
        let pdr = Some(TestName::PlaybackDrainRate);
        assert!(
            selector_wants(pdr, TestName::PlaybackDrainRate)
                || selector_wants(pdr, TestName::FullDuplexRxIntegrity)
        );
        assert!(!selector_wants(pdr, TestName::FullDuplexRxIntegrity));

        let fdri = Some(TestName::FullDuplexRxIntegrity);
        assert!(
            selector_wants(fdri, TestName::PlaybackDrainRate)
                || selector_wants(fdri, TestName::FullDuplexRxIntegrity)
        );
        assert!(!selector_wants(fdri, TestName::PlaybackDrainRate));
    }

    use pod_transport::test_support::{FakePort, make_harness};

    fn make_build_id(commit: &str, dirty: bool) -> BuildId {
        let mut c = heapless::String::<40>::new();
        let _ = c.push_str(&commit[..commit.len().min(40)]);
        BuildId { commit: c, dirty }
    }

    // ── Build-ID compare tests ────────────────────────────────────────────────

    /// Equal build IDs → pass (no error).
    #[test]
    fn build_id_equal_pass() {
        let id = make_build_id("abc123", false);
        assert_eq!(id, id.clone());
    }

    /// Differing commits → not equal.
    #[test]
    fn build_id_different_commit_fail() {
        let a = make_build_id("aaa", false);
        let b = make_build_id("bbb", false);
        assert_ne!(a, b);
    }

    /// Dirty vs clean at same commit → not equal.
    #[test]
    fn build_id_dirty_mismatch_fail() {
        let a = make_build_id("abc", true);
        let b = make_build_id("abc", false);
        assert_ne!(a, b);
    }

    // ── Registry set tests ────────────────────────────────────────────────────

    /// Reported tests match expected exactly → check 3 passes (sets are equal).
    /// `expected_set` is built from REGISTERED_TESTS (the host side); `reported` is
    /// built from the same slice to simulate a device that reports exactly the registry.
    /// This exercises the BTreeSet<u8> collection and discriminant-mapping path used
    /// by check 3 in run(). The `registered_tests_covers_all_variants` test in
    /// device-protocol owns the exhaustiveness invariant; this test owns the
    /// check-3 comparison mechanics.
    #[test]
    fn registry_exact_match() {
        // Simulate the device side: push all REGISTERED_TESTS into a heapless::Vec
        // exactly as respeaker-pod's run_handler Identify arm does.
        let mut reported: heapless::Vec<TestName, MAX_TESTS> = heapless::Vec::new();
        for &t in REGISTERED_TESTS {
            reported.push(t).unwrap();
        }
        // Simulate check 3 — map both sides through test_name_discriminant.
        let expected_set: BTreeSet<u8> = REGISTERED_TESTS
            .iter()
            .map(test_name_discriminant)
            .collect();
        let reported_set: BTreeSet<u8> = reported.iter().map(test_name_discriminant).collect();
        assert_eq!(expected_set, reported_set);
        // Sanity: the sets are non-empty (guards against an accidentally cleared registry).
        assert!(
            !expected_set.is_empty(),
            "REGISTERED_TESTS must be non-empty; check-3 cannot verify an empty registry"
        );
    }

    /// Reported tests missing one entry → check 3 fails.
    #[test]
    fn registry_missing_entry_fail() {
        let mut reported: heapless::Vec<TestName, MAX_TESTS> = heapless::Vec::new();
        // Only push the first one.
        reported.push(REGISTERED_TESTS[0]).unwrap();
        let expected_set: BTreeSet<u8> = REGISTERED_TESTS
            .iter()
            .map(test_name_discriminant)
            .collect();
        let reported_set: BTreeSet<u8> = reported.iter().map(test_name_discriminant).collect();
        assert_ne!(expected_set, reported_set);
    }

    /// Device reports an extra test the harness does not expect → check 3 fails.
    ///
    /// Mirrors the missing-entry direction: `registry_missing_entry_fail` tests the
    /// "device is missing a test" branch; this test exercises the "device has an extra
    /// test" branch by simulating a harness compiled against a *subset* of the registry
    /// (as if an older host talks to a device that has been updated with a new test).
    ///
    /// Both sides are built via `test_name_discriminant` — the same production path as
    /// check-3 in `run()` — to ensure that function is exercised in both set-diff
    /// directions.
    #[test]
    fn registry_extra_entry_fail() {
        // Simulate an older harness: expected set is all tests except the last one.
        let expected_subset = &REGISTERED_TESTS[..REGISTERED_TESTS.len() - 1];
        let expected_set: BTreeSet<u8> =
            expected_subset.iter().map(test_name_discriminant).collect();

        // Device reports the full registry (as in production).
        let mut reported: heapless::Vec<TestName, MAX_TESTS> = heapless::Vec::new();
        for &t in REGISTERED_TESTS {
            reported.push(t).unwrap();
        }
        let reported_set: BTreeSet<u8> = reported.iter().map(test_name_discriminant).collect();

        // The sets must differ.
        assert_ne!(expected_set, reported_set);

        // The extra element must be exactly the last test's discriminant.
        let extra: Vec<u8> = reported_set.difference(&expected_set).copied().collect();
        assert_eq!(
            extra,
            vec![test_name_discriminant(REGISTERED_TESTS.last().unwrap())],
            "symmetric difference must contain exactly the last registered test"
        );
    }

    // ── check4_test_result helper tests ─────────────────────────────────────────
    //
    // These tests exercise the check4_test_result helper (extracted from run())
    // so that the rejection branch is actually reached and verified.

    fn report_resp(status: Status, detail: &str, data: TestData) -> Response {
        let mut d = heapless::String::<192>::new();
        d.push_str(detail).unwrap();
        Response {
            id: 1,
            status,
            payload: Payload::TestReport(TestReport { detail: d, data }),
        }
    }

    /// An Ok `TestReport` needs no detail — `data` is authoritative.
    #[test]
    fn check4_test_report_ok_accepts_empty_detail() {
        let resp = report_resp(
            Status::Ok,
            "",
            TestData::CapturePeriodicLine { chunks_fed: 5 },
        );
        let report = check4_test_report(&resp).expect("Ok report with empty detail is valid");
        assert_eq!(report.data, TestData::CapturePeriodicLine { chunks_fed: 5 });
    }

    /// A failing report with no diagnostic is a protocol violation.
    #[test]
    fn check4_test_report_fail_rejects_empty_detail() {
        let resp = report_resp(Status::Fail, "", TestData::None);
        assert!(
            check4_test_report(&resp).is_err(),
            "Fail with empty detail must be rejected"
        );
    }

    #[test]
    fn check4_test_report_fail_accepts_detail() {
        let resp = report_resp(Status::Fail, "i2c bus stuck low", TestData::None);
        let report = check4_test_report(&resp).expect("Fail with detail is valid");
        assert_eq!(report.detail.as_str(), "i2c bus stuck low");
    }

    /// An Ok report carrying neither typed data nor detail says nothing at all; the
    /// string-era contract rejected the equivalent empty message and so does this one.
    #[test]
    fn check4_test_report_ok_rejects_empty_data_and_detail() {
        let resp = report_resp(Status::Ok, "", TestData::None);
        assert!(
            check4_test_report(&resp).is_err(),
            "Ok with no data and no detail must be rejected"
        );
    }

    /// An ungated test (GpioSelfTest) carries its diagnostic as detail with no data.
    #[test]
    fn check4_test_report_ok_accepts_detail_without_data() {
        let resp = report_resp(Status::Ok, "all pins toggled", TestData::None);
        let report = check4_test_report(&resp).expect("Ok with detail only is valid");
        assert_eq!(report.detail.as_str(), "all pins toggled");
    }

    /// The name→predicate table binds each gated test to *its own* criterion. Each
    /// predicate must accept its test's healthy data and reject every other gated
    /// test's healthy data, so a swapped pairing fails here rather than on hardware.
    #[test]
    fn check4_typed_gate_pairs_each_test_with_its_own_predicate() {
        let healthy: [(TestName, TestData); 8] = [
            (
                TestName::I2sWaveformSanity,
                waveform_data(-32706, 32510, 680),
            ),
            (TestName::SpeakerOutput, speaker_data(true)),
            (
                TestName::I2cBusScan,
                i2c_scan_data(&[XVF3800_ADDR, AIC3104_ADDR], 0),
            ),
            (
                TestName::Xvf3800RegRead,
                reg_read_data(0x00, XVF3800_EXPECTED_VERSION),
            ),
            (TestName::Xvf3800DoAPlausibility, doa_data(0, [0.0; 4])),
            (TestName::Xvf3800SpEnergy, sp_energy_data(0, [1.0; 4])),
            (
                TestName::AmpAlwaysOnGpoInert,
                TestData::AmpGpoInert {
                    x0d31: 0x00,
                    write_status: 0x00,
                },
            ),
            (
                TestName::PsramIdentity,
                psram_data(true, PSRAM_EXPECTED_SIZE_U32, MallocProbe::Null),
            ),
        ];

        for (name, data) in &healthy {
            let (pred, _) = check4_typed_gate_for(name)
                .unwrap_or_else(|| panic!("{name:?} must have a typed gate"));
            assert!(pred(data), "{name:?} gate must accept its own healthy data");
            for (other_name, other_data) in &healthy {
                if other_name == name {
                    continue;
                }
                assert!(
                    !pred(other_data),
                    "{name:?} gate must reject {other_name:?} data — predicates are swapped"
                );
            }
        }
    }

    #[test]
    fn check4_test_report_rejects_other_payload() {
        let resp = Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        };
        assert!(check4_test_report(&resp).is_err());
    }

    /// The report's `detail` is control-char-escaped before it reaches operator output,
    /// so no device-authored text can emit a raw terminal escape sequence. Succeeds
    /// `check4_test_result_escapes_control_chars`; the empty-payload and wrong-payload
    /// cases are covered by the `check4_test_report_*` family above.
    #[test]
    fn report_detail_escapes_control_chars() {
        let out = escape_device_str("\x1b[31mPASS\x1b[0m forged\n");
        assert!(!out.contains('\x1b'), "raw ESC survived: {out:?}");
        assert!(!out.contains('\n'), "raw newline survived: {out:?}");
        assert_eq!(out, "\\u{1b}[31mPASS\\u{1b}[0m forged\\n");
    }

    // ── GpioSelfTest check-4 behavioral tests ────────────────────────────────

    /// GpioSelfTest response with Status::Ok and a descriptive report detail is
    /// accepted by the harness (check-4 pass path).
    #[test]
    fn gpio_self_test_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "GPIO21 high+low pad readback correct",
            TestData::None,
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::GpioSelfTest))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("Ok report with detail is valid");
        assert_eq!(report.data, TestData::None);
        assert!(
            !report.detail.is_empty(),
            "GpioSelfTest carries its verdict as detail"
        );
    }

    // ── DeviceHealthCheck check-4 behavioral tests ───────────────────────────

    /// DeviceHealthCheck response with Status::Ok carries the typed `DeviceHealth`
    /// metrics and no detail (check-4 pass path).
    #[test]
    fn device_health_check_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            healthy_device_health(),
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::DeviceHealthCheck))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("Ok report with data is valid");
        assert_eq!(report.data, healthy_device_health());
    }

    /// DeviceHealthCheck response with Status::Fail carries the threshold-violation
    /// narrative as detail and no typed data.
    ///
    /// Status propagation to the exit code is exercised in run(), driven by the
    /// Status::Fail match upstream.
    #[test]
    fn device_health_check_fail_path_has_detail() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Fail,
            "FAIL heap_free=1024<51200 min_heap=1000 stack_hwm=500 tx_write_failures=42",
            TestData::None,
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::DeviceHealthCheck))
            .unwrap();
        assert_eq!(resp.status, Status::Fail);
        let report = check4_test_report(&resp).expect("Fail report with detail is valid");
        assert!(
            report.detail.as_str().contains("tx_write_failures="),
            "fail path must carry a non-empty diagnostic; got {:?}",
            report.detail
        );
    }

    /// Representative healthy `DeviceHealth` metrics for the check-4 e2e tests.
    fn healthy_device_health() -> TestData {
        TestData::DeviceHealth {
            heap_free: 250_000,
            min_heap: 200_000,
            stack_hwm: 6_000,
            supervisor_hwm: 1_200,
            streamer_hwm: 1_500,
            writer_anomalies: 0,
            encode_failures: 0,
            tx_write_failures: 0,
        }
    }

    // ── Check-1 e2e tests ─────────────────────────────────────────────────────

    /// Matching build IDs → compare_build_ids returns Ok.
    #[test]
    fn check1_matching_build_ids_pass() {
        let id = make_build_id("abc123def456abc123def456abc123def456abc1", false);
        assert!(
            compare_build_ids(&id, &id.clone()).is_ok(),
            "identical BuildIds must compare equal"
        );
    }

    /// Mismatched commit → compare_build_ids returns Err containing a useful message.
    #[test]
    fn check1_mismatched_commit_returns_err_with_message() {
        let expected = make_build_id("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false);
        let device = make_build_id("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false);
        let result = compare_build_ids(&expected, &device);
        assert!(result.is_err(), "mismatched commits must produce Err");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("mismatch"),
            "error message must mention mismatch; got: {msg}"
        );
        assert!(
            msg.contains("aaaa"),
            "error message must contain expected commit; got: {msg}"
        );
        assert!(
            msg.contains("bbbb"),
            "error message must contain device commit; got: {msg}"
        );
    }

    /// Dirty-vs-clean mismatch at same commit → compare_build_ids returns Err.
    #[test]
    fn check1_dirty_flag_mismatch_returns_err() {
        let expected = make_build_id("abc", false);
        let device = make_build_id("abc", true);
        assert!(
            compare_build_ids(&expected, &device).is_err(),
            "dirty-flag mismatch must produce Err"
        );
    }

    /// Harness receives a mismatched Identify payload via FakePort and
    /// compare_build_ids detects the mismatch — exercises the check-1 code path
    /// end-to-end with the same FakePort infrastructure used in other harness tests.
    #[test]
    fn check1_e2e_mismatch_via_fake_port() {
        let expected = make_build_id("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false);
        let device_build = make_build_id("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", false);

        let mut port = FakePort::new();
        let mut tests: heapless::Vec<TestName, MAX_TESTS> = heapless::Vec::new();
        tests.push(TestName::Ping).unwrap();
        tests.push(TestName::Identify).unwrap();
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Identify {
                build: device_build.clone(),
                tests,
            },
        }));

        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Identify))
            .unwrap();
        let resp_build = match resp.payload {
            Payload::Identify { build, .. } => build,
            other => panic!("expected Identify payload; got {other:?}"),
        };

        // check-1: compare the harness-side expected ID against what came back
        let result = compare_build_ids(&expected, &resp_build);
        assert!(
            result.is_err(),
            "check-1 must fail when device commit differs from expected"
        );
    }

    // ── Overflow path test ────────────────────────────────────────────────────

    /// Queue a frame larger than CobsAccumulator<1024> followed by a valid small
    /// frame; the harness must eventually return the valid response (OverFull
    /// reset-and-continue path). This exercises the reset logic in
    /// send_command_timeout so an infinite loop or dropped-bytes regression is caught.
    #[test]
    fn overflow_then_valid_frame_returns_response() {
        let mut port = FakePort::new();

        // Build a raw byte sequence that exceeds 1024 bytes so the accumulator
        // hits OverFull. We inject raw bytes directly (not a valid COBS frame)
        // because any COBS frame that legitimately decodes will be consumed first.
        // 1025 non-zero bytes followed by a zero delimiter ensures OverFull fires.
        let mut oversized: Vec<u8> = vec![0xAAu8; 1025];
        oversized.push(0x00); // COBS frame delimiter
        port.rx.extend(oversized.iter().copied());

        // Queue a valid response frame after the oversized junk.
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Pong({
                let mut s = heapless::String::new();
                s.push_str("pong").unwrap();
                s
            }),
        }));

        let mut harness = make_harness(port);
        // Use a short timeout so the test does not take 5 s if behavior regresses.
        let result = harness
            .send_command_timeout(Command::RunTest(TestName::Ping), Duration::from_millis(500));
        // After OverFull reset the harness must find the valid frame.
        assert!(
            matches!(result, Ok(ref r) if r.status == Status::Ok),
            "expected Ok response after overflow reset; got: {result:?}"
        );
    }

    // ── Timeout path test ─────────────────────────────────────────────────────

    /// No device response → Timeout error (not a hang).
    ///
    /// Uses a 50 ms injected timeout via `send_command_timeout` so this test
    /// runs in `cargo test` without the 5-second production delay (AC9 coverage).
    #[test]
    fn timeout_path_returns_timeout_error() {
        let port = FakePort::new(); // always TimedOut on read
        let mut harness = make_harness(port);
        let result = harness
            .send_command_timeout(Command::RunTest(TestName::Ping), Duration::from_millis(50));
        assert!(
            matches!(result, Err(HarnessError::Timeout)),
            "expected Timeout; got: {result:?}"
        );
    }

    // ── I2cBusScan check-4 behavioral tests ────────────────────────────────────

    /// Build an `I2cScan` `TestData` from a found-address slice and a bus-error count.
    fn i2c_scan_data(found: &[u8], bus_errors: u32) -> TestData {
        TestData::I2cScan {
            found: heapless::Vec::from_slice(found).expect("found-list fits I2C_SCAN_MAX_ADDRS"),
            bus_errors,
        }
    }

    /// I2cBusScan response with Status::Ok and a found-list containing both required
    /// addresses (0x18 and 0x2c) is accepted by the harness (check-4 pass path).
    #[test]
    fn i2c_bus_scan_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::TestReport(TestReport {
                detail: heapless::String::new(),
                data: i2c_scan_data(&[AIC3104_ADDR, XVF3800_ADDR], 0),
            }),
        }));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2cBusScan))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("pass-path report must be well-formed");
        assert!(
            eval_i2c_scan_pass(&report.data),
            "pass-path data must carry 0x{XVF3800_ADDR:02x} and 0x{AIC3104_ADDR:02x}; \
             got: {:?}",
            report.data,
        );
    }

    /// I2cBusScan response with Status::Fail (expected devices did not ACK) carries the
    /// diagnostic in `detail` with `TestData::None`; check4_test_report accepts the shape
    /// (Status::Fail drives the exit code upstream).
    #[test]
    fn i2c_bus_scan_fail_path_has_report_payload() {
        let mut port = FakePort::new();
        let mut detail = heapless::String::<192>::new();
        detail
            .push_str("FAIL xvf=NACK aic=NACK found=[] bus_errors=0[]")
            .unwrap();
        port.queue_frame(&DeviceFrame::Response(Response {
            id: 1,
            status: Status::Fail,
            payload: Payload::TestReport(TestReport {
                detail,
                data: TestData::None,
            }),
        }));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2cBusScan))
            .unwrap();
        assert_eq!(resp.status, Status::Fail);
        let report =
            check4_test_report(&resp).expect("fail report with detail must be accepted by check4");
        assert!(
            !eval_i2c_scan_pass(&report.data),
            "TestData::None must never satisfy the scan criterion"
        );
    }

    // ── I2cBusScan pass-criterion boundary tests ─────────────────────────────
    //
    // These tests mirror the pass-criterion predicate in `run_i2c_bus_scan` on the
    // device via `eval_i2c_scan_pass`. They pin the boundary behavior independently
    // of hardware, catching regressions where the device changes the required
    // addresses without updating the host mirror.

    /// Both XVF3800 (0x2c) and AIC3104 (0x18) found → eval_i2c_scan_pass returns true.
    #[test]
    fn i2c_scan_both_present_pass() {
        assert!(
            eval_i2c_scan_pass(&i2c_scan_data(&[0x18, 0x2c], 0)),
            "both addresses present must return true"
        );
    }

    /// Only XVF3800 present (AIC3104 absent) → eval_i2c_scan_pass returns false.
    #[test]
    fn i2c_scan_only_xvf_present_fail() {
        assert!(
            !eval_i2c_scan_pass(&i2c_scan_data(&[0x2c], 0)),
            "only XVF present must return false (AIC absent)"
        );
    }

    /// Only AIC3104 present (XVF3800 absent) → eval_i2c_scan_pass returns false.
    #[test]
    fn i2c_scan_only_aic_present_fail() {
        assert!(
            !eval_i2c_scan_pass(&i2c_scan_data(&[0x18], 0)),
            "only AIC present must return false (XVF absent)"
        );
    }

    /// Empty found-list → eval_i2c_scan_pass returns false: a device bug returning
    /// Status::Ok without the required addresses must not pass the host criterion.
    #[test]
    fn i2c_scan_empty_list_fail() {
        assert!(
            !eval_i2c_scan_pass(&i2c_scan_data(&[], 0)),
            "empty found-list must return false"
        );
    }

    /// Non-zero `bus_errors` is rejected even with both addresses present — a device
    /// returning Status::Ok on a dirty bus (regression: dropped `bus_errors == 0` guard
    /// in run_i2c_bus_scan) must not pass the host criterion.
    #[test]
    fn i2c_scan_bus_errors_fails_predicate() {
        assert!(
            !eval_i2c_scan_pass(&i2c_scan_data(&[0x18, 0x2c], 1)),
            "non-zero bus_errors must fail even if both addresses present"
        );
    }

    /// Another test's data can never satisfy the scan criterion.
    #[test]
    fn i2c_scan_wrong_variant_fails_predicate() {
        assert!(
            !eval_i2c_scan_pass(&TestData::CapturePeriodicLine { chunks_fed: 3 }),
            "a foreign TestData variant must fail the scan predicate"
        );
    }

    // ── format_addr_list unit tests ───────────────────────────────────────────
    //
    // Pin the address-list formatting contract: {addr:#04x} per address, comma
    // separator, no spaces. These tests are the host-side specification of the
    // device-side `addr_list` construction loop in `run_i2c_bus_scan`. A device
    // change to case, prefix, separator, or width must break these tests first.

    /// Empty slice → empty string (the `found=[]` case).
    #[test]
    fn format_addr_list_empty_slice_produces_empty_string() {
        assert_eq!(
            format_addr_list(&[]),
            "",
            "empty slice must produce empty string"
        );
    }

    /// Single address → just the formatted address with no comma.
    #[test]
    fn format_addr_list_single_address_no_comma() {
        // 0x2c = XVF3800; {:#04x} → "0x2c"
        assert_eq!(format_addr_list(&[0x2c]), "0x2c");
    }

    /// Two addresses → comma-separated, no spaces.
    #[test]
    fn format_addr_list_two_addresses_comma_separated() {
        assert_eq!(format_addr_list(&[0x18, 0x2c]), "0x18,0x2c");
    }

    /// Low-nibble-zero address: 0x08 must render as "0x08" (two digits, not "0x8").
    /// This tests that `{:#04x}` zero-pads to exactly 2 hex digits.
    #[test]
    fn format_addr_list_low_nibble_zero_pads_to_two_digits() {
        assert_eq!(format_addr_list(&[0x08]), "0x08");
    }

    /// Multiple addresses: ordering and separator both correct for >2 entries.
    #[test]
    fn format_addr_list_multiple_addresses_correct_ordering_and_separator() {
        assert_eq!(format_addr_list(&[0x08, 0x18, 0x2c]), "0x08,0x18,0x2c");
    }

    /// Output is lower-case hex: `0x2c` not `0x2C`.
    #[test]
    fn format_addr_list_is_lowercase() {
        // 0x2C in uppercase would be "0x2C"; {:#04x} must produce lower-case "0x2c".
        let result = format_addr_list(&[0x2C]);
        assert_eq!(result, "0x2c", "address must be lower-case; got: {result}");
    }

    // ── format_error_detail unit tests ────────────────────────────────────────
    //
    // Pin the bus-error detail formatting contract and the `<80>` capacity math:
    // `[({eaddr:#04x},{ecode}),...]`, comma separator, no spaces, empty → `"[]"`.
    // These tests are the host-side specification of the device-side `error_detail`
    // construction loop in `run_i2c_bus_scan`. A device change to format or a
    // capacity regression must break these tests first.

    /// Empty list → "[]". Pins the device-absent-but-clean-bus FAIL case, where the
    /// message carries `error_detail = "[]"`.
    #[test]
    fn format_error_detail_empty_list_produces_brackets() {
        assert_eq!(format_error_detail(&[]).as_str(), "[]");
    }

    /// Single entry → no separator. 0x2c = XVF3800; 263 = ESP_ERR_TIMEOUT.
    #[test]
    fn format_error_detail_single_entry_no_separator() {
        assert_eq!(format_error_detail(&[(0x2c, 263)]).as_str(), "[(0x2c,263)]");
    }

    /// Two entries → comma-separated, no spaces. Pins separator placement.
    #[test]
    fn format_error_detail_two_entries_comma_separated() {
        assert_eq!(
            format_error_detail(&[(0x2c, 263), (0x18, 259)]).as_str(),
            "[(0x2c,263),(0x18,259)]"
        );
    }

    /// Low-nibble-zero address: 0x08 must render as "0x08" (two digits, not "0x8").
    /// Pins `{:#04x}` zero-padding. 259 = ESP_ERR_INVALID_STATE.
    #[test]
    fn format_error_detail_low_nibble_zero_pads_to_two_digits() {
        assert_eq!(format_error_detail(&[(0x08, 259)]).as_str(), "[(0x08,259)]");
    }

    /// Full 4-entry worst case: each entry `(0xff, i32::MIN)` = 18 chars, the
    /// documented maximum. Asserts the exact 77-char output and length, pinning the
    /// arithmetic against the `<80>` capacity. This test panics on the `expect`s if
    /// the capacity proof is ever violated.
    ///
    /// Note: the device scan range is `0x08..=0x77`, so `0xff` cannot occur in
    /// practice; the capacity proof is stated for the type's full `u8` domain
    /// (4 hex-display chars for any address), and this test exercises the proof, not
    /// the scan range.
    #[test]
    fn format_error_detail_worst_case_fills_77_of_80() {
        let entries = [
            (0xff, i32::MIN),
            (0xff, i32::MIN),
            (0xff, i32::MIN),
            (0xff, i32::MIN),
        ];
        let s = format_error_detail(&entries);
        assert_eq!(
            s.as_str(),
            "[(0xff,-2147483648),(0xff,-2147483648),(0xff,-2147483648),(0xff,-2147483648)]"
        );
        assert_eq!(s.len(), 77);
    }

    // ── Xvf3800RegRead check-4 behavioral tests ───────────────────────────────

    /// Xvf3800RegRead response with Status::Ok and pass data is accepted end-to-end.
    #[test]
    fn xvf3800_reg_read_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            TestData::Xvf3800RegRead {
                status: 0x00,
                version: XVF3800_EXPECTED_VERSION,
            },
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Xvf3800RegRead))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("pass-path report must be accepted");
        assert!(
            eval_xvf3800_reg_read_pass(&report.data),
            "pass-path data must satisfy the presence criterion; got: {:?}",
            report.data
        );
    }

    /// Fail-path response: non-empty `detail`, `TestData::None`. `check4_test_report`
    /// accepts it (Status drives the exit code) and the eval rejects the data.
    ///
    /// This replaces the former "FAIL-prefixed message" case: a fail prefix is no longer
    /// a string to match, it is the `None` variant.
    #[test]
    fn xvf3800_reg_read_fail_path_has_report_payload() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Fail,
            "FAIL status=0x01 v=[0x00,0x00,0x00]",
            TestData::None,
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Xvf3800RegRead))
            .unwrap();
        assert_eq!(resp.status, Status::Fail);
        let report = check4_test_report(&resp)
            .expect("non-empty detail on the fail path must be accepted by check4_test_report");
        assert!(
            !eval_xvf3800_reg_read_pass(&report.data),
            "eval_xvf3800_reg_read_pass must reject fail-path data"
        );
    }

    // ── eval_xvf3800_reg_read_pass unit tests ─────────────────────────────────

    /// Build `Xvf3800RegRead` data.
    fn reg_read_data(status: u8, version: [u8; 3]) -> TestData {
        TestData::Xvf3800RegRead { status, version }
    }

    /// Pinned version with a DONE status → pass.
    #[test]
    fn reg_read_expected_version_passes() {
        assert!(eval_xvf3800_reg_read_pass(&reg_read_data(
            0x00,
            XVF3800_EXPECTED_VERSION
        )));
    }

    /// A different firmware version is a discovery, not a pass.
    #[test]
    fn reg_read_unexpected_version_fails() {
        assert!(!eval_xvf3800_reg_read_pass(&reg_read_data(
            0x00,
            [0x02, 0x00, 0x00]
        )));
    }

    /// A non-DONE status byte fails even with the pinned version.
    #[test]
    fn reg_read_non_done_status_fails() {
        assert!(!eval_xvf3800_reg_read_pass(&reg_read_data(
            0x01,
            XVF3800_EXPECTED_VERSION
        )));
    }

    /// The device's implausible payloads (all-zero, all-0xFF) cannot match the pinned
    /// version — the former `implausible` substring guard is subsumed structurally.
    #[test]
    fn reg_read_implausible_payloads_fail() {
        assert!(!eval_xvf3800_reg_read_pass(&reg_read_data(
            0x00,
            [0x00, 0x00, 0x00]
        )));
        assert!(!eval_xvf3800_reg_read_pass(&reg_read_data(
            0x00,
            [0xff, 0xff, 0xff]
        )));
    }

    /// Another test's data must never satisfy this eval.
    #[test]
    fn reg_read_wrong_variant_fails() {
        assert!(!eval_xvf3800_reg_read_pass(&TestData::None));
        assert!(!eval_xvf3800_reg_read_pass(&doa_data(
            0x00,
            [0.0, 0.0, 0.0, 0.0]
        )));
    }

    // ── PsramIdentity pass-criterion boundary tests ──────────────────────────

    /// Build a `PsramIdentity` `TestData` with the expected size unless overridden.
    fn psram_data(init: bool, size: u32, malloc_probe: MallocProbe) -> TestData {
        TestData::PsramIdentity {
            init,
            size,
            spiram_free: 8_323_072,
            malloc_probe,
        }
    }

    /// Expected size as the `u32` the wire carries.
    const PSRAM_EXPECTED_SIZE_U32: u32 = PSRAM_EXPECTED_SIZE_BYTES as u32;

    /// Valid pass data: initialized, expected 8 MiB, probe did not spill → true.
    #[test]
    fn psram_identity_pass_criterion_valid_pass() {
        assert!(
            eval_psram_identity_pass(&psram_data(
                true,
                PSRAM_EXPECTED_SIZE_U32,
                MallocProbe::Null
            )),
            "initialized with expected size must return true"
        );
    }

    /// `spiram_free` is observability only — free-heap headroom is graded by
    /// DeviceHealthCheck, not by the identity pin. This records the `_` binding in
    /// `eval_psram_identity_pass` as a decision: a zero-free report still passes identity.
    #[test]
    fn psram_identity_pass_ignores_spiram_free() {
        assert!(
            eval_psram_identity_pass(&TestData::PsramIdentity {
                init: true,
                size: PSRAM_EXPECTED_SIZE_U32,
                spiram_free: 0,
                malloc_probe: MallocProbe::Null,
            }),
            "spiram_free is not part of the identity criterion"
        );
    }

    /// `MallocProbe::External` (plain malloc spilled to PSRAM) → false even with a
    /// well-formed init/size pass, so a Status::Ok emitted alongside a spill is caught.
    #[test]
    fn psram_identity_pass_criterion_malloc_spill_fails() {
        assert!(
            !eval_psram_identity_pass(&psram_data(
                true,
                PSRAM_EXPECTED_SIZE_U32,
                MallocProbe::External
            )),
            "MallocProbe::External must return false (allocator spilled to PSRAM)"
        );
    }

    /// `MallocProbe::Internal` (probe resolved in internal RAM) is a valid pass.
    #[test]
    fn psram_identity_pass_criterion_malloc_internal_passes() {
        assert!(
            eval_psram_identity_pass(&psram_data(
                true,
                PSRAM_EXPECTED_SIZE_U32,
                MallocProbe::Internal
            )),
            "MallocProbe::Internal must return true"
        );
    }

    /// The former "FAIL prefix" case: a fail path now carries `TestData::None`, which can
    /// never satisfy the criterion.
    #[test]
    fn psram_identity_pass_criterion_fail_data_none_fails() {
        assert!(
            !eval_psram_identity_pass(&TestData::None),
            "TestData::None must return false"
        );
    }

    /// Not initialized → false (isolates the `init` guard).
    #[test]
    fn psram_identity_pass_criterion_not_initialized_fails() {
        assert!(
            !eval_psram_identity_pass(&psram_data(
                false,
                PSRAM_EXPECTED_SIZE_U32,
                MallocProbe::Null
            )),
            "init=false must return false"
        );
    }

    /// Wrong size (a 4 MiB part, or QUAD-misconfigured) → false (identity regression).
    #[test]
    fn psram_identity_pass_criterion_wrong_size_fails() {
        assert!(
            !eval_psram_identity_pass(&psram_data(true, 4 * 1024 * 1024, MallocProbe::Null)),
            "size other than 8 MiB must return false (identity regression guard)"
        );
    }

    /// The former "size substring superset" case: with a typed `u32`, 10x the expected
    /// size is simply a different number, not a token that could substring-match.
    #[test]
    fn psram_identity_pass_criterion_ten_times_size_fails() {
        assert!(
            !eval_psram_identity_pass(&psram_data(
                true,
                PSRAM_EXPECTED_SIZE_U32 * 10,
                MallocProbe::Null
            )),
            "10x the expected size must not satisfy the identity criterion"
        );
    }

    /// Another test's data can never satisfy the PSRAM criterion.
    #[test]
    fn psram_identity_wrong_variant_fails_predicate() {
        assert!(
            !eval_psram_identity_pass(&i2c_scan_data(&[0x18, 0x2c], 0)),
            "a foreign TestData variant must fail the PSRAM predicate"
        );
    }

    // ── SpeakerOutput pass-criterion boundary tests ──────────────────────────
    // The host-side second enforcement point (eval_speaker_pass) re-derives the device's
    // programmatic contract (codec_ok) from the typed report, independent of Status. There
    // is no amp-enable field — the amp is always-on hardware (design realfix §2.3/§2.5).
    // Acoustic acceptance is the operator's ear; these only pin the data check.

    fn speaker_data(codec_ok: bool) -> TestData {
        TestData::SpeakerOutput {
            freq: 440,
            amp: 50,
            dur_ms: 1500,
            codec_ok,
        }
    }

    /// Canonical device pass data → true.
    #[test]
    fn speaker_pass_criterion_valid_pass() {
        assert!(
            eval_speaker_pass(&speaker_data(true)),
            "canonical SpeakerOutput report with codec_ok must return true"
        );
    }

    /// The tone parameters are observability only — the device-side asserts own them, and
    /// acoustic acceptance is the operator's ear. This records that the `_` bindings in
    /// `eval_speaker_pass` are a decision, not an oversight: degenerate tone values still
    /// pass the host gate.
    #[test]
    fn speaker_pass_ignores_tone_parameters() {
        assert!(
            eval_speaker_pass(&TestData::SpeakerOutput {
                freq: 0,
                amp: 0,
                dur_ms: 0,
                codec_ok: true,
            }),
            "freq/amp/dur_ms are not host-graded; only codec_ok gates"
        );
    }

    /// `codec_ok: false` (codec init absent or faulted) → false.
    #[test]
    fn speaker_pass_criterion_missing_codec_fails() {
        assert!(
            !eval_speaker_pass(&speaker_data(false)),
            "codec_ok=false must return false (catches stale/absent codec init)"
        );
    }

    /// Fail paths carry `TestData::None`, so the whole family of device FAIL shapes —
    /// codec-init faults and the distinct step-3b DAC-unmute faults (`reason=codec
    /// reg=0x2b|0x2c`, design §2.5/§2.7, write-err / readback-err / mismatch) — reduces to
    /// one structural case. A refactor emitting Status::Ok for a stuck-muted DAC (silent
    /// tone) is still caught here, and the per-shape string cases are gone with the strings.
    #[test]
    fn speaker_pass_criterion_fail_data_fails() {
        assert!(
            !eval_speaker_pass(&TestData::None),
            "a fail-path report (TestData::None) must not satisfy the pass criterion"
        );
    }

    /// Another test's data must never satisfy this criterion. Replaces the old
    /// token-position, embedded-substring (`codec=ok_partial`) and stale-`amp_gpo=ok`
    /// cases: token spelling and position are no longer a contract surface at all.
    #[test]
    fn speaker_pass_criterion_wrong_variant_fails() {
        assert!(
            !eval_speaker_pass(&i2c_scan_data(&[0x18, 0x2c], 0)),
            "a foreign TestData variant must fail the speaker predicate"
        );
    }

    // ── AmpAlwaysOnGpoInert pass-criterion boundary tests ───────────────────
    //
    // The host-side second enforcement point (eval_amp_gpo_inert_pass) re-derives the
    // device contract from the report data, independent of Status. Hardware-free.
    //
    // The former string suite's "extra token", "token position", "embedded token" and
    // "uninit bus string" cases had no successor: token spelling, ordering and substring
    // ambiguity are not a contract surface once the data is typed.

    /// Canonical device PASS data → true.
    #[test]
    fn amp_gpo_inert_pass_criterion_valid_pass() {
        assert!(eval_amp_gpo_inert_pass(&TestData::AmpGpoInert {
            x0d31: 0x00,
            write_status: 0x00,
        }));
    }

    /// `x0d31` is informational — either logic level passes when the write was DONE.
    #[test]
    fn amp_gpo_inert_pass_criterion_x0d31_is_informational() {
        assert!(eval_amp_gpo_inert_pass(&TestData::AmpGpoInert {
            x0d31: 0x01,
            write_status: 0x00,
        }));
    }

    /// A non-DONE write status → false. (The device also fails this path itself; the host
    /// gate is the independent second enforcement point.)
    #[test]
    fn amp_gpo_inert_pass_criterion_non_done_write_status_fails() {
        assert!(!eval_amp_gpo_inert_pass(&TestData::AmpGpoInert {
            x0d31: 0x00,
            write_status: 0x02,
        }));
    }

    /// Every device rejection path (write took effect, I2C error, uninitialized bus)
    /// carries `TestData::None` — the structural successor of the old `FAIL` prefix and
    /// `gpo_write=took` cases.
    #[test]
    fn amp_gpo_inert_pass_criterion_fail_data_fails() {
        assert!(!eval_amp_gpo_inert_pass(&TestData::None));
    }

    /// Another test's data must never satisfy this eval.
    #[test]
    fn amp_gpo_inert_pass_criterion_wrong_variant_fails() {
        assert!(!eval_amp_gpo_inert_pass(&reg_read_data(
            0x00,
            XVF3800_EXPECTED_VERSION
        )));
    }

    // ── Xvf3800DoAPlausibility check-4 behavioral tests ─────────────────────

    /// Build `Xvf3800Doa` data.
    fn doa_data(status: u8, az: [f32; 4]) -> TestData {
        TestData::Xvf3800Doa { status, az }
    }

    /// Xvf3800DoAPlausibility pass: status=0x00, scanner finite, all within [-π, π].
    #[test]
    fn doa_plausibility_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            doa_data(0x00, [1.57, 2.5, 0.8, 1.2]),
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Xvf3800DoAPlausibility))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("pass-path report must be accepted");
        assert!(
            eval_doa_plausibility_pass(&report.data),
            "pass-path data must satisfy the plausibility criterion; got: {:?}",
            report.data
        );
    }

    /// Focused trackers (idx 0/1) and winner (idx 3) may be NaN — a quiet room. Only the
    /// scanner (idx 2) must be finite. NaN crosses the wire as a real `f32`, so the old
    /// `"nan"`-spelling parse cases have no successor.
    #[test]
    fn doa_plausibility_nan_focused_trackers_accepted() {
        assert!(eval_doa_plausibility_pass(&doa_data(
            0x00,
            [f32::NAN, f32::NAN, 1.5, f32::NAN]
        )));
    }

    /// Scanner (idx 2) NaN → fails.
    #[test]
    fn doa_plausibility_scanner_nan_fails() {
        assert!(!eval_doa_plausibility_pass(&doa_data(
            0x00,
            [1.0, 1.0, f32::NAN, 1.0]
        )));
    }

    /// Every device rejection path carries `TestData::None` — successor of the old
    /// `FAIL`-prefix case.
    #[test]
    fn doa_plausibility_fail_data_rejected() {
        assert!(!eval_doa_plausibility_pass(&TestData::None));
    }

    /// Non-zero status byte → fails.
    #[test]
    fn doa_plausibility_nonzero_status_fails() {
        assert!(!eval_doa_plausibility_pass(&doa_data(
            0x40,
            [1.0, 1.0, 1.0, 1.0]
        )));
    }

    /// Inf on a non-scanner index (idx 1) must fail the plausibility check.
    #[test]
    fn doa_plausibility_inf_on_non_scanner_fails() {
        assert!(!eval_doa_plausibility_pass(&doa_data(
            0x00,
            [1.0, f32::INFINITY, 1.0, 1.0]
        )));
    }

    /// Inf on the scanner (idx 2) must fail — both via `doa_azimuth_ok` and the az[2] guard.
    #[test]
    fn doa_plausibility_inf_on_scanner_fails() {
        assert!(!eval_doa_plausibility_pass(&doa_data(
            0x00,
            [1.0, 1.0, f32::INFINITY, 1.0]
        )));
    }

    /// Out-of-range (|x| > π) must fail for each individual index, not just idx 0.
    ///
    /// Guards against an iteration bug that skips idx 1/2/3.
    #[test]
    fn doa_plausibility_out_of_range_fails_each_index() {
        for idx in 0..4 {
            let mut az = [1.0_f32; 4];
            az[idx] = 4.0;
            assert!(
                !eval_doa_plausibility_pass(&doa_data(0x00, az)),
                "az[{idx}]=4.0 > π must fail plausibility check"
            );
        }
    }

    /// Another test's data must never satisfy this eval.
    #[test]
    fn doa_plausibility_wrong_variant_fails() {
        assert!(!eval_doa_plausibility_pass(&sp_energy_data(
            0x00,
            [0.0, 0.0, 0.0, 0.0]
        )));
    }

    /// Status::Ok with plausibility-failing data must be caught by the second enforcement
    /// point — the condition that triggers the host's non-zero exit in production.
    #[test]
    fn doa_plausibility_ok_status_but_failing_data_detected() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            doa_data(0x00, [f32::NAN, 1.0, f32::NAN, 1.0]),
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::Xvf3800DoAPlausibility))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("report must be accepted");
        assert!(
            !eval_doa_plausibility_pass(&report.data),
            "scanner NaN with Status::Ok must fail the plausibility predicate"
        );
    }

    // ── eval_sp_energy_pass unit tests ─────────────────────────────────────────

    /// Build `Xvf3800SpEnergy` data.
    fn sp_energy_data(status: u8, sp: [f32; 4]) -> TestData {
        TestData::Xvf3800SpEnergy { status, sp }
    }

    /// All four finite, positive, non-zero values → pass.
    #[test]
    fn sp_energy_pass_all_positive() {
        assert!(eval_sp_energy_pass(&sp_energy_data(
            0x00,
            [10.5, 20.3, 5.1, 8.8]
        )));
    }

    /// Every device rejection path carries `TestData::None` — successor of the old
    /// `FAIL`-prefix case.
    #[test]
    fn sp_energy_fail_data_rejected() {
        assert!(!eval_sp_energy_pass(&TestData::None));
    }

    /// Non-zero status byte → fails.
    #[test]
    fn sp_energy_nonzero_status_fails() {
        assert!(!eval_sp_energy_pass(&sp_energy_data(
            0x40,
            [1.0, 2.0, 3.0, 4.0]
        )));
    }

    /// NaN in any position → fails (unlike DoA, SPENERGY NaN is never acceptable).
    /// Covers each index, guarding against an iteration bug.
    #[test]
    fn sp_energy_nan_fails_each_index() {
        for idx in 0..4 {
            let mut sp = [2.0_f32; 4];
            sp[idx] = f32::NAN;
            assert!(
                !eval_sp_energy_pass(&sp_energy_data(0x00, sp)),
                "NaN at sp[{idx}] must return false"
            );
        }
    }

    /// Negative value → fails (energy is always non-negative).
    #[test]
    fn sp_energy_negative_fails() {
        assert!(!eval_sp_energy_pass(&sp_energy_data(
            0x00,
            [1.0, -0.001, 3.0, 4.0]
        )));
    }

    /// All-zero → passes (0.0 = no speech present; valid in an unattended room).
    #[test]
    fn sp_energy_all_zero_passes() {
        assert!(eval_sp_energy_pass(&sp_energy_data(
            0x00,
            [0.0, 0.0, 0.0, 0.0]
        )));
    }

    /// Inf → fails (not finite).
    #[test]
    fn sp_energy_inf_fails() {
        assert!(!eval_sp_energy_pass(&sp_energy_data(
            0x00,
            [f32::INFINITY, 2.0, 3.0, 4.0]
        )));
    }

    /// Another test's data must never satisfy this eval. Successor of the old
    /// absent-`sp=[...]` and missing-`status` parse cases: there is no format to lose,
    /// only the wrong variant.
    #[test]
    fn sp_energy_wrong_variant_fails() {
        assert!(!eval_sp_energy_pass(&doa_data(0x00, [0.0, 0.0, 0.0, 0.0])));
    }

    /// Values survive the postcard round-trip bit-exactly, including the small and large
    /// magnitudes the old `DebugF32` scientific-notation format had to encode as text.
    /// Successor of `sp_energy_device_format_round_trip` — the format-drift class it
    /// guarded against no longer exists, but wire fidelity still matters.
    #[test]
    fn sp_energy_values_survive_wire_round_trip() {
        let sp = [10.5_f32, 0.0_f32, 1234.5_f32, 0.000_1_f32];
        let report = TestReport {
            detail: heapless::String::new(),
            data: sp_energy_data(0x00, sp),
        };
        let mut buf = [0u8; 256];
        let encoded = postcard::to_slice(&report, &mut buf).expect("encode");
        let decoded: TestReport = postcard::from_bytes(encoded).expect("decode");
        assert_eq!(decoded.data, sp_energy_data(0x00, sp));
        assert!(eval_sp_energy_pass(&decoded.data));
    }

    // ── Registry membership / count guard ───────────────────────────────────
    // `registered_tests_covers_all_variants` in device-protocol owns exhaustive
    // membership and count assertions for REGISTERED_TESTS; no separate spot-check
    // needed here.

    // ── I2sWaveformSanity tests ───────────────────────────────────────────────
    //
    // Pass data (mono ring snapshot): `TestData::I2sWaveform { min, max, rms, sat_pct,
    // samples, ac1 }`, where ac1 is normalized lag-1 autocorrelation × 1000 (RNG noise ≈ 0,
    // acoustic ≈ 680). Fail paths carry `TestData::None` plus a `FAIL src=ring reason=…`
    // detail line.

    fn waveform_data(min: i32, max: i32, ac1: i32) -> TestData {
        TestData::I2sWaveform {
            min,
            max,
            rms: 5000,
            sat_pct: 0,
            samples: 4000,
            ac1,
        }
    }

    /// Healthy mono ring stats → true.
    #[test]
    fn i2s_waveform_pass_healthy_data() {
        assert!(
            eval_i2s_waveform_pass(&waveform_data(-32706, 32510, 680)),
            "healthy waveform data must pass host criterion"
        );
    }

    /// Every device fail path — all-zero, stuck-constant, saturated, low-autocorr and the
    /// ring-fill timeout — carries `TestData::None`, so one structural case replaces the
    /// former FAIL-prefix, lowercase-`fail`, empty-message and `reason=ring-not-filled`
    /// string cases, and `eval_i2s_waveform_msg_is_fail` is gone with them: a Status/data
    /// mismatch can no longer be expressed.
    #[test]
    fn i2s_waveform_pass_rejects_fail_data() {
        assert!(
            !eval_i2s_waveform_pass(&TestData::None),
            "a fail-path report (TestData::None) must be rejected by the pass criterion"
        );
    }

    /// Another test's data must never satisfy this criterion. Replaces the old
    /// missing-`max=` / missing-`ac1=` parse-failure cases: an absent field is now a
    /// compile error, not a runtime parse miss.
    #[test]
    fn i2s_waveform_pass_rejects_wrong_variant() {
        assert!(
            !eval_i2s_waveform_pass(&i2c_scan_data(&[0x18, 0x2c], 0)),
            "a foreign TestData variant must fail the waveform predicate"
        );
    }

    /// All-zero → false (dead line: max_abs=0 and spread=0).
    #[test]
    fn i2s_waveform_pass_rejects_all_zero() {
        assert!(
            !eval_i2s_waveform_pass(&waveform_data(0, 0, 680)),
            "all-zero data must be rejected (max_abs=0 ≤ floor, spread=0 ≤ floor)"
        );
    }

    /// `ac1` below the autocorrelation floor → false (RNG-noise regression guard).
    ///
    /// High RMS but low autocorrelation (= full-scale random noise pre-fix) must be
    /// rejected. This guards against reverting the I2S role/slot-width change.
    #[test]
    fn i2s_waveform_pass_rejects_low_autocorr() {
        // ac1=5 (0.005) is well below the 200 (0.2) floor — mimics RNG noise statistics.
        assert!(
            !eval_i2s_waveform_pass(&waveform_data(-18000, 18000, 5)),
            "ac1 below the autocorr floor must be rejected (RNG-noise guard)"
        );
    }

    /// `ac1` exactly at the floor (200) → false (predicate is strict greater-than, not >=).
    ///
    /// Pins the comparison direction and constant value. r1=0.200 rounds to ac1=200.
    /// min=-200/max=200 give max_abs=200 (> 16) and spread=400 (> 32) so those guards pass
    /// and only the ac1 check determines the outcome.
    #[test]
    fn i2s_waveform_pass_rejects_ac1_at_floor() {
        assert!(
            !eval_i2s_waveform_pass(&waveform_data(-200, 200, I2S_HOST_AUTOCORR_FLOOR)),
            "ac1 at floor (200) must be rejected — predicate is strict greater-than"
        );
    }

    /// `ac1` one unit above the floor (201) → true.
    #[test]
    fn i2s_waveform_pass_accepts_ac1_just_above_floor() {
        assert!(
            eval_i2s_waveform_pass(&waveform_data(-200, 200, I2S_HOST_AUTOCORR_FLOOR + 1)),
            "ac1 just above floor (201) must be accepted"
        );
    }

    /// Quiet-but-healthy signal (confirmed quiet-room window) → true.
    ///
    /// THE regression guard for ADR 2026-06-17: a genuinely quiet room produces low
    /// rms/spread/max_abs but high autocorrelation. This window (settle-probe win12:
    /// rms=11, spread=76, max_abs=38, ac1=0.910) used to FAIL on the old rms/spread/max
    /// floors; it must PASS now. Reverting any floor above these values reintroduces the bug.
    #[test]
    fn i2s_waveform_pass_accepts_quiet_healthy_signal() {
        assert!(
            eval_i2s_waveform_pass(&TestData::I2sWaveform {
                min: -38,
                max: 38,
                rms: 11,
                sat_pct: 0,
                samples: 4000,
                ac1: 910,
            }),
            "quiet-but-correlated audio must PASS (no minimum-loudness floor)"
        );
    }

    /// Frozen / constant line → false even though ac1 is high.
    ///
    /// A constant value has ac1 ≈ 1.0, so the autocorr gate alone cannot catch it; the
    /// spread floor is the dedicated anti-frozen guard (spread=0 ≤ floor).
    #[test]
    fn i2s_waveform_pass_rejects_frozen_constant() {
        assert!(
            !eval_i2s_waveform_pass(&waveform_data(5000, 5000, 999)),
            "frozen/constant line must be rejected by the spread floor despite high ac1"
        );
    }

    /// spread exactly at the floor (32) → false (predicate is strict greater-than).
    ///
    /// min=-1/max=31 give spread=32 (= floor, not >), max_abs=31 (> 16), ac1=900 (> 200),
    /// so only the spread guard decides — pins the anti-frozen floor value + direction.
    #[test]
    fn i2s_waveform_pass_rejects_spread_at_floor() {
        assert!(
            !eval_i2s_waveform_pass(&waveform_data(-1, I2S_HOST_SPREAD_FLOOR - 1, 900)),
            "spread at floor (32) must be rejected — predicate is strict greater-than"
        );
    }

    /// spread one unit above the floor (33) → true.
    #[test]
    fn i2s_waveform_pass_accepts_spread_just_above_floor() {
        assert!(
            eval_i2s_waveform_pass(&waveform_data(-1, I2S_HOST_SPREAD_FLOOR, 900)),
            "spread just above floor (33) must be accepted"
        );
    }

    // ── FakePort behavioral tests ─────────────────────────────────────────────

    /// I2sWaveformSanity: Status::Ok with healthy data → accepted (exit 0 path).
    #[test]
    fn i2s_waveform_sanity_pass_path_accepted() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            waveform_data(-32706, 32510, 680),
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2sWaveformSanity))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("healthy report must extract");
        assert!(
            eval_i2s_waveform_pass(&report.data),
            "healthy waveform report must satisfy host pass criterion"
        );
    }

    /// I2sWaveformSanity: Status::Fail carries the diagnostic as `detail` with no data.
    #[test]
    fn i2s_waveform_sanity_fail_path_has_report_payload() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Fail,
            "FAIL src=ring reason=all-zero ch min=0 max=0 rms=0 sat=0% samples=4000 ac1=0",
            TestData::None,
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2sWaveformSanity))
            .unwrap();
        assert_eq!(resp.status, Status::Fail);
        let report = check4_test_report(&resp).expect("fail report with detail must extract");
        assert_eq!(report.data, TestData::None, "fail data is uniformly None");
        assert!(
            report.detail.contains("reason=all-zero"),
            "the human diagnostic must survive on the fail path: {:?}",
            report.detail
        );
    }

    /// Second enforcement point: Status::Ok carrying fail-path data → rejected.
    ///
    /// The typed successor of the old Status/message-mismatch check: a device bug that
    /// reports Ok without constructing the variant cannot substring-match its way through.
    #[test]
    fn i2s_waveform_ok_status_but_fail_data_detected() {
        let mut port = FakePort::new();
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "waveform sampled",
            TestData::None,
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2sWaveformSanity))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("Ok report extracts");
        assert!(
            !eval_i2s_waveform_pass(&report.data),
            "Status::Ok with fail-path data must be caught by the second enforcement point"
        );
    }

    /// Second enforcement point (numeric): Status::Ok but ac1 below floor → rejected.
    ///
    /// Exercises the `gate_check4_typed` branch in `run()` that catches a plausibility
    /// regression where the device reports Ok but the numeric criterion fails — e.g.
    /// high-RMS random noise before the I2S clocking fix.
    #[test]
    fn i2s_waveform_ok_status_but_low_autocorr_rejected() {
        let mut port = FakePort::new();
        // High RMS (looks like signal) but ac1=5 (0.005) — far below the 200 floor.
        port.queue_frame(&DeviceFrame::Response(report_resp(
            Status::Ok,
            "",
            waveform_data(-18000, 18000, 5),
        )));
        let mut harness = make_harness(port);
        let resp = harness
            .send_command(Command::RunTest(TestName::I2sWaveformSanity))
            .unwrap();
        assert_eq!(resp.status, Status::Ok);
        let report = check4_test_report(&resp).expect("Ok report extracts");
        assert!(
            !eval_i2s_waveform_pass(&report.data),
            "Status::Ok with ac1 below floor must fail the host-side numeric criterion"
        );
    }

    // ── per-test timeout map tests ────────────────────────────────────────────

    /// WifiAssociate gets the 25 s extended timeout (≥20 s, AC-B2.5).
    #[test]
    fn test_timeout_wifi_associate_is_25s() {
        assert_eq!(
            test_timeout(&TestName::WifiAssociate),
            Duration::from_secs(25),
            "WifiAssociate must use a 25 s timeout"
        );
    }

    /// Reachability tests get the 15 s timeout. `TlsInboundFrames` is not among them:
    /// it carries the drain-loop budget plus a TLS handshake and is pinned separately
    /// by `test_timeout_remaining_network_budgets_pinned`.
    #[test]
    fn test_timeout_reachability_tests_are_15s() {
        for t in [TestName::UdpRoundtrip, TestName::TlsReachability] {
            assert_eq!(
                test_timeout(&t),
                Duration::from_secs(15),
                "{t:?} must use a 15 s timeout"
            );
        }
    }

    /// WifiReassociation gets the 120 s extended timeout (device poll 90 s + margin).
    #[test]
    fn test_timeout_wifi_reassociation_is_120s() {
        assert_eq!(
            test_timeout(&TestName::WifiReassociation),
            Duration::from_secs(120),
            "WifiReassociation must use a 120 s timeout"
        );
    }

    /// CapturePeriodicLine gets a 15 s timeout (≈2.5 s feed + device pre-work + margin).
    #[test]
    fn test_timeout_capture_periodic_line_is_15s() {
        assert_eq!(
            test_timeout(&TestName::CapturePeriodicLine),
            Duration::from_secs(15),
            "CapturePeriodicLine must use a 15 s timeout"
        );
    }

    /// PlaybackDrainRate gets a 20 s timeout (≈5 s feed + device pre-work + margin). The budget
    /// must stay above `PLAYBACK_DRAIN_RATE_FEED_MS` or the host times out before the typed
    /// TestReport arrives and logs "device not responding" instead of a drain measurement (test-1).
    #[test]
    fn test_timeout_playback_drain_rate_is_20s() {
        assert_eq!(
            test_timeout(&TestName::PlaybackDrainRate),
            Duration::from_secs(20),
            "PlaybackDrainRate must use a 20 s timeout (5 s feed + device pre-work + margin)"
        );
    }

    /// FullDuplexRxIntegrity runs the same ~5 s saturating feed as PlaybackDrainRate, so it gets
    /// the same 20 s budget (no pretest settle — it runs right after PlaybackDrainRate warm).
    #[test]
    fn test_timeout_full_duplex_rx_integrity_is_20s() {
        assert_eq!(
            test_timeout(&TestName::FullDuplexRxIntegrity),
            Duration::from_secs(20),
            "FullDuplexRxIntegrity must use a 20 s timeout (5 s feed + device pre-work + margin)"
        );
    }

    /// The remaining non-default Network timeouts are pinned to their exact values.
    ///
    /// `test_meta_phase_matches_expected_partition` checks only `phase` for Network tests,
    /// and the per-test tests above cover the reachability/wifi budgets — but these
    /// carry non-default budgets with no other value assertion. Without this an accidental
    /// edit (e.g. StreamRealtimeDuplex 40 s → 4 s) would compile and pass every test,
    /// silently reintroducing the timeout-too-short failure mode the arm comments warn about.
    #[test]
    fn test_timeout_remaining_network_budgets_pinned() {
        for (t, secs) in [
            (TestName::TlsInboundFrames, 20),
            (TestName::TlsSendBackpressure, 35),
            (TestName::TlsInboundBackpressure, 35),
            (TestName::GatewayProbeGate, 130),
            (TestName::PollReadinessBidir, 20),
            (TestName::StreamRealtimeDuplex, 45),
            (TestName::TlsPskHandshake, 25),
            (TestName::TlsPskWrongKeyRejected, 30),
        ] {
            assert_eq!(
                test_timeout(&t),
                Duration::from_secs(secs),
                "{t:?} must use a {secs} s timeout"
            );
        }
    }

    /// Every `Generic`-phase test uses the default timeout.
    ///
    /// This encodes the real invariant: the generic check-4 loop dispatches with plain
    /// `send_command`, which ignores per-test timeouts, so a custom timeout on a
    /// `Generic` test would be dead configuration. Iterating `REGISTERED_TESTS` means a
    /// future Generic variant is covered automatically. WifiScan and WifiPowerSaveCheck
    /// are `Network`-phase (so the Generic iteration skips them) yet use the default
    /// timeout — the two quirks — so each is pinned explicitly here to keep an assertion
    /// on it.
    #[test]
    fn generic_phase_tests_use_default_timeout() {
        use device_protocol::REGISTERED_TESTS;
        for t in REGISTERED_TESTS {
            if test_meta(t).phase == TestPhase::Generic {
                assert_eq!(
                    test_meta(t).timeout,
                    RESPONSE_TIMEOUT,
                    "{t:?} is Generic-phase; its timeout is ignored by the loop and must \
                     stay RESPONSE_TIMEOUT to avoid dead configuration"
                );
            }
        }
        assert_eq!(
            test_timeout(&TestName::WifiScan),
            RESPONSE_TIMEOUT,
            "WifiScan is Network-phase but uses the default timeout"
        );
        assert_eq!(
            test_timeout(&TestName::WifiPowerSaveCheck),
            RESPONSE_TIMEOUT,
            "WifiPowerSaveCheck is Network-phase but uses the default timeout"
        );
    }

    // ── eval_no_credentials_park tests ─────────────────────────────────────────

    fn park_resp(status: Status, detail: &str) -> Response {
        let mut d = heapless::String::<192>::new();
        d.push_str(detail).unwrap();
        Response {
            id: 1,
            status,
            payload: Payload::TestReport(TestReport {
                detail: d,
                data: TestData::None,
            }),
        }
    }

    fn park_logs() -> Vec<String> {
        vec![
            "wifi: disconnected reason=8".to_string(),
            format!("{WIFI_PARKED_NO_CREDS} (provision to connect)"),
        ]
    }

    /// Fail + no-credentials detail + park line + no spam is the passing shape.
    #[test]
    fn eval_no_credentials_park_passes() {
        let resp = park_resp(Status::Fail, "no NVS credentials — provision first");
        assert!(eval_no_credentials_park(&resp, &park_logs()).is_ok());
    }

    /// An Ok status means the credentials survived the clear.
    #[test]
    fn eval_no_credentials_park_ok_status_fails() {
        let resp = park_resp(Status::Ok, "no NVS credentials — provision first");
        let err = eval_no_credentials_park(&resp, &park_logs()).unwrap_err();
        assert!(
            err.contains("credentials appear to survive"),
            "error must name the surviving-credentials problem: {err}"
        );
    }

    /// A non-TestReport payload cannot prove anything about the detail.
    #[test]
    fn eval_no_credentials_park_wrong_payload_fails() {
        let resp = Response {
            id: 1,
            status: Status::Fail,
            payload: Payload::Empty,
        };
        let err = eval_no_credentials_park(&resp, &park_logs()).unwrap_err();
        assert!(
            err.contains("expected a TestReport detail"),
            "error must name the payload problem: {err}"
        );
    }

    /// A generic failure detail is not proof the keys are gone.
    #[test]
    fn eval_no_credentials_park_wrong_detail_fails() {
        let resp = park_resp(Status::Fail, "association timed out");
        let err = eval_no_credentials_park(&resp, &park_logs()).unwrap_err();
        assert!(
            err.contains(NO_NVS_CREDENTIALS),
            "error must name the expected detail: {err}"
        );
    }

    /// The supervisor must actually announce the park.
    #[test]
    fn eval_no_credentials_park_missing_park_line_fails() {
        let resp = park_resp(Status::Fail, "no NVS credentials — provision first");
        let logs = vec!["wifi: disconnected reason=8".to_string()];
        let err = eval_no_credentials_park(&resp, &logs).unwrap_err();
        assert!(
            err.contains("did not announce the park"),
            "error must name the missing park line: {err}"
        );
    }

    /// A retry-attempt line means the park arm charged backoff — a regression.
    #[test]
    fn eval_no_credentials_park_retry_attempt_spam_fails() {
        let resp = park_resp(Status::Fail, "no NVS credentials — provision first");
        let mut logs = park_logs();
        logs.push(format!("{WIFI_REASSOC_ATTEMPT_FAILED} (3): boom"));
        let err = eval_no_credentials_park(&resp, &logs).unwrap_err();
        assert!(
            err.contains("retry spam"),
            "error must name the retry spam: {err}"
        );
    }

    /// The slow-lane line is equally disqualifying.
    #[test]
    fn eval_no_credentials_park_slow_lane_spam_fails() {
        let resp = park_resp(Status::Fail, "no NVS credentials — provision first");
        let mut logs = park_logs();
        logs.push(format!(
            "{WIFI_CONSECUTIVE_FAILURES} (n=9) — check credentials/AP"
        ));
        let err = eval_no_credentials_park(&resp, &logs).unwrap_err();
        assert!(
            err.contains("retry spam"),
            "error must name the retry spam: {err}"
        );
    }

    /// A retry line *before* the park announcement is an in-flight attempt from before the
    /// clear landed, not park-arm backoff — it must not fail the step.
    #[test]
    fn eval_no_credentials_park_pre_park_retry_line_passes() {
        let resp = park_resp(Status::Fail, "no NVS credentials — provision first");
        let mut logs = vec![format!(
            "{WIFI_REASSOC_ATTEMPT_FAILED} (2): pre-clear attempt"
        )];
        logs.extend(park_logs());
        assert!(
            eval_no_credentials_park(&resp, &logs).is_ok(),
            "retry lines preceding the park line must not fail the step"
        );
    }

    // ── parse_attempt_counter / BootAssociationRetry eval tests ──────────────────

    #[test]
    fn parse_attempt_counter_parses_the_parenthesized_number() {
        let line = format!("{WIFI_REASSOC_ATTEMPT_FAILED} (3): timed out");
        assert_eq!(
            parse_attempt_counter(&line, WIFI_REASSOC_ATTEMPT_FAILED),
            Some(3)
        );
    }

    #[test]
    fn parse_attempt_counter_none_when_token_absent() {
        assert_eq!(
            parse_attempt_counter("wifi: connected", WIFI_REASSOC_ATTEMPT_FAILED),
            None
        );
    }

    #[test]
    fn parse_attempt_counter_none_when_unparseable() {
        let line = format!("{WIFI_REASSOC_ATTEMPT_FAILED} (oops): timed out");
        assert_eq!(
            parse_attempt_counter(&line, WIFI_REASSOC_ATTEMPT_FAILED),
            None
        );
    }

    fn failure_line(instant: Instant, attempt: u32) -> (Instant, String) {
        (
            instant,
            format!("{WIFI_REASSOC_ATTEMPT_FAILED} ({attempt}): connect timed out"),
        )
    }

    fn start_line(instant: Instant, attempt: u32) -> (Instant, String) {
        (instant, format!("{WIFI_REASSOC_ATTEMPT_START} ({attempt})"))
    }

    /// Two strictly-increasing failures, two strictly-increasing starts spaced >= 20s
    /// apart, no reboot line — the passing shape.
    #[test]
    fn eval_boot_association_retry_failures_passes() {
        let t0 = Instant::now();
        let logs = vec![
            start_line(t0, 1),
            failure_line(t0 + Duration::from_secs(2), 1),
            start_line(t0 + Duration::from_secs(30), 2),
            failure_line(t0 + Duration::from_secs(32), 2),
        ];
        assert!(eval_boot_association_retry_failures(&logs).is_ok());
    }

    /// Fewer than two parseable failures means retry-with-backoff was not observed.
    #[test]
    fn eval_boot_association_retry_failures_too_few_fails() {
        let t0 = Instant::now();
        let logs = vec![start_line(t0, 1), failure_line(t0, 1)];
        let err = eval_boot_association_retry_failures(&logs).unwrap_err();
        assert!(
            err.contains("expected >= 2"),
            "error must name the too-few-failures problem: {err}"
        );
    }

    /// A non-increasing failed-attempt counter (reboot resets `attempt_counter`) must
    /// fail.
    #[test]
    fn eval_boot_association_retry_failures_non_increasing_counter_fails() {
        let t0 = Instant::now();
        let logs = vec![
            start_line(t0, 3),
            failure_line(t0, 3),
            start_line(t0 + Duration::from_secs(30), 1),
            failure_line(t0 + Duration::from_secs(30), 1),
        ];
        let err = eval_boot_association_retry_failures(&logs).unwrap_err();
        assert!(
            err.contains("not strictly increasing"),
            "error must name the counter regression: {err}"
        );
    }

    /// A non-increasing attempt-start counter must also fail, even if the failed-line
    /// counters look fine.
    #[test]
    fn eval_boot_association_retry_failures_non_increasing_start_counter_fails() {
        let t0 = Instant::now();
        let logs = vec![
            start_line(t0, 3),
            failure_line(t0, 1),
            start_line(t0 + Duration::from_secs(30), 1),
            failure_line(t0 + Duration::from_secs(30), 2),
        ];
        let err = eval_boot_association_retry_failures(&logs).unwrap_err();
        assert!(
            err.contains("attempt-start counters not strictly increasing"),
            "error must name the start-counter regression: {err}"
        );
    }

    /// Consecutive attempt-start lines spaced < 20s apart means backoff was not honored,
    /// even though the (attempt-end) failure lines here are spaced exactly 30s apart —
    /// the spacing check must key off starts, not ends.
    #[test]
    fn eval_boot_association_retry_failures_too_close_together_fails() {
        let t0 = Instant::now();
        let logs = vec![
            start_line(t0, 1),
            failure_line(t0, 1),
            start_line(t0 + Duration::from_secs(5), 2),
            failure_line(t0 + Duration::from_secs(30), 2),
        ];
        let err = eval_boot_association_retry_failures(&logs).unwrap_err();
        assert!(
            err.contains("backoff not honored"),
            "error must name the spacing problem: {err}"
        );
    }

    /// A supervisor-start line during the window means an unexpected reboot occurred.
    #[test]
    fn eval_boot_association_retry_failures_reboot_line_fails() {
        let t0 = Instant::now();
        let mut logs = vec![
            start_line(t0, 1),
            failure_line(t0, 1),
            start_line(t0 + Duration::from_secs(30), 2),
            failure_line(t0 + Duration::from_secs(30), 2),
        ];
        logs.push((t0, WIFI_SUPERVISOR_STARTED.to_string()));
        let err = eval_boot_association_retry_failures(&logs).unwrap_err();
        assert!(
            err.contains("unexpected reboot"),
            "error must name the reboot problem: {err}"
        );
    }

    /// A `WIFI_REASSOCIATED` line with no reboot line is the passing recovery shape.
    #[test]
    fn eval_boot_association_retry_recovery_passes() {
        let logs = vec![(Instant::now(), WIFI_REASSOCIATED.to_string())];
        assert!(eval_boot_association_retry_recovery(&logs).is_ok());
    }

    /// No `WIFI_REASSOCIATED` line means the supervisor did not recover autonomously.
    #[test]
    fn eval_boot_association_retry_recovery_missing_reassociated_fails() {
        let logs = vec![(Instant::now(), "wifi: connected".to_string())];
        let err = eval_boot_association_retry_recovery(&logs).unwrap_err();
        assert!(
            err.contains("did not recover autonomously"),
            "error must name the missing recovery line: {err}"
        );
    }

    /// A reboot line during the recovery window must fail even if recovery also occurred.
    #[test]
    fn eval_boot_association_retry_recovery_reboot_line_fails() {
        let logs = vec![
            (Instant::now(), WIFI_SUPERVISOR_STARTED.to_string()),
            (Instant::now(), WIFI_REASSOCIATED.to_string()),
        ];
        let err = eval_boot_association_retry_recovery(&logs).unwrap_err();
        assert!(
            err.contains("unexpected reboot"),
            "error must name the reboot problem: {err}"
        );
    }

    // ── eval_wifi_info tests ───────────────────────────────────────────────────

    /// Valid WifiAssociate verdict with reasonable IP/gateway/RSSI passes.
    #[test]
    fn eval_wifi_info_valid_passes() {
        let data = TestData::WifiAssociate {
            ip: [192, 168, 1, 50],
            gateway: [192, 168, 1, 1],
            rssi: -55,
        };
        assert!(
            eval_wifi_info(&data).is_ok(),
            "valid WifiAssociate verdict must pass"
        );
    }

    /// Zero IP fails (AC-B2.1).
    #[test]
    fn eval_wifi_info_zero_ip_fails() {
        let data = TestData::WifiAssociate {
            ip: [0, 0, 0, 0],
            gateway: [192, 168, 1, 1],
            rssi: -55,
        };
        let result = eval_wifi_info(&data);
        assert!(result.is_err(), "zero IP must fail AC-B2.1");
        assert!(
            result.unwrap_err().contains("zero IP"),
            "error must describe the zero-IP problem"
        );
    }

    /// Loopback IP (127.x.x.x) fails (AC-B2.1).
    #[test]
    fn eval_wifi_info_loopback_ip_fails() {
        let data = TestData::WifiAssociate {
            ip: [127, 0, 0, 1],
            gateway: [192, 168, 1, 1],
            rssi: -55,
        };
        let result = eval_wifi_info(&data);
        assert!(result.is_err(), "loopback IP must fail AC-B2.1");
        assert!(
            result.unwrap_err().contains("loopback"),
            "error must describe the loopback problem"
        );
    }

    /// Zero gateway fails (AC-B2.2).
    #[test]
    fn eval_wifi_info_zero_gateway_fails() {
        let data = TestData::WifiAssociate {
            ip: [192, 168, 1, 50],
            gateway: [0, 0, 0, 0],
            rssi: -55,
        };
        let result = eval_wifi_info(&data);
        assert!(result.is_err(), "zero gateway must fail AC-B2.2");
        assert!(
            result.unwrap_err().contains("zero gateway"),
            "error must describe the zero-gateway problem"
        );
    }

    /// RSSI == 0 fails (AC-B2.3 bogus value).
    #[test]
    fn eval_wifi_info_rssi_zero_fails() {
        let data = TestData::WifiAssociate {
            ip: [192, 168, 1, 50],
            gateway: [192, 168, 1, 1],
            rssi: 0,
        };
        let result = eval_wifi_info(&data);
        assert!(result.is_err(), "RSSI=0 must fail AC-B2.3");
        assert!(
            result.unwrap_err().contains("RSSI=0"),
            "error must mention RSSI=0"
        );
    }

    /// RSSI <= -80 dBm fails (AC-B2.3 floor).
    #[test]
    fn eval_wifi_info_rssi_at_floor_fails() {
        let data = TestData::WifiAssociate {
            ip: [192, 168, 1, 50],
            gateway: [192, 168, 1, 1],
            rssi: -80,
        };
        let result = eval_wifi_info(&data);
        assert!(result.is_err(), "RSSI at -80 dBm must fail AC-B2.3");
        assert!(
            result.unwrap_err().contains("-80"),
            "error must mention the -80 dBm floor"
        );
    }

    /// RSSI == -79 dBm passes (just above floor).
    #[test]
    fn eval_wifi_info_rssi_just_above_floor_passes() {
        let data = TestData::WifiAssociate {
            ip: [192, 168, 1, 50],
            gateway: [192, 168, 1, 1],
            rssi: -79,
        };
        assert!(
            eval_wifi_info(&data).is_ok(),
            "RSSI at -79 dBm (just above floor) must pass"
        );
    }

    /// Another test's verdict data is rejected. Succeeds the old non-`WifiInfo`
    /// payload case: the wrong-payload class is now a wrong-variant class.
    #[test]
    fn eval_wifi_info_wrong_variant_rejected() {
        let result = eval_wifi_info(&TestData::None);
        assert!(
            result.is_err(),
            "TestData::None must be rejected by eval_wifi_info"
        );
        assert!(
            result.unwrap_err().contains("WifiAssociate"),
            "error must mention the expected WifiAssociate variant"
        );
    }

    // ── eval_udp_roundtrip tests ───────────────────────────────────────────────

    /// A `UdpEcho` verdict passes — reaching that variant is the echo-match assertion.
    #[test]
    fn eval_udp_roundtrip_echo_verdict_passes() {
        let data = TestData::UdpEcho {
            bytes: 16,
            peer_ip: [192, 168, 1, 5],
            peer_port: 12345,
        };
        assert!(
            eval_udp_roundtrip(&data).is_ok(),
            "a UdpEcho verdict must pass"
        );
    }

    /// Any other variant fails. Succeeds the string-era "missing 'echo match' token" case:
    /// every non-echo device outcome now carries `TestData::None`, not a differently-worded
    /// message.
    #[test]
    fn eval_udp_roundtrip_rejects_non_echo_data() {
        for data in [
            TestData::None,
            TestData::TcpEcho {
                bytes: 16,
                peer_ip: [192, 168, 1, 5],
                peer_port: 12345,
            },
        ] {
            let result = eval_udp_roundtrip(&data);
            assert!(result.is_err(), "non-UdpEcho data must fail: {data:?}");
            assert!(
                result.unwrap_err().contains("UdpEcho"),
                "error must name the expected verdict"
            );
        }
    }

    // ── eval_tls_reachability tests ────────────────────────────────────────────

    /// A `TlsHandshake` verdict passes.
    #[test]
    fn eval_tls_reachability_handshake_verdict_passes() {
        let data = TestData::TlsHandshake {
            peer_ip: [1, 2, 3, 4],
            peer_port: 443,
        };
        assert!(
            eval_tls_reachability(&data).is_ok(),
            "a TlsHandshake verdict must pass"
        );
    }

    /// Any other variant fails. Succeeds the "missing 'handshake ok' token" case: a failed
    /// handshake now carries `TestData::None`.
    #[test]
    fn eval_tls_reachability_rejects_non_handshake_data() {
        let result = eval_tls_reachability(&TestData::None);
        assert!(result.is_err(), "a failed handshake must fail");
        assert!(
            result.unwrap_err().contains("TlsHandshake"),
            "error must name the expected verdict"
        );
    }

    // ── eval_tls_inbound_frames tests ────────────────────────────────────────

    /// Build an inbound-frames verdict with the given frame count.
    fn inbound_verdict(inbound_frames: u32) -> TestData {
        TestData::TlsInboundFrames {
            inbound_frames,
            peer_ip: [192, 168, 1, 5],
            peer_port: 17382,
        }
    }

    /// The exact expected frame count passes.
    #[test]
    fn eval_tls_inbound_frames_exact_count_passes() {
        assert!(
            eval_tls_inbound_frames(&inbound_verdict(INBOUND_FRAMES_COUNT)).is_ok(),
            "the exact count must pass"
        );
    }

    /// Fewer frames than expected fails (partial delivery).
    #[test]
    fn eval_tls_inbound_frames_partial_count_fails() {
        let result = eval_tls_inbound_frames(&inbound_verdict(INBOUND_FRAMES_COUNT - 1));
        assert!(result.is_err(), "partial delivery must fail");
        assert!(
            result.unwrap_err().contains("expected"),
            "error must mention the expected count"
        );
    }

    /// `inbound_frames=0` fails (no frames received).
    #[test]
    fn eval_tls_inbound_frames_zero_count_fails() {
        assert!(
            eval_tls_inbound_frames(&inbound_verdict(0)).is_err(),
            "zero frames must fail"
        );
    }

    /// Fail-path data is rejected. Succeeds three string-era cases at once — the "FAIL
    /// prefix", the idle-fail-fast stall payload, and the "missing inbound_frames= token"
    /// message — all of which are now the single `TestData::None` fail shape.
    #[test]
    fn eval_tls_inbound_frames_rejects_fail_data() {
        let result = eval_tls_inbound_frames(&TestData::None);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("TlsInboundFrames verdict"),
            "error must name the expected verdict"
        );
    }

    /// Another test's verdict cannot be accepted as this one's.
    #[test]
    fn eval_tls_inbound_frames_rejects_wrong_variant() {
        let wrong = TestData::TcpEcho {
            bytes: 16,
            peer_ip: [192, 168, 1, 5],
            peer_port: 17382,
        };
        assert!(
            eval_tls_inbound_frames(&wrong).is_err(),
            "a wrong variant must fail"
        );
    }

    // ── eval_capture_periodic_line tests (audio-pipeline-observability §5) ────

    /// Build `n` distinct capture-summary log lines carrying the pinned token. The layout
    /// matches the §2.1-split line-1 (`capture: playback tx …`) production format: the
    /// cross-check tokens only (`rx_frames`/`rx_window_us` first, then `chunks`,
    /// `write_us(mean/max)`, `max_backlog`, and the saturation pair). The observability
    /// counters (`writefail`/`preroll_waits`/`resume_unmutes`/`eoa_mutes`) moved to the
    /// separate `capture: playback obs …` line and are NOT on line 1 (quality-2).
    /// `eval_capture_periodic_line` only counts lines containing the token, so the exact token
    /// set is documentation, not behavior — but it must mirror the real wire format.
    fn capture_lines(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| {
                format!(
                    "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
                     {RX_WINDOW_US}1000000 {CHUNKS}{} write_us(mean/max)=20000/21000 max_backlog=1 \
                     {NONEMPTY_POLLS}{} {POLL_EMPTY}0",
                    i, i,
                )
            })
            .collect()
    }

    /// The device's own verdict data for a healthy capture-feed run.
    fn capture_verdict() -> TestData {
        TestData::CapturePeriodicLine { chunks_fed: 125 }
    }

    /// The right variant plus ≥2 periodic lines passes (the cadence).
    #[test]
    fn eval_capture_periodic_line_pass_with_cadence() {
        let logs = capture_lines(CAPTURE_PERIODIC_LINE_MIN_COUNT);
        assert!(
            eval_capture_periodic_line(&capture_verdict(), &logs).is_ok(),
            "the CapturePeriodicLine variant with ≥2 periodic lines must pass"
        );
    }

    /// Exactly one periodic line fails: a single line is presence, not cadence.
    #[test]
    fn eval_capture_periodic_line_single_line_fails_cadence() {
        let logs = capture_lines(1);
        let result = eval_capture_periodic_line(&capture_verdict(), &logs);
        assert!(
            result.is_err(),
            "a single periodic line must fail the cadence check"
        );
        assert!(
            result.unwrap_err().contains("cadence"),
            "error must explain the cadence shortfall"
        );
    }

    /// Zero periodic lines (the regression this guard exists to catch) fails.
    #[test]
    fn eval_capture_periodic_line_no_lines_fails() {
        // Unrelated logs only — none carry the capture-summary token.
        let logs = vec![
            "[device Info] respeaker_pod: streamer: connected".to_string(),
            "[device Warn] respeaker_pod: something else".to_string(),
        ];
        let result = eval_capture_periodic_line(&capture_verdict(), &logs);
        assert!(
            result.is_err(),
            "absence of the periodic line is the core regression and must fail"
        );
    }

    /// A device-side failure carries `TestData::None`, which the eval rejects even when the
    /// periodic lines are present. Succeeds the string-era `FAIL`-prefix case: the prefix
    /// convention is gone, and a fail status now shows up structurally.
    #[test]
    fn eval_capture_periodic_line_rejects_fail_data() {
        let logs = capture_lines(CAPTURE_PERIODIC_LINE_MIN_COUNT);
        let result = eval_capture_periodic_line(&TestData::None, &logs);
        assert!(result.is_err(), "TestData::None must be rejected");
        assert!(
            result.unwrap_err().contains("CapturePeriodicLine"),
            "error must name the expected variant"
        );
    }

    /// `chunks_fed` is observability only — this test's assertion is the log-line cadence.
    /// Records the `_` binding as a decision: a zero-chunk report still passes when the
    /// cadence is present.
    #[test]
    fn eval_capture_periodic_line_ignores_chunks_fed() {
        let logs = capture_lines(CAPTURE_PERIODIC_LINE_MIN_COUNT);
        assert!(
            eval_capture_periodic_line(&TestData::CapturePeriodicLine { chunks_fed: 0 }, &logs)
                .is_ok(),
            "chunks_fed is not graded; the log cadence is the criterion"
        );
    }

    /// Another test's data is rejected. Succeeds the string-era "missing `src=capture`" and
    /// "missing `chunks_fed=`" token cases: neither token exists any more, and the only way to
    /// present the wrong shape is to present a different variant — a compile error on the
    /// device side, and a loud runtime failure if it ever reached the host.
    #[test]
    fn eval_capture_periodic_line_rejects_wrong_variant() {
        let logs = capture_lines(CAPTURE_PERIODIC_LINE_MIN_COUNT);
        let result = eval_capture_periodic_line(
            &TestData::FullDuplexRxIntegrity {
                chunks_fed: 125,
                feed_full: 3,
                feed_ms: 5000,
            },
            &logs,
        );
        assert!(result.is_err(), "another test's variant must be rejected");
        assert!(
            result.unwrap_err().contains("CapturePeriodicLine"),
            "error must name the expected variant"
        );
    }

    // ── eval_tls_send_backpressure tests (single-verdict A) ────────
    //
    // Only the A saturate-then-drain profile remains. The eval rejects every cross-wired
    // / out-of-bound A verdict. These helpers build a fully-valid PASS line and let each
    // test corrupt exactly one field, isolating the gate under test.

    /// A fully-valid single-verdict PASS line: the A sub-case at a passing value.
    fn bp_pass_line() -> TestData {
        bp_verdict(true, 2, true)
    }

    /// Build a single-verdict (A) backpressure verdict from its parts.
    fn bp_verdict(a_resumed: bool, a_rc: u32, a_ru: bool) -> TestData {
        TestData::TlsSendBackpressure {
            a_resumed,
            a_rc,
            a_ru,
        }
    }

    /// A fully-drained, keep-up Scenario A observation (the pass baseline).
    fn rtd_pass_obs() -> RtdObservation {
        RtdObservation {
            connected: true,
            segment_start_seen: true,
            declared_preroll: 16_000,
            // Paced pre-roll drain (50 frames × 5 ms = 250 ms): a realistic passing burst,
            // under the 350 ms ceiling. The ceiling boundary itself is exercised by the
            // burst_drain_ms = MAX+1 rejection tests below.
            burst_drain_ms: Some(250),
            total_samples: rtd_expected_samples(),
            audio_frames: 300,
            end_reason: Some("VadRelease".to_string()),
            catch_up_ms: Some(5_050),
            playback_frames_sent: 0,
            error: None,
        }
    }

    /// A keep-up Scenario B observation: the same outbound shape plus the host-paced
    /// playback-frame count.
    fn rtd_pass_obs_b() -> RtdObservation {
        RtdObservation {
            playback_frames_sent: RTD_PLAYBACK_FRAMES as u32,
            ..rtd_pass_obs()
        }
    }

    /// A keep-up Scenario B device verdict: zero underruns, exact consumed count.
    fn rtd_verdict_b() -> TestData {
        rtd_verdict(0, 0, RTD_PLAYBACK_FRAMES)
    }

    /// An RTD verdict with explicit fake-DAC accounting.
    fn rtd_verdict(underruns: u64, gap_ms: u64, consumed: u64) -> TestData {
        TestData::Rtd {
            underruns,
            gap_ms,
            consumed,
        }
    }

    #[test]
    fn eval_rtd_b_pass_baseline_passes() {
        assert!(
            eval_stream_realtime_duplex_b(&rtd_verdict_b(), &rtd_pass_obs_b()).is_ok(),
            "zero-underrun duplex observation with matched consumed count must pass"
        );
    }

    #[test]
    fn eval_rtd_b_underrun_fails() {
        let data = rtd_verdict(7, 140, RTD_PLAYBACK_FRAMES);
        assert!(
            eval_stream_realtime_duplex_b(&data, &rtd_pass_obs_b()).is_err(),
            "any fake-DAC underrun must fail (the field-symptom assertion)"
        );
    }

    #[test]
    fn eval_rtd_b_consumed_mismatch_fails() {
        let data = rtd_verdict(0, 0, RTD_PLAYBACK_FRAMES - 3);
        assert!(
            eval_stream_realtime_duplex_b(&data, &rtd_pass_obs_b()).is_err(),
            "a consumed count below the host's sent count must fail integrity"
        );
    }

    /// Succeeds `eval_rtd_b_missing_field_fails`: a report with no fake-DAC accounting is
    /// now structurally a non-`Rtd` variant (fail paths carry `TestData::None`) rather than
    /// a message with absent tokens.
    #[test]
    fn eval_rtd_b_fail_data_rejected() {
        assert!(
            eval_stream_realtime_duplex_b(&TestData::None, &rtd_pass_obs_b()).is_err(),
            "a fail-path report must fail Scenario B"
        );
    }

    #[test]
    fn eval_rtd_b_outbound_failure_propagates() {
        let mut o = rtd_pass_obs_b();
        o.burst_drain_ms = Some(RTD_BURST_DRAIN_MAX_MS + 1);
        assert!(
            eval_stream_realtime_duplex_b(&rtd_verdict_b(), &o).is_err(),
            "an outbound burst-drain failure must still fail under duplex load"
        );
    }

    #[test]
    fn eval_rtd_pass_baseline_passes() {
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &rtd_pass_obs()).is_ok(),
            "fully-drained keep-up observation must pass"
        );
    }

    #[test]
    fn eval_rtd_slow_burst_fails() {
        let mut o = rtd_pass_obs();
        o.burst_drain_ms = Some(RTD_BURST_DRAIN_MAX_MS + 1);
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &o).is_err(),
            "burst drain past the ceiling must fail (the structural failure)"
        );
    }

    #[test]
    fn eval_rtd_overrun_reason_fails() {
        let mut o = rtd_pass_obs();
        o.end_reason = Some("Overrun".to_string());
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &o).is_err(),
            "Overrun end reason must fail"
        );
    }

    #[test]
    fn eval_rtd_sample_count_mismatch_fails() {
        let mut o = rtd_pass_obs();
        o.total_samples -= 320;
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &o).is_err(),
            "a short sample count must fail the integrity check"
        );
    }

    #[test]
    fn eval_rtd_slow_catch_up_fails() {
        let mut o = rtd_pass_obs();
        o.catch_up_ms = Some(RTD_CATCH_UP_MAX_MS + 1);
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &o).is_err(),
            "catch-up wall past the ceiling must fail"
        );
    }

    /// The paced catch-up drain must fit under both host-side keep-up ceilings.
    ///
    /// The device rate-limits the outbound audio drain to `CATCH_UP_PACE_MULTIPLIER`×
    /// real time (pace arithmetic in `audio_pipeline::pace`). This guards the derived
    /// bounds against a future change to the multiplier, the frame geometry, or a
    /// ceiling: if any drift makes the paced drain exceed a ceiling, this fails in the
    /// host test lane instead of on hardware.
    #[test]
    fn paced_catch_up_fits_under_host_ceilings() {
        use audio_pipeline::pace::{AUDIO_FRAME_PERIOD_US, paced_drain_us};
        use audio_pipeline::ring::PREROLL_SAMPLES;
        use audio_pipeline::wire::AUDIO_SAMPLES_PER_FRAME;

        // Pre-roll (50 frames, 1 s of buffered history) is the backlog the pace gate throttles.
        let preroll_frames = PREROLL_SAMPLES / AUDIO_SAMPLES_PER_FRAME as u64;
        let paced_preroll_ms = paced_drain_us(preroll_frames) / 1_000;

        // Burst ceiling: the paced pre-roll drain (50 frames × 5 ms = 250 ms) must
        // reach the host within RTD_BURST_DRAIN_MAX_MS, under the 350 ms ceiling.
        assert!(
            paced_preroll_ms <= RTD_BURST_DRAIN_MAX_MS,
            "paced pre-roll drain {paced_preroll_ms} ms exceeds burst ceiling {RTD_BURST_DRAIN_MAX_MS} ms"
        );

        // Catch-up ceiling: SegmentStart→SegmentEnd wall clock. The 250 post-pre-roll
        // frames are produced at 1× real time, and the paced cadence (4×) never
        // throttles below production, so the segment is production-bound. Upper bound:
        // the real-time production window plus a fully-serialized paced pre-roll drain
        // (they overlap in practice, so this over-estimates).
        let production_window_ms = RTD_PRODUCER_FRAMES * AUDIO_FRAME_PERIOD_US / 1_000;
        let catch_up_upper_ms = production_window_ms + paced_preroll_ms;
        assert!(
            catch_up_upper_ms <= RTD_CATCH_UP_MAX_MS,
            "paced catch-up upper bound {catch_up_upper_ms} ms exceeds catch-up ceiling {RTD_CATCH_UP_MAX_MS} ms"
        );
    }

    #[test]
    fn eval_rtd_never_connected_fails() {
        let o = RtdObservation::default();
        assert!(
            eval_stream_realtime_duplex(&rtd_verdict_b(), &o).is_err(),
            "a missing device connection must fail"
        );
    }

    /// Succeeds `eval_rtd_wrong_device_message_fails`: another test's data can no longer be
    /// mistaken for an rtd verdict — it is a different `TestData` variant.
    #[test]
    fn eval_rtd_wrong_variant_fails() {
        assert!(
            eval_stream_realtime_duplex(
                &TestData::CapturePeriodicLine { chunks_fed: 3 },
                &rtd_pass_obs()
            )
            .is_err(),
            "another test's data must fail"
        );
    }

    /// The canonical all-pass A verdict passes.
    #[test]
    fn eval_tls_send_backpressure_a_verdict_passes() {
        assert!(
            eval_tls_send_backpressure(&bp_pass_line()).is_ok(),
            "A resumed (≥1 cycle, reusable) must pass"
        );
    }

    // ── Sub-case A (saturate-then-drain → resume) ────────────────────────────

    /// `a_resumed=false` fails: the blocked boundary frame never resumed to `Sent`.
    /// Succeeds the string-era
    /// `ceiling_dead`/`aligned` mislabel cases — both non-resumed outcomes now reach the
    /// host as this one field being false (or, from the real device, as `TestData::None`).
    #[test]
    fn eval_tls_send_backpressure_a_not_resumed_fails() {
        let result = eval_tls_send_backpressure(&bp_verdict(false, 2, true));
        assert!(result.is_err(), "a non-resumed outcome must fail");
        assert!(
            result.unwrap_err().contains("a_resumed must be true"),
            "error must call out the A resume regression"
        );
    }

    /// A resume_cycles at the floor (== BACKPRESSURE_A_MIN_RESUME_CYCLES) passes.
    #[test]
    fn eval_tls_send_backpressure_a_cycles_at_floor_passes() {
        assert!(
            eval_tls_send_backpressure(&bp_verdict(true, BACKPRESSURE_A_MIN_RESUME_CYCLES, true))
                .is_ok(),
            "a_rc exactly at the ≥1 floor must pass"
        );
    }

    /// A resume_cycles one below the floor (== 0) fails: every write was accepted
    /// outright, so the resume path was never exercised on real lwIP.
    #[test]
    fn eval_tls_send_backpressure_a_cycles_below_floor_fails() {
        let result = eval_tls_send_backpressure(&bp_verdict(
            true,
            BACKPRESSURE_A_MIN_RESUME_CYCLES - 1,
            true,
        ));
        assert!(result.is_err(), "a_rc below floor must fail");
        assert!(
            result.unwrap_err().contains("a_rc"),
            "error must mention the resume-cycle floor"
        );
    }

    /// A non-reusable connection after a resumed frame fails.
    #[test]
    fn eval_tls_send_backpressure_a_not_reusable_fails() {
        let result = eval_tls_send_backpressure(&bp_verdict(true, 2, false));
        assert!(result.is_err(), "a_ru=false must fail");
        assert!(
            result.unwrap_err().contains("a_ru"),
            "error must mention A reusability"
        );
    }

    // ── Whole-verdict guards ─────────────────────────────────────────────────

    /// Fail-path data is rejected. Succeeds the string-era "FAIL prefix", "missing a_rc
    /// token" and "legacy flat single-verdict line" cases: a device that never reached the
    /// A verdict — for any reason, including a stale build with a different result shape —
    /// cannot produce this variant at all.
    #[test]
    fn eval_tls_send_backpressure_rejects_fail_data() {
        let result = eval_tls_send_backpressure(&TestData::None);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("TlsSendBackpressure verdict"),
            "error must name the expected verdict"
        );
    }

    /// Another test's verdict cannot be accepted as this one's.
    #[test]
    fn eval_tls_send_backpressure_rejects_wrong_variant() {
        let wrong = TestData::PollReadiness {
            pollin: true,
            pollout: true,
            both: true,
            read_bytes: 16,
        };
        assert!(
            eval_tls_send_backpressure(&wrong).is_err(),
            "a wrong variant must fail"
        );
    }

    // ── eval_tls_inbound_backpressure tests ───────────────────────────────────

    /// Build a `TlsInboundBackpressure` verdict from its parts.
    fn inbound_bp_verdict(inbound_frames: u32, sink_full_events: u32) -> TestData {
        TestData::TlsInboundBackpressure {
            inbound_frames,
            sink_full_events,
            peer_ip: [10, 0, 0, 3],
            peer_port: 9003,
        }
    }

    /// The exact-count, nonzero-full verdict passes.
    #[test]
    fn eval_tls_inbound_backpressure_pass_verdict_passes() {
        assert!(
            eval_tls_inbound_backpressure(&inbound_bp_verdict(INBOUND_BP_FLOOD_FRAMES, 12)).is_ok(),
            "exact frame count with sink_full_events > 0 must pass"
        );
    }

    /// A frame-count shortfall (fullness drop or partial delivery) fails.
    #[test]
    fn eval_tls_inbound_backpressure_short_count_fails() {
        let result =
            eval_tls_inbound_backpressure(&inbound_bp_verdict(INBOUND_BP_FLOOD_FRAMES - 1, 12));
        assert!(result.is_err(), "a frame shortfall must fail");
        assert!(
            result.unwrap_err().contains("expected"),
            "error must name the expected count"
        );
    }

    /// A frame-count excess (codec/accounting bug) fails — symmetrical with the shortfall
    /// case, same doctrine as `eval_tls_inbound_frames`.
    #[test]
    fn eval_tls_inbound_backpressure_excess_count_fails() {
        assert!(
            eval_tls_inbound_backpressure(&inbound_bp_verdict(INBOUND_BP_FLOOD_FRAMES + 1, 12))
                .is_err(),
            "a frame excess must fail"
        );
    }

    /// `sink_full_events == 0` fails: the ring never backpressured — either the producer
    /// is unwired (silent drop) or delivery averaged under real time. This is the guard
    /// against the unwired-producer silent-drop mode (inbound-backpressure-hil design §3).
    #[test]
    fn eval_tls_inbound_backpressure_zero_full_events_fails() {
        let result = eval_tls_inbound_backpressure(&inbound_bp_verdict(INBOUND_BP_FLOOD_FRAMES, 0));
        assert!(result.is_err(), "zero full events must fail");
        assert!(
            result.unwrap_err().contains("sink_full_events=0"),
            "error must name the zero full-events cause"
        );
    }

    /// Fail-path data is rejected.
    #[test]
    fn eval_tls_inbound_backpressure_rejects_fail_data() {
        let result = eval_tls_inbound_backpressure(&TestData::None);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result
                .unwrap_err()
                .contains("TlsInboundBackpressure verdict"),
            "error must name the expected verdict"
        );
    }

    /// Another test's verdict cannot be accepted as this one's.
    #[test]
    fn eval_tls_inbound_backpressure_rejects_wrong_variant() {
        let wrong = TestData::TlsInboundFrames {
            inbound_frames: INBOUND_FRAMES_COUNT,
            peer_ip: [10, 0, 0, 2],
            peer_port: 9002,
        };
        assert!(
            eval_tls_inbound_backpressure(&wrong).is_err(),
            "a wrong variant must fail"
        );
    }

    // ── eval_poll_readiness_bidir tests ─────────────────────────────────────────

    // ── TLS-PSK self-test evals and listener fixture ──────────────────────────

    /// Build a `TlsPskHandshake` verdict from its parts.
    fn psk_verdict(version: &str, suite: &str, echo_bytes: u32) -> TestData {
        let mut v = device_protocol::TlsVersionStr::new();
        v.push_str(version).unwrap();
        let mut c = device_protocol::TlsSuiteStr::new();
        c.push_str(suite).unwrap();
        TestData::TlsPskHandshake {
            peer_ip: [10, 0, 0, 4],
            peer_port: 17386,
            handshake_ms: 210,
            version: v,
            ciphersuite: c,
            echo_bytes,
        }
    }

    /// The expected version + suite + a non-empty echo passes.
    #[test]
    fn eval_tls_psk_handshake_expected_negotiation_passes() {
        assert!(
            eval_tls_psk_handshake(&psk_verdict(
                EXPECTED_TLS_VERSION,
                EXPECTED_MBEDTLS_SUITE,
                16
            ))
            .is_ok(),
            "the pinned version and suite with a real echo must pass"
        );
    }

    /// A protocol-version downgrade fails even though the handshake completed.
    #[test]
    fn eval_tls_psk_handshake_rejects_other_version() {
        let err = eval_tls_psk_handshake(&psk_verdict("TLSv1.3", EXPECTED_MBEDTLS_SUITE, 16))
            .unwrap_err();
        assert!(
            err.contains("negotiated version"),
            "error must name the version mismatch: {err}"
        );
    }

    /// A suite without ECDHE would drop forward secrecy; it must fail.
    #[test]
    fn eval_tls_psk_handshake_rejects_non_ecdhe_suite() {
        let err = eval_tls_psk_handshake(&psk_verdict(
            EXPECTED_TLS_VERSION,
            "TLS-PSK-WITH-CHACHA20-POLY1305-SHA256",
            16,
        ))
        .unwrap_err();
        assert!(
            err.contains("negotiated suite"),
            "error must name the suite mismatch: {err}"
        );
    }

    /// A handshake that carried no application bytes proves nothing about the tunnel.
    #[test]
    fn eval_tls_psk_handshake_rejects_empty_echo() {
        let err = eval_tls_psk_handshake(&psk_verdict(
            EXPECTED_TLS_VERSION,
            EXPECTED_MBEDTLS_SUITE,
            0,
        ))
        .unwrap_err();
        assert!(
            err.contains("echo_bytes=0"),
            "error must name the empty echo: {err}"
        );
    }

    /// A wrong-variant verdict is rejected rather than silently accepted.
    #[test]
    fn eval_tls_psk_handshake_rejects_wrong_variant() {
        assert!(
            eval_tls_psk_handshake(&TestData::None).is_err(),
            "fail-path data must be rejected"
        );
    }

    /// A promptly refused handshake passes.
    #[test]
    fn eval_tls_psk_wrong_key_rejected_prompt_refusal_passes() {
        assert!(
            eval_tls_psk_wrong_key_rejected(&TestData::TlsPskRejected {
                peer_ip: [10, 0, 0, 4],
                peer_port: 17387,
                reject_ms: 240,
            })
            .is_ok(),
            "an alert-speed refusal must pass"
        );
    }

    /// A refusal that took far longer than the exchange costs is an unexpected
    /// reading, not a pass.
    #[test]
    fn eval_tls_psk_wrong_key_rejected_rejects_a_slow_refusal() {
        let err = eval_tls_psk_wrong_key_rejected(&TestData::TlsPskRejected {
            peer_ip: [10, 0, 0, 4],
            peer_port: 17387,
            reject_ms: 2900,
        })
        .unwrap_err();
        assert!(
            err.contains("2900 ms"),
            "error must name the observed latency: {err}"
        );
    }

    /// Two generated keys differ and neither is all-zero — the fixture never hands
    /// the device a degenerate secret.
    #[test]
    fn generate_audio_psk_produces_distinct_nonzero_keys() {
        let a = super::generate_audio_psk().expect("urandom read failed");
        let b = super::generate_audio_psk().expect("urandom read failed");
        assert_ne!(a, b, "two generated PSKs must differ");
        assert_ne!(a, [0u8; AUDIO_PSK_LEN], "a generated PSK must not be zeros");
    }

    /// The wrong-key derivation keeps the identity and changes every byte of the key.
    #[test]
    fn pod_psk_with_wrong_key_shares_identity_and_differs_everywhere() {
        let good = PodPsk {
            identity: "pod-aabbcc".to_string(),
            key: [0x5au8; AUDIO_PSK_LEN],
        };
        let bad = good.with_wrong_key();
        assert_eq!(bad.identity, good.identity, "identity must be preserved");
        for (i, (g, b)) in good.key.iter().zip(bad.key.iter()).enumerate() {
            assert_ne!(g, b, "key byte {i} must differ");
        }
    }

    /// `PodPsk`'s `Debug` names the identity and never the key bytes.
    #[test]
    fn pod_psk_debug_redacts_the_key() {
        let psk = PodPsk {
            identity: "pod-aabbcc".to_string(),
            key: [0xABu8; AUDIO_PSK_LEN],
        };
        let rendered = format!("{psk:?}");
        assert!(
            rendered.contains("pod-aabbcc"),
            "Debug must name the identity: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>") && !rendered.contains("171"),
            "Debug must not render key bytes: {rendered}"
        );
    }

    /// End-to-end over loopback: a client presenting the fixture's identity and key
    /// completes the handshake and gets its payload echoed; a client with the
    /// wrong-key derivation is refused. This is the fixture's own self-test — it
    /// proves the listener discriminates before any hardware is involved.
    #[test]
    fn tls_psk_listener_accepts_the_key_and_refuses_the_wrong_one() {
        use openssl::ssl::{SslContext, SslMethod, SslVersion};
        use std::io::{Read as _, Write as _};

        let psk = PodPsk {
            identity: "pod-fixture".to_string(),
            key: [0x11u8; AUDIO_PSK_LEN],
        };
        // Port 0: the fixture's spawn helper binds a fixed port, so this test drives
        // `tls_psk_serve` directly over an ephemeral listener instead.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
        let addr = listener.local_addr().expect("local_addr failed");
        let server_ctx = psk_server_context(&psk).expect("server context failed");
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (stream, peer) = listener.accept().expect("accept failed");
                tls_psk_serve(&server_ctx, stream, &peer, "tls-psk-test");
            }
        });

        let client_ctx = |key: [u8; AUDIO_PSK_LEN], identity: String| -> SslContext {
            let mut builder = SslContext::builder(SslMethod::tls_client()).unwrap();
            builder
                .set_min_proto_version(Some(SslVersion::TLS1_2))
                .unwrap();
            builder
                .set_max_proto_version(Some(SslVersion::TLS1_2))
                .unwrap();
            builder.set_cipher_list(PSK_CIPHERSUITE).unwrap();
            builder.set_psk_client_callback(move |_ssl, _hint, identity_out, secret| {
                let bytes = identity.as_bytes();
                identity_out[..bytes.len()].copy_from_slice(bytes);
                identity_out[bytes.len()] = 0;
                secret[..key.len()].copy_from_slice(&key);
                Ok(key.len())
            });
            builder.build()
        };

        // Right key: handshake completes and the payload comes back.
        let ctx = client_ctx(psk.key, psk.identity.clone());
        let tcp = std::net::TcpStream::connect(addr).expect("connect failed");
        let ssl = openssl::ssl::Ssl::new(&ctx).unwrap();
        let mut tls = openssl::ssl::SslStream::new(ssl, tcp).unwrap();
        tls.connect()
            .expect("handshake with the right key must succeed");
        assert_eq!(
            tls.ssl().version_str(),
            EXPECTED_TLS_VERSION,
            "the fixture must negotiate the pinned protocol version"
        );
        tls.write_all(b"ping").unwrap();
        let mut echo = [0u8; 4];
        tls.read_exact(&mut echo).expect("echo read failed");
        assert_eq!(&echo, b"ping", "the tunnel must echo what was written");
        drop(tls);

        // Wrong key, same identity: refused.
        let bad = psk.with_wrong_key();
        let ctx = client_ctx(bad.key, bad.identity);
        let tcp = std::net::TcpStream::connect(addr).expect("connect failed");
        let ssl = openssl::ssl::Ssl::new(&ctx).unwrap();
        let mut tls = openssl::ssl::SslStream::new(ssl, tcp).unwrap();
        assert!(
            tls.connect().is_err(),
            "a handshake with the wrong key must be refused"
        );

        server.join().expect("listener thread panicked");
    }

    /// A plaintext client hitting a TLS fixture must be refused at the handshake, never
    /// served. Guards the invariant that all fixtures speak TLS: a resurrected plaintext
    /// path fails here.
    #[test]
    fn tls_accept_refuses_a_plaintext_client() {
        use std::io::Write as _;

        let psk = PodPsk {
            identity: "pod-fixture".to_string(),
            key: [0x22u8; AUDIO_PSK_LEN],
        };
        let server_ctx = psk_server_context(&psk).expect("server context");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let server = thread::spawn(move || {
            let (tcp, peer) = listener.accept().expect("accept");
            tls_accept(&server_ctx, tcp, &peer, "plaintext-test").is_some()
        });

        let mut tcp = std::net::TcpStream::connect(addr).expect("connect loopback");
        // Raw application bytes where a ClientHello belongs.
        let _ = tcp.write_all(b"GET / HTTP/1.0\r\n\r\n");
        let _ = tcp.flush();

        assert!(
            !server.join().expect("server thread panicked"),
            "tls_accept must refuse a plaintext client rather than serving its bytes"
        );
    }

    /// Encode one `StreamFrame` to an owned buffer (fixture-side pre-encode shape).
    fn encoded_frame_bytes(frame: &audio_pipeline::wire::StreamFrame) -> Vec<u8> {
        use audio_pipeline::wire::{MAX_FRAME_BYTES, encode_frame};
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(frame, &mut buf).expect("frame encodes");
        buf[..n].to_vec()
    }

    /// Run `inbound_frames_serve` over a loopback TLS-PSK pair with `selector` written
    /// first by the client. Returns (frames the fixture reported writing, bytes the
    /// client read out of the tunnel before EOF, encoded Hello len, encoded Audio len).
    fn drive_inbound_frames_fixture(selector: u8) -> (u32, usize, usize, usize) {
        use std::io::{Read as _, Write as _};

        let (mut server, mut client) = tls_loopback_pair();
        client.write_all(&[selector]).expect("selector write");
        client.flush().expect("selector flush");

        let hello = encoded_frame_bytes(&rtd_test_hello());
        let audio = encoded_frame_bytes(&rtd_test_audio());
        let (hello_len, audio_len) = (hello.len(), audio.len());
        let peer = client.get_ref().local_addr().expect("local_addr");
        let stop = Arc::new(Mutex::new(false));
        let jh = thread::spawn(move || {
            inbound_frames_serve(&mut server, &peer, &stop, &hello, &audio)
            // server drops → FIN → the client below sees EOF.
        });

        let mut sink = Vec::new();
        client.read_to_end(&mut sink).expect("read to EOF");
        let written = jh.join().expect("fixture thread panicked");
        (written, sink.len(), hello_len, audio_len)
    }

    /// Happy-path loopback coverage for the inbound-frames fixture: the selector byte is
    /// read *inside* the tunnel and the Hello plus `INBOUND_FRAMES_COUNT` Audio frames
    /// cross it. Dropping `tls_accept` from this fixture (or serving plaintext) breaks it.
    #[test]
    fn inbound_frames_fixture_serves_the_happy_path_profile_over_tls() {
        let (written, bytes, hello_len, audio_len) = drive_inbound_frames_fixture(b'N');
        assert_eq!(
            written, INBOUND_FRAMES_COUNT,
            "the happy-path profile must write INBOUND_FRAMES_COUNT Audio frames"
        );
        assert_eq!(
            bytes,
            hello_len + audio_len * INBOUND_FRAMES_COUNT as usize,
            "the client must receive the leading Hello plus every Audio frame"
        );
    }

    /// The `'F'` selector — read post-handshake, inside the tunnel — selects the flood
    /// profile on the same fixture.
    #[test]
    fn inbound_frames_fixture_flood_selector_sends_the_flood_count() {
        let (written, bytes, hello_len, audio_len) = drive_inbound_frames_fixture(b'F');
        assert_eq!(
            written, INBOUND_BP_FLOOD_FRAMES,
            "the 'F' selector must select the unpaced flood profile"
        );
        assert_eq!(
            bytes,
            hello_len + audio_len * INBOUND_BP_FLOOD_FRAMES as usize,
            "every flood frame must cross the tunnel"
        );
    }

    /// Loopback coverage for the backpressure fixture: it consumes the in-band selector
    /// byte and then drains application bytes inside the tunnel to a clean EOF.
    #[test]
    fn backpressure_fixture_consumes_selector_then_drains_over_tls() {
        use std::io::Write as _;

        let (mut server, mut client) = tls_loopback_pair();
        let peer = client.get_ref().local_addr().expect("local_addr");
        let stop = Arc::new(Mutex::new(false));
        let jh = thread::spawn(move || {
            consume_backpressure_selector_byte(&mut server, &peer);
            backpressure_profile_a(&mut server, &peer, &stop)
        });

        // Selector byte, then a payload the fixture must drain (larger than the socket
        // buffers, so completing the write proves the server side kept reading).
        client.write_all(b"A").expect("selector write");
        let payload = vec![0x5au8; 512 * 1024];
        client.write_all(&payload).expect("payload write");
        client.flush().expect("payload flush");
        // Clean close → the drain sees EOF and returns.
        client.shutdown().expect("tls shutdown");
        drop(client);

        assert!(
            !jh.join().expect("fixture thread panicked"),
            "the drain must end on the client's EOF, not on the stop flag"
        );
    }

    /// Loopback coverage for the poll-readiness adversary: it consumes the device's
    /// in-band trigger byte inside the tunnel and queues the fixed payload back.
    #[test]
    fn poll_readiness_fixture_answers_the_trigger_byte_over_tls() {
        use std::io::{Read as _, Write as _};

        let (mut server, mut client) = tls_loopback_pair();
        let peer = client.get_ref().local_addr().expect("local_addr");
        let jh = thread::spawn(move || poll_readiness_serve(&mut server, &peer));

        client.write_all(b"P").expect("trigger write");
        client.flush().expect("trigger flush");
        let mut payload = [0u8; POLL_READINESS_PAYLOAD_BYTES];
        client.read_exact(&mut payload).expect("payload read");
        assert_eq!(
            payload, [0xA5u8; POLL_READINESS_PAYLOAD_BYTES],
            "the adversary must queue its fixed payload inside the tunnel"
        );
        jh.join().expect("fixture thread panicked");
    }

    /// Build a poll-readiness verdict from its parts.
    fn poll_verdict(pollin: bool, pollout: bool, both: bool, read_bytes: u32) -> TestData {
        TestData::PollReadiness {
            pollin,
            pollout,
            both,
            read_bytes,
        }
    }

    /// The fully-proven verdict passes: POLLIN, POLLOUT, both, and real read data.
    #[test]
    fn eval_poll_readiness_bidir_full_verdict_passes() {
        assert!(
            eval_poll_readiness_bidir(&poll_verdict(true, true, true, 16)).is_ok(),
            "a fully-proven readiness verdict must pass"
        );
    }

    /// Fail-path data is rejected. Succeeds the string-era "FAIL prefix" and "missing
    /// read_bytes= token" cases — a device-side assertion failure now carries
    /// `TestData::None`, and no field can go missing from the typed verdict.
    #[test]
    fn eval_poll_readiness_bidir_rejects_fail_data() {
        let result = eval_poll_readiness_bidir(&TestData::None);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("PollReadiness verdict"),
            "error must name the expected verdict"
        );
    }

    /// Another test's verdict cannot be accepted as this one's.
    #[test]
    fn eval_poll_readiness_bidir_rejects_wrong_variant() {
        assert!(
            eval_poll_readiness_bidir(&bp_pass_line()).is_err(),
            "a wrong variant must fail"
        );
    }

    /// `pollin=false` fails — the never-before-exercised read-readiness path is the
    /// load-bearing assertion (design §5 risk #1).
    #[test]
    fn eval_poll_readiness_bidir_pollin_false_fails() {
        let result = eval_poll_readiness_bidir(&poll_verdict(false, true, false, 0));
        assert!(result.is_err(), "pollin=false must fail");
        assert!(
            result.unwrap_err().contains("pollin"),
            "error must name the missing POLLIN readiness"
        );
    }

    /// `pollout=false` fails — even the already-proven write-readiness path must hold
    /// on the same fd this test multiplexes.
    #[test]
    fn eval_poll_readiness_bidir_pollout_false_fails() {
        let result = eval_poll_readiness_bidir(&poll_verdict(true, false, false, 16));
        assert!(result.is_err(), "pollout=false must fail");
        assert!(
            result.unwrap_err().contains("pollout"),
            "error must name the missing POLLOUT readiness"
        );
    }

    /// `both=false` fails — the one-syscall multiplex proof is required (design §2.1).
    #[test]
    fn eval_poll_readiness_bidir_both_false_fails() {
        let result = eval_poll_readiness_bidir(&poll_verdict(true, true, false, 16));
        assert!(result.is_err(), "both=false must fail");
        assert!(
            result.unwrap_err().contains("both"),
            "error must name the missing multiplex (both) condition"
        );
    }

    /// `read_bytes=0` fails — a POLLIN not backed by real readable data is a false
    /// readiness signal, worse than no signal.
    #[test]
    fn eval_poll_readiness_bidir_zero_read_bytes_fails() {
        let result = eval_poll_readiness_bidir(&poll_verdict(true, true, true, 0));
        assert!(result.is_err(), "read_bytes=0 must fail");
        assert!(
            result.unwrap_err().contains("read_bytes"),
            "error must name the empty read"
        );
    }

    // ── eval_wifi_scan tests ──────────────────────────────────────────────────

    /// Build a scanned-SSID list from (already ≤16-byte) SSIDs.
    fn ssid_list(ssids: &[&str]) -> heapless::Vec<heapless::String<SSID_TRUNC_BYTES>, 3> {
        let mut list = heapless::Vec::new();
        for s in ssids {
            let mut entry = heapless::String::<SSID_TRUNC_BYTES>::new();
            entry.push_str(s).expect("test SSID exceeds 16 bytes");
            list.push(entry).expect("test supplies at most 3 SSIDs");
        }
        list
    }

    /// Build a `WifiScan` verdict with `aps` APs and the given (already ≤16-byte) SSIDs.
    fn wifi_scan_verdict(aps: u32, ssids: &[&str]) -> TestData {
        TestData::WifiScan {
            aps,
            best_rssi: -48,
            ssids: ssid_list(ssids),
        }
    }

    /// Message with aps=N (N > 0) passes.
    #[test]
    fn eval_wifi_scan_aps_present_passes() {
        assert!(
            eval_wifi_scan_with(&wifi_scan_verdict(7, &["homenet", "neighbor"]), None).is_ok(),
            "aps=7 must pass"
        );
    }

    /// Message with aps=0 fails.
    #[test]
    fn eval_wifi_scan_zero_aps_fails() {
        let result = eval_wifi_scan_with(&wifi_scan_verdict(0, &[]), None);
        assert!(result.is_err(), "aps=0 must fail");
        assert!(
            result.unwrap_err().contains("0 APs"),
            "error must report the zero AP count"
        );
    }

    /// A `TestData::None` (fail-path) verdict is rejected. Succeeds the old
    /// "message without aps= token" case: an absent count is now a wrong variant.
    #[test]
    fn eval_wifi_scan_fail_data_rejected() {
        let result = eval_wifi_scan_with(&TestData::None, None);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("WifiScan"),
            "error must mention the expected WifiScan variant"
        );
    }

    // ── eval_wifi_power_save tests ────────────────────────────────────────────

    /// ps_mode=0 (WIFI_PS_NONE) passes — power save off is the expected value.
    #[test]
    fn eval_wifi_power_save_none_passes() {
        assert!(
            eval_wifi_power_save(&TestData::WifiPowerSaveCheck { ps_mode: 0 }).is_ok(),
            "ps_mode=0 (WIFI_PS_NONE) must pass"
        );
    }

    /// ps_mode=1 (MIN_MODEM) fails — power save silently on is the incident signature.
    #[test]
    fn eval_wifi_power_save_min_modem_fails() {
        let result = eval_wifi_power_save(&TestData::WifiPowerSaveCheck { ps_mode: 1 });
        assert!(result.is_err(), "ps_mode=1 (MIN_MODEM) must fail");
        assert!(
            result.unwrap_err().contains("ps_mode=1"),
            "error must report the offending raw mode"
        );
    }

    /// A non-`WifiPowerSaveCheck` verdict is rejected.
    #[test]
    fn eval_wifi_power_save_wrong_variant_rejected() {
        assert!(
            eval_wifi_power_save(&TestData::None).is_err(),
            "a non-WifiPowerSaveCheck verdict must not satisfy eval_wifi_power_save"
        );
    }

    /// Another test's verdict data is rejected — a WifiScan eval can never accept it.
    #[test]
    fn eval_wifi_scan_wrong_variant_rejected() {
        assert!(
            eval_wifi_scan_with(
                &TestData::WifiAssociate {
                    ip: [192, 168, 1, 10],
                    gateway: [192, 168, 1, 1],
                    rssi: -55,
                },
                None
            )
            .is_err(),
            "a WifiAssociate verdict must not satisfy eval_wifi_scan"
        );
    }

    /// RSSI strictly below the -80 dBm floor (-81) must fail.
    #[test]
    fn eval_wifi_info_rssi_below_floor_fails() {
        let result = eval_wifi_info(&TestData::WifiAssociate {
            ip: [192, 168, 1, 10],
            gateway: [192, 168, 1, 1],
            rssi: -81,
        });
        assert!(result.is_err(), "rssi=-81 must fail (below -80 floor)");
        assert!(
            result.unwrap_err().contains("-81"),
            "error must mention the offending RSSI value"
        );
    }

    /// SSID-presence path returns Ok even when the target SSID is absent from the scan
    /// list (non-fatal: only a warning is emitted, the hard gate is the AP count).
    #[test]
    fn eval_wifi_scan_ssid_absent_is_non_fatal() {
        let result = eval_wifi_scan_with(&wifi_scan_verdict(3, &["neighbor", "other"]), None);
        assert!(
            result.is_ok(),
            "scan with aps=3 must pass regardless of SSID names"
        );
        assert_eq!(
            configured_ssid_seen(None, &ssid_list(&["neighbor", "other"])),
            None,
            "no configured SSID must skip the presence check"
        );
    }

    /// An empty configured SSID (an exported-but-blank provisioning variable) skips
    /// the presence check rather than searching for the empty string.
    #[test]
    fn eval_wifi_scan_empty_configured_ssid_skips_check() {
        assert_eq!(
            configured_ssid_seen(Some(""), &ssid_list(&["neighbor", "other"])),
            None,
            "an empty configured SSID must skip the presence check"
        );
        assert!(
            eval_wifi_scan_with(&wifi_scan_verdict(3, &["neighbor", "other"]), Some("")).is_ok(),
            "an empty configured SSID must skip the check, not fail"
        );
    }

    /// The `RESPEAKER_WIFI_SSID`-set branch: the configured SSID (truncated by the
    /// shared rule) is looked up in the typed `ssids` list. The outcome is asserted
    /// directly because the branch is diagnostic-only — it never changes the Ok/Err
    /// verdict, so `eval_wifi_scan_with` alone cannot distinguish match from miss.
    #[test]
    fn eval_wifi_scan_configured_ssid_branch() {
        assert_eq!(
            configured_ssid_seen(Some("homenet"), &ssid_list(&["homenet", "neighbor"])),
            Some(true),
            "a configured SSID present in the scan list must be reported as seen"
        );
        assert_eq!(
            configured_ssid_seen(Some("homenet"), &ssid_list(&["neighbor", "other"])),
            Some(false),
            "a configured SSID absent from the scan list must be reported as unseen"
        );
        assert!(
            eval_wifi_scan_with(
                &wifi_scan_verdict(3, &["neighbor", "other"]),
                Some("homenet")
            )
            .is_ok(),
            "an unseen configured SSID is diagnostic-only, so the verdict stays Ok"
        );
    }

    /// A non-ASCII SSID longer than `SSID_TRUNC_BYTES`: the device ships the
    /// char-boundary-truncated prefix, so the host must truncate identically before
    /// comparing or the SSID would never be found.
    #[test]
    fn eval_wifi_scan_non_ascii_ssid_matches_device_prefix() {
        let long_ssid = "0123456789abcde\u{20ac}"; // '€' straddles the 16-byte boundary
        let device_prefix = truncate_utf8_prefix(long_ssid, SSID_TRUNC_BYTES);
        assert_eq!(
            device_prefix, "0123456789abcde",
            "truncation must stop at the char boundary before the 16th byte"
        );
        assert_eq!(
            configured_ssid_seen(Some(long_ssid), &ssid_list(&[device_prefix])),
            Some(true),
            "the truncated prefix must match the device's reported SSID"
        );
        assert_eq!(
            configured_ssid_seen(Some(long_ssid), &ssid_list(&["0123456789abcd"])),
            Some(false),
            "a one-byte-shorter prefix must not count as the same network"
        );
    }

    /// A non-report payload is rejected. Succeeds the old `Payload::TestResult`- and
    /// `Payload::WifiInfo`-rejected cases — neither variant exists any more.
    #[test]
    fn check4_test_report_wrong_payload_rejected() {
        let resp = Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        };
        let result = check4_test_report(&resp);
        assert!(
            result.is_err(),
            "Payload::Empty must be rejected by check4_test_report"
        );
        assert!(
            result.unwrap_err().contains("unexpected payload"),
            "error message must describe the unexpected-payload problem"
        );
    }

    // ── eval_wifi_reassociation_pass tests ───────────────────────────────────

    fn make_reassoc_logs() -> Vec<String> {
        // These strings match what format_log produces from actual device output.
        // format_log produces "[device <Level>] <target>: <message>".
        // The wifi event callbacks (StaDisconnected, StaConnected, IpEvent) use
        // log macros without an explicit target, so the Rust module path ("wifi")
        // becomes the target, AND the message itself starts with "wifi: " — giving
        // the apparent doubled prefix "[device Warn] wifi: wifi: disconnected reason=8".
        // The supervisor uses target "wifi-supervisor" with message "wifi-supervisor: re-associated…".
        // The search tokens (e.g. "wifi: disconnected reason=") are present in both
        // this fixture and actual device output regardless of the target field.
        // Do NOT "clean up" the doubled prefix — it reflects the real device log format.
        vec![
            format!("[device Warn] wifi: {WIFI_DISCONNECTED}8"),
            format!("[device Info] wifi: {WIFI_CONNECTED}"),
            format!("[device Info] wifi: {WIFI_DHCP_LEASE} ip=192.168.1.100 gw=192.168.1.1"),
            format!(
                "[device Info] wifi-supervisor: {WIFI_REASSOCIATED} ip=192.168.1.100 gw=192.168.1.1 rssi=-55"
            ),
        ]
    }

    /// A `WifiReassociation` verdict with the given `reconnected` flag.
    fn reassoc_verdict(reconnected: bool) -> TestData {
        TestData::WifiReassociation {
            reconnected,
            ip: [192, 168, 1, 100],
            gateway: [192, 168, 1, 1],
            rssi: -55,
        }
    }

    /// Full pass: verdict + all four required log lines → Ok.
    #[test]
    fn eval_wifi_reassociation_full_pass() {
        let msg = reassoc_verdict(true);
        let logs = make_reassoc_logs();
        assert!(
            eval_wifi_reassociation_pass(&msg, &logs).is_ok(),
            "full pass must return Ok"
        );
    }

    /// Fail-path data → rejected even with all log lines present. Succeeds the old
    /// "FAIL message rejected" case; the FAIL prefix is now variant structure.
    #[test]
    fn eval_wifi_reassociation_fail_data_rejected() {
        let logs = make_reassoc_logs();
        let result = eval_wifi_reassociation_pass(&TestData::None, &logs);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("WifiReassociation"),
            "error must mention the expected WifiReassociation variant"
        );
    }

    /// Another test's verdict data → rejected.
    #[test]
    fn eval_wifi_reassociation_wrong_variant_rejected() {
        let logs = make_reassoc_logs();
        assert!(
            eval_wifi_reassociation_pass(&wifi_scan_verdict(3, &[]), &logs).is_err(),
            "a WifiScan verdict must not satisfy eval_wifi_reassociation_pass"
        );
    }

    /// `reconnected=false` → fails. Succeeds the old missing-`reconnected=true`-token case.
    #[test]
    fn eval_wifi_reassociation_not_reconnected_fails() {
        let logs = make_reassoc_logs();
        let result = eval_wifi_reassociation_pass(&reassoc_verdict(false), &logs);
        assert!(result.is_err(), "reconnected=false must fail");
        assert!(
            result.unwrap_err().contains("reconnected=false"),
            "error must report the negative reconnect verdict"
        );
    }

    /// Missing `wifi: disconnected reason=` log line → fails (WifiEvent subscription check).
    #[test]
    fn eval_wifi_reassociation_missing_disconnected_log_fails() {
        let msg = reassoc_verdict(true);
        let logs: Vec<String> = make_reassoc_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_DISCONNECTED))
            .collect();
        let result = eval_wifi_reassociation_pass(&msg, &logs);
        assert!(result.is_err(), "missing disconnected log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_DISCONNECTED),
            "error must name the missing log token"
        );
    }

    /// Missing `wifi: connected` log line → fails.
    #[test]
    fn eval_wifi_reassociation_missing_connected_log_fails() {
        let msg = reassoc_verdict(true);
        let logs: Vec<String> = make_reassoc_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_CONNECTED))
            .collect();
        let result = eval_wifi_reassociation_pass(&msg, &logs);
        assert!(result.is_err(), "missing connected log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_CONNECTED),
            "error must name the missing log token"
        );
    }

    /// Missing `wifi: dhcp lease` log line → fails (IpEvent subscription check).
    #[test]
    fn eval_wifi_reassociation_missing_dhcp_log_fails() {
        let msg = reassoc_verdict(true);
        let logs: Vec<String> = make_reassoc_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_DHCP_LEASE))
            .collect();
        let result = eval_wifi_reassociation_pass(&msg, &logs);
        assert!(result.is_err(), "missing dhcp lease log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_DHCP_LEASE),
            "error must name the missing log token"
        );
    }

    /// A lease-*lost* warn is emitted as `"<WIFI_DHCP_LEASE> lost"`, so it satisfies the bare
    /// lease match. Pins that documented sharing: the eval passes with only the lost line.
    #[test]
    fn eval_wifi_reassociation_lease_lost_line_satisfies_dhcp_check() {
        let msg = reassoc_verdict(true);
        let logs: Vec<String> = make_reassoc_logs()
            .into_iter()
            .map(|l| {
                if l.contains(WIFI_DHCP_LEASE) {
                    format!("[device Warn] {WIFI_DHCP_LEASE} lost")
                } else {
                    l
                }
            })
            .collect();
        assert!(
            eval_wifi_reassociation_pass(&msg, &logs).is_ok(),
            "lease-lost line shares the lease prefix and satisfies the check"
        );
    }

    /// Missing `wifi-supervisor: re-associated` log line → fails (supervisor completion check).
    #[test]
    fn eval_wifi_reassociation_missing_reassociated_log_fails() {
        let msg = reassoc_verdict(true);
        let logs: Vec<String> = make_reassoc_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_REASSOCIATED))
            .collect();
        let result = eval_wifi_reassociation_pass(&msg, &logs);
        assert!(result.is_err(), "missing re-associated log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_REASSOCIATED),
            "error must name the missing log token"
        );
    }

    // ── eval_gateway_probe_gate tests ────────────────────────────────────────

    /// Fixture: a log window from a GatewayProbeGate unreachable-half run — all four
    /// required supervisor log lines present.  Mirrors make_reassoc_logs() format.
    fn make_gate_logs() -> Vec<String> {
        vec![
            format!("[device Warn] wifi: {WIFI_DISCONNECTED}8"),
            format!("[device Info] wifi: {WIFI_CONNECTED}"),
            format!("[device Info] wifi: {WIFI_DHCP_LEASE} ip=192.168.1.100 gw=192.168.1.1"),
            format!(
                "[device Info] wifi-supervisor: {WIFI_REASSOCIATED} ip=192.168.1.100 gw=192.168.1.1 rssi=-55"
            ),
        ]
    }

    // ── eval_gateway_probe_gate_reachable ─────────────────────────────────────

    /// Full pass: PASS + probe=reachable + reassociated=false → Ok.
    #[test]
    fn eval_gateway_probe_gate_reachable_full_pass() {
        let msg = "PASS probe=reachable reassociated=false";
        assert!(
            eval_gateway_probe_gate_reachable(msg).is_ok(),
            "full pass must return Ok"
        );
    }

    /// FAIL prefix → rejected even if tokens are present.
    #[test]
    fn eval_gateway_probe_gate_reachable_fail_message_rejected() {
        let msg = "FAIL probe=reachable reassociated=false";
        let result = eval_gateway_probe_gate_reachable(msg);
        assert!(result.is_err(), "FAIL message must be rejected");
        assert!(
            result.unwrap_err().contains("PASS"),
            "error must mention expected PASS prefix"
        );
    }

    /// Missing probe=reachable token → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_reachable_missing_probe_token_fails() {
        let msg = "PASS reassociated=false";
        let result = eval_gateway_probe_gate_reachable(msg);
        assert!(result.is_err(), "missing probe=reachable must fail");
        assert!(
            result.unwrap_err().contains("probe=reachable"),
            "error must name the missing token"
        );
    }

    /// Missing reassociated=false token → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_reachable_missing_reassociated_token_fails() {
        let msg = "PASS probe=reachable";
        let result = eval_gateway_probe_gate_reachable(msg);
        assert!(result.is_err(), "missing reassociated=false must fail");
        assert!(
            result.unwrap_err().contains("reassociated=false"),
            "error must name the missing token"
        );
    }

    // ── eval_gateway_probe_gate_unreachable ───────────────────────────────────

    /// A `GatewayProbeGate` verdict with the given probe/re-association outcome.
    fn gate_verdict(blackhole_reachable: bool, reassociated: bool) -> TestData {
        TestData::GatewayProbeGate {
            blackhole_reachable,
            reassociated,
            ip: [192, 168, 1, 100],
            gateway: [192, 168, 1, 1],
            rssi: -55,
        }
    }

    /// Full pass: blackhole unreachable + re-associated + all four log lines → Ok.
    #[test]
    fn eval_gateway_probe_gate_unreachable_full_pass() {
        let msg = gate_verdict(false, true);
        let logs = make_gate_logs();
        assert!(
            eval_gateway_probe_gate_unreachable(&msg, &logs).is_ok(),
            "full pass must return Ok"
        );
    }

    /// Fail-path data → rejected even with all log lines present. Succeeds the old
    /// FAIL-prefix case; the prefix is now variant structure.
    #[test]
    fn eval_gateway_probe_gate_unreachable_fail_data_rejected() {
        let logs = make_gate_logs();
        let result = eval_gateway_probe_gate_unreachable(&TestData::None, &logs);
        assert!(result.is_err(), "fail-path data must be rejected");
        assert!(
            result.unwrap_err().contains("GatewayProbeGate"),
            "error must mention the expected GatewayProbeGate variant"
        );
    }

    /// Another test's verdict data → rejected.
    #[test]
    fn eval_gateway_probe_gate_unreachable_wrong_variant_rejected() {
        let logs = make_gate_logs();
        assert!(
            eval_gateway_probe_gate_unreachable(&reassoc_verdict(true), &logs).is_err(),
            "a WifiReassociation verdict must not satisfy the gate eval"
        );
    }

    /// `blackhole_reachable=true` → fails. Succeeds the old missing-`probe=unreachable`
    /// token case: an absent token is now an explicit boolean.
    #[test]
    fn eval_gateway_probe_gate_unreachable_blackhole_reachable_fails() {
        let logs = make_gate_logs();
        let result = eval_gateway_probe_gate_unreachable(&gate_verdict(true, true), &logs);
        assert!(result.is_err(), "blackhole_reachable=true must fail");
        assert!(
            result.unwrap_err().contains("blackhole_reachable=true"),
            "error must report the answering blackhole address"
        );
    }

    /// `reassociated=false` → fails. Succeeds the old missing-`reassociated=true` case.
    #[test]
    fn eval_gateway_probe_gate_unreachable_not_reassociated_fails() {
        let logs = make_gate_logs();
        let result = eval_gateway_probe_gate_unreachable(&gate_verdict(false, false), &logs);
        assert!(result.is_err(), "reassociated=false must fail");
        assert!(
            result.unwrap_err().contains("reassociated=false"),
            "error must report the negative re-association verdict"
        );
    }

    /// Missing `wifi: disconnected reason=` log line → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_unreachable_missing_disconnected_log_fails() {
        let msg = gate_verdict(false, true);
        let logs: Vec<String> = make_gate_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_DISCONNECTED))
            .collect();
        let result = eval_gateway_probe_gate_unreachable(&msg, &logs);
        assert!(result.is_err(), "missing disconnected log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_DISCONNECTED),
            "error must name the missing log token"
        );
    }

    /// Missing `wifi: connected` log line → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_unreachable_missing_connected_log_fails() {
        let msg = gate_verdict(false, true);
        let logs: Vec<String> = make_gate_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_CONNECTED))
            .collect();
        let result = eval_gateway_probe_gate_unreachable(&msg, &logs);
        assert!(result.is_err(), "missing connected log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_CONNECTED),
            "error must name the missing log token"
        );
    }

    /// Missing `wifi: dhcp lease` log line → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_unreachable_missing_dhcp_log_fails() {
        let msg = gate_verdict(false, true);
        let logs: Vec<String> = make_gate_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_DHCP_LEASE))
            .collect();
        let result = eval_gateway_probe_gate_unreachable(&msg, &logs);
        assert!(result.is_err(), "missing dhcp lease log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_DHCP_LEASE),
            "error must name the missing log token"
        );
    }

    /// Missing `wifi-supervisor: re-associated` log line → fails naming the missing token.
    #[test]
    fn eval_gateway_probe_gate_unreachable_missing_reassociated_log_fails() {
        let msg = gate_verdict(false, true);
        let logs: Vec<String> = make_gate_logs()
            .into_iter()
            .filter(|l| !l.contains(WIFI_REASSOCIATED))
            .collect();
        let result = eval_gateway_probe_gate_unreachable(&msg, &logs);
        assert!(result.is_err(), "missing re-associated log must fail");
        assert!(
            result.unwrap_err().contains(WIFI_REASSOCIATED),
            "error must name the missing log token"
        );
    }

    // ── test_meta phase-partition test ────────────────────────────────────────

    /// Every registered variant's `test_meta` phase matches the expected partition.
    ///
    /// Completeness is compile-enforced (`test_meta` has no `_` arm), so this test pins
    /// *values* while iterating `REGISTERED_TESTS`: a future variant must be classified
    /// in `test_meta` to compile, and this test fails if its classification disagrees
    /// with the expected sets below — forcing a conscious update rather than silent drift.
    #[test]
    fn test_meta_phase_matches_expected_partition() {
        use device_protocol::{REGISTERED_TESTS, TestName};

        let network = [
            TestName::WifiAssociate,
            TestName::UdpRoundtrip,
            TestName::TlsReachability,
            TestName::TlsInboundFrames,
            TestName::TlsSendBackpressure,
            TestName::TlsInboundBackpressure,
            TestName::PollReadinessBidir,
            TestName::WifiScan,
            TestName::WifiPowerSaveCheck,
            TestName::WifiReassociation,
            TestName::GatewayProbeGate,
            TestName::StreamRealtimeDuplex,
            TestName::TlsPskHandshake,
            TestName::TlsPskWrongKeyRejected,
        ];
        // CapturePeriodicLine drives the local production capture thread, not a network
        // peer; it and the two ~5 s-feed drain tests run in dedicated collect-logs blocks.
        let dedicated_local = [
            TestName::CapturePeriodicLine,
            TestName::PlaybackDrainRate,
            TestName::FullDuplexRxIntegrity,
        ];

        for t in REGISTERED_TESTS {
            let expected = if network.contains(t) {
                TestPhase::Network
            } else if dedicated_local.contains(t) {
                TestPhase::DedicatedLocal
            } else {
                // AmpAlwaysOnGpoInert is a local I2C self-test (a few I2C round-trips),
                // not a network test, so it lands in the generic loop by default.
                TestPhase::Generic
            };
            assert_eq!(
                test_meta(t).phase,
                expected,
                "{t:?} must be classified as {expected:?}"
            );
        }
    }

    // ── TlsInboundFrames timeout-budget invariant ─────────────────────────────

    /// Regression guard for the `TlsInboundFrames` timeout *inversion* (the AC9
    /// "device not responding" hang).
    ///
    /// The device's `run_tls_inbound_frames` must always produce a `TestReport`
    /// strictly inside the host's RunTest window, or the host reports a silent
    /// "not responding" instead of a typed PASS/Fail. The device's post-connect
    /// worst case is `MAX_IDLE_RETRIES × INBOUND_READ_TIMEOUT_SECS` (idle fail-fast),
    /// and connect (`INBOUND_CONNECT_TIMEOUT_SECS`) is on a mutually-exclusive path,
    /// so the *true* device worst case is the max of the two. This test asserts the
    /// strictly-conservative SUM bound (an upper bound on the max): if the sum sits
    /// under the host budget with margin, the max certainly does too. A future bump to
    /// the read timeout / `MAX_IDLE_RETRIES`, or a shrink of `test_timeout`, that
    /// re-creates the inversion fails here. The drain constants are imported from
    /// `device-protocol` (their single source) so this is a single-site assertion.
    #[test]
    fn tcp_inbound_frames_device_budget_under_host_timeout() {
        use device_protocol::{
            INBOUND_CONNECT_TIMEOUT_SECS, INBOUND_READ_TIMEOUT_SECS, MAX_IDLE_RETRIES,
            TLS_HANDSHAKE_TIMEOUT_SECS, TestName,
        };

        // Serial round-trip + connect/transfer margin: the device must finish, encode,
        // and ship the Response back over the serial link well before the host deadline.
        // Basis: the actual serial round-trip for a ~50-byte Response over the USB-serial
        // link is sub-millisecond; 3s is a deliberately conservative stand-in covering
        // connect + frame transfer + serial RTT pending a measured HIL figure. It is not
        // an engineered number.
        const SERIAL_ROUND_TRIP_MARGIN_SECS: u64 = 3;

        let host_budget = test_timeout(&TestName::TlsInboundFrames);
        // `.as_secs()` truncates sub-second components. The budget invariant compares
        // whole seconds on both sides, which is only sound if the host budget is a whole
        // number of seconds — assert that precondition so a future fractional budget
        // (e.g. Duration::from_millis(14_999)) cannot silently truncate to 14s and hide
        // the inversion under sub-second load.
        assert_eq!(
            host_budget.subsec_millis(),
            0,
            "TlsInboundFrames host budget must be whole seconds for the .as_secs() \
             comparison below to be sound; got {host_budget:?}",
        );
        let host_budget_secs = host_budget.as_secs();
        // The margin must be a real headroom, not larger than the budget itself: a future
        // shrink of test_timeout below the margin would make the subtraction-equivalent
        // check meaningless (and, in the worst case, satisfy the inequality with zero real
        // headroom). Catch that here rather than passing a vacuous assertion downstream.
        assert!(
            host_budget_secs > SERIAL_ROUND_TRIP_MARGIN_SECS,
            "host budget ({host_budget_secs}s) must exceed the serial round-trip margin \
             ({SERIAL_ROUND_TRIP_MARGIN_SECS}s) for the margin to be meaningful",
        );

        // Conservative upper bound on the device worst case (sum, not max). The handshake
        // term is charged on top of connect (the handshake runs under its own deadline
        // after the socket is up).
        let device_conservative_bound_secs = u64::from(MAX_IDLE_RETRIES)
            * INBOUND_READ_TIMEOUT_SECS
            + INBOUND_CONNECT_TIMEOUT_SECS
            + TLS_HANDSHAKE_TIMEOUT_SECS;

        assert!(
            device_conservative_bound_secs + SERIAL_ROUND_TRIP_MARGIN_SECS < host_budget_secs,
            "TlsInboundFrames timeout inversion: device conservative worst case \
             ({}s = {} retries × {}s read + {}s connect + {}s TLS handshake) + {}s serial \
             margin must be < host budget ({}s). Shrink the device drain constants in \
             device-protocol or raise test_timeout(TlsInboundFrames).",
            device_conservative_bound_secs,
            MAX_IDLE_RETRIES,
            INBOUND_READ_TIMEOUT_SECS,
            INBOUND_CONNECT_TIMEOUT_SECS,
            TLS_HANDSHAKE_TIMEOUT_SECS,
            SERIAL_ROUND_TRIP_MARGIN_SECS,
            host_budget_secs,
        );
    }

    /// Negative companion to `tcp_inbound_frames_device_budget_under_host_timeout`:
    /// proves the budget inequality actually *fires* when the device worst case exceeds
    /// the host budget. Guards against a future edit that flips `<` to `<=`, swaps the
    /// operands, or otherwise neuters the only regression guard for the timeout inversion.
    /// Uses inline values chosen to violate the bound (3 retries × 5s read + 5s connect =
    /// 20s, + 3s margin = 23s, vs a 15s budget) and asserts the same expression the live
    /// test asserts evaluates to `false`.
    #[test]
    fn tcp_inbound_frames_budget_invariant_fires_when_violated() {
        const RETRIES: u64 = 3;
        const READ_SECS: u64 = 5;
        const CONNECT_SECS: u64 = 5;
        const MARGIN_SECS: u64 = 3;
        const BUDGET_SECS: u64 = 15;

        let device_conservative_bound_secs = RETRIES * READ_SECS + CONNECT_SECS;
        // Evaluate the *same* inequality the live guard asserts; with these violating
        // values it must be false. If a refactor weakened the operator (`<` → `<=`) or
        // swapped sides, `within_budget` would become true and this test would fail,
        // surfacing the broken guard.
        let within_budget = device_conservative_bound_secs + MARGIN_SECS < BUDGET_SECS;
        assert!(
            !within_budget,
            "budget invariant must REJECT a device worst case ({device_conservative_bound_secs}s) \
             + margin ({MARGIN_SECS}s) that exceeds the host budget ({BUDGET_SECS}s)",
        );
    }

    // ── TlsInboundBackpressure timeout-budget invariant ───────────────────────

    /// Regression guard mirroring `tcp_inbound_frames_device_budget_under_host_timeout`:
    /// the device's `run_tls_inbound_backpressure` wall-clock deadline
    /// (`INBOUND_BP_DEADLINE_SECS`) plus connect (`INBOUND_CONNECT_TIMEOUT_SECS`) plus one
    /// in-flight blocking read (`INBOUND_READ_TIMEOUT_SECS`) must land strictly inside the
    /// host's `TlsInboundBackpressure` RunTest budget, so the host always sees a typed
    /// `TestReport` (a deadline-exceeded typed Fail on a wedged flood) rather than a
    /// silent "not responding" hang. The read timeout is folded in because the deadline is
    /// only checked at the top of the loop: a `drain_inbound` read that begins just before
    /// the deadline can block up to `INBOUND_READ_TIMEOUT_SECS` past it before the next
    /// check fires, so the device's true worst case is deadline + connect + one read, not
    /// deadline + connect.
    /// The `TlsInboundBackpressure` budget-inversion predicate, shared by the positive
    /// (`tcp_inbound_backpressure_device_budget_under_host_timeout`) and negative
    /// (`tcp_inbound_backpressure_budget_invariant_fires_when_violated`) tests below, so
    /// the negative case exercises the actual expression the live guard evaluates rather
    /// than a copy-pasted literal inequality that could silently drift out of sync with it.
    fn inbound_bp_budget_ok(
        device_conservative_bound_secs: u64,
        margin_secs: u64,
        host_budget_secs: u64,
    ) -> bool {
        device_conservative_bound_secs + margin_secs < host_budget_secs
    }

    #[test]
    fn tcp_inbound_backpressure_device_budget_under_host_timeout() {
        use device_protocol::{
            INBOUND_BP_DEADLINE_SECS, INBOUND_CONNECT_TIMEOUT_SECS, INBOUND_READ_TIMEOUT_SECS,
            TLS_HANDSHAKE_TIMEOUT_SECS, TestName,
        };

        // The bound (20s deadline + 5s connect + 3s handshake + 2s read overshoot = 30s)
        // leaves ~3s of spare inside the 35s host budget, less this margin — tight but
        // positive today; a future bump to any of the four device constants must shrink
        // another in step or this guard fires.
        const SERIAL_ROUND_TRIP_MARGIN_SECS: u64 = 2;

        let host_budget = test_timeout(&TestName::TlsInboundBackpressure);
        assert_eq!(
            host_budget.subsec_millis(),
            0,
            "TlsInboundBackpressure host budget must be whole seconds for the .as_secs() \
             comparison below to be sound; got {host_budget:?}",
        );
        let host_budget_secs = host_budget.as_secs();
        assert!(
            host_budget_secs > SERIAL_ROUND_TRIP_MARGIN_SECS,
            "host budget ({host_budget_secs}s) must exceed the serial round-trip margin \
             ({SERIAL_ROUND_TRIP_MARGIN_SECS}s) for the margin to be meaningful",
        );

        let device_conservative_bound_secs = INBOUND_BP_DEADLINE_SECS
            + INBOUND_CONNECT_TIMEOUT_SECS
            + TLS_HANDSHAKE_TIMEOUT_SECS
            + INBOUND_READ_TIMEOUT_SECS;

        assert!(
            inbound_bp_budget_ok(
                device_conservative_bound_secs,
                SERIAL_ROUND_TRIP_MARGIN_SECS,
                host_budget_secs
            ),
            "TlsInboundBackpressure timeout inversion: device conservative worst case \
             ({}s = {}s deadline + {}s connect + {}s TLS handshake + {}s in-flight read \
             overshoot) + {}s serial margin must be < host budget ({}s). Shrink the device \
             constants in device-protocol or raise test_timeout(TlsInboundBackpressure).",
            device_conservative_bound_secs,
            INBOUND_BP_DEADLINE_SECS,
            INBOUND_CONNECT_TIMEOUT_SECS,
            TLS_HANDSHAKE_TIMEOUT_SECS,
            INBOUND_READ_TIMEOUT_SECS,
            SERIAL_ROUND_TRIP_MARGIN_SECS,
            host_budget_secs,
        );
    }

    /// Negative companion: proves `inbound_bp_budget_ok` — the same predicate the live
    /// guard above asserts — actually rejects a device worst case that exceeds the host
    /// budget. Guards against a future edit that flips `<` to `<=`, swaps the operands,
    /// or otherwise neuters the shared predicate (same rationale as
    /// `tcp_inbound_frames_budget_invariant_fires_when_violated`).
    #[test]
    fn tcp_inbound_backpressure_budget_invariant_fires_when_violated() {
        const DEADLINE_SECS: u64 = 20;
        const CONNECT_SECS: u64 = 5;
        const MARGIN_SECS: u64 = 3;
        const BUDGET_SECS: u64 = 25; // 20 + 5 + 3 = 28 > 25

        let device_conservative_bound_secs = DEADLINE_SECS + CONNECT_SECS;
        assert!(
            !inbound_bp_budget_ok(device_conservative_bound_secs, MARGIN_SECS, BUDGET_SECS),
            "budget invariant must REJECT a device worst case ({device_conservative_bound_secs}s) \
             + margin ({MARGIN_SECS}s) that exceeds the host budget ({BUDGET_SECS}s)",
        );
    }

    // ── TLS-PSK pair timeout-budget invariant ─────────────────────────────────

    /// The TLS-PSK budget-inversion predicate, shared by the positive and negative
    /// tests below so the negative case exercises the expression the live guard
    /// evaluates rather than a copy of it.
    fn tls_psk_budget_ok(
        device_conservative_bound_secs: u64,
        margin_secs: u64,
        host_budget_secs: u64,
    ) -> bool {
        device_conservative_bound_secs + margin_secs < host_budget_secs
    }

    /// Regression guard for a timeout inversion on the TLS-PSK pair, mirroring
    /// `tcp_inbound_frames_device_budget_under_host_timeout`.
    ///
    /// `TlsPskHandshake` charges a connect, then a handshake, then the echo
    /// round-trip (each budget anchored after the previous stage completes, so the
    /// sum is the worst case, not an over-count). `TlsPskWrongKeyRejected` charges
    /// its reachability pre-probe connect **and** the TLS connect — two full
    /// connect budgets — plus the handshake deadline. Both must land strictly
    /// inside the host's RunTest window or the host prints "device not responding"
    /// and drops the typed `TestReport` carrying the very timing evidence these
    /// tests exist to produce. The device constants are imported from
    /// `device-protocol`, their single source, so this is a single-site assertion.
    #[test]
    fn tls_psk_device_budgets_under_host_timeouts() {
        use device_protocol::{
            TLS_HANDSHAKE_TIMEOUT_SECS, TLS_PSK_CONNECT_TIMEOUT_SECS, TLS_PSK_ECHO_TIMEOUT_SECS,
            TestName,
        };

        // Same conservative stand-in as the inbound guards: serial round-trip for a
        // small Response is sub-millisecond; 3s covers encode + transfer + RTT.
        const SERIAL_ROUND_TRIP_MARGIN_SECS: u64 = 3;

        for (test, device_bound_secs, terms) in [
            (
                TestName::TlsPskHandshake,
                TLS_PSK_CONNECT_TIMEOUT_SECS
                    + TLS_HANDSHAKE_TIMEOUT_SECS
                    + TLS_PSK_ECHO_TIMEOUT_SECS,
                "connect + TLS handshake + echo round-trip",
            ),
            (
                TestName::TlsPskWrongKeyRejected,
                2 * TLS_PSK_CONNECT_TIMEOUT_SECS + TLS_HANDSHAKE_TIMEOUT_SECS,
                "reachability pre-probe connect + connect + TLS handshake",
            ),
        ] {
            let host_budget = test_timeout(&test);
            assert_eq!(
                host_budget.subsec_millis(),
                0,
                "{test:?} host budget must be whole seconds for the .as_secs() comparison \
                 below to be sound; got {host_budget:?}",
            );
            let host_budget_secs = host_budget.as_secs();
            assert!(
                host_budget_secs > SERIAL_ROUND_TRIP_MARGIN_SECS,
                "{test:?} host budget ({host_budget_secs}s) must exceed the serial \
                 round-trip margin ({SERIAL_ROUND_TRIP_MARGIN_SECS}s) for the margin to \
                 be meaningful",
            );
            assert!(
                tls_psk_budget_ok(
                    device_bound_secs,
                    SERIAL_ROUND_TRIP_MARGIN_SECS,
                    host_budget_secs
                ),
                "{test:?} timeout inversion: device worst case ({device_bound_secs}s = \
                 {terms}) + {SERIAL_ROUND_TRIP_MARGIN_SECS}s serial margin must be < host \
                 budget ({host_budget_secs}s). Shrink TLS_PSK_CONNECT_TIMEOUT_SECS in \
                 device-protocol or raise test_timeout({test:?}).",
            );
        }
    }

    /// Negative companion: proves `tls_psk_budget_ok` rejects a device worst case
    /// that exceeds the host budget, so a future edit flipping `<` to `<=` or
    /// swapping the operands cannot silently neuter the guard above.
    #[test]
    fn tls_psk_budget_invariant_fires_when_violated() {
        const DEVICE_BOUND_SECS: u64 = 23;
        const MARGIN_SECS: u64 = 3;
        const BUDGET_SECS: u64 = 15;

        assert!(
            !tls_psk_budget_ok(DEVICE_BOUND_SECS, MARGIN_SECS, BUDGET_SECS),
            "budget invariant must REJECT a device worst case ({DEVICE_BOUND_SECS}s) + \
             margin ({MARGIN_SECS}s) that exceeds the host budget ({BUDGET_SECS}s)",
        );
    }

    // ── backpressure listener receive-buffer clamp ─────────────────────────────

    /// The clamp must be inherited by accepted sockets, which is the whole point of
    /// applying it before `listen(2)`: it is the accepted socket's window that bounds how
    /// much the device can push during the withhold. The kernel doubles the request and
    /// enforces its own floor, so the absolute assertion is a generous ceiling rather than
    /// an exact value.
    ///
    /// The ceiling alone would not establish that the *clamp* produced the value: on a host
    /// whose `net.ipv4.tcp_rmem` middle value is already small, an unclamped socket passes
    /// it too, so deleting the `set_recv_buffer_size` call would leave the test green. Hence
    /// the plain-`TcpListener` control: the clamped socket must come out strictly smaller
    /// than an unclamped one on this same host. Where the host default is itself under the
    /// ceiling the comparison is skipped with an explanatory message, so the test states
    /// what it proved instead of silently passing.
    #[test]
    fn backpressure_listener_clamps_accepted_socket_recv_buffer() {
        /// Accept one loopback connection on `listener` and report the accepted socket's
        /// effective `SO_RCVBUF`.
        fn accepted_recv_buffer(listener: &TcpListener) -> usize {
            let port = listener.local_addr().expect("local addr").port();
            let _client =
                std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect to listener");
            let (accepted, _peer) = listener.accept().expect("accept");
            effective_recv_buffer_size(&accepted).expect("query SO_RCVBUF")
        }

        let clamped = accepted_recv_buffer(
            &bind_clamped_rcvbuf_listener(0, BACKPRESSURE_RCVBUF_BYTES)
                .expect("bind clamped loopback listener"),
        );
        assert!(
            clamped <= BACKPRESSURE_RCVBUF_CEILING_BYTES,
            "accepted socket must inherit the listener's clamped receive buffer (requested \
             {BACKPRESSURE_RCVBUF_BYTES}, ceiling {BACKPRESSURE_RCVBUF_CEILING_BYTES}); got \
             {clamped}"
        );

        let plain = accepted_recv_buffer(
            &TcpListener::bind(("127.0.0.1", 0)).expect("bind plain loopback listener"),
        );
        if plain > BACKPRESSURE_RCVBUF_CEILING_BYTES {
            assert!(
                clamped < plain,
                "the clamp, not the host default, must produce the small buffer: clamped \
                 {clamped} must be strictly below this host's unclamped default {plain}"
            );
        } else {
            eprintln!(
                "NOTE: this host's default accepted SO_RCVBUF is {plain}, already at or below \
                 the {BACKPRESSURE_RCVBUF_CEILING_BYTES} ceiling — the clamp-vs-default \
                 comparison proves nothing here and was skipped; only the ceiling bound \
                 ({clamped}) was checked. Raise net.ipv4.tcp_rmem to exercise it."
            );
        }
    }

    // ── read_trigger_byte ──────────────────────────────────────────────────────

    /// A caller (device) that writes the selector byte before the timeout gets it back
    /// verbatim — pins the `Some(byte)` side of the inbound-frames-source flood dispatch
    /// (`selector == Some(b'F')`) that `run_tls_inbound_backpressure`'s selector write
    /// depends on.
    #[test]
    fn read_trigger_byte_returns_byte_when_written() {
        use std::io::Write as _;

        let (mut server, mut client) = tls_loopback_pair();
        let peer = server.get_ref().peer_addr().expect("peer addr");

        client.write_all(b"F").expect("write selector byte");
        client.flush().expect("flush");

        let got = read_trigger_byte(&mut server, &peer, "test", "unused", Duration::from_secs(2));
        assert_eq!(got, Some(b'F'), "selector byte must round-trip verbatim");
    }

    /// A caller that never writes the selector byte (a straggler firmware build that omits it,
    /// or the inbound-frames stream after the byte is consumed) gets `None` back once the
    /// timeout elapses — the documented compat guarantee the inbound-frames-source flood
    /// dispatch falls back to the happy-path profile on. A short timeout keeps the test fast.
    #[test]
    fn read_trigger_byte_returns_none_on_timeout() {
        let (mut server, _client) = tls_loopback_pair(); // client held open, no write
        let peer = server.get_ref().peer_addr().expect("peer addr");

        let got = read_trigger_byte(
            &mut server,
            &peer,
            "test",
            "falling back to happy path",
            Duration::from_millis(200),
        );
        assert_eq!(
            got, None,
            "no bytes written before the timeout must fall back to None"
        );
    }

    /// A caller that closes the connection before writing anything also gets `None` — the
    /// other branch of the same fallback contract, reached without waiting out a timeout.
    #[test]
    fn read_trigger_byte_returns_none_on_eof() {
        let (mut server, client) = tls_loopback_pair();
        let peer = server.get_ref().peer_addr().expect("peer addr");
        drop(client); // close before any write

        let got = read_trigger_byte(
            &mut server,
            &peer,
            "test",
            "falling back to happy path",
            Duration::from_secs(2),
        );
        assert_eq!(got, None, "peer EOF before the byte must fall back to None");
    }

    // ── resolve_secrets / load_hil_secrets tests ──────────────────────────────

    /// Both WiFi creds present → `secrets.wifi` is `Some` with correct values.
    #[test]
    fn resolve_secrets_both_wifi_creds_present() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_WIFI_SSID".to_string(), "HomeNetwork".to_string());
        map.insert("RESPEAKER_WIFI_PASS".to_string(), "hunter2".to_string());

        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(s.wifi.is_some(), "both creds present → wifi must be Some");
        let wifi = s.wifi.unwrap();
        assert_eq!(wifi.ssid, "HomeNetwork");
        assert_eq!(wifi.pass, "hunter2");
    }

    /// Neither WiFi cred present → `secrets.wifi` is `None` (NVS fallback path).
    #[test]
    fn resolve_secrets_no_wifi_creds_returns_none_wifi() {
        let map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.wifi.is_none(),
            "no creds present → wifi must be None (NVS fallback)"
        );
    }

    /// Only SSID set (PASS absent) → `secrets.wifi` is `None`; run continues.
    #[test]
    fn resolve_secrets_only_ssid_set_returns_none_wifi() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_WIFI_SSID".to_string(), "HomeNetwork".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.wifi.is_none(),
            "only SSID set → wifi must be None (partial config treated as unset)"
        );
    }

    /// Only PASS set (SSID absent) → `secrets.wifi` is `None`; run continues.
    #[test]
    fn resolve_secrets_only_pass_set_returns_none_wifi() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_WIFI_PASS".to_string(), "hunter2".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.wifi.is_none(),
            "only PASS set → wifi must be None (partial config treated as unset)"
        );
    }

    /// Both TLS config keys present → `secrets.tls` is `Some` with correct values.
    #[test]
    fn resolve_secrets_tls_config_present() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_TLS_HOST".to_string(), "1.1.1.1".to_string());
        map.insert("RESPEAKER_TLS_PORT".to_string(), "443".to_string());

        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(s.tls.is_some(), "TLS config present → tls must be Some");
        let tls = s.tls.unwrap();
        assert_eq!(tls.host, [1, 1, 1, 1]);
        assert_eq!(tls.port, 443);
    }

    /// TLS host absent → `secrets.tls` is `None`; run continues (test skipped).
    #[test]
    fn resolve_secrets_tls_host_absent_returns_none_tls() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_TLS_PORT".to_string(), "443".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.tls.is_none(),
            "TLS host absent → tls must be None (TlsReachability skipped)"
        );
    }

    /// TLS port absent → `secrets.tls` is `None`; run continues (test skipped).
    #[test]
    fn resolve_secrets_tls_port_absent_returns_none_tls() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_TLS_HOST".to_string(), "1.2.3.4".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.tls.is_none(),
            "TLS port absent → tls must be None (TlsReachability skipped)"
        );
    }

    /// Neither TLS key set → `secrets.tls` is `None`.
    #[test]
    fn resolve_secrets_no_tls_config_returns_none_tls() {
        let map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(s.tls.is_none(), "no TLS config → tls must be None");
    }

    /// Unparseable TLS host → `secrets.tls` is `None`; run continues.
    #[test]
    fn resolve_secrets_bad_tls_host_returns_none_tls() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_TLS_HOST".to_string(), "not-an-ip".to_string());
        map.insert("RESPEAKER_TLS_PORT".to_string(), "443".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(
            s.tls.is_none(),
            "bad TLS host → tls must be None (TlsReachability skipped)"
        );
    }

    /// All four keys present → both wifi and tls are Some.
    #[test]
    fn resolve_secrets_all_keys_present_both_some() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_WIFI_SSID".to_string(), "HomeNetwork".to_string());
        map.insert("RESPEAKER_WIFI_PASS".to_string(), "hunter2".to_string());
        map.insert("RESPEAKER_TLS_HOST".to_string(), "1.1.1.1".to_string());
        map.insert("RESPEAKER_TLS_PORT".to_string(), "443".to_string());

        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert!(s.wifi.is_some(), "all keys present → wifi must be Some");
        assert!(s.tls.is_some(), "all keys present → tls must be Some");
        let wifi = s.wifi.unwrap();
        assert_eq!(wifi.ssid, "HomeNetwork");
        assert_eq!(wifi.pass, "hunter2");
        let tls = s.tls.unwrap();
        assert_eq!(tls.host, [1, 1, 1, 1]);
        assert_eq!(tls.port, 443);
    }

    // ── resolve_secrets port resolution tests ────────────────────────────────

    /// Default constants are distinct, all in 5-digit range below OS ephemeral range, none equals 7380 (AC2).
    #[test]
    fn hil_port_defaults_satisfy_ac2() {
        assert_ne!(
            DEFAULT_HIL_UDP_PORT, DEFAULT_HIL_INBOUND_FRAMES_PORT,
            "UDP and inbound-frames default ports must be distinct"
        );
        assert_ne!(
            DEFAULT_HIL_UDP_PORT, 7380,
            "UDP default must not equal audio port 7380"
        );
        assert_ne!(
            DEFAULT_HIL_INBOUND_FRAMES_PORT, 7380,
            "inbound-frames default must not equal audio port 7380"
        );
        const {
            assert!(
                DEFAULT_HIL_UDP_PORT >= 10000,
                "UDP default must be in 5-digit range (≥10000)"
            );
            assert!(
                DEFAULT_HIL_INBOUND_FRAMES_PORT >= 10000,
                "inbound-frames default must be in 5-digit range (≥10000)"
            );
            assert!(
                DEFAULT_HIL_UDP_PORT < 32768,
                "default UDP port should be below OS ephemeral range (AC2 soft constraint)"
            );
            assert!(
                DEFAULT_HIL_INBOUND_FRAMES_PORT < 32768,
                "default inbound-frames port should be below OS ephemeral range (AC2 soft constraint)"
            );
            assert!(
                DEFAULT_HIL_BACKPRESSURE_PORT < 32768,
                "default backpressure port should be below OS ephemeral range (AC2 soft constraint)"
            );
        }
    }

    /// Absent port keys → compiled-in defaults, no warning.
    #[test]
    fn resolve_secrets_absent_port_keys_return_defaults() {
        let map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.udp_echo_port, DEFAULT_HIL_UDP_PORT,
            "absent RESPEAKER_HIL_UDP_PORT → default"
        );
        assert_eq!(
            s.inbound_frames_port, DEFAULT_HIL_INBOUND_FRAMES_PORT,
            "absent RESPEAKER_HIL_INBOUND_FRAMES_PORT → default"
        );
        assert_eq!(
            s.backpressure_port, DEFAULT_HIL_BACKPRESSURE_PORT,
            "absent RESPEAKER_HIL_BACKPRESSURE_PORT → default"
        );
    }

    /// Backpressure port override / fallback resolution.
    #[test]
    fn resolve_secrets_backpressure_port_override_and_fallback() {
        // Valid override is used.
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert(
            "RESPEAKER_HIL_BACKPRESSURE_PORT".to_string(),
            "19003".to_string(),
        );
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.backpressure_port, 19003,
            "overridden backpressure port must be used"
        );

        // Unparseable → default.
        let mut bad: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        bad.insert(
            "RESPEAKER_HIL_BACKPRESSURE_PORT".to_string(),
            "not-a-port".to_string(),
        );
        let s = resolve_secrets(|key| bad.get(key).cloned(), "<test>");
        assert_eq!(
            s.backpressure_port, DEFAULT_HIL_BACKPRESSURE_PORT,
            "unparseable backpressure port → default"
        );

        // Zero → default (port 0 = ephemeral, rejected).
        let mut zero: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        zero.insert(
            "RESPEAKER_HIL_BACKPRESSURE_PORT".to_string(),
            "0".to_string(),
        );
        let s = resolve_secrets(|key| zero.get(key).cloned(), "<test>");
        assert_eq!(
            s.backpressure_port, DEFAULT_HIL_BACKPRESSURE_PORT,
            "port 0 (ephemeral) must be rejected → default"
        );
    }

    /// Valid override → the overridden value is used.
    #[test]
    fn resolve_secrets_valid_port_override_is_used() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_HIL_UDP_PORT".to_string(), "19000".to_string());
        map.insert(
            "RESPEAKER_HIL_INBOUND_FRAMES_PORT".to_string(),
            "19002".to_string(),
        );
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(s.udp_echo_port, 19000, "overridden UDP port must be used");
        assert_eq!(
            s.inbound_frames_port, 19002,
            "overridden inbound frames port must be used"
        );
    }

    /// Unparseable UDP port override → falls back to default (resolved value is default).
    #[test]
    fn resolve_secrets_unparseable_udp_port_falls_back_to_default() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert(
            "RESPEAKER_HIL_UDP_PORT".to_string(),
            "not-a-port".to_string(),
        );
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.udp_echo_port, DEFAULT_HIL_UDP_PORT,
            "unparseable UDP port → default"
        );
    }

    /// Zero UDP port override → falls back to default (port 0 = ephemeral, rejected).
    #[test]
    fn resolve_secrets_zero_udp_port_falls_back_to_default() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert("RESPEAKER_HIL_UDP_PORT".to_string(), "0".to_string());
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.udp_echo_port, DEFAULT_HIL_UDP_PORT,
            "port 0 (ephemeral) must be rejected → default"
        );
    }

    /// Unparseable inbound-frames port override → falls back to default.
    #[test]
    fn resolve_secrets_unparseable_inbound_frames_port_falls_back_to_default() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert(
            "RESPEAKER_HIL_INBOUND_FRAMES_PORT".to_string(),
            "not-a-port".to_string(),
        );
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.inbound_frames_port, DEFAULT_HIL_INBOUND_FRAMES_PORT,
            "unparseable inbound-frames port → default"
        );
    }

    /// Zero inbound-frames port override → falls back to default (port 0 = ephemeral, rejected).
    #[test]
    fn resolve_secrets_zero_inbound_frames_port_falls_back_to_default() {
        let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        map.insert(
            "RESPEAKER_HIL_INBOUND_FRAMES_PORT".to_string(),
            "0".to_string(),
        );
        let s = resolve_secrets(|key| map.get(key).cloned(), "<test>");
        assert_eq!(
            s.inbound_frames_port, DEFAULT_HIL_INBOUND_FRAMES_PORT,
            "port 0 (ephemeral) must be rejected → default"
        );
    }

    /// Env-over-file precedence: env value wins when the resolver returns it first.
    ///
    /// Scope: exercises `resolve_secrets` only; the env-over-file ordering in
    /// `load_hil_secrets` is not separately unit-tested.
    #[test]
    fn resolve_secrets_env_wins_over_file_for_ports() {
        let get = |key: &str| -> Option<String> {
            match key {
                "RESPEAKER_HIL_UDP_PORT" => Some("18000".to_string()),
                "RESPEAKER_HIL_INBOUND_FRAMES_PORT" => Some("18002".to_string()),
                _ => None,
            }
        };
        let s = resolve_secrets(get, "<test>");
        assert_eq!(
            s.udp_echo_port, 18000,
            "resolver-returned value must be used for UDP port"
        );
        assert_eq!(
            s.inbound_frames_port, 18002,
            "resolver-returned value must be used for inbound-frames port"
        );
    }

    // ── parse_ipv4 tests ──────────────────────────────────────────────────────

    /// Valid dotted-decimal IPv4 parses correctly.
    #[test]
    fn parse_ipv4_valid() {
        assert_eq!(parse_ipv4("192.168.1.50").unwrap(), [192, 168, 1, 50]);
        assert_eq!(parse_ipv4("0.0.0.0").unwrap(), [0, 0, 0, 0]);
        assert_eq!(parse_ipv4("255.255.255.255").unwrap(), [255, 255, 255, 255]);
    }

    /// Malformed input returns Err.
    #[test]
    fn parse_ipv4_invalid_returns_err() {
        assert!(parse_ipv4("not-an-ip").is_err());
        assert!(parse_ipv4("192.168.1").is_err()); // only 3 octets
        assert!(parse_ipv4("192.168.1.256").is_err()); // octet out of range
    }

    // ── PeerServers fixed-port bind tests ─────────────────────────────────────
    //
    // These tests bind on loopback (127.0.0.1) to avoid interfering with a real
    // hil-host running on 0.0.0.0 during development.  They exercise the "fixed
    // port, no :0 fallback" property (design §2.2, §2.4 / AC7).

    /// Binding the default UDP echo port on loopback succeeds.
    #[test]
    fn udp_echo_default_port_binds_on_loopback() {
        // Use a port in the test range to avoid clashing with a real hil-host;
        // we just verify the bind path works at a fixed port (not :0).
        let sock = UdpSocket::bind(("127.0.0.1", DEFAULT_HIL_UDP_PORT));
        // If another test or process already holds the port we skip rather than
        // fail — the important property is that we never fall back to :0.
        // Known limitation: when the port is in use the test exits without
        // asserting anything; the no-fallback property is covered separately by
        // `fixed_port_second_bind_fails` which uses an isolated test-only port.
        match sock {
            Ok(s) => {
                assert_eq!(
                    s.local_addr().unwrap().port(),
                    DEFAULT_HIL_UDP_PORT,
                    "bound port must equal the requested fixed port (no :0 fallback)"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                // Port legitimately in use; skip.
                eprintln!(
                    "SKIP udp_echo_default_port_binds_on_loopback: port {DEFAULT_HIL_UDP_PORT} already in use"
                );
            }
            Err(e) => panic!("unexpected bind error: {e}"),
        }
    }

    /// A second bind on the same fixed port fails (proves "no :0 fallback", AC7).
    ///
    /// Uses OS-assigned ephemeral port for the first bind so there is no
    /// pre-existing holder and no skip path — the test is unconditional.
    #[test]
    fn fixed_port_second_bind_fails() {
        // Bind on :0 to get an OS-assigned port, then immediately attempt to
        // re-bind the same port number.  The first listener is still live, so
        // the second bind must fail with EADDRINUSE regardless of which port
        // the OS chose.  No fixed port constant means no chance of a pre-existing
        // holder and no skip path.
        let first =
            TcpListener::bind(("127.0.0.1", 0)).expect("OS-assigned bind on loopback must succeed");
        let test_port = first.local_addr().unwrap().port();

        // Second bind on the same (now-occupied) port must fail.
        let second = TcpListener::bind(("127.0.0.1", test_port));
        assert!(
            second.is_err(),
            "second bind on the same fixed port must fail — no :0 fallback allowed (AC7)"
        );
        let err = second.unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::AddrInUse,
            "expected AddrInUse, got {err}"
        );
        // first listener stays live until here, keeping the port occupied.
        drop(first);
    }

    // ── --print-ports output format tests ────────────────────────────────────
    //
    // These tests use `super::format_port_lines` — the same function `print_ports()`
    // calls — so any change to the output format is automatically reflected in the
    // assertions.  This locks the single-source-of-truth contract (design §3.3):
    // drift between `print_ports()` and the firewall helper's parser is caught here
    // rather than in a field failure.

    /// With no overrides, the formatted output reflects the compiled-in defaults.
    #[test]
    fn print_ports_defaults_produce_correct_output() {
        let output = super::format_port_lines(
            DEFAULT_HIL_UDP_PORT,
            DEFAULT_HIL_INBOUND_FRAMES_PORT,
            DEFAULT_HIL_BACKPRESSURE_PORT,
            DEFAULT_HIL_POLL_READINESS_PORT,
            DEFAULT_HIL_RTD_PORT,
            DEFAULT_HIL_TLS_PSK_PORT,
            DEFAULT_HIL_TLS_PSK_BAD_PORT,
        );
        assert_eq!(
            output,
            format!(
                "udp_port={DEFAULT_HIL_UDP_PORT}/udp\ninbound_frames_port={DEFAULT_HIL_INBOUND_FRAMES_PORT}/tcp\nbackpressure_port={DEFAULT_HIL_BACKPRESSURE_PORT}/tcp\npoll_readiness_port={DEFAULT_HIL_POLL_READINESS_PORT}/tcp\nrtd_port={DEFAULT_HIL_RTD_PORT}/tcp\ntls_psk_port={DEFAULT_HIL_TLS_PSK_PORT}/tcp\ntls_psk_bad_port={DEFAULT_HIL_TLS_PSK_BAD_PORT}/tcp\n"
            ),
            "--print-ports default output must be `udp_port=17380/udp\\ninbound_frames_port=17382/tcp\\nbackpressure_port=17383/tcp\\npoll_readiness_port=17384/tcp\\nrtd_port=17385/tcp\\ntls_psk_port=17386/tcp\\ntls_psk_bad_port=17387/tcp\\n`"
        );
        // Verify the exact documented defaults.
        assert!(
            output.contains("udp_port=17380"),
            "default UDP port line must be `udp_port=17380`"
        );
        assert!(
            output.contains("inbound_frames_port=17382"),
            "default inbound-frames port line must be `inbound_frames_port=17382`"
        );
        assert!(
            output.contains("backpressure_port=17383"),
            "default backpressure port line must be `backpressure_port=17383`"
        );
        assert!(
            output.contains("poll_readiness_port=17384"),
            "default poll-readiness port line must be `poll_readiness_port=17384`"
        );
        assert!(
            output.contains("rtd_port=17385"),
            "default rtd port line must be `rtd_port=17385`"
        );
        assert!(
            output.contains("tls_psk_port=17386"),
            "default TLS-PSK port line must be `tls_psk_port=17386`"
        );
        assert!(
            output.contains("tls_psk_bad_port=17387"),
            "default wrong-key TLS-PSK port line must be `tls_psk_bad_port=17387`"
        );
        // Each line carries its firewall protocol so the helper never encodes a
        // port-name-to-proto table: udp_port is the sole `/udp`, all others `/tcp`.
        assert!(
            output.contains("udp_port=17380/udp"),
            "udp port line must carry the `/udp` proto suffix"
        );
        assert!(
            output.contains("rtd_port=17385/tcp"),
            "rtd port line must carry the `/tcp` proto suffix"
        );
        assert_eq!(
            output.matches("/udp").count(),
            1,
            "exactly one port (udp_port) is UDP; every other line must be `/tcp`"
        );
        assert_eq!(
            output.matches("/tcp").count(),
            6,
            "all six non-echo-UDP ports must be emitted as `/tcp`"
        );
    }

    /// With overrides, the formatted output reflects the overridden values.
    ///
    /// This is the core AC8 cross-agreement assertion: the helper opens whatever
    /// `--print-ports` prints, so if overrides change the printed value the
    /// helper opens the overridden port (not the default).
    #[test]
    fn print_ports_overrides_produce_correct_output() {
        let output = super::format_port_lines(18000, 18002, 18003, 18004, 18005, 18006, 18007);
        assert!(
            output.contains("udp_port=18000"),
            "overridden UDP port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("inbound_frames_port=18002"),
            "overridden inbound-frames port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("backpressure_port=18003"),
            "overridden backpressure port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("poll_readiness_port=18004"),
            "overridden poll-readiness port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("rtd_port=18005"),
            "overridden rtd port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("tls_psk_port=18006"),
            "overridden TLS-PSK port must appear verbatim in --print-ports output"
        );
        assert!(
            output.contains("tls_psk_bad_port=18007"),
            "overridden wrong-key TLS-PSK port must appear verbatim in --print-ports output"
        );
    }

    /// `--print-ports` output via `resolve_secrets` uses the same resolved values
    /// that `PeerServers::start` receives (single source of truth, design §3.3).
    #[test]
    fn print_ports_uses_resolved_secrets() {
        // Simulate an override for UDP only.
        let get = |key: &str| -> Option<String> {
            match key {
                "RESPEAKER_HIL_UDP_PORT" => Some("19000".to_string()),
                _ => None,
            }
        };
        let secrets = resolve_secrets(get, "<test>");
        let output = super::format_port_lines(
            secrets.udp_echo_port,
            secrets.inbound_frames_port,
            secrets.backpressure_port,
            secrets.poll_readiness_port,
            secrets.rtd_port,
            secrets.tls_psk_port,
            secrets.tls_psk_bad_port,
        );
        assert!(
            output.contains("udp_port=19000"),
            "--print-ports must reflect UDP override resolved via resolve_secrets"
        );
        assert!(
            output.contains(&format!(
                "inbound_frames_port={DEFAULT_HIL_INBOUND_FRAMES_PORT}"
            )),
            "--print-ports must use default inbound-frames port when only UDP is overridden"
        );
        assert!(
            output.contains(&format!(
                "backpressure_port={DEFAULT_HIL_BACKPRESSURE_PORT}"
            )),
            "--print-ports must use default backpressure port when only UDP is overridden"
        );
        assert!(
            output.contains(&format!(
                "poll_readiness_port={DEFAULT_HIL_POLL_READINESS_PORT}"
            )),
            "--print-ports must use default poll-readiness port when only UDP is overridden"
        );
    }

    // ── eval_playback_drain_rate tests (design §5 "HIL self-tests") ──
    //
    // Pure evaluator tests: they verify the host-side raw-drain-rate metric and saturation
    // selection, not the hardware. A saturated feed draining at ≥ real-time PASSES; a saturated
    // feed draining below real-time FAILS; and the precondition/parse-guard cases fail loudly.
    // Lines use the exact production `capture: playback tx …` format (firmware `capture.rs`),
    // the same fixture style as `capture_lines` above. Real-time is 50 write-units/s (32 000 B/s
    // ÷ 640 B/write-unit), so a 1 s window draining 50 `chunks` reads exactly 1.0×.

    /// A fully-valid device verdict: the handler ran and fed audio. `feed_full` is
    /// overridable so the "no saturated window" split can be exercised both ways.
    fn drain_verdict(feed_full: u32) -> TestData {
        TestData::PlaybackDrainRate {
            chunks_fed: 250,
            feed_full,
            feed_ms: 5000,
            tx_wf: 0,
        }
    }

    /// One production-format `capture: playback tx …` periodic line. Only the eval-scored tokens
    /// vary (`chunks`, the saturation pair, `rx_window_us`); `write_us`/`max_backlog`/`rx_frames`
    /// carry fixed realistic values (`write_us` ≈ 0 under NON_BLOCK TX) so the fixture still
    /// matches the real wire format the eval no longer parses those from.
    /// The line-1 half only. Callers that need a complete window use [`drain_line`], which
    /// appends the paired obs line the eval correlates.
    fn drain_tx_line(
        chunks: u64,
        nonempty_polls: u64,
        empty_polls: u64,
        rx_window_us: u64,
    ) -> String {
        format!(
            "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
             {RX_WINDOW_US}{rx_window_us} {CHUNKS}{chunks} write_us(mean/max)=28/45 max_backlog=1 \
             {NONEMPTY_POLLS}{nonempty_polls} {POLL_EMPTY}{empty_polls}"
        )
    }

    /// One production-format `capture: playback obs …` line — the line-2 half of a window.
    fn drain_obs_line(preroll_waits: u64, preroll_rearms: u64) -> String {
        format!(
            "[device Info] respeaker_pod: {CAPTURE_OBS_LINE}writefail=0 \
             {PREROLL_WAITS}{preroll_waits} {PREROLL_REARMS}{preroll_rearms} resume_unmutes=0 \
             eoa_mutes=0 {RX_WIN_OK}1 {RX_DEFICIT}0 {PRIO}10 {CORE}1"
        )
    }

    /// A complete window: the tx line plus its adjacent obs line, as the firmware emits them.
    /// Pre-roll counters are clean; [`preroll_window`] builds the contaminated variant.
    fn drain_line(
        chunks: u64,
        nonempty_polls: u64,
        empty_polls: u64,
        rx_window_us: u64,
    ) -> Vec<String> {
        vec![
            drain_tx_line(chunks, nonempty_polls, empty_polls, rx_window_us),
            drain_obs_line(0, 0),
        ]
    }

    /// A window that looks saturated on line 1 but carries nonzero pre-roll counters on line 2 —
    /// the post-underrun refill window the exclusion filter exists to drop.
    fn preroll_window(
        chunks: u64,
        rx_window_us: u64,
        preroll_waits: u64,
        preroll_rearms: u64,
    ) -> Vec<String> {
        vec![
            drain_tx_line(chunks, 200, 0, rx_window_us),
            drain_obs_line(preroll_waits, preroll_rearms),
        ]
    }

    /// A fully-saturated window (`empty_polls=0`, ample non-empty polls) draining `chunks`
    /// write-units over `rx_window_us`. The common case for the rate tests.
    fn sat_line(chunks: u64, rx_window_us: u64) -> Vec<String> {
        drain_line(chunks, 200, 0, rx_window_us)
    }

    /// Healthy saturated windows draining at ≥ real-time (≈50 write-units per 1 s window) → PASS.
    /// The first window is dropped as warmup, so we supply a warmup window plus ≥1 healthy
    /// saturated window.
    #[test]
    fn eval_playback_drain_rate_healthy_realtime_passes() {
        let logs = [
            // Warmup/ring-fill window (dropped); its figures are irrelevant to the verdict.
            sat_line(40, 1_000_000),
            // Healthy saturated steady-state: 50–51 write-units over 1 s ⇒ ≈1.0–1.02× real-time.
            sat_line(51, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "saturated windows draining at ≥ real-time must PASS: {result:?}"
        );
    }

    /// Saturated windows draining well below real-time (the pre-fix latch / a drain regression):
    /// 30 write-units over a 1 s window is ~0.6× real-time. The eval must FAIL, naming the
    /// keep-up shortfall.
    #[test]
    fn eval_playback_drain_rate_slow_drain_below_realtime_fails() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            // 30 chunks × 640 B / 1 s = 19 200 B/s = 0.60× real-time.
            sat_line(30, 1_000_000),
            sat_line(31, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(200), &logs);
        let err = result.expect_err("a sub-real-time drain must FAIL the keep-up floor");
        assert!(
            err.contains("did not sustain real-time drain") && err.contains("keep_up=0.6"),
            "failure must name the sustained-drain shortfall and surface the ~0.6× keep-up: {err}"
        );
    }

    /// Idle / non-saturated windows (`empty_polls` nonzero, no drain activity) are discarded —
    /// the eval must not read a rate from them. With feed_full=0 and no saturated window, that is
    /// the healthy "drain kept up" PASS.
    #[test]
    fn eval_playback_drain_rate_idle_window_discarded() {
        let logs = [
            drain_line(0, 0, 5, 1_000_000), // warmup (dropped)
            drain_line(0, 0, 5, 1_000_000), // idle: empty_polls≠0 ⇒ not saturated, discarded
        ]
        .concat();
        // feed_full=0 ⇒ the feed never had to back off ⇒ drain kept up ⇒ healthy PASS.
        let result = eval_playback_drain_rate(&drain_verdict(0), &logs);
        assert!(
            result.is_ok(),
            "an idle window with feed_full=0 must be treated as drain-kept-up (PASS): {result:?}"
        );
    }

    /// The raw-drain rate divides by the window's **real** elapsed duration, not a nominal 1 s.
    /// A window draining 50 write-units over a stretched 1.3 s span is 0.77× real-time (FAIL); a
    /// nominal-1 s divisor would read it as exactly 1.0× and wrongly PASS. Pins `rx_window_us` as
    /// load-bearing and guards against a regression back to a frames-÷-1 s shortcut.
    #[test]
    fn eval_playback_drain_rate_rate_uses_real_window_duration() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            // 50 chunks × 640 B / 1.3 s = 24 615 B/s = 0.77× real-time — FAIL. Under a nominal-1 s
            // divisor this would read 32 000 B/s = 1.0× and PASS.
            sat_line(50, 1_300_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(150), &logs);
        let err = result.expect_err(
            "50 units over a real 1.3 s window is 0.77× real-time and must FAIL — the rate must \
             divide by the real duration, not a nominal 1 s",
        );
        assert!(
            err.contains("keep_up=0.77") && err.contains("over 1300ms"),
            "the rate must use the real 1.3 s window (0.77×), not a nominal 1 s (1.0×): {err}"
        );
    }

    /// Saturation requires `empty_polls == 0` (design §5 "ring non-empty at every pass start").
    /// A window with even one empty poll is NOT scored — it is not fully saturated. Here the only
    /// post-warmup window has empty_polls=1 and a healthy drain rate; because it is excluded and
    /// feed_full>0, the eval reports uninterpretable rather than scoring it.
    #[test]
    fn eval_playback_drain_rate_saturation_requires_empty_polls_zero() {
        let logs = [
            sat_line(50, 1_000_000), // warmup (dropped)
            // empty_polls=1 ⇒ the ring emptied on at least one pass ⇒ not fully saturated, even
            // though the drain rate would be healthy. Excluded from scoring.
            drain_line(50, 200, 1, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(200), &logs);
        let err = result.expect_err(
            "a window with empty_polls≠0 is not fully saturated and must not be scored; with \
             feed_full>0 the eval is uninterpretable",
        );
        assert!(
            err.contains("never saturated") || err.contains("uninterpretable"),
            "an empty_polls≠0 window must be excluded, leaving the eval uninterpretable: {err}"
        );
    }

    /// No saturated window but feed_full=0 ⇒ the drain kept up with an at-least-real-time feed
    /// ⇒ healthy PASS. Distinct from the uninterpretable case purely on feed_full.
    ///
    /// A genuinely-keeping-up consumer empties the ring often, so its outer passes frequently find
    /// it empty — `empty_polls` is nonzero and the `empty_polls == 0` saturation test correctly
    /// rejects these as not-saturated.
    #[test]
    fn eval_playback_drain_rate_unsaturated_feed_full_zero_is_healthy() {
        let logs = [
            drain_line(50, 10, 40, 1_000_000), // warmup (dropped)
            drain_line(50, 10, 40, 1_000_000), // keeping up: empties present ⇒ not saturated
            drain_line(50, 10, 40, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(0), &logs);
        assert!(
            result.is_ok(),
            "a keeping-up feed (empties present) with feed_full=0 is the healthy state: {result:?}"
        );
    }

    /// Insufficient saturated windows (only the warmup window saturates, nothing after) with
    /// feed_full>0 must fail-fast as uninterpretable rather than computing on insufficient data.
    #[test]
    fn eval_playback_drain_rate_insufficient_saturated_windows_fails_fast() {
        let logs = [
            sat_line(50, 1_000_000), // the only saturated window — but it's warmup
            // post-warmup, NOT saturated: empties present (nonempty=5/empty=45), so excluded.
            drain_line(45, 5, 45, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(99), &logs);
        let err = result.expect_err("zero post-warmup saturated windows must fail-fast");
        assert!(
            err.contains("never saturated") || err.contains("uninterpretable"),
            "must fail with the uninterpretable/feed-never-saturated message: {err}"
        );
    }

    /// A periodic line carrying the prefix but missing a scored token is a format drift: the eval
    /// must FAIL loudly, not silently skip the line. Each of `chunks`, `rx_window_us`,
    /// `nonempty_polls`, and `empty_polls` is guarded.
    #[test]
    fn eval_playback_drain_rate_missing_token_fails_not_silent() {
        // `chunks=` absent (all other tokens present).
        let missing_chunks = vec![format!(
            "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
             {RX_WINDOW_US}1000000 write_us(mean/max)=28/45 max_backlog=1 {NONEMPTY_POLLS}200 \
             {POLL_EMPTY}0"
        )];
        let err = eval_playback_drain_rate(&drain_verdict(0), &missing_chunks)
            .expect_err("a line missing chunks= must fail loudly");
        assert!(
            err.contains(CHUNKS),
            "error must name the missing {CHUNKS} token: {err}"
        );

        // `rx_window_us=` absent — the load-bearing rate divisor.
        let missing_window = vec![format!(
            "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
             {CHUNKS}50 write_us(mean/max)=28/45 max_backlog=1 {NONEMPTY_POLLS}200 {POLL_EMPTY}0"
        )];
        let err = eval_playback_drain_rate(&drain_verdict(0), &missing_window)
            .expect_err("a line missing rx_window_us= must fail loudly");
        assert!(
            err.contains(RX_WINDOW_US),
            "error must name the missing {RX_WINDOW_US} token: {err}"
        );

        // `nonempty_polls=` absent.
        let missing_nonempty = vec![format!(
            "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
             {RX_WINDOW_US}1000000 {CHUNKS}50 write_us(mean/max)=28/45 max_backlog=1 {POLL_EMPTY}0"
        )];
        let err = eval_playback_drain_rate(&drain_verdict(0), &missing_nonempty)
            .expect_err("a line missing nonempty_polls= must fail loudly");
        assert!(
            err.contains(NONEMPTY_POLLS),
            "error must name the missing {NONEMPTY_POLLS} token: {err}"
        );

        // Poll-empty token absent.
        let missing_empty = vec![format!(
            "[device Info] respeaker_pod: {CAPTURE_TX_LINE}rx_frames=16000 \
             {RX_WINDOW_US}1000000 {CHUNKS}50 write_us(mean/max)=28/45 max_backlog=1 \
             {NONEMPTY_POLLS}200"
        )];
        let err = eval_playback_drain_rate(&drain_verdict(0), &missing_empty)
            .expect_err("a line missing the poll-empty token must fail loudly");
        // `POLL_EMPTY` is not a substring of `NONEMPTY_POLLS` (guarded in `log_tokens`), so its
        // absence is detected rather than silently satisfied by the non-empty field's text.
        assert!(
            err.contains(POLL_EMPTY),
            "error must name the missing {POLL_EMPTY} token: {err}"
        );
    }

    /// The device verdict data is the gate: anything that is not this test's own variant must
    /// fail before any drain figure is read. Succeeds the four string-era cases (wrong `FAIL`
    /// prefix, missing `src=`/`feed_full=`/`chunks_fed=` tokens): a fail path now carries
    /// `TestData::None`, and a variant either has all its fields or does not compile, so the
    /// three "missing token" shapes collapse into the wrong-variant case.
    #[test]
    fn eval_playback_drain_rate_bad_verdict_rejected() {
        let healthy = [sat_line(40, 1_000_000), sat_line(50, 1_000_000)].concat();
        let fail_data = eval_playback_drain_rate(&TestData::None, &healthy)
            .expect_err("a device-side failure (TestData::None) must be rejected");
        assert!(fail_data.contains("PlaybackDrainRate"), "{fail_data}");

        let wrong_variant = eval_playback_drain_rate(
            &TestData::FullDuplexRxIntegrity {
                chunks_fed: 1,
                feed_full: 0,
                feed_ms: 5000,
            },
            &healthy,
        )
        .expect_err("another test's variant must be rejected");
        assert!(
            wrong_variant.contains("PlaybackDrainRate"),
            "{wrong_variant}"
        );
    }

    /// The warmup window (index 0) is dropped even when it is itself saturated. Here the warmup
    /// window drains below real-time (30 units/1 s = 0.6×); the post-warmup windows are healthy.
    /// The verdict must be PASS — proving the warmup drain did not contaminate the rate. If the
    /// `&windows[1..]` slice were removed, the slow warmup window would drag the aggregate rate
    /// below the keep-up floor and FAIL.
    #[test]
    fn eval_playback_drain_rate_warmup_window_excluded() {
        let logs = [
            // Warmup: saturated but slow (0.6× real-time). Must be excluded purely because it is
            // index 0.
            sat_line(30, 1_000_000),
            // Post-warmup: healthy (≥ real-time).
            sat_line(51, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "the warmup window's sub-real-time drain must be excluded; post-warmup is healthy ⇒ \
             PASS: {result:?}"
        );
    }

    /// A one-window run (only the warmup arrived) must fail loudly with "collected N, need ≥ K",
    /// NOT fall through to a false "drain kept up" PASS. After the warmup drop there is zero
    /// steady-state data. Critically this must fire even with feed_full=0 (the path that would
    /// otherwise PASS).
    #[test]
    fn eval_playback_drain_rate_single_window_fails_loudly() {
        let logs = [sat_line(50, 1_000_000)].concat();
        // feed_full=0 is the path that would have wrongly PASSed before the collected-count guard.
        let result = eval_playback_drain_rate(&drain_verdict(0), &logs);
        let err = result.expect_err(
            "a single (warmup-only) window must fail loudly, not PASS as drain-kept-up",
        );
        assert!(
            err.contains("collected") && err.contains("need ≥"),
            "must name the insufficient-window count, distinct from the feed_full split: {err}"
        );
    }

    /// A contradictory device verdict (`chunks_fed=0`) with periodic windows present must fail
    /// loudly as uninterpretable, not be buried in the measured data.
    #[test]
    fn eval_playback_drain_rate_zero_chunks_fed_is_uninterpretable() {
        let logs = [sat_line(40, 1_000_000), sat_line(50, 1_000_000)].concat();
        let result = eval_playback_drain_rate(
            &TestData::PlaybackDrainRate {
                chunks_fed: 0,
                feed_full: 0,
                feed_ms: 5000,
                tx_wf: 0,
            },
            &logs,
        );
        let err =
            result.expect_err("chunks_fed=0 with periodic windows is a contradiction — must fail");
        assert!(
            err.contains("chunks_fed=0") && err.contains("never accepted"),
            "must name the contradictory chunks_fed=0 feed state: {err}"
        );
    }

    /// A cold-start ramp-up window must be dropped by the minimum-saturated-poll gate, not scored.
    /// When the consumer is still parked in `preroll_waits`, only ONE outer pass finds data
    /// (`nonempty_polls=1`, `empty_polls=0`, `chunks=2`); that passes the `empty_polls == 0`
    /// saturation test on a sample of ONE, scoring a never-saturated slow window and sinking the
    /// verdict. The minimum-saturated-poll gate must DROP it so the following genuinely-saturated
    /// healthy windows decide the verdict ⇒ PASS.
    #[test]
    fn eval_playback_drain_rate_cold_ramp_up_window_dropped_not_scored() {
        let logs = [
            // Warmup (dropped by index).
            sat_line(40, 1_000_000),
            // Cold ramp-up: nonempty_polls=1 passes `empty_polls==0` on a one-poll sample but is
            // NOT saturated — the min-poll gate drops it. chunks=2 is the sub-real-time cold figure
            // that would otherwise sink the verdict.
            drain_line(2, 1, 0, 1_000_000),
            // Genuinely-saturated healthy steady-state windows.
            sat_line(50, 1_000_000),
            sat_line(51, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "the cold ramp-up window (nonempty_polls=1, chunks=2) must be dropped, not scored — \
             the healthy saturated windows decide the verdict: {result:?}"
        );
    }

    /// The min-poll gate must NOT mask a genuine drain regression. A window that IS saturated —
    /// a HIGH poll count (the ring stayed full at every consumer arrival) — but drains too slowly
    /// is a real defect and must STILL be scored and STILL fail. The gate keys on saturation
    /// ACTIVITY (`nonempty_polls`), not on the measured `chunks`, so this window clears the floor
    /// and reaches the sub-real-time FAIL. This is the inverse of the cold ramp-up window (low
    /// poll count) the gate correctly drops.
    #[test]
    fn eval_playback_drain_rate_saturated_slow_regression_still_scored() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            // Saturated (200 non-empty polls ≫ floor) but only 30 chunks/window ⇒ 0.6× real-time,
            // a real regression. Must NOT be dropped by the poll gate.
            sat_line(30, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(180), &logs);
        let err = result.expect_err(
            "a saturated window (high poll count) that drains too slowly is a real regression — \
             the min-poll gate must not drop it",
        );
        assert!(
            err.contains("did not sustain real-time drain"),
            "the saturated-but-slow window must reach the sub-real-time FAIL, not be silently \
             dropped by the poll gate: {err}"
        );
    }

    /// If the minimum-saturated-poll gate drops EVERY post-warmup window (a cold run that
    /// collected only warmup + ramp-up windows), the eval must report an inconclusive/
    /// insufficient-data failure, never a silent pass on zero scored windows. A ramp-up run has
    /// the consumer parked in `preroll` while the feed fills the ring, so `feed_full` climbs (>0)
    /// ⇒ the uninterpretable branch fires.
    #[test]
    fn eval_playback_drain_rate_all_ramp_up_windows_inconclusive_not_silent_pass() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped by index)
            // Post-warmup but cold ramp-up: nonempty_polls below the floor ⇒ dropped by the gate.
            drain_line(2, 1, 0, 1_000_000),
            drain_line(3, 2, 0, 1_000_000),
        ]
        .concat();
        // feed_full>0: the feed backed off while the consumer ramped up ⇒ uninterpretable.
        let result = eval_playback_drain_rate(&drain_verdict(200), &logs);
        let err = result.expect_err(
            "a run with only ramp-up windows (all dropped by the poll gate) must fail \
             inconclusive, not silently pass on zero scored windows",
        );
        assert!(
            err.contains("never saturated") || err.contains("uninterpretable"),
            "must report the inconclusive/uninterpretable failure, not a silent pass: {err}"
        );
    }

    /// Boundary of the min-saturated-poll gate: a window AT the floor
    /// (`nonempty_polls == PLAYBACK_DRAIN_MIN_SATURATED_POLLS`) is scored; one just BELOW is
    /// dropped. Both windows are fully saturated (`empty_polls=0`) and drain below real-time, so
    /// only the sample-size floor distinguishes them — pinning the comparison is `>=`, not `>`.
    #[test]
    fn eval_playback_drain_rate_min_saturated_polls_boundary() {
        let at_floor = PLAYBACK_DRAIN_MIN_SATURATED_POLLS;
        let below_floor = PLAYBACK_DRAIN_MIN_SATURATED_POLLS - 1;
        // At the floor: scored. A sub-real-time drain (30 units/1 s = 0.6×) ⇒ FAIL (proves scored).
        let logs_at = [
            sat_line(40, 1_000_000), // warmup
            drain_line(30, at_floor, 0, 1_000_000),
        ]
        .concat();
        let err = eval_playback_drain_rate(&drain_verdict(200), &logs_at).expect_err(
            "a window at the poll floor must be scored and FAIL the sub-real-time drain",
        );
        assert!(
            err.contains("did not sustain real-time drain"),
            "a window with nonempty_polls == floor must be scored (reach the rate verdict): {err}"
        );
        // Just below the floor: dropped. With feed_full=0 and no other saturated window, the eval
        // falls to the healthy keep-up PASS — proving the window was excluded (had it been scored,
        // the 0.6× drain would FAIL).
        let logs_below = [
            sat_line(40, 1_000_000), // warmup
            drain_line(30, below_floor, 0, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(0), &logs_below);
        assert!(
            result.is_ok(),
            "a window just below the poll floor must be dropped (not scored); with feed_full=0 \
             the eval reports healthy keep-up: {result:?}"
        );
    }

    /// A post-underrun refill window must be excluded from the saturated set. On line 1 it is
    /// indistinguishable from a saturated window (200 non-empty polls, zero empty polls) but its
    /// `chunks` is depressed because the consumer was parked on the pre-roll gate — line 2's
    /// `preroll_waits` is the only discriminator. The genuine windows must decide the verdict.
    #[test]
    fn eval_playback_drain_rate_refill_window_excluded() {
        // `preroll_rearms=0` isolates the `preroll_waits` term: dropping that half of the
        // predicate must fail this test rather than be covered by the rearms half
        // (`..._rearm_window_excluded` covers rearms-only).
        let refill = preroll_window(5, 1_000_000, 40, 0);
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            refill.clone(),
            sat_line(51, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "a refill window (preroll_waits>0, depressed chunks) must be excluded so the genuine \
             saturated windows decide the verdict: {result:?}"
        );

        // The fixture is load-bearing: with the refill window's obs line reporting clean
        // pre-roll counters (i.e. without the discriminator) the same figures sink the
        // aggregate below the keep-up floor — exactly the misattributed FAIL this filter prevents.
        let logs_undiscriminated = [
            sat_line(40, 1_000_000),
            drain_line(5, 200, 0, 1_000_000),
            sat_line(51, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let err = eval_playback_drain_rate(&drain_verdict(120), &logs_undiscriminated)
            .expect_err("without the preroll discriminator the same window must sink the verdict");
        assert!(
            err.contains("did not sustain real-time drain"),
            "the depressed window must be capable of failing the aggregate — otherwise the \
             exclusion test proves nothing: {err}"
        );
    }

    /// The `preroll_rearms` half of the exclusion: a window where a mid-stream underrun re-armed
    /// the gate and the ring refilled before any first poll found it empty reads
    /// `preroll_waits=0` yet still spent part of the window not draining. It must be excluded.
    #[test]
    fn eval_playback_drain_rate_rearm_window_excluded() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            // Depressed drain (0.1×) that would fail the aggregate if scored.
            preroll_window(5, 1_000_000, 0, 1),
            sat_line(51, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "a rearm window (preroll_waits=0, preroll_rearms>0) must be excluded: {result:?}"
        );
    }

    /// The exclusion must not swallow a genuine regression: a saturated window with clean
    /// pre-roll counters draining below real-time still scores and still FAILs.
    #[test]
    fn eval_playback_drain_rate_clean_obs_regression_still_fails() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            sat_line(30, 1_000_000), // 0.6× real-time, obs clean ⇒ scored
        ]
        .concat();
        let err = eval_playback_drain_rate(&drain_verdict(180), &logs).expect_err(
            "a saturated window with clean pre-roll counters is a genuine regression and must FAIL",
        );
        assert!(
            err.contains("did not sustain real-time drain"),
            "the clean-obs slow window must reach the rate verdict: {err}"
        );
    }

    /// A dropped obs log frame leaves its window uncorrelated. An uncorrelated window cannot be
    /// proven refill-free, so it is excluded — the remaining windows still score, and the
    /// exclusion is reported so an operator can attribute it to log loss.
    #[test]
    fn eval_playback_drain_rate_missing_obs_line_window_excluded() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            // Obs frame lost: only the tx half arrives, with a depressed drain that would sink
            // the aggregate if scored. The NEXT window's obs line must attach to that next
            // window (pairing self-heals), not back-fill this one.
            vec![drain_tx_line(5, 200, 0, 1_000_000)],
            // Exactly one following window, so attachment position decides the verdict: correct
            // forward pairing leaves this window clean-and-fast (PASS) while the tx-only window
            // stays uncorrelated and excluded. Back-fill or nearest-preceding semantics would
            // instead score the depressed window at 0.1× and strand this one obs-less — the only
            // saturated window would then be the slow one and the run would FAIL.
            sat_line(50, 1_000_000),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(120), &logs);
        assert!(
            result.is_ok(),
            "a window whose obs line was lost must be excluded and the next obs line must attach \
             forward to the next window, leaving it to decide the verdict: {result:?}"
        );
    }

    /// The exclusion-count fold that feeds both the per-run `println!` and the Step-4 error,
    /// exercised directly so the PASS-path reporting contract is covered (it is unreachable
    /// through the eval's return value).
    #[test]
    fn exclusion_counts_itemizes_only_otherwise_saturated_windows() {
        let window = |nonempty: u64, empty: u64, obs: Option<DrainObs>| DrainWindow {
            chunks: 50,
            nonempty_polls: nonempty,
            empty_polls: empty,
            rx_window_us: 1_000_000,
            obs,
        };
        let clean = DrainObs {
            preroll_waits: 0,
            preroll_rearms: 0,
        };
        let windows = [
            // Saturated + pre-roll contaminated: one via waits, one via rearms.
            window(
                200,
                0,
                Some(DrainObs {
                    preroll_waits: 40,
                    preroll_rearms: 0,
                }),
            ),
            window(
                200,
                0,
                Some(DrainObs {
                    preroll_waits: 0,
                    preroll_rearms: 1,
                }),
            ),
            // Saturated, obs frame lost.
            window(200, 0, None),
            // Saturated and clean — scored, counted in neither bucket.
            window(200, 0, Some(clean)),
            // Not line-1-saturated (empty polls / too few polls): never a candidate, so counted
            // in neither bucket regardless of its obs state.
            window(200, 3, None),
            window(1, 0, None),
        ];
        assert_eq!(
            exclusion_counts(&windows),
            (2, 1),
            "only otherwise-saturated windows are itemized, split by reason"
        );
    }

    /// Every post-warmup window excluded for pre-roll activity with feed_full>0 routes to the
    /// existing uninterpretable error, whose message now itemizes the exclusion reasons so an
    /// operator can tell a transient-heavy run from device→host log loss.
    #[test]
    fn eval_playback_drain_rate_all_excluded_feed_full_reports_reasons() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            preroll_window(5, 1_000_000, 40, 0),
            vec![drain_tx_line(5, 200, 0, 1_000_000)], // obs frame lost
        ]
        .concat();
        let err = eval_playback_drain_rate(&drain_verdict(200), &logs)
            .expect_err("all post-warmup windows excluded with feed_full>0 is uninterpretable");
        assert!(
            err.contains("preroll-excluded=1") && err.contains("obs-missing=1"),
            "the error must itemize per-reason exclusion counts: {err}"
        );
    }

    /// The same all-excluded state with feed_full=0 stays on the existing healthy arm: the feed
    /// never had to back off, so the consumer drained at least as fast as it was fed.
    #[test]
    fn eval_playback_drain_rate_all_excluded_feed_full_zero_is_healthy() {
        let logs = [
            sat_line(40, 1_000_000), // warmup (dropped)
            preroll_window(5, 1_000_000, 40, 0),
        ]
        .concat();
        let result = eval_playback_drain_rate(&drain_verdict(0), &logs);
        assert!(
            result.is_ok(),
            "all-excluded with feed_full=0 is the existing healthy keep-up arm: {result:?}"
        );
    }

    /// An obs line with no un-paired window (here: before any tx line) means the device's emit
    /// order/contract drifted. The eval must fail loudly rather than guess an attachment.
    #[test]
    fn eval_playback_drain_rate_orphaned_obs_line_fails_loudly() {
        let logs = [
            vec![drain_obs_line(0, 0)], // orphan: no window to attach to
            sat_line(40, 1_000_000),
            sat_line(50, 1_000_000),
        ]
        .concat();
        let err = eval_playback_drain_rate(&drain_verdict(120), &logs)
            .expect_err("an orphaned obs line must fail loudly, not be silently dropped");
        assert!(
            err.contains("orphaned"),
            "the error must name the orphaned obs line / emit-order drift: {err}"
        );
    }

    /// An obs line missing either pre-roll token is a format drift, held to the same fail-loud
    /// discipline as the line-1 tokens — silently treating it as "no pre-roll activity" would
    /// re-open the misattributed-FAIL this change closes.
    #[test]
    fn eval_playback_drain_rate_missing_obs_token_fails_not_silent() {
        for token in [PREROLL_WAITS, PREROLL_REARMS] {
            // Token name elided (missing), and token present with an unparseable value — both
            // halves of the "missing/invalid" contract, since a lossy parse of either would read
            // as "no pre-roll activity".
            let mangled = [
                drain_obs_line(0, 0).replace(token, "elided="),
                drain_obs_line(0, 0).replace(&format!("{token}0"), &format!("{token}-1")),
                drain_obs_line(0, 0).replace(&format!("{token}0"), &format!("{token}x")),
                drain_obs_line(0, 0).replace(&format!("{token}0 "), &format!("{token} ")),
            ];
            for line in mangled {
                let logs = [sat_line(40, 1_000_000), vec![line.clone()]].concat();
                let err = eval_playback_drain_rate(&drain_verdict(120), &logs).expect_err(
                    "an obs line with a missing or invalid pre-roll token must fail loudly",
                );
                assert!(
                    err.contains(token),
                    "the error must name the {token} token: {err} (line: {line})"
                );
            }
        }
    }

    // ── eval_full_duplex_rx_integrity tests (design §5 "HIL self-tests") ──
    //
    // Pure evaluator tests: they verify the host-side mic-RX-deficit assertion and its
    // saturation/warmup gating, not the hardware. Under a saturating feed (feed_full>0) every
    // post-warmup `capture: playback obs …` window must report rx_deficit=0; a nonzero deficit
    // FAILS and the precondition/parse-guard cases fail loudly. Lines use the exact production
    // `capture: playback obs …` format (firmware `capture.rs`).

    /// A fully-valid device verdict: the handler ran, fed audio, and the feed saturated the
    /// ring. `feed_full` is overridable so the saturation-validity guard can be exercised.
    fn rx_verdict(feed_full: u32) -> TestData {
        TestData::FullDuplexRxIntegrity {
            chunks_fed: 250,
            feed_full,
            feed_ms: 5000,
        }
    }

    /// One production-format `capture: playback obs …` periodic line carrying `rx_deficit`, with
    /// the capture thread reporting the expected core-1 pin and priority-10 elevation.
    fn obs_line(rx_deficit: u64) -> String {
        obs_line_pin(rx_deficit, CAPTURE_EXPECTED_PRIO, CAPTURE_EXPECTED_CORE)
    }

    /// Same, but with the reported `prio`/`core` overridable so the pin-regression guard can be
    /// exercised. The other tokens hold fixed realistic values the RX eval does not score.
    fn obs_line_pin(rx_deficit: u64, prio: u64, core: u64) -> String {
        obs_line_full(rx_deficit, prio, core, 1)
    }

    /// The full obs-line skeleton with every scored token overridable, so every fixture routes
    /// through one copy of the production line format. `win_ok` selects the measurement-validity
    /// token: `1` = real measurement, `0` = tone-test-suppressed window.
    fn obs_line_full(rx_deficit: u64, prio: u64, core: u64, win_ok: u64) -> String {
        format!(
            "[device Info] respeaker_pod: {CAPTURE_OBS_LINE}writefail=0 preroll_waits=0 \
             preroll_rearms=0 resume_unmutes=1 eoa_mutes=1 {RX_WIN_OK}{win_ok} \
             {RX_DEFICIT}{rx_deficit} {PRIO}{prio} {CORE}{core}"
        )
    }

    /// A tone-test-suppressed window: `rx_win_ok=0` with a suppressed `rx_deficit=0` (the device
    /// forces the deficit to 0 on any window that ran a tone test). The host must exclude it from
    /// scoring rather than count it as a clean pass. Pin tokens are the expected values so the
    /// per-line pin assertion still holds.
    fn obs_line_suppressed() -> String {
        obs_line_full(0, CAPTURE_EXPECTED_PRIO, CAPTURE_EXPECTED_CORE, 0)
    }

    /// A suppressed window (`rx_win_ok=0`) interleaved with valid clean windows does not spuriously
    /// fail-close a healthy run: with three valid clean windows around it the run still PASSes. (A
    /// suppressed line's deficit is 0 by construction, so this cannot distinguish exclusion from a
    /// no-op; the floor test below is the real proof that exclusion affects the valid-window count.)
    #[test]
    fn eval_full_duplex_rx_integrity_suppressed_window_does_not_fail_healthy_run() {
        let logs = vec![obs_line(0), obs_line(0), obs_line_suppressed(), obs_line(0)];
        let result = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs);
        assert!(
            result.is_ok(),
            "a tone-test-suppressed window must not spuriously fail a healthy run: {result:?}"
        );
    }

    /// Suppressed windows do not satisfy the window floor: one valid window plus two suppressed
    /// windows leaves a single valid window (below MIN+1=2 after the warmup drop) → FAIL naming the
    /// suppressed count.
    #[test]
    fn eval_full_duplex_rx_integrity_suppression_does_not_satisfy_floor() {
        let logs = vec![obs_line(0), obs_line_suppressed(), obs_line_suppressed()];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("one valid window (rest suppressed) is below the floor and must FAIL");
        assert!(
            err.contains("valid") && err.contains("suppressed") && err.contains("need ≥"),
            "failure must state the valid-window floor and the suppressed count: {err}"
        );
    }

    /// An all-suppressed run fails closed with a distinct message: lines were collected, but none
    /// carries a valid mic-RX measurement, so scoring it as clean would launder discarded data.
    #[test]
    fn eval_full_duplex_rx_integrity_all_suppressed_fails_closed() {
        let logs = vec![
            obs_line_suppressed(),
            obs_line_suppressed(),
            obs_line_suppressed(),
        ];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("an all-suppressed run must fail closed");
        assert!(
            err.contains("all 3") && err.contains("tone-test-suppressed"),
            "failure must name the all-suppressed condition and the count: {err}"
        );
    }

    /// A `capture: playback obs …` line carrying the prefix but missing `rx_win_ok=` is a format
    /// drift — fail-closed, do not silently score it.
    #[test]
    fn eval_full_duplex_rx_integrity_missing_rx_win_ok_fails_closed() {
        let logs = vec![
            obs_line(0),
            format!(
                "[device Info] respeaker_pod: {CAPTURE_OBS_LINE}writefail=0 preroll_waits=0 \
                 preroll_rearms=0 resume_unmutes=1 eoa_mutes=1 {RX_DEFICIT}0 {PRIO}10 {CORE}1"
            ),
        ];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a prefix-present line missing rx_win_ok must fail-closed");
        assert!(
            err.contains("missing/invalid") && err.contains(RX_WIN_OK),
            "failure must name the missing rx_win_ok token: {err}"
        );
    }

    /// An out-of-range `rx_win_ok` value (neither 0 nor 1) is format drift — fail-closed with the
    /// distinct range error, never silently treated as suppressed. Guards the `> 1` range branch
    /// against a regression that drops or widens it (which would let a `rx_win_ok=2` line pass).
    #[test]
    fn eval_full_duplex_rx_integrity_out_of_range_rx_win_ok_fails_closed() {
        let logs = vec![
            obs_line(0),
            obs_line_full(0, CAPTURE_EXPECTED_PRIO, CAPTURE_EXPECTED_CORE, 2),
        ];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("an out-of-range rx_win_ok value must fail-closed");
        assert!(
            err.contains(RX_WIN_OK) && err.contains("expected 0 or 1"),
            "failure must name the invalid rx_win_ok token and the 0-or-1 constraint: {err}"
        );
    }

    /// The per-line pin assertion runs on suppressed windows too: a suppressed line with a dropped
    /// core-1 pin still FAILs the pin-regression guard (the pin cannot change per window).
    #[test]
    fn eval_full_duplex_rx_integrity_pin_regression_on_suppressed_line_fails() {
        let bad_pin = obs_line_full(0, CAPTURE_EXPECTED_PRIO, 0, 0);
        let logs = vec![obs_line(0), bad_pin];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a suppressed line with a dropped pin must still FAIL");
        assert!(
            err.contains("pin regressed") && err.contains("core=0"),
            "failure must name the pin regression even on a suppressed line: {err}"
        );
    }

    /// Saturated feed with every steady-state window reporting rx_deficit=0 → PASS. The first
    /// window is dropped as warmup, so supply a warmup window plus ≥1 clean steady window.
    #[test]
    fn eval_full_duplex_rx_integrity_zero_deficit_passes() {
        let logs = vec![obs_line(0), obs_line(0), obs_line(0)];
        let result = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs);
        assert!(
            result.is_ok(),
            "a saturated feed with all steady-state windows at rx_deficit=0 must PASS: {result:?}"
        );
    }

    /// The core-1 pin is the mechanism that keeps mic RX on cadence; a regression that drops it
    /// (obs line reports core=0) must FAIL even with zero deficit, so the guard is not left to a
    /// human eyeballing the log line (CLAUDE.md HIL doctrine).
    #[test]
    fn eval_full_duplex_rx_integrity_core_pin_regression_fails() {
        let logs = vec![obs_line(0), obs_line_pin(0, CAPTURE_EXPECTED_PRIO, 0)];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a capture thread that lost its core-1 pin must FAIL");
        assert!(
            err.contains("pin regressed") && err.contains("core=0"),
            "failure must name the pin regression and the observed core: {err}"
        );
    }

    /// A priority regression (the default 5 instead of the elevated 10) also FAILs — the pin guard
    /// covers both axes of the spawn config.
    #[test]
    fn eval_full_duplex_rx_integrity_priority_regression_fails() {
        let logs = vec![obs_line(0), obs_line_pin(0, 5, CAPTURE_EXPECTED_CORE)];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a capture thread at the default priority must FAIL");
        assert!(
            err.contains("pin regressed") && err.contains("prio=5"),
            "failure must name the pin regression and the observed priority: {err}"
        );
    }

    /// A nonzero rx_deficit in a steady-state window is the mic-RX starvation this test guards
    /// against — it must FAIL, naming the shortfall and the worst deficit. The device already
    /// dead-bands, so any value reaching the host is a real loss (never laundered into a pass).
    #[test]
    fn eval_full_duplex_rx_integrity_nonzero_deficit_fails() {
        let logs = vec![
            obs_line(0),    // warmup (dropped)
            obs_line(0),    // clean steady window
            obs_line(4200), // starved window — must fail the whole test
        ];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a nonzero rx_deficit in a steady window must FAIL");
        assert!(
            err.contains("starved under playback") && err.contains("worst 4200"),
            "failure must name the RX starvation and surface the worst deficit: {err}"
        );
    }

    /// Saturation validity: feed_full=0 means the ring never filled, so TX was not drain-bound and
    /// a zero deficit proves nothing. The eval must reject it (vacuous), not pass.
    #[test]
    fn eval_full_duplex_rx_integrity_unsaturated_feed_is_vacuous_fail() {
        let logs = vec![obs_line(0), obs_line(0)];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(0), &logs)
            .expect_err("feed_full=0 leaves the zero-deficit reading vacuous and must FAIL");
        assert!(
            err.contains("never saturated") && err.contains("vacuous"),
            "failure must name the missing saturation and the vacuous reading: {err}"
        );
    }

    /// The warmup window is always dropped: a deficit that appears ONLY in window 1 (the
    /// ramp/straddle transient) must not fail the test when the steady windows are clean. Pins the
    /// warmup drop — the pre-fix defect starved RX in every window, so dropping window 1 cannot
    /// mask a genuine regression.
    #[test]
    fn eval_full_duplex_rx_integrity_warmup_deficit_excluded() {
        let logs = vec![
            obs_line(9999), // warmup transient — dropped, must not fail the test
            obs_line(0),
            obs_line(0),
        ];
        let result = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs);
        assert!(
            result.is_ok(),
            "a deficit confined to the dropped warmup window must not FAIL a clean steady run: \
             {result:?}"
        );
    }

    /// Only a warmup window collected (no steady-state window) → fail loudly on insufficient data
    /// rather than passing on the dropped window alone.
    #[test]
    fn eval_full_duplex_rx_integrity_single_window_fails_loudly() {
        let logs = vec![obs_line(0)];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("one window (warmup only) is insufficient and must fail loudly");
        assert!(
            err.contains("need ≥") && err.contains("window(s)"),
            "failure must state the collected-vs-required window floor: {err}"
        );
    }

    /// No observability lines collected at all → fail loudly (the capture thread emitted no
    /// summary during the feed), not a silent pass.
    #[test]
    fn eval_full_duplex_rx_integrity_no_obs_lines_fails() {
        let logs: Vec<String> = vec![];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("no obs lines must fail loudly");
        assert!(
            err.contains("no ") && err.contains("periodic lines collected"),
            "failure must name the missing periodic lines: {err}"
        );
    }

    /// A `capture: playback obs …` line carrying the prefix but missing `rx_deficit=` is a format
    /// drift — fail-closed, do not silently skip it.
    #[test]
    fn eval_full_duplex_rx_integrity_missing_token_fails_not_silent() {
        let logs = vec![
            obs_line(0),
            format!(
                "[device Info] respeaker_pod: {CAPTURE_OBS_LINE}writefail=0 preroll_waits=0 \
                 preroll_rearms=0 resume_unmutes=1 eoa_mutes=1 {RX_WIN_OK}1 {PRIO}10 {CORE}1"
            ),
        ];
        let err = eval_full_duplex_rx_integrity(&rx_verdict(300), &logs)
            .expect_err("a prefix-present line missing rx_deficit must fail-closed");
        assert!(
            err.contains("missing/invalid") && err.contains(RX_DEFICIT),
            "failure must name the missing rx_deficit token: {err}"
        );
    }

    /// Verdict data that is not this test's own variant is rejected before any log parsing.
    /// Succeeds the string-era "not a PASS" and "wrong `src=` tag" cases: a device-side failure
    /// carries `TestData::None`, and the `src=` tag's job — proving the data belongs to *this*
    /// test — is now the variant itself.
    #[test]
    fn eval_full_duplex_rx_integrity_bad_verdict_rejected() {
        let logs = vec![obs_line(0), obs_line(0)];
        let fail_data = eval_full_duplex_rx_integrity(&TestData::None, &logs)
            .expect_err("a device-side failure (TestData::None) must be rejected");
        assert!(fail_data.contains("FullDuplexRxIntegrity"), "{fail_data}");

        let wrong_variant = eval_full_duplex_rx_integrity(
            &TestData::PlaybackDrainRate {
                chunks_fed: 250,
                feed_full: 300,
                feed_ms: 5000,
                tx_wf: 0,
            },
            &logs,
        )
        .expect_err("another test's variant must be rejected");
        assert!(
            wrong_variant.contains("FullDuplexRxIntegrity"),
            "{wrong_variant}"
        );
    }

    /// chunks_fed==0 means the feed accepted nothing — the RX-deficit verdict is uninterpretable.
    #[test]
    fn eval_full_duplex_rx_integrity_zero_chunks_fed_is_uninterpretable() {
        let logs = vec![obs_line(0), obs_line(0)];
        let err = eval_full_duplex_rx_integrity(
            &TestData::FullDuplexRxIntegrity {
                chunks_fed: 0,
                feed_full: 300,
                feed_ms: 5000,
            },
            &logs,
        )
        .expect_err("chunks_fed=0 must be uninterpretable and fail loudly");
        assert!(
            err.contains("chunks_fed=0") && err.contains("uninterpretable"),
            "failure must name the zero-chunk feed: {err}"
        );
    }

    // ── rtd_serve publish / final-store visibility contract ───────────────────
    //
    // These two tests drive `rtd_serve` over a real loopback socket — the one
    // concurrency-sensitive path in the RTD listener. Everything else in the RTD
    // suite exercises the pure evaluator functions against hand-built
    // `RtdObservation` literals; this covers the publish closure and the terminal
    // final store that those literals bypass.

    /// Build a client-side TLS-PSK context presenting `identity`/`key`, matching the server
    /// context every fixture builds. Used by the loopback unit tests to drive a real handshake.
    fn psk_client_context(identity: &str, key: [u8; AUDIO_PSK_LEN]) -> openssl::ssl::SslContext {
        use openssl::ssl::{SslContext, SslMethod, SslVersion};
        let identity = identity.to_string();
        let mut b = SslContext::builder(SslMethod::tls_client()).expect("client ctx builder");
        b.set_min_proto_version(Some(SslVersion::TLS1_2)).unwrap();
        b.set_max_proto_version(Some(SslVersion::TLS1_2)).unwrap();
        b.set_cipher_list(PSK_CIPHERSUITE).unwrap();
        b.set_psk_client_callback(move |_ssl, _hint, id_buf, key_buf| {
            let id = identity.as_bytes();
            if id_buf.len() < id.len() + 1 || key_buf.len() < key.len() {
                return Ok(0);
            }
            id_buf[..id.len()].copy_from_slice(id);
            id_buf[id.len()] = 0; // NUL-terminated identity
            key_buf[..key.len()].copy_from_slice(&key);
            Ok(key.len())
        });
        b.build()
    }

    /// Connected server/client TLS-PSK pair over loopback. The server side is already through
    /// [`tls_accept`]; the client side has completed its handshake. Both share one identity+key.
    fn tls_loopback_pair() -> (
        TlsServerStream,
        openssl::ssl::SslStream<std::net::TcpStream>,
    ) {
        use std::net::{TcpListener, TcpStream};
        let identity = "test-pod";
        let key = [0x5au8; AUDIO_PSK_LEN];
        let psk = PodPsk {
            identity: identity.to_string(),
            key,
        };
        let server_ctx = psk_server_context(&psk).expect("server ctx");
        let client_ctx = psk_client_context(identity, key);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let server_jh = thread::spawn(move || {
            let (tcp, peer) = listener.accept().expect("accept loopback");
            tls_accept(&server_ctx, tcp, &peer, "test-server").expect("server handshake")
        });
        let tcp = TcpStream::connect(addr).expect("connect loopback");
        let ssl = openssl::ssl::Ssl::new(&client_ctx).expect("client Ssl");
        let mut client = openssl::ssl::SslStream::new(ssl, tcp).expect("client SslStream");
        client.connect().expect("client handshake");
        let server = server_jh.join().expect("server thread");
        (server, client)
    }

    /// Encode one `StreamFrame` and write it whole to `stream` (test client side).
    fn rtd_test_write_frame(
        stream: &mut impl std::io::Write,
        frame: &audio_pipeline::wire::StreamFrame,
    ) {
        use audio_pipeline::wire::{MAX_FRAME_BYTES, encode_frame};
        let mut buf = vec![0u8; MAX_FRAME_BYTES + 2];
        let n = encode_frame(frame, &mut buf).expect("test frame encodes");
        stream
            .write_all(&buf[..n])
            .expect("test frame writes to loopback");
    }

    fn rtd_test_hello() -> audio_pipeline::wire::StreamFrame {
        use audio_pipeline::wire::{
            AUDIO_PROTOCOL_VERSION, ChannelSource, DEVICE_PLAYBACK_FORMAT, Hello, StreamFrame,
        };
        StreamFrame::Hello(Hello {
            version: AUDIO_PROTOCOL_VERSION,
            pod_id: heapless::String::new(),
            sample_rate_hz: DEVICE_PLAYBACK_FORMAT.sample_rate_hz,
            bits_per_sample: DEVICE_PLAYBACK_FORMAT.bits_per_sample,
            channels: DEVICE_PLAYBACK_FORMAT.channels,
            codec: DEVICE_PLAYBACK_FORMAT.codec,
            channel_source: ChannelSource::CommunicationBeam,
        })
    }

    fn rtd_test_segment_start(preroll_samples: u32) -> audio_pipeline::wire::StreamFrame {
        use audio_pipeline::wire::{SegmentStart, StreamFrame};
        StreamFrame::SegmentStart(SegmentStart {
            segment_id: 0,
            base_sample_index: 0,
            base_device_ts_us: 0,
            preroll_samples,
        })
    }

    fn rtd_test_audio() -> audio_pipeline::wire::StreamFrame {
        use audio_pipeline::wire::{AudioFrame, StreamFrame};
        StreamFrame::Audio(AudioFrame {
            segment_id: 0,
            first_sample_index: 0,
            device_ts_us: 0,
            pcm: heapless::Vec::new(),
        })
    }

    /// Accept a loopback connection and run `rtd_serve` (non-paced) on it in a
    /// background thread. Returns the connected client end, the shared slot, and the
    /// serve join handle. Dropping the client closes the read side → clean EOF →
    /// `rtd_serve` returns → the handle joins.
    fn rtd_spawn_serve(
        slot: &Arc<Mutex<RtdObservation>>,
        conn: u32,
    ) -> (
        openssl::ssl::SslStream<std::net::TcpStream>,
        thread::JoinHandle<()>,
    ) {
        use std::net::{TcpListener, TcpStream};
        let identity = "test-pod";
        let key = [0x5au8; AUDIO_PSK_LEN];
        let psk = PodPsk {
            identity: identity.to_string(),
            key,
        };
        let server_ctx = psk_server_context(&psk).expect("server ctx");
        let client_ctx = psk_client_context(identity, key);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local_addr");
        let slot_c = Arc::clone(slot);
        let handle = thread::spawn(move || {
            let (tcp, peer) = listener.accept().expect("accept loopback");
            let mut s = tls_accept(&server_ctx, tcp, &peer, "test-server").expect("handshake");
            let stop = Arc::new(Mutex::new(false));
            rtd_serve(&mut s, &peer, &slot_c, false, &stop, conn);
        });
        let tcp = TcpStream::connect(addr).expect("connect loopback");
        let ssl = openssl::ssl::Ssl::new(&client_ctx).expect("client Ssl");
        let mut client = openssl::ssl::SslStream::new(ssl, tcp).expect("client SslStream");
        client.connect().expect("client handshake");
        (client, handle)
    }

    /// Property 1: an evaluator that snapshots the slot mid-flight — after the device
    /// connected and opened a segment but before `SegmentEnd` — sees the in-progress
    /// observation (`connected` + `segment_start_seen` + the declared pre-roll), never
    /// an empty/disconnected slot. This is what the initial and per-frame `publish`
    /// calls guarantee.
    #[test]
    fn rtd_serve_publishes_in_progress_observation_mid_flight() {
        use std::time::Instant;

        let slot = Arc::new(Mutex::new(RtdObservation::default()));
        let (mut client, handle) = rtd_spawn_serve(&slot, 0);

        // Open a segment but withhold SegmentEnd — the connection stays mid-flight.
        rtd_test_write_frame(&mut client, &rtd_test_hello());
        rtd_test_write_frame(&mut client, &rtd_test_segment_start(320));

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            {
                let s = slot.lock().unwrap();
                if s.connected && s.segment_start_seen {
                    assert_eq!(
                        s.declared_preroll, 320,
                        "mid-flight snapshot must carry the SegmentStart pre-roll"
                    );
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "rtd_serve never published connected + segment_start_seen mid-flight"
            );
            thread::sleep(Duration::from_millis(5));
        }

        // EOF ends the serve loop; join so the socket/thread are cleaned up.
        drop(client);
        handle.join().expect("serve thread joins");
    }

    /// Property 2: the `publish` guard never overwrites an already-terminal slot with
    /// in-progress state. Every `conn_index >= 1` routes to the same B slot, so a
    /// spurious extra connection accepted after the real B completed must not blank
    /// B's completion markers while the runner is still polling for them.
    #[test]
    fn rtd_serve_publish_never_blanks_terminal_slot_mid_flight() {
        // Pre-seed the slot with a completed observation carrying a sentinel field.
        let terminal = RtdObservation {
            connected: true,
            segment_start_seen: true,
            end_reason: Some("VadRelease".to_string()),
            audio_frames: 42, // sentinel: survives iff the guard suppresses in-progress publishes
            ..Default::default()
        };
        let slot = Arc::new(Mutex::new(terminal));
        let (mut client, handle) = rtd_spawn_serve(&slot, 1);

        // Drive frames that would each trigger an in-progress publish. The initial
        // publish already ran unconditionally at serve start; these add per-frame ones.
        rtd_test_write_frame(&mut client, &rtd_test_hello());
        rtd_test_write_frame(&mut client, &rtd_test_segment_start(320));
        for _ in 0..3 {
            rtd_test_write_frame(&mut client, &rtd_test_audio());
        }

        // Keep the client open so the serve blocks between reads and never reaches its
        // (unguarded) final store. The only thing that could mutate the slot in this
        // window is an in-progress publish — the guard must suppress every one.
        for _ in 0..40 {
            {
                let s = slot.lock().unwrap();
                assert_eq!(
                    s.end_reason.as_deref(),
                    Some("VadRelease"),
                    "terminal end_reason must not be blanked by an in-progress publish"
                );
                assert_eq!(
                    s.audio_frames, 42,
                    "terminal sentinel must not be overwritten by in-progress state"
                );
            }
            thread::sleep(Duration::from_millis(5));
        }

        // Now let the connection finish: EOF triggers the final store, which is
        // deliberately unguarded (it only runs when this connection's own serve
        // returns). That the slot then reflects this connection's own frames proves
        // the serve did process them above — so the guard assertions were not vacuous.
        drop(client);
        handle.join().expect("serve thread joins");
        let s = slot.lock().unwrap();
        assert_eq!(
            s.audio_frames, 3,
            "final store must carry this connection's own frame count"
        );
        assert!(
            s.end_reason.is_none() && s.segment_start_seen,
            "final store reflects the in-progress observation (no SegmentEnd was sent)"
        );
    }
}
