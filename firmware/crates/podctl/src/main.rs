//! `podctl` — operator CLI for provisioning respeaker pods.
//!
//! Provisions a respeaker pod's persistent config (NVS) over the existing
//! framed USB-serial-JTAG protocol: WiFi credentials, network peer config,
//! and audio receiver address.
//!
//! Usage:
//!   podctl provision-wifi      [--ssid <S>] [--passphrase <P>] [--port <PATH>] [--serial <SN>]
//!   podctl provision-peer      [--host <IP>] [--udp-port <N>] [--tcp-port <N>]
//!                              [--tls-host <IP>] [--tls-port <N>]
//!                              [--inbound-frames-port <N>] [--port <PATH>] [--serial <SN>]
//!   podctl provision-audio     [--host <IP>] [--port <N>] [--serial-port <PATH>] [--serial <SN>]
//!   podctl set-vad-threshold   --threshold <F32> [--serial-port <PATH>] [--serial <SN>]
//!   podctl set-vad-hangover    --hangover-ms <U32> [--serial-port <PATH>] [--serial <SN>]
//!   podctl set-temp-wifi       --ssid <S> --passphrase <P> [--port <PATH>] [--serial <SN>]
//!   podctl clear-temp-wifi     [--port <PATH>] [--serial <SN>]
//!   podctl logs                [--log-jsonl <PATH>] [--port <PATH>] [--serial <SN>]
//!
//! Each input has an env-var fallback; explicit flags override env vars.
//! Env vars: PODCTL_WIFI_SSID, PODCTL_WIFI_PASS, PODCTL_PEER_HOST,
//!           PODCTL_UDP_PORT, PODCTL_TCP_PORT, PODCTL_TLS_HOST, PODCTL_TLS_PORT,
//!           PODCTL_INBOUND_FRAMES_PORT,
//!           PODCTL_AUDIO_HOST, PODCTL_AUDIO_PORT,
//!           PODCTL_VAD_THRESHOLD, PODCTL_VAD_HANGOVER_MS,
//!           PODCTL_LOG_JSONL, PODCTL_PORT, PODCTL_SERIAL.
//!
//! podctl never reads .hil-secrets and never reads RESPEAKER_* variables.

use clap::{Parser, Subcommand};
use device_protocol::{Command, DeviceFrame, LogFrame, LogLevel, Payload, Response, Status};
use pod_transport::{
    enumerate_pods, escape_device_str, format_log, open_port, FrameReader, HarnessError, PodMode,
    PodPort, Transport, RESPONSE_TIMEOUT,
};
use serde::Serialize;
use std::fs::File;
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

// ── CLI argument surface ──────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "podctl",
    about = "Operator CLI for provisioning respeaker pods",
    after_help = "Env vars: PODCTL_WIFI_SSID, PODCTL_WIFI_PASS, PODCTL_PEER_HOST, \
        PODCTL_UDP_PORT, PODCTL_TCP_PORT, PODCTL_TLS_HOST, PODCTL_TLS_PORT, \
        PODCTL_INBOUND_FRAMES_PORT, PODCTL_BACKPRESSURE_PORT, PODCTL_LOG_JSONL, \
        PODCTL_PORT, PODCTL_SERIAL"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Provisioning subcommands, flattened so they stay top-level (`podctl provision-wifi`).
    #[command(flatten)]
    Provision(ProvisionCmd),

    /// Apply a RAM-only temporary WiFi config override, bypassing NVS.
    ///
    /// Never touches the device's persisted credentials: a reboot always reverts to
    /// them. Useful for trialing new credentials with a guaranteed revert-on-reboot
    /// safety net. Overwriting an existing override is allowed (last write wins).
    SetTempWifi {
        /// WiFi SSID (max 32 bytes, non-empty). Also $PODCTL_TEMP_WIFI_SSID.
        #[arg(long, env = "PODCTL_TEMP_WIFI_SSID")]
        ssid: Option<String>,

        /// WiFi passphrase (max 64 bytes). Also $PODCTL_TEMP_WIFI_PASS. NOTE: the device
        /// always associates with WPA2-Personal auth; an empty passphrase does NOT select
        /// an open network — it will retry-and-fail against a WPA2 AP expecting a key.
        #[arg(long, env = "PODCTL_TEMP_WIFI_PASS")]
        passphrase: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long, env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Clear a RAM-only temporary WiFi config override, if any.
    ///
    /// A no-op if no override is set. If one is set, the device reverts to its
    /// persisted (NVS) credentials — or parks if NVS holds none.
    ClearTempWifi {
        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long, env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Stream device debug logs live until interrupted (Ctrl-C).
    ///
    /// Opens the device USB-serial port and prints every DeviceFrame::Log as
    /// `[device <Level>] <target>: <message>` to stdout, indefinitely.
    /// Exits on device disconnect (exit 1) or Ctrl-C (exit 130).
    Logs {
        /// Also write logs as JSONL (one JSON object per line) to this file.
        /// Truncates the file on open. Also $PODCTL_LOG_JSONL.
        #[arg(long, env = "PODCTL_LOG_JSONL")]
        log_jsonl: Option<PathBuf>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long, env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },
}

#[derive(Subcommand)]
enum ProvisionCmd {
    /// Provision WiFi credentials (SSID + passphrase) into device NVS.
    ///
    /// SSID: max 32 bytes (protocol limit, PODCTL_WIFI_SSID).
    /// Passphrase: max 64 bytes (protocol limit, PODCTL_WIFI_PASS). The device always
    /// associates with WPA2-Personal auth; an empty passphrase does NOT select an open
    /// network — it will retry-and-fail against a WPA2 AP expecting a key.
    ProvisionWifi {
        /// WiFi SSID (max 32 bytes). Also $PODCTL_WIFI_SSID.
        #[arg(long, env = "PODCTL_WIFI_SSID")]
        ssid: Option<String>,

        /// WiFi passphrase (max 64 bytes). Also $PODCTL_WIFI_PASS. NOTE: the device
        /// always associates with WPA2-Personal auth; an empty passphrase does NOT
        /// select an open network — it will retry-and-fail against a WPA2 AP expecting
        /// a key.
        #[arg(long, env = "PODCTL_WIFI_PASS")]
        passphrase: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long, env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Provision the network peer config (host, ports) into device NVS.
    ///
    /// All IPs are dotted IPv4 (e.g. 192.168.1.10).
    /// Ports are 0–65535.
    ProvisionPeer {
        /// Peer IPv4 address for UDP/TCP echo (dotted notation). Also $PODCTL_PEER_HOST.
        #[arg(long, env = "PODCTL_PEER_HOST")]
        host: Option<String>,

        /// UDP echo port (0–65535). Also $PODCTL_UDP_PORT.
        #[arg(long, env = "PODCTL_UDP_PORT")]
        udp_port: Option<String>,

        /// TCP echo port (0–65535). Also $PODCTL_TCP_PORT.
        #[arg(long, env = "PODCTL_TCP_PORT")]
        tcp_port: Option<String>,

        /// TLS endpoint IPv4 address (dotted notation). Also $PODCTL_TLS_HOST.
        #[arg(long, env = "PODCTL_TLS_HOST")]
        tls_host: Option<String>,

        /// TLS endpoint port (0–65535). Also $PODCTL_TLS_PORT.
        #[arg(long, env = "PODCTL_TLS_PORT")]
        tls_port: Option<String>,

        /// Inbound-frames TCP port (0–65535) for the TcpInboundFrames self-test.
        /// The host IP is the same as --host. Also $PODCTL_INBOUND_FRAMES_PORT.
        #[arg(long, env = "PODCTL_INBOUND_FRAMES_PORT")]
        inbound_frames_port: Option<String>,

        /// Backpressure-source TCP port (0–65535) for the TcpSendBackpressure self-test.
        /// The host IP is the same as --host. Also $PODCTL_BACKPRESSURE_PORT.
        #[arg(long, env = "PODCTL_BACKPRESSURE_PORT")]
        backpressure_port: Option<String>,

        /// Poll-readiness adversary TCP port (0–65535) for the PollReadinessBidir self-test.
        /// The host IP is the same as --host. Also $PODCTL_POLL_READINESS_PORT.
        #[arg(long, env = "PODCTL_POLL_READINESS_PORT")]
        poll_readiness_port: Option<String>,

        /// StreamRealtimeDuplex listener TCP port (0–65535) for the StreamRealtimeDuplex
        /// self-test. The host IP is the same as --host. Also $PODCTL_RTD_PORT.
        #[arg(long, env = "PODCTL_RTD_PORT")]
        rtd_port: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long, env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Provision the audio receiver address (host, port) into device NVS.
    ///
    /// Writes NVS keys audio_ip + audio_port in the "wifi" namespace.
    /// These are separate from the echo-server keys used by provision-peer.
    ProvisionAudio {
        /// Audio receiver IPv4 address (dotted notation). Also $PODCTL_AUDIO_HOST.
        #[arg(long, env = "PODCTL_AUDIO_HOST")]
        host: Option<String>,

        /// Audio receiver port (0–65535). Also $PODCTL_AUDIO_PORT.
        #[arg(long = "audio-port", env = "PODCTL_AUDIO_PORT")]
        audio_port: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long = "serial-port", env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Provision the VAD gate threshold into device NVS.
    ///
    /// Writes the threshold as a 4-byte LE f32 blob ("vad_threshold") in the "audio"
    /// NVS namespace. Applied on next device reboot. Use binary-search calibration
    /// (calibration-runbook.md) to find the right value.
    SetVadThreshold {
        /// VAD gate threshold (non-negative finite f32, dimensionless SPENERGY unit).
        /// Also $PODCTL_VAD_THRESHOLD.
        #[arg(long, env = "PODCTL_VAD_THRESHOLD")]
        threshold: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long = "serial-port", env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },

    /// Provision the device VAD hangover (milliseconds) into device NVS.
    ///
    /// Writes the hangover as a 4-byte LE u32 blob ("vad_hangover_ms") in the
    /// "audio" NVS namespace. Applied on next device reboot. The hangover is how
    /// long the device VAD gate stays open after the signal drops below threshold;
    /// raising it keeps the transport segment open across mid-utterance pauses.
    SetVadHangover {
        /// VAD hangover in milliseconds (u32). Also $PODCTL_VAD_HANGOVER_MS.
        #[arg(long = "hangover-ms", env = "PODCTL_VAD_HANGOVER_MS")]
        hangover_ms: Option<String>,

        /// Target device by serial port path (e.g. /dev/ttyACM0). Also $PODCTL_PORT.
        #[arg(long = "serial-port", env = "PODCTL_PORT")]
        port: Option<String>,

        /// Target device by USB serial number (best-effort). Also $PODCTL_SERIAL.
        #[arg(long, env = "PODCTL_SERIAL")]
        serial: Option<String>,
    },
}

// ── Validation ────────────────────────────────────────────────────────────────

/// Error produced by `validate()`.
#[derive(Debug, PartialEq)]
enum ValidationError {
    /// AC8: required input missing from both flag and env.
    Missing {
        input: &'static str,
        flag: &'static str,
        env: &'static str,
    },
    /// AC8a: empty SSID supplied.
    EmptySsid,
    /// AC9: SSID exceeds 32-byte protocol limit.
    SsidTooLong { bytes: usize },
    /// AC10: passphrase exceeds 64-byte protocol limit.
    PassphraseTooLong { bytes: usize },
    /// AC11: invalid IPv4 address or port number.
    InvalidField {
        field: &'static str,
        expected: &'static str,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::Missing { input, flag, env } => {
                write!(f, "missing {input}; supply --{flag} or ${env}")
            }
            ValidationError::EmptySsid => write!(f, "SSID must not be empty"),
            ValidationError::SsidTooLong { bytes } => {
                write!(f, "SSID is {bytes} bytes; protocol limit is 32 bytes")
            }
            ValidationError::PassphraseTooLong { bytes } => {
                write!(f, "passphrase is {bytes} bytes; protocol limit is 64 bytes")
            }
            ValidationError::InvalidField { field, expected } => {
                write!(f, "{field} is not a valid {expected}")
            }
        }
    }
}

/// Arguments for provision-wifi after clap parsing (all Option<String> at this layer).
struct WifiArgs {
    ssid: Option<String>,
    passphrase: Option<String>,
}

/// Arguments for provision-peer after clap parsing.
struct PeerArgs {
    host: Option<String>,
    udp_port: Option<String>,
    tcp_port: Option<String>,
    tls_host: Option<String>,
    tls_port: Option<String>,
    inbound_frames_port: Option<String>,
    backpressure_port: Option<String>,
    poll_readiness_port: Option<String>,
    rtd_port: Option<String>,
}

/// Arguments for provision-audio after clap parsing.
struct AudioArgs {
    host: Option<String>,
    audio_port: Option<String>,
}

/// Validate wifi args and construct the fully-formed Command.
///
/// Pure function — no USB access. Returns Err with the first validation failure.
fn validate_wifi(args: WifiArgs) -> Result<Command, ValidationError> {
    let ssid = args.ssid.ok_or(ValidationError::Missing {
        input: "SSID",
        flag: "ssid",
        env: "PODCTL_WIFI_SSID",
    })?;
    let passphrase = args.passphrase.ok_or(ValidationError::Missing {
        input: "passphrase",
        flag: "passphrase",
        env: "PODCTL_WIFI_PASS",
    })?;

    if ssid.is_empty() {
        return Err(ValidationError::EmptySsid);
    }
    if ssid.len() > 32 {
        return Err(ValidationError::SsidTooLong { bytes: ssid.len() });
    }
    if passphrase.len() > 64 {
        return Err(ValidationError::PassphraseTooLong {
            bytes: passphrase.len(),
        });
    }

    // Build heapless strings. Byte lengths were pre-checked above; push_str can
    // only fail if the heapless capacity is exceeded, which is impossible here
    // since len <= capacity. Map errors to the typed variants so user input can
    // never reach a panic path even under future refactors.
    let mut hs_ssid = heapless::String::<32>::new();
    hs_ssid
        .push_str(&ssid)
        .map_err(|_| ValidationError::SsidTooLong { bytes: ssid.len() })?;
    let mut hs_pass = heapless::String::<64>::new();
    hs_pass
        .push_str(&passphrase)
        .map_err(|_| ValidationError::PassphraseTooLong {
            bytes: passphrase.len(),
        })?;

    Ok(Command::ProvisionWifi {
        ssid: hs_ssid,
        passphrase: hs_pass,
    })
}

/// Validate temporary-wifi args and construct the fully-formed Command.
///
/// Same validation rules as `validate_wifi` (non-empty ssid ≤ 32 bytes, passphrase
/// ≤ 64 bytes, empty passphrase accepted by validation — though the device always
/// associates with WPA2-Personal, so an empty passphrase will not reach an open
/// network) — only the resulting `Command` variant differs. Pure function — no USB
/// access.
fn validate_set_temp_wifi(args: WifiArgs) -> Result<Command, ValidationError> {
    match validate_wifi(args)? {
        Command::ProvisionWifi { ssid, passphrase } => {
            Ok(Command::SetTemporaryWifiConfig { ssid, passphrase })
        }
        _ => unreachable!("validate_wifi always returns Command::ProvisionWifi"),
    }
}

/// Parse a dotted-IPv4 string into octets.
fn parse_ipv4(s: &str, field: &'static str) -> Result<[u8; 4], ValidationError> {
    s.parse::<Ipv4Addr>()
        .map(|a| a.octets())
        .map_err(|_| ValidationError::InvalidField {
            field,
            expected: "dotted IPv4",
        })
}

/// Parse a port string into u16.
fn parse_port(s: &str, field: &'static str) -> Result<u16, ValidationError> {
    s.parse::<u16>().map_err(|_| ValidationError::InvalidField {
        field,
        expected: "port (0–65535)",
    })
}

/// Validate peer args and construct the fully-formed Command.
///
/// Pure function — no USB access.
fn validate_peer(args: PeerArgs) -> Result<Command, ValidationError> {
    let host_str = args.host.ok_or(ValidationError::Missing {
        input: "host",
        flag: "host",
        env: "PODCTL_PEER_HOST",
    })?;
    let udp_str = args.udp_port.ok_or(ValidationError::Missing {
        input: "udp-port",
        flag: "udp-port",
        env: "PODCTL_UDP_PORT",
    })?;
    let tcp_str = args.tcp_port.ok_or(ValidationError::Missing {
        input: "tcp-port",
        flag: "tcp-port",
        env: "PODCTL_TCP_PORT",
    })?;
    let tls_host_str = args.tls_host.ok_or(ValidationError::Missing {
        input: "tls-host",
        flag: "tls-host",
        env: "PODCTL_TLS_HOST",
    })?;
    let tls_port_str = args.tls_port.ok_or(ValidationError::Missing {
        input: "tls-port",
        flag: "tls-port",
        env: "PODCTL_TLS_PORT",
    })?;
    let inbound_frames_str = args.inbound_frames_port.ok_or(ValidationError::Missing {
        input: "inbound-frames-port",
        flag: "inbound-frames-port",
        env: "PODCTL_INBOUND_FRAMES_PORT",
    })?;
    let backpressure_str = args.backpressure_port.ok_or(ValidationError::Missing {
        input: "backpressure-port",
        flag: "backpressure-port",
        env: "PODCTL_BACKPRESSURE_PORT",
    })?;
    let poll_readiness_str = args.poll_readiness_port.ok_or(ValidationError::Missing {
        input: "poll-readiness-port",
        flag: "poll-readiness-port",
        env: "PODCTL_POLL_READINESS_PORT",
    })?;
    let rtd_str = args.rtd_port.ok_or(ValidationError::Missing {
        input: "rtd-port",
        flag: "rtd-port",
        env: "PODCTL_RTD_PORT",
    })?;

    let host = parse_ipv4(&host_str, "host")?;
    let udp_port = parse_port(&udp_str, "udp-port")?;
    let tcp_port = parse_port(&tcp_str, "tcp-port")?;
    let tls_host = parse_ipv4(&tls_host_str, "tls-host")?;
    let tls_port = parse_port(&tls_port_str, "tls-port")?;
    let inbound_frames_port = parse_port(&inbound_frames_str, "inbound-frames-port")?;
    let backpressure_port = parse_port(&backpressure_str, "backpressure-port")?;
    let poll_readiness_port = parse_port(&poll_readiness_str, "poll-readiness-port")?;
    let rtd_port = parse_port(&rtd_str, "rtd-port")?;

    Ok(Command::ProvisionPeer {
        host,
        udp_port,
        tcp_port,
        tls_host,
        tls_port,
        inbound_frames_port,
        backpressure_port,
        poll_readiness_port,
        rtd_port,
    })
}

/// Validate audio args and construct the fully-formed Command.
///
/// Pure function — no USB access.
fn validate_audio(args: AudioArgs) -> Result<Command, ValidationError> {
    let host_str = args.host.ok_or(ValidationError::Missing {
        input: "host",
        flag: "host",
        env: "PODCTL_AUDIO_HOST",
    })?;
    let port_str = args.audio_port.ok_or(ValidationError::Missing {
        input: "audio-port",
        flag: "audio-port",
        env: "PODCTL_AUDIO_PORT",
    })?;

    let host = parse_ipv4(&host_str, "host")?;
    let port = parse_port(&port_str, "audio-port")?;

    Ok(Command::ProvisionAudio { host, port })
}

/// Validate VAD threshold args and construct the fully-formed Command.
///
/// Pure function — no USB access.
fn validate_vad_threshold(threshold: Option<String>) -> Result<Command, ValidationError> {
    let threshold_str = threshold.ok_or(ValidationError::Missing {
        input: "threshold",
        flag: "threshold",
        env: "PODCTL_VAD_THRESHOLD",
    })?;
    let threshold: f32 =
        threshold_str
            .parse::<f32>()
            .map_err(|_| ValidationError::InvalidField {
                field: "threshold",
                expected: "non-negative finite float",
            })?;
    if !threshold.is_finite() || threshold < 0.0 {
        return Err(ValidationError::InvalidField {
            field: "threshold",
            expected: "non-negative finite float",
        });
    }
    Ok(Command::SetVadThreshold { threshold })
}

/// Validate the `set-vad-hangover` argument into a `SetVadHangover` command.
///
/// Pure function — no USB access.
fn validate_vad_hangover(hangover_ms: Option<String>) -> Result<Command, ValidationError> {
    let hangover_str = hangover_ms.ok_or(ValidationError::Missing {
        input: "hangover_ms",
        flag: "hangover-ms",
        env: "PODCTL_VAD_HANGOVER_MS",
    })?;
    let hangover_ms = hangover_str
        .parse::<u32>()
        .map_err(|_| ValidationError::InvalidField {
            field: "hangover_ms",
            expected: "non-negative integer milliseconds (u32)",
        })?;
    Ok(Command::SetVadHangover { hangover_ms })
}

// ── Device selection ──────────────────────────────────────────────────────────

/// Error from device selection (carries owned identity strings to avoid borrow issues).
#[derive(Debug)]
enum SelErr {
    /// AC3: no respeaker pods at all.
    NonePresent,
    /// AC4: the only respeaker pod(s) are in DFU/bootloader mode.
    OnlyDfu,
    /// AC4: explicit target resolved to a DFU-mode pod.
    SelectedDfu,
    /// AC5: multiple app-mode pods and no explicit target.
    Ambiguous(Vec<String>),
    /// AC7: explicit target matched nothing among attached pods.
    NotFound(Vec<String>),
}

/// Select the pod to provision from a list of enumerated pods.
///
/// Pure function — no I/O. Selection policy:
/// 1. Explicit `--port <PATH>`: match by port_name.
/// 2. Explicit `--serial <SN>` (no --port): match app pods by serial_number.
/// 3. No target: select if exactly one app pod; AC4/AC5/AC3 otherwise.
///
/// A DFU pod blocks only if it is the explicitly-selected or only respeaker pod.
/// An unrelated DFU pod on the bus does not block an unambiguous app-mode selection.
fn select<'a>(
    pods: &'a [PodPort],
    port: Option<&str>,
    serial: Option<&str>,
) -> Result<&'a PodPort, SelErr> {
    if let Some(path) = port {
        // --port given: match by port_name.
        match pods.iter().find(|p| p.port_name == path) {
            Some(p) if p.mode == PodMode::App => return Ok(p),
            Some(_) => return Err(SelErr::SelectedDfu),
            None => {
                let attached: Vec<String> = pods.iter().map(|p| p.identity()).collect();
                return Err(SelErr::NotFound(attached));
            }
        }
    }

    if let Some(sn) = serial {
        // --serial given (no --port): best-effort match on app pods.
        let mut matches: Vec<&PodPort> = pods
            .iter()
            .filter(|p| p.mode == PodMode::App && p.serial_number.as_deref() == Some(sn))
            .collect();
        if matches.len() == 1 {
            return Ok(matches.remove(0));
        }
        if matches.len() > 1 {
            // Degenerate: multiple app pods with same SN — AC5.
            let ids: Vec<String> = matches.iter().map(|p| p.identity()).collect();
            return Err(SelErr::Ambiguous(ids));
        }
        // Zero app matches — could be DFU or truly absent.
        // TODO(podctl-dfu-serial): if a DFU pod matches the SN, return AC4 (SelectedDfu).
        // Currently unverified: ESP32-S3 DFU bootloader may report serial_number=None,
        // making this branch unreachable in practice. Until confirmed via HIL, fall to AC7.
        let attached: Vec<String> = pods.iter().map(|p| p.identity()).collect();
        return Err(SelErr::NotFound(attached));
    }

    // No explicit target: partition by mode.
    let app_pods: Vec<&PodPort> = pods.iter().filter(|p| p.mode == PodMode::App).collect();
    let dfu_pods: Vec<&PodPort> = pods.iter().filter(|p| p.mode == PodMode::Dfu).collect();

    match (app_pods.len(), dfu_pods.len()) {
        (1, _) => Ok(app_pods[0]),
        (n, _) if n > 1 => {
            let ids: Vec<String> = app_pods.iter().map(|p| p.identity()).collect();
            Err(SelErr::Ambiguous(ids))
        }
        (0, d) if d >= 1 => Err(SelErr::OnlyDfu),
        _ => Err(SelErr::NonePresent),
    }
}

// ── Run logic ─────────────────────────────────────────────────────────────────

/// Enumerate pods, select one, and open its port. Returns the device identity
/// string and an open transport, or prints an error to stderr and returns an
/// exit code.
///
/// Shared by the provision path and `run_logs` so the error strings (port-busy,
/// udev hint, NonePresent, OnlyDfu, Ambiguous, NotFound) live in exactly one copy.
fn open_selected(
    port: Option<&str>,
    serial: Option<&str>,
) -> Result<(String, Box<dyn Transport>), i32> {
    let pods = match enumerate_pods() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: could not enumerate serial ports: {e}");
            return Err(1);
        }
    };

    let selected = match select(&pods, port, serial) {
        Ok(p) => p,
        Err(SelErr::NonePresent) => {
            eprintln!(
                "error: no respeaker pod found; expected VID:PID 0x303a:0x1001; \
                 check device-access permissions (udev rule for VID 0x303a)"
            );
            return Err(1);
        }
        Err(SelErr::OnlyDfu) | Err(SelErr::SelectedDfu) => {
            eprintln!(
                "error: pod is in DFU/bootloader mode; cannot provision; \
                 boot the app firmware first"
            );
            return Err(1);
        }
        Err(SelErr::Ambiguous(ids)) => {
            eprintln!(
                "error: multiple pods found: {}; disambiguate with --port or --serial",
                ids.join(", ")
            );
            return Err(1);
        }
        Err(SelErr::NotFound(ids)) => {
            let attached = if ids.is_empty() {
                "none".to_string()
            } else {
                ids.join(", ")
            };
            eprintln!("error: requested device not found; attached: {attached}");
            return Err(1);
        }
    };

    let identity = selected.identity();

    let transport = match open_port(&selected.port_name) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "error: could not open {}: {e}; check device-access permissions \
                 (udev rule for VID 0x303a) and whether another process \
                 (hil-host or a serial monitor) holds the port",
                selected.port_name
            );
            return Err(1);
        }
    };

    Ok((identity, transport))
}

/// Host-side JSONL record wrapping a `LogFrame` with a receive timestamp.
#[derive(Serialize)]
struct JsonlRecord<'a> {
    /// Host wall-clock receive time, Unix epoch milliseconds.
    ts_ms: u64,
    level: LogLevel,
    target: &'a str,
    message: &'a str,
}

/// Returns the current wall-clock time as Unix epoch milliseconds.
/// Returns 0 if the system clock is set before the epoch.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Serialize `log` as a JSONL record and write it to `f`, flushing after each line.
/// Serialization or write failures are warned on stderr (once per failure kind, to avoid
/// log spam on persistent disk-full); the function does not abort.
fn write_jsonl_record(f: &mut File, log: &LogFrame) {
    let rec = JsonlRecord {
        ts_ms: now_unix_ms(),
        level: log.level,
        target: &log.target,
        message: &log.message,
    };
    match serde_json::to_string(&rec) {
        Ok(line) => {
            if let Err(e) = writeln!(f, "{line}") {
                static WRITE_WARNED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !WRITE_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("warn: JSONL write failed (further failures suppressed): {e}");
                }
            } else if let Err(e) = f.flush() {
                static FLUSH_WARNED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !FLUSH_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("warn: JSONL flush failed (further failures suppressed): {e}");
                }
            }
        }
        Err(e) => eprintln!("warn: JSONL serialize failed: {e}"),
    }
}

/// Write one formatted log line to `out`.
///
/// `Ok(false)` means the output is gone (BrokenPipe) — stop emitting and exit
/// cleanly. `Err` is a real I/O failure.
fn emit_log_line(out: &mut impl Write, log: &LogFrame) -> std::io::Result<bool> {
    match writeln!(out, "{}", format_log(log)) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(false),
        Err(e) => Err(e),
    }
}

/// Pump frames from `reader` until stdout goes away, a write fails, or the
/// device disconnects. Returns the process exit code.
fn stream_logs(mut reader: FrameReader, mut jsonl: Option<File>, out: &mut impl Write) -> i32 {
    // Set inside the pump closure, which decides both that we stop and with
    // what code; checked after each `pump` call.
    let mut stop: Option<i32> = None;
    loop {
        match reader.pump(&mut |frame| match frame {
            DeviceFrame::Log(log) => {
                if let Some(f) = jsonl.as_mut() {
                    write_jsonl_record(f, &log);
                }
                // One pump call can dispatch several frames; once stdout is
                // gone, later frames in the batch must not write to it.
                if stop.is_none() {
                    match emit_log_line(out, &log) {
                        Ok(true) => {}
                        Ok(false) => stop = Some(0),
                        Err(e) => {
                            eprintln!("error: writing log to stdout failed: {e}");
                            stop = Some(1);
                        }
                    }
                }
            }
            DeviceFrame::Heartbeat => {}
            DeviceFrame::Response(_) => {
                eprintln!("WARN: unexpected response frame while streaming logs; ignoring");
            }
        }) {
            Ok(_) => {} // timeout (no bytes) or frames handled — keep looping
            Err(e) => {
                eprintln!("error: serial read failed: {e}; device disconnected?");
                return 1;
            }
        }
        if let Some(code) = stop {
            return code;
        }
    }
}

/// Stream device logs until device disconnect or Ctrl-C. Returns exit code.
fn run_logs(port: Option<String>, serial: Option<String>, log_jsonl: Option<PathBuf>) -> i32 {
    let jsonl: Option<File> = match log_jsonl {
        None => None,
        Some(path) => match File::create(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("error: cannot open JSONL file {}: {e}", path.display());
                return 1;
            }
        },
    };

    let (identity, transport) = match open_selected(port.as_deref(), serial.as_deref()) {
        Ok(pair) => pair,
        Err(code) => return code,
    };

    eprintln!("streaming logs from {identity}; press Ctrl-C to stop");

    let reader = FrameReader::with_label(transport, &identity);
    stream_logs(reader, jsonl, &mut std::io::stdout())
}

/// Format an actionable success line for the given subcommand.
/// Never includes the passphrase — only the SSID may appear.
fn success_line(identity: &str, cmd: &Command) -> String {
    match cmd {
        Command::ProvisionWifi { ssid, .. } => {
            format!("provisioned wifi on {identity}: SSID \"{ssid}\"")
        }
        Command::ProvisionPeer { .. } => format!("provisioned peer config on {identity}"),
        Command::ProvisionAudio { host, port } => {
            format!(
                "provisioned audio receiver on {identity}: {}.{}.{}.{}:{}",
                host[0], host[1], host[2], host[3], port
            )
        }
        Command::SetVadThreshold { threshold } => {
            format!("provisioned VAD threshold {threshold} on {identity} (reboot to apply)")
        }
        Command::SetVadHangover { hangover_ms } => {
            format!("provisioned VAD hangover {hangover_ms} ms on {identity} (reboot to apply)")
        }
        Command::ClearWifiCredentials => format!("cleared wifi credentials on {identity}"),
        Command::SetTemporaryWifiConfig { ssid, .. } => {
            format!("applied temporary wifi config on {identity}: SSID \"{ssid}\" (RAM-only; reboot reverts)")
        }
        Command::ClearTemporaryWifiConfig => {
            format!("cleared temporary wifi config on {identity} (reverted to persisted credentials, if any)")
        }
        // RunTest is dispatched by hil-host, not by podctl; podctl should not reach this arm.
        Command::RunTest(_) => format!("sent command on {identity}"),
    }
}

/// Core run logic; returns exit code (0 or 1). Separated for testability.
fn run() -> i32 {
    let cli = Cli::parse();

    // `logs` builds no Command and runs no validation, so it does not fit the
    // `(cmd_result, …)` destructuring used by the provision/temp-wifi arms.
    let sub = match cli.command {
        Cmd::Logs {
            port,
            serial,
            log_jsonl,
        } => return run_logs(port, serial, log_jsonl),
        Cmd::Provision(sub) => sub,
        Cmd::SetTempWifi {
            ssid,
            passphrase,
            port,
            serial,
        } => {
            let result = validate_set_temp_wifi(WifiArgs { ssid, passphrase });
            return run_command(result, port, serial);
        }
        Cmd::ClearTempWifi { port, serial } => {
            return run_command(Ok(Command::ClearTemporaryWifiConfig), port, serial);
        }
    };

    // Extract device-targeting args and the subcommand.
    let (cmd_result, port_target, serial_target) = match sub {
        ProvisionCmd::ProvisionWifi {
            ssid,
            passphrase,
            port,
            serial,
        } => {
            let result = validate_wifi(WifiArgs { ssid, passphrase });
            (result, port, serial)
        }
        ProvisionCmd::ProvisionPeer {
            host,
            udp_port,
            tcp_port,
            tls_host,
            tls_port,
            inbound_frames_port,
            backpressure_port,
            poll_readiness_port,
            rtd_port,
            port,
            serial,
        } => {
            let result = validate_peer(PeerArgs {
                host,
                udp_port,
                tcp_port,
                tls_host,
                tls_port,
                inbound_frames_port,
                backpressure_port,
                poll_readiness_port,
                rtd_port,
            });
            (result, port, serial)
        }
        ProvisionCmd::ProvisionAudio {
            host,
            audio_port,
            port,
            serial,
        } => {
            let result = validate_audio(AudioArgs { host, audio_port });
            (result, port, serial)
        }
        ProvisionCmd::SetVadThreshold {
            threshold,
            port,
            serial,
        } => {
            let result = validate_vad_threshold(threshold);
            (result, port, serial)
        }
        ProvisionCmd::SetVadHangover {
            hangover_ms,
            port,
            serial,
        } => {
            let result = validate_vad_hangover(hangover_ms);
            (result, port, serial)
        }
    };

    run_command(cmd_result, port_target, serial_target)
}

/// Validate, select a device, send `cmd_result`'s command, and report the outcome.
/// Returns the process exit code. Shared by every subcommand that sends a single
/// `Command` and prints a success/error line (provisioning + temp-wifi arms).
fn run_command(
    cmd_result: Result<Command, ValidationError>,
    port_target: Option<String>,
    serial_target: Option<String>,
) -> i32 {
    // Step 1: validate args (no USB until this passes).
    let cmd = match cmd_result {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    // Steps 2-4: enumerate → select → open (shared with run_logs via open_selected).
    let (identity, transport) =
        match open_selected(port_target.as_deref(), serial_target.as_deref()) {
            Ok(pair) => pair,
            Err(code) => return code,
        };

    println!("targeting {identity}");

    // Pre-compute the success line before the command is moved into send_command.
    let success = success_line(&identity, &cmd);

    // Step 5: send command and handle response.
    let mut harness = pod_transport::Harness::new(transport);
    let result = harness.send_command(cmd);
    let (msg, to_stdout, exit) = map_response(result, &success, &identity);
    if to_stdout {
        println!("{msg}");
    } else {
        eprintln!("{msg}");
    }
    exit
}

/// Map a harness send result to an output message, stream (true=stdout, false=stderr), and
/// exit code. Pure function; extracted for unit-testability without a real serial port.
///
/// `identity` is included in I/O error messages so the operator knows which port failed.
fn map_response(
    result: Result<Response, HarnessError>,
    success_msg: &str,
    identity: &str,
) -> (String, bool, i32) {
    match result {
        Ok(resp) => match resp.status {
            Status::Ok => (success_msg.to_owned(), true, 0),
            Status::Fail => {
                let detail = if let Payload::TestReport(report) = &resp.payload {
                    format!(": {}", escape_device_str(&report.detail))
                } else {
                    String::new()
                };
                (
                    format!("error: device rejected the command{detail}"),
                    false,
                    1,
                )
            }
            Status::Unsupported => (
                "error: firmware does not support this command \
                 (firmware/protocol mismatch)"
                    .to_owned(),
                false,
                1,
            ),
        },
        Err(HarnessError::Timeout) => (
            format!(
                "error: timed out after {} s waiting for device response",
                RESPONSE_TIMEOUT.as_secs()
            ),
            false,
            1,
        ),
        Err(e) => (format!("error on {identity}: {e}"), false, 1),
    }
}

fn main() {
    std::process::exit(run());
}

// ── Unit tests (hardware-free) ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use device_protocol::Response;
    use pod_transport::{PodMode, PodPort};

    /// Process-wide mutex for tests that mutate or depend on env vars. There is
    /// one process environment shared by all test threads; any test that calls
    /// `set_var` / `remove_var` (or asserts a field parsed from env) must hold
    /// this lock for the duration of the parse. Any future test that mutates
    /// PODCTL_PORT, PODCTL_SERIAL, or any other env-backed CLI flag must also
    /// acquire this lock — not a separate per-var mutex.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Every `env = "…"` var the CLI grammar declares. Tests that assert on a parse
    /// must clear these so an ambient shell value cannot feed the parse.
    const PODCTL_ENV_VARS: &[&str] = &[
        "PODCTL_AUDIO_HOST",
        "PODCTL_AUDIO_PORT",
        "PODCTL_BACKPRESSURE_PORT",
        "PODCTL_INBOUND_FRAMES_PORT",
        "PODCTL_LOG_JSONL",
        "PODCTL_PEER_HOST",
        "PODCTL_POLL_READINESS_PORT",
        "PODCTL_PORT",
        "PODCTL_RTD_PORT",
        "PODCTL_SERIAL",
        "PODCTL_TCP_PORT",
        "PODCTL_TEMP_WIFI_PASS",
        "PODCTL_TEMP_WIFI_SSID",
        "PODCTL_TLS_HOST",
        "PODCTL_TLS_PORT",
        "PODCTL_UDP_PORT",
        "PODCTL_VAD_HANGOVER_MS",
        "PODCTL_VAD_THRESHOLD",
        "PODCTL_WIFI_PASS",
        "PODCTL_WIFI_SSID",
    ];

    // ── CLI grammar ───────────────────────────────────────────────────────────

    #[test]
    fn cli_debug_assert() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn cli_grammar_is_flat() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // The parsed variants declare `env = "PODCTL_*"`; an ambient value in the
        // developer's or CI shell would otherwise feed the parse and make these pins
        // environment-dependent. The mutex serializes, it does not clean.
        let saved: Vec<(&str, Option<String>)> = PODCTL_ENV_VARS
            .iter()
            .map(|v| (*v, std::env::var(v).ok()))
            .collect();
        unsafe {
            for (v, _) in &saved {
                std::env::remove_var(v);
            }
        }
        let restore = || unsafe {
            for (v, old) in &saved {
                match old {
                    Some(val) => std::env::set_var(v, val),
                    None => std::env::remove_var(v),
                }
            }
        };
        // Parse everything before asserting so a failure cannot leak the cleared env.
        let wifi = Cli::try_parse_from([
            "podctl",
            "provision-wifi",
            "--ssid",
            "x",
            "--passphrase",
            "y",
        ])
        .map(|c| c.command);
        let vad = Cli::try_parse_from(["podctl", "set-vad-threshold", "--threshold", "1.0"])
            .map(|c| c.command);
        let logs = Cli::try_parse_from(["podctl", "logs"]).map(|c| c.command);
        let group_err = Cli::try_parse_from(["podctl", "provision"])
            .map(|_| ())
            .err();
        restore();

        assert!(matches!(
            wifi,
            Ok(Cmd::Provision(ProvisionCmd::ProvisionWifi { .. }))
        ));
        assert!(matches!(
            vad,
            Ok(Cmd::Provision(ProvisionCmd::SetVadThreshold { .. }))
        ));
        assert!(matches!(logs, Ok(Cmd::Logs { .. })));
        // No `provision` group command exists: the provisioning enum is flattened. The
        // error *kind* is the assertion — a bare `is_err()` would keep passing if
        // `provision` started failing for an unrelated reason (renamed subcommands, a
        // newly required global arg), leaving flatness silently unpinned.
        assert_eq!(
            group_err.map(|e| e.kind()),
            Some(clap::error::ErrorKind::InvalidSubcommand),
            "`podctl provision` must fail as an unknown subcommand"
        );
    }

    /// `set-temp-wifi`/`clear-temp-wifi` parse as top-level subcommands (not folded
    /// into the flattened `ProvisionCmd` group) — they never touch NVS, unlike every
    /// `ProvisionCmd` variant.
    #[test]
    fn cli_grammar_temp_wifi_subcommands() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let saved: Vec<(&str, Option<String>)> = PODCTL_ENV_VARS
            .iter()
            .map(|v| (*v, std::env::var(v).ok()))
            .collect();
        unsafe {
            for (v, _) in &saved {
                std::env::remove_var(v);
            }
        }
        let restore = || unsafe {
            for (v, old) in &saved {
                match old {
                    Some(val) => std::env::set_var(v, val),
                    None => std::env::remove_var(v),
                }
            }
        };
        let set_temp = Cli::try_parse_from([
            "podctl",
            "set-temp-wifi",
            "--ssid",
            "x",
            "--passphrase",
            "y",
        ])
        .map(|c| c.command);
        let clear_temp = Cli::try_parse_from(["podctl", "clear-temp-wifi"]).map(|c| c.command);
        restore();

        assert!(matches!(set_temp, Ok(Cmd::SetTempWifi { .. })));
        assert!(matches!(clear_temp, Ok(Cmd::ClearTempWifi { .. })));
    }

    // ── Validation: provision-wifi ────────────────────────────────────────────

    #[test]
    fn validate_wifi_happy_path() {
        let cmd = validate_wifi(WifiArgs {
            ssid: Some("homenet".into()),
            passphrase: Some("s3cr3t".into()),
        })
        .unwrap();
        match cmd {
            Command::ProvisionWifi { ssid, passphrase } => {
                assert_eq!(ssid.as_str(), "homenet");
                assert_eq!(passphrase.as_str(), "s3cr3t");
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_wifi_empty_passphrase_allowed() {
        // Open network: empty passphrase must pass through.
        let cmd = validate_wifi(WifiArgs {
            ssid: Some("opennet".into()),
            passphrase: Some(String::new()),
        })
        .unwrap();
        match cmd {
            Command::ProvisionWifi { passphrase, .. } => {
                assert_eq!(passphrase.as_str(), "");
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_wifi_missing_ssid() {
        let err = validate_wifi(WifiArgs {
            ssid: None,
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert!(
            matches!(err, ValidationError::Missing { input: "SSID", .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_wifi_missing_passphrase() {
        let err = validate_wifi(WifiArgs {
            ssid: Some("net".into()),
            passphrase: None,
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "passphrase",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_wifi_empty_ssid_rejected() {
        let err = validate_wifi(WifiArgs {
            ssid: Some(String::new()),
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::EmptySsid);
    }

    #[test]
    fn validate_wifi_ssid_33_bytes_rejected() {
        // 33-byte SSID (33 ASCII chars).
        let ssid = "a".repeat(33);
        let err = validate_wifi(WifiArgs {
            ssid: Some(ssid),
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::SsidTooLong { bytes: 33 });
    }

    #[test]
    fn validate_wifi_ssid_32_bytes_accepted() {
        let ssid = "a".repeat(32);
        assert!(validate_wifi(WifiArgs {
            ssid: Some(ssid),
            passphrase: Some("pass".into()),
        })
        .is_ok());
    }

    #[test]
    fn validate_wifi_passphrase_65_bytes_rejected() {
        let pass = "p".repeat(65);
        let err = validate_wifi(WifiArgs {
            ssid: Some("net".into()),
            passphrase: Some(pass),
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::PassphraseTooLong { bytes: 65 });
    }

    #[test]
    fn validate_wifi_passphrase_64_bytes_accepted() {
        let pass = "p".repeat(64);
        assert!(validate_wifi(WifiArgs {
            ssid: Some("net".into()),
            passphrase: Some(pass),
        })
        .is_ok());
    }

    #[test]
    fn validate_wifi_multibyte_ssid_byte_length_checked() {
        // 11 x 3-byte UTF-8 chars = 33 bytes; must be rejected even though char count is 11.
        // '中' is 3 bytes UTF-8; 11 * 3 = 33 bytes.
        let ssid_cjk = "中".repeat(11);
        assert_eq!(ssid_cjk.len(), 33); // sanity-check
        let err = validate_wifi(WifiArgs {
            ssid: Some(ssid_cjk),
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::SsidTooLong { bytes: 33 });

        // 10 * 3 = 30 bytes — should pass.
        let ssid_ok = "中".repeat(10);
        assert_eq!(ssid_ok.len(), 30);
        assert!(validate_wifi(WifiArgs {
            ssid: Some(ssid_ok),
            passphrase: Some("pass".into()),
        })
        .is_ok());
    }

    // ── Validation: set-temp-wifi ─────────────────────────────────────────────

    #[test]
    fn validate_set_temp_wifi_happy_path() {
        let cmd = validate_set_temp_wifi(WifiArgs {
            ssid: Some("homenet".into()),
            passphrase: Some("s3cr3t".into()),
        })
        .unwrap();
        match cmd {
            Command::SetTemporaryWifiConfig { ssid, passphrase } => {
                assert_eq!(ssid.as_str(), "homenet");
                assert_eq!(passphrase.as_str(), "s3cr3t");
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_set_temp_wifi_empty_ssid_rejected() {
        let err = validate_set_temp_wifi(WifiArgs {
            ssid: Some(String::new()),
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert_eq!(err, ValidationError::EmptySsid);
    }

    #[test]
    fn validate_set_temp_wifi_missing_ssid() {
        let err = validate_set_temp_wifi(WifiArgs {
            ssid: None,
            passphrase: Some("pass".into()),
        })
        .unwrap_err();
        assert!(
            matches!(err, ValidationError::Missing { input: "SSID", .. }),
            "got {err:?}"
        );
    }

    // ── Validation: provision-peer ────────────────────────────────────────────

    #[test]
    fn validate_peer_happy_path() {
        let cmd = validate_peer(PeerArgs {
            host: Some("192.168.1.10".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap();
        match cmd {
            Command::ProvisionPeer {
                host,
                udp_port,
                tcp_port,
                tls_host,
                tls_port,
                inbound_frames_port,
                backpressure_port,
                poll_readiness_port,
                rtd_port,
            } => {
                assert_eq!(host, [192, 168, 1, 10]);
                assert_eq!(udp_port, 5000);
                assert_eq!(tcp_port, 5001);
                assert_eq!(tls_host, [10, 0, 0, 1]);
                assert_eq!(tls_port, 8883);
                assert_eq!(inbound_frames_port, 17382);
                assert_eq!(backpressure_port, 17383);
                assert_eq!(poll_readiness_port, 17384);
                assert_eq!(rtd_port, 17385);
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_peer_missing_host() {
        let err = validate_peer(PeerArgs {
            host: None,
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(matches!(
            err,
            ValidationError::Missing { input: "host", .. }
        ));
    }

    #[test]
    fn validate_peer_bad_ipv4_host() {
        let err = validate_peer(PeerArgs {
            host: Some("not-an-ip".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "host",
                    expected: "dotted IPv4"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_bad_ipv4_tls_host() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("999.999.999.999".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "tls-host",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_non_numeric_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("notaport".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "udp-port",
                    expected: "port (0–65535)"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_port_overflow() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("65536".into()), // > u16::MAX
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "udp-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_inbound_frames_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: None,
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "inbound-frames-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_backpressure_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: None,
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "backpressure-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_poll_readiness_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: None,
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "poll-readiness-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_rtd_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: None,
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "rtd-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_non_numeric_inbound_frames_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("notaport".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "inbound-frames-port",
                    expected: "port (0–65535)"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_inbound_frames_port_overflow() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("65536".into()), // > u16::MAX
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "inbound-frames-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    // ── Validation: provision-audio ───────────────────────────────────────────

    #[test]
    fn validate_audio_happy_path() {
        let cmd = validate_audio(AudioArgs {
            host: Some("192.168.1.100".into()),
            audio_port: Some("7380".into()),
        })
        .unwrap();
        match cmd {
            Command::ProvisionAudio { host, port } => {
                assert_eq!(host, [192, 168, 1, 100]);
                assert_eq!(port, 7380);
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_audio_missing_host() {
        let err = validate_audio(AudioArgs {
            host: None,
            audio_port: Some("7380".into()),
        })
        .unwrap_err();
        assert!(
            matches!(err, ValidationError::Missing { input: "host", .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_audio_missing_port() {
        let err = validate_audio(AudioArgs {
            host: Some("192.168.1.100".into()),
            audio_port: None,
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "audio-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_audio_bad_ipv4() {
        let err = validate_audio(AudioArgs {
            host: Some("not-an-ip".into()),
            audio_port: Some("7380".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "host",
                    expected: "dotted IPv4"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_audio_bad_port() {
        let err = validate_audio(AudioArgs {
            host: Some("192.168.1.1".into()),
            audio_port: Some("99999".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "audio-port",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn success_line_audio_shows_host_and_port() {
        let cmd = Command::ProvisionAudio {
            host: [192, 168, 1, 100],
            port: 7380,
        };
        let line = success_line("/dev/ttyACM0", &cmd);
        assert!(
            line.contains("192.168.1.100"),
            "success_line must contain host; got: {line:?}"
        );
        assert!(
            line.contains("7380"),
            "success_line must contain port; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0"),
            "success_line must contain identity; got: {line:?}"
        );
    }

    // ── Validation: set-vad-threshold ─────────────────────────────────────────

    #[test]
    fn validate_vad_threshold_happy_path() {
        let cmd = validate_vad_threshold(Some("1.5".into())).unwrap();
        match cmd {
            Command::SetVadThreshold { threshold } => {
                assert!(
                    (threshold - 1.5f32).abs() < 1e-6,
                    "threshold mismatch: {threshold}"
                );
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_vad_threshold_zero_accepted() {
        // 0.0 is permitted: strict > in the FSM means any energy > 0 opens the gate.
        let cmd = validate_vad_threshold(Some("0.0".into())).unwrap();
        match cmd {
            Command::SetVadThreshold { threshold } => {
                assert_eq!(threshold, 0.0f32);
            }
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_vad_threshold_missing_rejected() {
        let err = validate_vad_threshold(None).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "threshold",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_threshold_non_numeric_rejected() {
        let err = validate_vad_threshold(Some("not-a-float".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "threshold",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_threshold_negative_rejected() {
        let err = validate_vad_threshold(Some("-1.0".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "threshold",
                    expected: "non-negative finite float"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_threshold_nan_rejected() {
        let err = validate_vad_threshold(Some("NaN".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "threshold",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_threshold_inf_rejected() {
        let err = validate_vad_threshold(Some("inf".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "threshold",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_threshold_neg_inf_rejected() {
        // Rust's f32 parser accepts "-inf" as f32::NEG_INFINITY; verify it is
        // caught by the is_finite() guard (not just the >= 0.0 guard).
        let err = validate_vad_threshold(Some("-inf".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "threshold",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn success_line_set_vad_threshold_contains_value_and_reboot_note() {
        let cmd = Command::SetVadThreshold { threshold: 1.5 };
        let line = success_line("/dev/ttyACM0", &cmd);
        assert!(
            line.contains("1.5"),
            "success_line must contain threshold value; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0"),
            "success_line must contain identity; got: {line:?}"
        );
        assert!(
            line.contains("reboot"),
            "success_line must mention reboot requirement; got: {line:?}"
        );
    }

    // ── Validation: set-vad-hangover ───────────────────────────────────────────

    #[test]
    fn validate_vad_hangover_happy_path() {
        let cmd = validate_vad_hangover(Some("3000".into())).unwrap();
        match cmd {
            Command::SetVadHangover { hangover_ms } => assert_eq!(hangover_ms, 3000),
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_vad_hangover_zero_accepted() {
        let cmd = validate_vad_hangover(Some("0".into())).unwrap();
        match cmd {
            Command::SetVadHangover { hangover_ms } => assert_eq!(hangover_ms, 0),
            _ => panic!("wrong command type"),
        }
    }

    #[test]
    fn validate_vad_hangover_missing_rejected() {
        let err = validate_vad_hangover(None).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "hangover_ms",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_hangover_non_numeric_rejected() {
        let err = validate_vad_hangover(Some("not-an-int".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "hangover_ms",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_vad_hangover_negative_rejected() {
        // u32 parse rejects a leading minus.
        let err = validate_vad_hangover(Some("-1".into())).unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::InvalidField {
                    field: "hangover_ms",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn success_line_set_vad_hangover_contains_value_and_reboot_note() {
        let cmd = Command::SetVadHangover { hangover_ms: 3000 };
        let line = success_line("/dev/ttyACM0", &cmd);
        assert!(
            line.contains("3000"),
            "success_line must contain hangover value; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0"),
            "success_line must contain identity; got: {line:?}"
        );
        assert!(
            line.contains("reboot"),
            "success_line must mention reboot requirement; got: {line:?}"
        );
    }

    // ── Selection policy ──────────────────────────────────────────────────────

    fn app(name: &str) -> PodPort {
        PodPort {
            port_name: name.to_string(),
            serial_number: None,
            mode: PodMode::App,
        }
    }

    fn app_sn(name: &str, sn: &str) -> PodPort {
        PodPort {
            port_name: name.to_string(),
            serial_number: Some(sn.to_string()),
            mode: PodMode::App,
        }
    }

    fn dfu(name: &str) -> PodPort {
        PodPort {
            port_name: name.to_string(),
            serial_number: None,
            mode: PodMode::Dfu,
        }
    }

    #[test]
    fn select_no_pods_ac3() {
        let pods: Vec<PodPort> = vec![];
        assert!(matches!(
            select(&pods, None, None),
            Err(SelErr::NonePresent)
        ));
    }

    #[test]
    fn select_only_dfu_ac4() {
        let pods = vec![dfu("/dev/ttyACM0")];
        assert!(matches!(select(&pods, None, None), Err(SelErr::OnlyDfu)));
    }

    #[test]
    fn select_one_app_pod_selected() {
        let pods = vec![app("/dev/ttyACM0")];
        let p = select(&pods, None, None).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM0");
    }

    #[test]
    fn select_one_app_with_unrelated_dfu_selected() {
        // DFU pod on bus must NOT block selection of the only app pod.
        let pods = vec![app("/dev/ttyACM0"), dfu("/dev/ttyACM1")];
        let p = select(&pods, None, None).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM0");
    }

    #[test]
    fn select_multiple_app_pods_ac5() {
        let pods = vec![app("/dev/ttyACM0"), app("/dev/ttyACM1")];
        assert!(matches!(
            select(&pods, None, None),
            Err(SelErr::Ambiguous(_))
        ));
    }

    #[test]
    fn select_explicit_port_app_ac6() {
        let pods = vec![app("/dev/ttyACM0"), app("/dev/ttyACM1")];
        let p = select(&pods, Some("/dev/ttyACM1"), None).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM1");
    }

    #[test]
    fn select_explicit_port_with_dfu_present_selects_app() {
        // --port points to app pod; DFU pod on bus must not block.
        let pods = vec![app("/dev/ttyACM0"), dfu("/dev/ttyACM1")];
        let p = select(&pods, Some("/dev/ttyACM0"), None).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM0");
    }

    #[test]
    fn select_explicit_port_to_dfu_ac4() {
        let pods = vec![app("/dev/ttyACM0"), dfu("/dev/ttyACM1")];
        assert!(matches!(
            select(&pods, Some("/dev/ttyACM1"), None),
            Err(SelErr::SelectedDfu)
        ));
    }

    #[test]
    fn select_explicit_port_not_found_ac7() {
        let pods = vec![app("/dev/ttyACM0")];
        assert!(matches!(
            select(&pods, Some("/dev/ttyACM9"), None),
            Err(SelErr::NotFound(_))
        ));
    }

    #[test]
    fn select_by_serial_match() {
        let pods = vec![app_sn("/dev/ttyACM0", "ABC123"), app("/dev/ttyACM1")];
        let p = select(&pods, None, Some("ABC123")).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM0");
    }

    #[test]
    fn select_by_serial_not_found_ac7() {
        let pods = vec![app_sn("/dev/ttyACM0", "ABC123")];
        assert!(matches!(
            select(&pods, None, Some("ZZZZZ")),
            Err(SelErr::NotFound(_))
        ));
    }

    #[test]
    fn select_port_wins_over_serial() {
        // --port and --serial both given; --port is authoritative.
        let pods = vec![
            app_sn("/dev/ttyACM0", "SN-A"),
            app_sn("/dev/ttyACM1", "SN-B"),
        ];
        let p = select(&pods, Some("/dev/ttyACM1"), Some("SN-A")).unwrap();
        assert_eq!(p.port_name, "/dev/ttyACM1"); // port wins, not serial
    }

    // ── Identity string ───────────────────────────────────────────────────────

    #[test]
    fn identity_with_sn() {
        let p = app_sn("/dev/ttyACM0", "XYZ");
        assert_eq!(p.identity(), "/dev/ttyACM0 (SN XYZ)");
    }

    #[test]
    fn identity_without_sn() {
        let p = app("/dev/ttyACM0");
        assert_eq!(p.identity(), "/dev/ttyACM0");
    }

    // ── Secret hygiene ────────────────────────────────────────────────────────

    #[test]
    fn success_line_never_contains_passphrase() {
        // Build a ProvisionWifi command with a known passphrase.
        let mut hs_ssid = heapless::String::<32>::new();
        hs_ssid.push_str("homenet").unwrap();
        let mut hs_pass = heapless::String::<64>::new();
        hs_pass.push_str("supersecret").unwrap();
        let cmd = Command::ProvisionWifi {
            ssid: hs_ssid,
            passphrase: hs_pass,
        };
        let line = success_line("/dev/ttyACM0", &cmd);
        assert!(
            !line.contains("supersecret"),
            "success_line must not contain passphrase; got: {line:?}"
        );
    }

    #[test]
    fn success_line_wifi_contains_ssid() {
        let mut hs_ssid = heapless::String::<32>::new();
        hs_ssid.push_str("homenet").unwrap();
        let mut hs_pass = heapless::String::<64>::new();
        hs_pass.push_str("pass").unwrap();
        let cmd = Command::ProvisionWifi {
            ssid: hs_ssid,
            passphrase: hs_pass,
        };
        let line = success_line("/dev/ttyACM0 (SN ABC)", &cmd);
        assert!(
            line.contains("homenet"),
            "success_line should mention SSID; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0"),
            "success_line should mention port; got: {line:?}"
        );
    }

    /// The destructive clear must name the clear action and the device it hit, so the
    /// operator cannot mistake it for one of the provisioning commands.
    #[test]
    fn success_line_clear_wifi_names_clear_action_and_identity() {
        let line = success_line("/dev/ttyACM0 (SN ABC)", &Command::ClearWifiCredentials);
        assert!(
            line.contains("cleared"),
            "success_line must name the clear action; got: {line:?}"
        );
        assert!(
            line.contains("wifi credentials"),
            "success_line must name what was cleared; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0 (SN ABC)"),
            "success_line must contain identity; got: {line:?}"
        );
        assert!(
            !line.contains("provisioned"),
            "success_line must not read as a provisioning action; got: {line:?}"
        );
    }

    #[test]
    fn success_line_set_temp_wifi_contains_ssid_not_passphrase() {
        let mut hs_ssid = heapless::String::<32>::new();
        hs_ssid.push_str("homenet").unwrap();
        let mut hs_pass = heapless::String::<64>::new();
        hs_pass.push_str("supersecret").unwrap();
        let cmd = Command::SetTemporaryWifiConfig {
            ssid: hs_ssid,
            passphrase: hs_pass,
        };
        let line = success_line("/dev/ttyACM0 (SN ABC)", &cmd);
        assert!(
            line.contains("homenet"),
            "success_line should mention SSID; got: {line:?}"
        );
        assert!(
            !line.contains("supersecret"),
            "success_line must not contain passphrase; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0 (SN ABC)"),
            "success_line must contain identity; got: {line:?}"
        );
    }

    #[test]
    fn success_line_clear_temp_wifi_names_action_and_identity() {
        let line = success_line("/dev/ttyACM0 (SN ABC)", &Command::ClearTemporaryWifiConfig);
        assert!(
            line.contains("cleared"),
            "success_line must name the clear action; got: {line:?}"
        );
        assert!(
            line.contains("temporary wifi"),
            "success_line must name what was cleared; got: {line:?}"
        );
        assert!(
            line.contains("/dev/ttyACM0 (SN ABC)"),
            "success_line must contain identity; got: {line:?}"
        );
    }

    // ── Response mapping ──────────────────────────────────────────────────────

    fn ok_resp() -> Result<Response, HarnessError> {
        Ok(Response {
            id: 1,
            status: Status::Ok,
            payload: Payload::Empty,
        })
    }

    fn fail_resp(msg: Option<&str>) -> Result<Response, HarnessError> {
        let payload = if let Some(m) = msg {
            let mut detail = heapless::String::<192>::new();
            detail.push_str(m).unwrap();
            Payload::TestReport(device_protocol::TestReport {
                detail,
                data: device_protocol::TestData::None,
            })
        } else {
            Payload::Empty
        };
        Ok(Response {
            id: 1,
            status: Status::Fail,
            payload,
        })
    }

    fn unsupported_resp() -> Result<Response, HarnessError> {
        Ok(Response {
            id: 1,
            status: Status::Unsupported,
            payload: Payload::Empty,
        })
    }

    #[test]
    fn map_response_ok_exits_zero_stdout() {
        let (msg, to_stdout, exit) = map_response(
            ok_resp(),
            "provisioned wifi on /dev/ttyACM0",
            "/dev/ttyACM0",
        );
        assert_eq!(exit, 0);
        assert!(to_stdout, "Status::Ok must go to stdout");
        assert_eq!(msg, "provisioned wifi on /dev/ttyACM0");
    }

    #[test]
    fn map_response_fail_with_test_report_exits_nonzero_stderr() {
        let (msg, to_stdout, exit) = map_response(
            fail_resp(Some("nvs write failed")),
            "ignored",
            "/dev/ttyACM0",
        );
        assert_eq!(exit, 1);
        assert!(!to_stdout, "Status::Fail must go to stderr");
        assert!(
            msg.contains("device rejected the command"),
            "AC12 phrase missing; got: {msg:?}"
        );
        assert!(
            msg.contains("nvs write failed"),
            "AC12 detail missing; got: {msg:?}"
        );
    }

    #[test]
    fn map_response_fail_escapes_device_authored_detail() {
        let (msg, _to_stdout, _exit) = map_response(
            fail_resp(Some("\x1b[31mforged\x1b[0m\nline")),
            "ignored",
            "/dev/ttyACM0",
        );
        assert!(
            !msg.contains('\x1b') && !msg.contains('\n'),
            "device detail must be escaped before hitting the terminal; got: {msg:?}"
        );
        assert!(
            msg.contains("\\u{1b}") && msg.contains("\\n"),
            "expected escaped control sequences; got: {msg:?}"
        );
    }

    #[test]
    fn map_response_fail_without_payload_exits_nonzero() {
        let (msg, to_stdout, exit) = map_response(fail_resp(None), "ignored", "/dev/ttyACM0");
        assert_eq!(exit, 1);
        assert!(!to_stdout);
        assert!(
            msg.contains("device rejected the command"),
            "AC12 phrase missing; got: {msg:?}"
        );
        // No extra detail appended after "command".
        assert!(
            msg.ends_with("command"),
            "unexpected trailing detail; got: {msg:?}"
        );
    }

    #[test]
    fn map_response_unsupported_exits_nonzero_stderr() {
        let (msg, to_stdout, exit) = map_response(unsupported_resp(), "ignored", "/dev/ttyACM0");
        assert_eq!(exit, 1);
        assert!(!to_stdout, "Status::Unsupported must go to stderr");
        assert!(
            msg.contains("firmware does not support"),
            "AC13 phrase missing; got: {msg:?}"
        );
        assert!(
            msg.contains("firmware/protocol mismatch"),
            "AC13 hint missing; got: {msg:?}"
        );
    }

    #[test]
    fn map_response_timeout_exits_nonzero_with_seconds() {
        let (msg, to_stdout, exit) =
            map_response(Err(HarnessError::Timeout), "ignored", "/dev/ttyACM0");
        assert_eq!(exit, 1);
        assert!(!to_stdout, "Timeout must go to stderr");
        assert!(
            msg.contains("timed out after"),
            "AC14 phrase missing; got: {msg:?}"
        );
        // Must not hardcode the value — derived from RESPONSE_TIMEOUT.
        let secs = RESPONSE_TIMEOUT.as_secs().to_string();
        assert!(
            msg.contains(&secs),
            "AC14 must include timeout seconds ({secs}); got: {msg:?}"
        );
    }

    // ── test-1: AC5/AC7 identity list content ─────────────────────────────────

    #[test]
    fn select_multiple_app_pods_ac5_identity_list() {
        // Ambiguous must carry correct identity strings for both pods.
        let pods = vec![app_sn("/dev/ttyACM0", "SN-A"), app("/dev/ttyACM1")];
        match select(&pods, None, None) {
            Err(SelErr::Ambiguous(ids)) => {
                assert!(!ids.is_empty(), "AC5 identity list must not be empty");
                assert!(
                    ids.iter()
                        .any(|s| s.contains("/dev/ttyACM0") && s.contains("SN-A")),
                    "AC5 list must contain identity for ttyACM0 (SN SN-A); got: {ids:?}"
                );
                assert!(
                    ids.iter().any(|s| s.contains("/dev/ttyACM1")),
                    "AC5 list must contain identity for ttyACM1; got: {ids:?}"
                );
            }
            other => panic!("expected Ambiguous; got {other:?}"),
        }
    }

    #[test]
    fn select_explicit_port_not_found_ac7_identity_list() {
        // NotFound must carry the attached pod's identity string.
        let pods = vec![app_sn("/dev/ttyACM0", "SN-X")];
        match select(&pods, Some("/dev/ttyACM9"), None) {
            Err(SelErr::NotFound(ids)) => {
                assert!(!ids.is_empty(), "AC7 identity list must not be empty");
                assert!(
                    ids.iter()
                        .any(|s| s.contains("/dev/ttyACM0") && s.contains("SN-X")),
                    "AC7 list must contain attached pod identity; got: {ids:?}"
                );
            }
            other => panic!("expected NotFound; got {other:?}"),
        }
    }

    // ── test-2: duplicate SN → AC5 Ambiguous ─────────────────────────────────

    #[test]
    fn select_by_serial_duplicate_sn_ac5() {
        // Two app pods with the same SN → AC5 Ambiguous.
        let pods = vec![
            app_sn("/dev/ttyACM0", "DUPE"),
            app_sn("/dev/ttyACM1", "DUPE"),
        ];
        assert!(
            matches!(select(&pods, None, Some("DUPE")), Err(SelErr::Ambiguous(_))),
            "duplicate SN with --serial must produce AC5 Ambiguous"
        );
    }

    // ── test-3: --serial to DFU pod falls to AC7 (TODO(podctl-dfu-serial) pin) ──

    #[test]
    fn select_by_serial_dfu_pod_falls_to_ac7() {
        // A DFU pod is present with a matching serial number (or None), no app pod.
        // Current contract (until HIL confirms DFU exposes SN): falls to AC7 NotFound.
        // If this test is intentionally changed, update TODO(podctl-dfu-serial).
        let dfu_with_sn = PodPort {
            port_name: "/dev/ttyACM0".to_string(),
            serial_number: Some("TARGET".to_string()),
            mode: PodMode::Dfu,
        };
        let pods = vec![dfu_with_sn];
        assert!(
            matches!(
                select(&pods, None, Some("TARGET")),
                Err(SelErr::NotFound(_))
            ),
            "--serial to DFU pod must fall to AC7 NotFound (not AC4) until HIL confirms"
        );
    }

    // ── test-4: validate_peer missing-field coverage for all five fields ───────

    #[test]
    fn validate_peer_missing_udp_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: None,
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "udp-port",
                    flag: "udp-port",
                    env: "PODCTL_UDP_PORT"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_tcp_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: None,
            tls_host: Some("10.0.0.1".into()),
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "tcp-port",
                    flag: "tcp-port",
                    env: "PODCTL_TCP_PORT"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_tls_host() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: None,
            tls_port: Some("8883".into()),
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "tls-host",
                    flag: "tls-host",
                    env: "PODCTL_TLS_HOST"
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_peer_missing_tls_port() {
        let err = validate_peer(PeerArgs {
            host: Some("192.168.1.1".into()),
            udp_port: Some("5000".into()),
            tcp_port: Some("5001".into()),
            tls_host: Some("10.0.0.1".into()),
            tls_port: None,
            inbound_frames_port: Some("17382".into()),
            backpressure_port: Some("17383".into()),
            poll_readiness_port: Some("17384".into()),
            rtd_port: Some("17385".into()),
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                ValidationError::Missing {
                    input: "tls-port",
                    flag: "tls-port",
                    env: "PODCTL_TLS_PORT"
                }
            ),
            "got {err:?}"
        );
    }

    // ── CLI parse: logs subcommand ────────────────────────────────────────────

    #[test]
    fn cli_logs_no_targeting() {
        // `podctl logs` with no targeting args parses to Cmd::Logs with all None.
        // Hold the env mutex so this test cannot race the *_from_env tests on the env.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cli = Cli::try_parse_from(["podctl", "logs"]).unwrap();
        match cli.command {
            Cmd::Logs {
                port,
                serial,
                log_jsonl,
            } => {
                assert!(port.is_none(), "port should be None; got {port:?}");
                assert!(serial.is_none(), "serial should be None; got {serial:?}");
                assert!(
                    log_jsonl.is_none(),
                    "log_jsonl should be None; got {log_jsonl:?}"
                );
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_with_port() {
        // Hold the env mutex so this test cannot race the *_from_env tests on the env.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cli = Cli::try_parse_from(["podctl", "logs", "--port", "/dev/ttyACM0"]).unwrap();
        match cli.command {
            Cmd::Logs {
                port,
                serial,
                log_jsonl: _,
            } => {
                assert_eq!(port.as_deref(), Some("/dev/ttyACM0"));
                assert!(serial.is_none());
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_with_serial() {
        // Hold the env mutex so this test cannot race the *_from_env tests on the env.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cli = Cli::try_parse_from(["podctl", "logs", "--serial", "ABC123"]).unwrap();
        match cli.command {
            Cmd::Logs {
                port,
                serial,
                log_jsonl: _,
            } => {
                assert!(port.is_none());
                assert_eq!(serial.as_deref(), Some("ABC123"));
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_with_port_and_serial() {
        // Hold the env mutex so this test cannot race the *_from_env tests on the env.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cli = Cli::try_parse_from([
            "podctl",
            "logs",
            "--port",
            "/dev/ttyACM1",
            "--serial",
            "XYZ",
        ])
        .unwrap();
        match cli.command {
            Cmd::Logs {
                port,
                serial,
                log_jsonl: _,
            } => {
                assert_eq!(port.as_deref(), Some("/dev/ttyACM1"));
                assert_eq!(serial.as_deref(), Some("XYZ"));
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_with_log_jsonl_flag() {
        // Hold the env mutex so this test cannot race the *_from_env tests on the env.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cli = Cli::try_parse_from(["podctl", "logs", "--log-jsonl", "/tmp/x.jsonl"]).unwrap();
        match cli.command {
            Cmd::Logs {
                port: _,
                serial: _,
                log_jsonl,
            } => {
                assert_eq!(
                    log_jsonl.as_deref(),
                    Some(std::path::Path::new("/tmp/x.jsonl")),
                    "log_jsonl should be Some(\"/tmp/x.jsonl\"); got {log_jsonl:?}"
                );
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    // Parse `podctl logs` with `var=value` set in the process environment. Holds the
    // shared env mutex across set → parse → remove so it cannot race the other env
    // tests, and removes the var before unwrapping so a parse failure cannot leak it
    // into sibling tests. The single home for the leak-safety ordering.
    fn parse_logs_with_env(var: &str, value: &str) -> Cli {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var(var, value);
        }
        let result = Cli::try_parse_from(["podctl", "logs"]);
        unsafe {
            std::env::remove_var(var);
        }
        result.unwrap()
    }

    #[test]
    fn cli_logs_log_jsonl_from_env() {
        // PODCTL_LOG_JSONL env var populates log_jsonl when the flag is absent.
        let cli = parse_logs_with_env("PODCTL_LOG_JSONL", "/tmp/env.jsonl");
        match cli.command {
            Cmd::Logs {
                port: _,
                serial: _,
                log_jsonl,
            } => {
                assert_eq!(
                    log_jsonl.as_deref(),
                    Some(std::path::Path::new("/tmp/env.jsonl")),
                    "log_jsonl from env should be Some(\"/tmp/env.jsonl\"); got {log_jsonl:?}"
                );
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_port_from_env() {
        // PODCTL_PORT env var populates port when the flag is absent. Guards against a
        // typo in the `env = "..."` string silently disabling the binding.
        let cli = parse_logs_with_env("PODCTL_PORT", "/dev/ttyENV");
        match cli.command {
            Cmd::Logs {
                port,
                serial: _,
                log_jsonl: _,
            } => {
                assert_eq!(
                    port.as_deref(),
                    Some("/dev/ttyENV"),
                    "port from env should be Some(\"/dev/ttyENV\"); got {port:?}"
                );
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn cli_logs_serial_from_env() {
        // PODCTL_SERIAL env var populates serial when the flag is absent. Guards against a
        // typo in the `env = "..."` string silently disabling the binding.
        let cli = parse_logs_with_env("PODCTL_SERIAL", "ENV123");
        match cli.command {
            Cmd::Logs {
                port: _,
                serial,
                log_jsonl: _,
            } => {
                assert_eq!(
                    serial.as_deref(),
                    Some("ENV123"),
                    "serial from env should be Some(\"ENV123\"); got {serial:?}"
                );
            }
            _other => panic!("expected Cmd::Logs; got another variant"),
        }
    }

    #[test]
    fn jsonl_record_shape() {
        use device_protocol::{LogFrame, LogLevel};
        use serde_json::Value;

        // Construct a LogFrame with a control char (\n) and non-ASCII char in message.
        let mut log = LogFrame {
            level: LogLevel::Info,
            target: heapless::String::new(),
            message: heapless::String::new(),
        };
        log.target.push_str("wifi::driver").unwrap();
        log.message.push_str("connect\nfailed: café").unwrap();

        // Serialize via the production helper path.
        let rec = crate::JsonlRecord {
            ts_ms: crate::now_unix_ms(),
            level: log.level,
            target: &log.target,
            message: &log.message,
        };
        let line = serde_json::to_string(&rec).expect("serialize must not fail");

        // Must parse as a JSON object.
        let v: Value = serde_json::from_str(&line).expect("output must be valid JSON");

        // level → variant-name string.
        assert_eq!(v["level"], Value::String("Info".to_owned()));

        // target and message round-trip exactly; serde_json escapes \n as \\n inside
        // the JSON string, but from_str gives us back the original bytes.
        assert_eq!(v["target"].as_str().unwrap(), "wifi::driver");
        assert_eq!(v["message"].as_str().unwrap(), "connect\nfailed: café");

        // ts_ms is a numeric field greater than 0 (assuming host clock > epoch).
        let ts = v["ts_ms"].as_u64().expect("ts_ms must be a u64");
        assert!(ts > 0, "ts_ms must be > 0; got {ts}");
    }

    #[test]
    fn write_jsonl_record_produces_valid_newline_terminated_json() {
        // Exercises the complete write_jsonl_record production path:
        // now_unix_ms → JsonlRecord → serde_json::to_string → writeln! → flush.
        use device_protocol::{LogFrame, LogLevel};
        use std::io::{Read as _, Seek, SeekFrom};

        let mut log = LogFrame {
            level: LogLevel::Warn,
            target: heapless::String::new(),
            message: heapless::String::new(),
        };
        log.target.push_str("net").unwrap();
        log.message.push_str("timeout").unwrap();

        // write_jsonl_record takes &mut File; use a tempfile to satisfy the type.
        let mut f = tempfile::tempfile().expect("tempfile");
        crate::write_jsonl_record(&mut f, &log);

        // Read back what was written.
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        f.read_to_end(&mut buf).unwrap();

        // Must be a single newline-terminated line.
        assert!(buf.ends_with(b"\n"), "output must end with newline");
        let line = std::str::from_utf8(&buf)
            .expect("output must be UTF-8")
            .trim_end_matches('\n');
        assert!(!line.contains('\n'), "output must be exactly one line");

        // Must parse as a valid JSON object with expected fields.
        let v: serde_json::Value = serde_json::from_str(line).expect("output must be valid JSON");
        assert_eq!(v["level"], serde_json::Value::String("Warn".to_owned()));
        assert_eq!(v["target"].as_str().unwrap(), "net");
        assert_eq!(v["message"].as_str().unwrap(), "timeout");
        let ts = v["ts_ms"].as_u64().expect("ts_ms must be a u64");
        assert!(ts > 0, "ts_ms must be > 0; got {ts}");
    }

    #[test]
    fn jsonl_level_variants_serialize_as_variant_name_strings() {
        // Pins the contract that LogLevel serializes to its variant name string
        // for all variants, not just Info. A serde rename on any variant would
        // break JSONL consumers silently without this test.
        use device_protocol::LogLevel;

        let cases = [
            (LogLevel::Error, "Error"),
            (LogLevel::Warn, "Warn"),
            (LogLevel::Info, "Info"),
            (LogLevel::Debug, "Debug"),
            (LogLevel::Trace, "Trace"),
        ];
        for (level, expected) in cases {
            let json = serde_json::to_string(&level).expect("serialize must not fail");
            assert_eq!(
                json,
                format!("\"{}\"", expected),
                "LogLevel::{expected:?} must serialize as \"{expected}\""
            );
        }
    }

    // `select` is already unit-tested for the provision path; the logs path calls
    // the same function via open_selected, so no new select tests are needed here.

    // ── test-5: map_response does not leak passphrase (structural note) ───────
    //
    // The passphrase only exists inside `Command::ProvisionWifi`, which is moved
    // into `harness.send_command(cmd)` *before* `map_response` is called (run():~464).
    // `map_response` receives only `Result<Response, HarnessError>` and the pre-built
    // success_msg string (which is guarded by `success_line_never_contains_passphrase`).
    // Because `Command` is consumed at the send site, no error path in `map_response`
    // can access it. The structural separation is the safety guarantee; this test
    // pins the Timeout error path as the furthest point the call stack reaches.
    #[test]
    fn map_response_io_error_includes_identity_not_passphrase() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Input/output error");
        let (msg, to_stdout, exit) = map_response(
            Err(HarnessError::Write(io_err)),
            "ignored",
            "/dev/ttyACM0 (SN ABC)",
        );
        assert_eq!(exit, 1);
        assert!(!to_stdout, "I/O error must go to stderr");
        // Identity included in error.
        assert!(
            msg.contains("/dev/ttyACM0"),
            "I/O error must include port identity; got: {msg:?}"
        );
        // Passphrase never reaches map_response; structural guarantee tested here.
        assert!(
            !msg.contains("supersecret"),
            "error must not contain passphrase; got: {msg:?}"
        );
    }

    // ── Log streaming: stdout hangup ──────────────────────────────────────────

    fn test_log(message: &str) -> LogFrame {
        let mut log = LogFrame {
            level: LogLevel::Info,
            target: heapless::String::new(),
            message: heapless::String::new(),
        };
        log.target.push_str("wifi::driver").unwrap();
        log.message.push_str(message).unwrap();
        log
    }

    /// A `Write` stub that accepts `ok_lines` complete lines (counted by newline,
    /// since `write_fmt` may issue several `write` calls per line) and then fails
    /// every subsequent write with `fail_kind`.
    struct FlakyWriter {
        buf: Vec<u8>,
        lines: usize,
        ok_lines: usize,
        fail_kind: std::io::ErrorKind,
        /// Number of `write` calls made after the writer started failing.
        attempts_after_fail: usize,
    }

    impl FlakyWriter {
        fn new(ok_lines: usize, fail_kind: std::io::ErrorKind) -> Self {
            Self {
                buf: Vec::new(),
                lines: 0,
                ok_lines,
                fail_kind,
                attempts_after_fail: 0,
            }
        }
    }

    impl Write for FlakyWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.lines >= self.ok_lines {
                self.attempts_after_fail += 1;
                return Err(std::io::Error::new(self.fail_kind, "stub write failure"));
            }
            self.buf.extend_from_slice(buf);
            self.lines += buf.iter().filter(|b| **b == b'\n').count();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Minimal in-memory transport: yields queued bytes, then times out forever.
    struct FakePort {
        rx: std::collections::VecDeque<u8>,
    }

    impl FakePort {
        fn with_logs(msgs: &[&str]) -> Self {
            let mut rx = std::collections::VecDeque::new();
            for m in msgs {
                let mut buf = [0u8; 512];
                let len = device_protocol::framing::encode_device_frame(
                    &DeviceFrame::Log(test_log(m)),
                    &mut buf,
                )
                .unwrap();
                rx.extend(buf[..len].iter().copied());
            }
            Self { rx }
        }
    }

    impl std::io::Read for FakePort {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.rx.is_empty() {
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "no data"));
            }
            let n = buf.len().min(self.rx.len());
            for (dst, src) in buf[..n].iter_mut().zip(self.rx.drain(..n)) {
                *dst = src;
            }
            Ok(n)
        }
    }

    impl Write for FakePort {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn reader_with_logs(msgs: &[&str]) -> FrameReader {
        FrameReader::with_label(Box::new(FakePort::with_logs(msgs)), "fake0")
    }

    #[test]
    fn emit_log_line_writes_formatted_line() {
        let log = test_log("hello");
        let mut out: Vec<u8> = Vec::new();
        assert!(emit_log_line(&mut out, &log).unwrap());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            format!("{}\n", format_log(&log))
        );
    }

    #[test]
    fn emit_log_line_broken_pipe_is_clean_stop() {
        let mut out = FlakyWriter::new(0, std::io::ErrorKind::BrokenPipe);
        assert!(!emit_log_line(&mut out, &test_log("hello")).unwrap());
    }

    #[test]
    fn emit_log_line_other_error_propagates() {
        let mut out = FlakyWriter::new(0, std::io::ErrorKind::Other);
        let err = emit_log_line(&mut out, &test_log("hello")).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn emit_log_line_against_real_closed_pipe() {
        let (rx, mut tx) = std::io::pipe().unwrap();
        drop(rx);
        assert!(!emit_log_line(&mut tx, &test_log("hello")).unwrap());
    }

    #[test]
    fn stream_logs_exits_zero_when_pipe_closes() {
        let mut out = FlakyWriter::new(1, std::io::ErrorKind::BrokenPipe);
        let code = stream_logs(reader_with_logs(&["one", "two"]), None, &mut out);
        assert_eq!(code, 0);
        assert_eq!(out.buf.iter().filter(|b| **b == b'\n').count(), 1);
        assert_eq!(
            out.attempts_after_fail, 1,
            "second frame of the batch must not be written to a dead stdout"
        );
    }

    #[test]
    fn stream_logs_exits_one_on_real_write_error() {
        let mut out = FlakyWriter::new(0, std::io::ErrorKind::Other);
        let code = stream_logs(reader_with_logs(&["one", "two"]), None, &mut out);
        assert_eq!(code, 1);
        assert_eq!(
            out.attempts_after_fail, 1,
            "a hard write error must be reported once, not once per frame in the batch"
        );
    }

    #[test]
    fn stream_logs_keeps_jsonl_after_stdout_dies() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let f = File::create(&path).unwrap();
        let mut out = FlakyWriter::new(0, std::io::ErrorKind::BrokenPipe);
        let code = stream_logs(reader_with_logs(&["one", "two"]), Some(f), &mut out);
        assert_eq!(code, 0);
        let jsonl = std::fs::read_to_string(&path).unwrap();
        assert_eq!(jsonl.lines().count(), 2, "both frames must reach JSONL");
        assert_eq!(
            out.attempts_after_fail, 1,
            "both frames arrived in one batch, but only the first may touch stdout"
        );
    }
}
