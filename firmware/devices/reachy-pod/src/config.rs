//! The pod's runtime configuration: where the audio host is, which key to present,
//! and how the capture and gate are tuned.
//!
//! One file, `audio.conf`, in [`CONF_DIR`] — RAM, beside the payload store, not the
//! device's flash. It is `KEY=VALUE` lines — the shape every other configuration file
//! on the device takes — and it holds the pod's pre-shared key, so it is owner-only
//! and pushed per unit rather than baked into a payload.
//!
//! Configuration is pushed the way the payload is pushed, and a reboot clears both.
//! That is what keeps normal operation off the eMMC, and it costs nothing: a pod that
//! starts before its file is back parks and re-reads every [`RECHECK_INTERVAL`], so
//! re-pushing after a reboot is the whole recovery.
//!
//! The pod id is not in the file. It is the host name, which provisioning sets per
//! unit and which doubles as the TLS-PSK identity, exactly as the ESP pod uses its
//! own id: two places to write one unit's name is two places for them to disagree.
//!
//! The tuning keys mirror the ESP pod's provisioned settings and fall back to the
//! same shared defaults, so a pod that says nothing about the gate behaves the way
//! both pods do out of the box.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use audio_pipeline::vad::{VAD_HANGOVER_MS, VAD_HANGOVER_MS_MAX, vad_threshold_ok};
use pod_streamer::telemetry::VAD_THRESHOLD_DEFAULT;
use psk_link::{MAX_IDENTITY_LEN, PSK_LEN, parse_psk_hex};

/// Where this pod's configuration is pushed: a directory in the same tmpfs the
/// payload store lives in. Compiled in rather than named by the environment —
/// where the pod reads its own configuration is the pod's knowledge, not something
/// the platform hands it, and one constant is one place for the pushing tool and
/// the reading pod to agree.
pub const CONF_DIR: &str = "/run/brenn-app/conf";

/// The configuration file's name inside [`CONF_DIR`].
pub const CONF_FILE_NAME: &str = "audio.conf";

/// How long to wait before looking again when the file is missing or unusable.
///
/// A pod that starts before its credentials are placed parks and re-reads rather
/// than exiting: the same five seconds the ESP pod waits on its own provisioning,
/// so an operator sees one cadence across both pods.
pub const RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// The two capture channels the board presents. Both are processed renderings of
/// the chip's one auto-selected look direction, so the wrong one costs at most the
/// processing flavor — never a direction.
pub const CHANNELS: usize = 2;

/// The capture channel used when the file does not name one. Which one to prefer
/// is a bring-up decision taken from a reviewed bench run, not a fact derived here.
pub const DEFAULT_CHANNEL: usize = 0;

/// Everything the pipeline needs that is not compiled in.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// The audio host, as an address and port. Not a name: the CM4 has no clock at
    /// boot and may have no resolver, and a link that cannot be dialed until DNS
    /// works is a link that stays down for reasons nothing here can report.
    pub addr: SocketAddr,
    /// This pod's pre-shared key. Never rendered — not in a log line, not in an
    /// error — so a configuration failure can be reported verbatim.
    pub psk: [u8; PSK_LEN],
    /// Which of the board's two capture channels is streamed.
    pub channel: usize,
    /// Speech-energy level above which the chip's telemetry counts as speech.
    pub vad_threshold: f32,
    /// How long the gate stays open after the energy drops below the threshold.
    pub vad_hangover_ms: u32,
}

/// Why a configuration could not be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The file is not there yet, or cannot be read as this account.
    Unreadable { path: PathBuf, reason: String },
    /// A line is not `KEY=VALUE`.
    ///
    /// The line itself is deliberately not carried. The likeliest malformation of
    /// this file is a mistyped `PSK` line — a space instead of `=`, or the key
    /// pasted bare on a line of its own — and echoing the offender would put the
    /// pre-shared key in the journal. The number is what an operator needs.
    Malformed { line_no: usize },
    /// A key appears twice. Taking the first would run the pod on a value nobody
    /// can see in the file's last word on the subject.
    Duplicate { line_no: usize, key: String },
    /// A key the file does not carry, and has no default.
    Missing { key: &'static str },
    /// A value that is present and unusable.
    BadValue { key: String, reason: String },
    /// A key nothing reads. Silently ignoring it is how a misspelled `CHANNEL`
    /// becomes a pod streaming the wrong channel with a configuration file that
    /// looks right.
    Unknown { line_no: usize, key: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { path, reason } => {
                write!(f, "cannot read {}: {reason}", path.display())
            }
            Self::Malformed { line_no } => write!(f, "line {line_no} is not KEY=VALUE"),
            Self::Duplicate { line_no, key } => {
                write!(f, "line {line_no} sets {key} a second time")
            }
            Self::Missing { key } => write!(f, "no {key} in the configuration"),
            Self::BadValue { key, reason } => write!(f, "{key}: {reason}"),
            Self::Unknown { line_no, key } => {
                write!(f, "line {line_no} sets {key}, which nothing reads")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// The configuration file's path.
pub fn conf_path() -> PathBuf {
    Path::new(CONF_DIR).join(CONF_FILE_NAME)
}

impl Config {
    /// Read and parse [`conf_path`].
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(&conf_path())
    }

    /// Read and parse one file.
    pub fn load_from(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Unreadable {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
        Self::parse(&text)
    }

    /// Parse the file's contents.
    ///
    /// Blank lines and `#` comments are skipped; everything else must be a key this
    /// understands. Values are trimmed, and a quoted value is taken as written —
    /// nothing here does shell quoting, and a key whose value arrives with quotes
    /// around it fails on the value rather than silently keying off `"…"`.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let mut addr: Option<SocketAddr> = None;
        let mut psk: Option<[u8; PSK_LEN]> = None;
        let mut channel: Option<usize> = None;
        let mut vad_threshold: Option<f32> = None;
        let mut vad_hangover_ms: Option<u32> = None;

        for (i, raw) in text.lines().enumerate() {
            let line_no = i + 1;
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(ConfigError::Malformed { line_no });
            };
            let key = key.trim();
            let value = value.trim();
            let seen = |taken: bool| {
                if taken {
                    Err(ConfigError::Duplicate {
                        line_no,
                        key: key.to_string(),
                    })
                } else {
                    Ok(())
                }
            };
            match key {
                "ADDR" => {
                    seen(addr.is_some())?;
                    addr = Some(parse_addr(value)?);
                }
                "PSK" => {
                    seen(psk.is_some())?;
                    // The label is the key, not the value: the parser's error text
                    // quotes what it was given, and what it was given is key material.
                    psk = Some(
                        parse_psk_hex("PSK", value).map_err(|_| ConfigError::BadValue {
                            key: key.to_string(),
                            reason: format!("not {} hexadecimal characters", PSK_LEN * 2),
                        })?,
                    );
                }
                "CHANNEL" => {
                    seen(channel.is_some())?;
                    channel = Some(parse_channel(value)?);
                }
                "VAD_THRESHOLD" => {
                    seen(vad_threshold.is_some())?;
                    vad_threshold = Some(parse_threshold(value)?);
                }
                "VAD_HANGOVER_MS" => {
                    seen(vad_hangover_ms.is_some())?;
                    vad_hangover_ms = Some(parse_hangover(value)?);
                }
                _ => {
                    return Err(ConfigError::Unknown {
                        line_no,
                        key: key.to_string(),
                    });
                }
            }
        }

        Ok(Self {
            addr: addr.ok_or(ConfigError::Missing { key: "ADDR" })?,
            psk: psk.ok_or(ConfigError::Missing { key: "PSK" })?,
            channel: channel.unwrap_or(DEFAULT_CHANNEL),
            vad_threshold: vad_threshold.unwrap_or(VAD_THRESHOLD_DEFAULT),
            vad_hangover_ms: vad_hangover_ms.unwrap_or(VAD_HANGOVER_MS),
        })
    }
}

/// The value is echoed here, unlike on the `PSK` path: what an operator has to see
/// to fix a bad address is the address. The trade-off is a file whose two values
/// were swapped — `ADDR=<the key>` — echoing key material. That costs a deliberate
/// transposition of two values rather than a slip on one line, and an address
/// reported as "unusable" without saying which one is a line nobody can act on.
fn parse_addr(value: &str) -> Result<SocketAddr, ConfigError> {
    value.parse().map_err(|_| ConfigError::BadValue {
        key: "ADDR".to_string(),
        reason: format!("{value:?} is not an address and port (198.51.100.7:5555)"),
    })
}

fn parse_channel(value: &str) -> Result<usize, ConfigError> {
    match value.parse::<usize>() {
        Ok(c) if c < CHANNELS => Ok(c),
        _ => Err(ConfigError::BadValue {
            key: "CHANNEL".to_string(),
            reason: format!("{value:?} is not one of the board's {CHANNELS} capture channels"),
        }),
    }
}

/// Both pods must accept the same threshold range, so a value one refuses is not a
/// value the other runs on.
fn parse_threshold(value: &str) -> Result<f32, ConfigError> {
    match value.parse::<f32>() {
        Ok(t) if vad_threshold_ok(t) => Ok(t),
        _ => Err(ConfigError::BadValue {
            key: "VAD_THRESHOLD".to_string(),
            reason: format!("{value:?} is not a finite, non-negative number"),
        }),
    }
}

/// Both pods must accept the same hangover range: zero would close the gate between
/// two words, and the upper bound is what keeps the tick conversion from
/// overflowing.
fn parse_hangover(value: &str) -> Result<u32, ConfigError> {
    match value.parse::<u32>() {
        Ok(ms) if (1..=VAD_HANGOVER_MS_MAX).contains(&ms) => Ok(ms),
        _ => Err(ConfigError::BadValue {
            key: "VAD_HANGOVER_MS".to_string(),
            reason: format!(
                "{value:?} is not a whole number of milliseconds in 1..={VAD_HANGOVER_MS_MAX}"
            ),
        }),
    }
}

/// This unit's host name — the pod id on the wire and the PSK identity in the
/// handshake.
///
/// Empty when the kernel gives no name, which is a device whose provisioning did not
/// apply; the caller reports that rather than dialing under a name the host has no
/// key for.
pub fn hostname() -> String {
    // A name longer than the buffer is truncated by the kernel with no error, so the
    // buffer is `HOST_NAME_MAX + 1` and the terminator is placed by hand: what
    // `gethostname` promises about NUL termination on truncation varies.
    let mut buf = [0u8; HOST_NAME_BUF];
    // SAFETY: the pointer and length describe `buf`, which outlives the call.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len() - 1) };
    name_from_buf(rc, &buf)
}

/// The buffer [`hostname`] reads into: `HOST_NAME_MAX` plus the terminator this
/// module places itself.
const HOST_NAME_BUF: usize = 65;

/// Decode what `gethostname` left in the buffer.
///
/// Pure, so every arm is decidable without an ambient host name: a call that
/// failed, a name shorter than the buffer, a name that filled it with no
/// terminator left, and bytes that are not UTF-8.
fn name_from_buf(rc: i32, buf: &[u8]) -> String {
    if rc != 0 {
        return String::new();
    }
    // The last slot is reserved for the terminator this module writes, so a name
    // that reaches it is a truncated one and ends there.
    let limit = buf.len().saturating_sub(1);
    let end = buf[..limit].iter().position(|b| *b == 0).unwrap_or(limit);
    String::from_utf8_lossy(&buf[..end]).trim().to_string()
}

/// Whether `name` can be presented as this pod's TLS-PSK identity, or why not.
///
/// The handshake refuses an identity outside `1..=MAX_IDENTITY_LEN`, and it does so
/// on every reconnect with a TLS-setup error that reads exactly like a wrong key or
/// a missing host-side table entry. Checked once at startup instead, so a unit whose
/// provisioned name is too long says so in one line an operator can act on.
pub fn check_pod_id(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(
            "the host name is empty, so this pod has no identity to present — provisioning \
             sets it per unit"
                .to_string(),
        );
    }
    if name.len() > MAX_IDENTITY_LEN {
        return Err(format!(
            "the host name {name:?} is {} bytes, and a TLS-PSK identity may be at most \
             {MAX_IDENTITY_LEN} — provisioning must give this unit a shorter name",
            name.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_HEX: &str = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

    fn minimal() -> String {
        format!("ADDR=198.51.100.7:5555\nPSK={KEY_HEX}\n")
    }

    #[test]
    fn a_minimal_file_parses_and_takes_the_shared_defaults() {
        let config = Config::parse(&minimal()).expect("parse");
        assert_eq!(config.addr, "198.51.100.7:5555".parse().expect("addr"));
        assert_eq!(config.psk[0], 0x01);
        assert_eq!(config.psk[PSK_LEN - 1], 0x20);
        assert_eq!(config.channel, DEFAULT_CHANNEL);
        // The defaults are the shared constants, so a pod that says nothing about
        // the gate behaves the same across both pod types.
        assert_eq!(config.vad_threshold, VAD_THRESHOLD_DEFAULT);
        assert_eq!(config.vad_hangover_ms, VAD_HANGOVER_MS);
    }

    #[test]
    fn comments_blank_lines_and_surrounding_space_are_ignored() {
        let text = format!(
            "# the audio host\n\n  ADDR = 198.51.100.7:5555  \n\t#PSK=not this one\nPSK={KEY_HEX}\nCHANNEL=1\n"
        );
        let config = Config::parse(&text).expect("parse");
        assert_eq!(config.channel, 1);
        assert_eq!(config.addr.port(), 5555);
    }

    #[test]
    fn every_tuning_key_is_read() {
        let text = format!(
            "{}CHANNEL=1\nVAD_THRESHOLD=2.5\nVAD_HANGOVER_MS=1200\n",
            minimal()
        );
        let config = Config::parse(&text).expect("parse");
        assert_eq!(config.channel, 1);
        assert_eq!(config.vad_threshold, 2.5);
        assert_eq!(config.vad_hangover_ms, 1200);
    }

    #[test]
    fn the_two_required_keys_are_named_when_absent() {
        let no_addr = Config::parse(&format!("PSK={KEY_HEX}\n")).unwrap_err();
        assert_eq!(no_addr, ConfigError::Missing { key: "ADDR" });
        let no_psk = Config::parse("ADDR=198.51.100.7:5555\n").unwrap_err();
        assert_eq!(no_psk, ConfigError::Missing { key: "PSK" });
        assert!(no_psk.to_string().contains("PSK"), "{no_psk}");
    }

    #[test]
    fn a_bad_psk_is_reported_without_echoing_the_value() {
        let text = format!("ADDR=198.51.100.7:5555\nPSK={}\n", "zz".repeat(PSK_LEN));
        let err = Config::parse(&text).unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("PSK"), "{rendered}");
        assert!(
            !rendered.contains("zz"),
            "key material must not reach a log line: {rendered}"
        );
        // A short key is refused for the same reason a wrong one is: the handshake
        // would fail with nothing local to point at.
        let short = "ADDR=198.51.100.7:5555\nPSK=0102\n";
        assert!(Config::parse(short).is_err());
    }

    #[test]
    fn an_address_must_carry_a_port() {
        let err = Config::parse(&format!("ADDR=198.51.100.7\nPSK={KEY_HEX}\n")).unwrap_err();
        assert!(err.to_string().contains("198.51.100.7"), "{err}");
        // A name is refused too: this pod dials an address, and a resolver failure
        // at boot is not a diagnosis anything here could give.
        assert!(Config::parse(&format!("ADDR=host.example:5555\nPSK={KEY_HEX}\n")).is_err());
    }

    #[test]
    fn a_channel_outside_the_boards_two_is_refused() {
        for value in ["2", "-1", "left", ""] {
            let text = format!("{}CHANNEL={value}\n", minimal());
            let err = Config::parse(&text).unwrap_err().to_string();
            assert!(err.contains("CHANNEL"), "{value}: {err}");
        }
        // Both real channels are accepted — what each one carries is empirical.
        for value in [0, 1] {
            let text = format!("{}CHANNEL={value}\n", minimal());
            assert_eq!(Config::parse(&text).expect("parse").channel, value);
        }
    }

    #[test]
    fn gate_values_are_bounded_the_way_the_esp_pods_provisioned_ones_are() {
        for value in ["-0.5", "nan", "inf", "loud"] {
            let text = format!("{}VAD_THRESHOLD={value}\n", minimal());
            assert!(
                Config::parse(&text).is_err(),
                "threshold {value} must not pass"
            );
        }
        assert_eq!(
            Config::parse(&format!("{}VAD_THRESHOLD=0\n", minimal()))
                .expect("parse")
                .vad_threshold,
            0.0
        );
        for value in ["0", "60001", "-5", "1.5"] {
            let text = format!("{}VAD_HANGOVER_MS={value}\n", minimal());
            assert!(
                Config::parse(&text).is_err(),
                "hangover {value} must not pass"
            );
        }
        assert_eq!(
            Config::parse(&format!(
                "{}VAD_HANGOVER_MS={VAD_HANGOVER_MS_MAX}\n",
                minimal()
            ))
            .expect("parse")
            .vad_hangover_ms,
            VAD_HANGOVER_MS_MAX
        );
    }

    #[test]
    fn a_repeated_key_and_an_unread_one_are_both_refused() {
        let repeated = Config::parse(&format!("{}CHANNEL=0\nCHANNEL=1\n", minimal())).unwrap_err();
        assert_eq!(
            repeated,
            ConfigError::Duplicate {
                line_no: 4,
                key: "CHANNEL".to_string()
            }
        );
        let unknown = Config::parse(&format!("{}CHANEL=1\n", minimal())).unwrap_err();
        assert!(
            unknown.to_string().contains("CHANEL"),
            "a misspelled key must be named: {unknown}"
        );
    }

    #[test]
    fn a_line_that_is_not_a_setting_is_refused_with_its_number() {
        let err = Config::parse(&format!("{}just some words\n", minimal())).unwrap_err();
        assert_eq!(err, ConfigError::Malformed { line_no: 3 });
        assert_eq!(err.to_string(), "line 3 is not KEY=VALUE");
    }

    #[test]
    fn a_malformed_line_is_never_echoed_because_it_may_be_the_key() {
        // The likeliest way to malform this file is to mistype the PSK line, and a
        // refusal that quotes the offender puts the key in the journal. Every one of
        // these carries the key material; none of them may render it.
        for line in [
            format!("PSK {KEY_HEX}"),
            format!("PSK:{KEY_HEX}"),
            KEY_HEX.to_string(),
        ] {
            let text = format!("ADDR=198.51.100.7:5555\n{line}\n");
            let rendered = Config::parse(&text).unwrap_err().to_string();
            assert!(
                !rendered.contains(KEY_HEX) && !rendered.contains(&KEY_HEX[..8]),
                "key material must not reach a log line: {rendered}"
            );
            assert!(rendered.contains("line 2"), "{rendered}");
        }
    }

    #[test]
    fn a_missing_file_names_the_path_it_looked_at() {
        let path = Path::new("/nonexistent/brenn/audio.conf");
        let err = Config::load_from(path).unwrap_err();
        assert!(
            err.to_string().contains("/nonexistent/brenn/audio.conf"),
            "{err}"
        );
        assert!(matches!(err, ConfigError::Unreadable { .. }));
    }

    #[test]
    fn a_file_on_disk_parses_through_the_same_path() {
        let dir = std::env::temp_dir().join(format!("reachy-pod-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("tempdir");
        let path = dir.join(CONF_FILE_NAME);
        std::fs::write(&path, minimal()).expect("write");
        let config = Config::load_from(&path).expect("load");
        assert_eq!(config.addr.port(), 5555);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn the_configuration_is_read_from_the_path_the_pushing_tool_writes() {
        // The pod's configuration path is the one thing in this device that spans
        // two languages with no compiler between them: the provisioning tool
        // composes it out of shell constants and this pod compiles its own copy.
        // Drift either way is a pod parked on a file nobody writes, re-reading it
        // every RECHECK_INTERVAL forever, so read the tool's constants and
        // compare rather than restating this side's.
        let tools = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools");
        let lib = std::fs::read_to_string(tools.join("lib.sh")).expect("read lib.sh");
        let tool = std::fs::read_to_string(tools.join("provision-reachy-pod.sh"))
            .expect("read provision-reachy-pod.sh");

        // One `name=value` assignment at the start of a line, unquoted.
        let assigned = |text: &str, name: &str| -> String {
            let prefix = format!("{name}=");
            let line = text
                .lines()
                .find(|l| l.starts_with(&prefix))
                .unwrap_or_else(|| panic!("no {prefix} line in the provisioning tool"));
            line[prefix.len()..].trim().trim_matches('"').to_string()
        };

        let store_mount = assigned(&lib, "store_mount");
        let conf_dir = assigned(&tool, "conf_dir").replace("${store_mount}", &store_mount);
        let conf_file = assigned(&tool, "conf_file").replace("${conf_dir}", &conf_dir);

        assert_eq!(
            conf_dir, CONF_DIR,
            "the provisioning tool pushes into a different directory than CONF_DIR"
        );
        assert_eq!(
            PathBuf::from(conf_file),
            conf_path(),
            "the provisioning tool writes a different file than the pod reads"
        );
    }

    #[test]
    fn the_host_name_decodes_out_of_whatever_the_kernel_left_in_the_buffer() {
        let mut short = [0u8; HOST_NAME_BUF];
        short[..8].copy_from_slice(b"reachy00");
        assert_eq!(name_from_buf(0, &short), "reachy00");

        // A call that failed is no name at all, not the buffer's zeroes.
        assert_eq!(name_from_buf(-1, &short), "");

        // A name that filled the buffer leaves no terminator: the kernel truncated
        // it, and the decode must stop at the reserved last slot rather than run on.
        let full = [b'n'; HOST_NAME_BUF];
        let decoded = name_from_buf(0, &full);
        assert_eq!(decoded.len(), HOST_NAME_BUF - 1);
        assert!(!decoded.contains('\0'), "{decoded:?}");

        let mut padded = [0u8; HOST_NAME_BUF];
        padded[..10].copy_from_slice(b"  pod01  \n");
        assert_eq!(name_from_buf(0, &padded), "pod01");

        // Bytes that are not UTF-8 are replaced rather than lost or panicked on.
        let mut invalid = [0u8; HOST_NAME_BUF];
        invalid[..3].copy_from_slice(&[b'p', 0xff, b'd']);
        let lossy = name_from_buf(0, &invalid);
        assert!(lossy.starts_with('p') && lossy.ends_with('d'), "{lossy:?}");
    }

    #[test]
    fn a_name_the_handshake_would_refuse_is_refused_at_startup_instead() {
        assert_eq!(check_pod_id("reachy00"), Ok(()));
        assert_eq!(check_pod_id(&"p".repeat(MAX_IDENTITY_LEN)), Ok(()));

        let empty = check_pod_id("").unwrap_err();
        assert!(empty.contains("empty"), "{empty}");

        // One byte past what `client_context` accepts. Without this the pod parks in
        // the connect loop forever, failing TLS setup in a way indistinguishable
        // from a wrong key.
        let long = "p".repeat(MAX_IDENTITY_LEN + 1);
        let err = check_pod_id(&long).unwrap_err();
        assert!(err.contains(&MAX_IDENTITY_LEN.to_string()), "{err}");
        assert!(psk_link::client_context(&long, [0u8; PSK_LEN]).is_err());
    }

    #[test]
    fn the_startup_check_agrees_with_the_handshake_on_whatever_name_this_host_has() {
        // The ambient name is whatever the lane runs under — a sandbox may have
        // none, and a workstation may have a long one — so what is asserted is the
        // property that holds either way: the startup check and the handshake reach
        // the same verdict on it, which is the whole point of checking early.
        let name = hostname();
        assert_eq!(name, name.trim());
        assert_eq!(
            check_pod_id(&name).is_ok(),
            psk_link::client_context(&name, [0u8; PSK_LEN]).is_ok(),
            "the startup check must accept exactly the names the handshake does: {name:?}"
        );
    }
}
