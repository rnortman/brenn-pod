//! The daemon's configuration file: which machine, whose intents, and on what
//! bus.
//!
//! One TOML file, named on the command line, `deny_unknown_fields` throughout —
//! a mistyped key is a startup refusal naming the key rather than a daemon
//! quietly running on a default nobody chose.
//!
//! The machine half of the configuration is deliberately *not* here.
//! `motion_config` names the same bench TOML the operator tool reads on this
//! unit, so the crank datum, the envelope, the bus timing and the move durations
//! have exactly one source of truth on the machine. Two files describing one
//! platform is two files to disagree, and the disagreement would be about the
//! numbers that keep the head out of the linkage's singular configurations.
//!
//! What is here is only what the daemon adds: whose intents to obey, which
//! channel they arrive on, how long an engaged intent is good for, how often the
//! motion loop comes up for air, and the bridge's own table nested whole.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

/// How long an engaged intent is good for when the file does not say.
///
/// Sized against the publisher's refresh cadence: it must survive two missed
/// refreshes, so a single dropped message and the reconnect after it do not stow
/// a head in the middle of a conversation.
const fn default_lease_ttl_ms() -> u64 {
    15_000
}

/// How long the motion loop watches the machine between polls of the lease, when
/// the file does not say.
///
/// This is the daemon's whole reaction latency to an intent, and it is also the
/// cadence at which the machine is monitored. Short enough that nobody perceives
/// the lag, long enough that the monitoring is not the dominant traffic on the
/// wire.
const fn default_hold_dwell_ms() -> u64 {
    200
}

/// The daemon's configuration, as the file is written.
///
/// Comparable whole so the shipped example can be asserted against a minimal
/// file field by field rather than key by key: the example is what an operator
/// copies, and a key added here without a default it actually writes is the
/// failure a file of that shape invites.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The bench configuration for this unit — the one the operator tool reads,
    /// not a copy.
    pub motion_config: PathBuf,
    /// The pod identity whose intents this daemon obeys. Bodies addressed to any
    /// other pod are reported and dropped — the channel is not assumed to carry
    /// one machine's traffic.
    pub pod: String,
    /// The channel presence intents arrive on. No default: which channel a
    /// deployment uses is operator topology, and a name invented here would be
    /// a convention two ends could silently disagree about.
    pub channel: String,
    /// How long an engaged intent stays good for, from the moment it arrived.
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
    /// How long the motion loop watches the machine between polls of the lease.
    #[serde(default = "default_hold_dwell_ms")]
    pub hold_dwell_ms: u64,
    /// The bus attachment. Nested whole so there is one description of a bridge
    /// and every embedder writes the same table.
    pub bridge: brenn_bridge::Config,
}

/// Why a configuration could not be used.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file is not there, or cannot be read as this account.
    #[error("cannot read the daemon configuration at {path}")]
    Read {
        /// The file named on the command line.
        path: PathBuf,
        /// What the read failed with.
        source: std::io::Error,
    },
    /// The file is not the TOML this daemon expects — a missing key, a key
    /// nothing reads, or a value of the wrong shape.
    #[error("cannot parse the daemon configuration at {path}")]
    Parse {
        /// The file named on the command line.
        path: PathBuf,
        /// What the parser refused, line and column included.
        source: toml::de::Error,
    },
    /// The file parsed and says something the daemon will not run on.
    #[error("{path}: {message}")]
    Invalid {
        /// The file named on the command line.
        path: PathBuf,
        /// Which value, and why it is refused.
        message: String,
    },
}

impl Config {
    /// Read, parse and validate the configuration at `path`.
    ///
    /// Nothing here opens a port, reads the bench configuration or touches the
    /// token file: this is the whole of what can be decided from text, and it is
    /// decided before the daemon acquires anything.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config = Self::parse(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate().map_err(|message| ConfigError::Invalid {
            path: path.to_path_buf(),
            message,
        })?;
        Ok(config)
    }

    /// Parse a configuration from TOML text. Semantic validation is separate —
    /// see [`Config::validate`].
    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    /// The checks the TOML grammar cannot express.
    ///
    /// Each one rejects a value whose runtime behaviour is a wedge rather than a
    /// slow daemon: a channel nothing can deliver on, an identity nothing can
    /// match, a term the loop can never observe held.
    pub fn validate(&self) -> Result<(), String> {
        if self.motion_config.as_os_str().is_empty() {
            return Err(
                "motion_config is empty; name the bench configuration for this unit".to_string(),
            );
        }
        if self.pod.is_empty() {
            return Err(
                "pod is empty; name the identity whose presence intents this daemon obeys"
                    .to_string(),
            );
        }
        // The grammar is the bridge's — it is that transport a `local:` address
        // never crosses — so it is answered by the bridge rather than copied
        // here and left to drift from the other end's copy.
        brenn_bridge::validate_channel_name(&self.channel)
            .map_err(|refusal| format!("channel {refusal}"))?;
        if self.lease_ttl_ms == 0 {
            return Err(
                "lease_ttl_ms must be at least 1 (0 expires every intent as it arrives)"
                    .to_string(),
            );
        }
        if self.hold_dwell_ms == 0 {
            return Err(
                "hold_dwell_ms must be at least 1 (0 polls the lease in a spin loop)".to_string(),
            );
        }
        if self.lease_ttl_ms <= self.hold_dwell_ms {
            return Err(format!(
                "lease_ttl_ms {} must exceed hold_dwell_ms {}; a term shorter than one dwell can \
                 lapse between two polls, so the loop would never see a lease held",
                self.lease_ttl_ms, self.hold_dwell_ms
            ));
        }
        self.bridge.validate()
    }

    /// How long an engaged intent stays good for.
    #[must_use]
    pub fn lease_ttl(&self) -> Duration {
        Duration::from_millis(self.lease_ttl_ms)
    }

    /// How long the motion loop watches the machine between polls of the lease.
    #[must_use]
    pub fn hold_dwell(&self) -> Duration {
        Duration::from_millis(self.hold_dwell_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The top-level half of the smallest file that runs.
    const TOP: &str = "\
motion_config = \"/run/brenn-app/conf/reachy-bench.toml\"
pod = \"reachy00\"
channel = \"brenn:reachy.presence\"
";

    /// The nested bridge table, which every fixture ends with — TOML puts a
    /// bare key after a table header *into* that table, so the top-level keys a
    /// case adds have to go in ahead of this.
    const BRIDGE: &str = "\
[bridge]
server_url = \"wss://bus.example/remote/reachy/ws\"
token_file = \"/run/brenn-app/conf/bridge.token\"
";

    /// The smallest file that runs, plus whatever top-level lines a case adds.
    fn file(extra: &str) -> String {
        format!("{TOP}{extra}\n{BRIDGE}")
    }

    fn minimal() -> Config {
        let config = Config::parse(&file("")).expect("the minimal file parses");
        config.validate().expect("the minimal file validates");
        config
    }

    /// The same file with one line replaced, for the refusal cases.
    fn with(line: &str, replacement: &str) -> Config {
        let text = file("").replace(line, replacement);
        assert_ne!(text, file(""), "the line to replace is in the fixture");
        Config::parse(&text).expect("the edited file parses")
    }

    #[test]
    fn the_minimal_file_carries_the_defaults() {
        let config = minimal();
        assert_eq!(config.pod, "reachy00");
        assert_eq!(config.channel, "brenn:reachy.presence");
        assert_eq!(
            config.motion_config,
            PathBuf::from("/run/brenn-app/conf/reachy-bench.toml")
        );
        assert_eq!(config.lease_ttl(), Duration::from_secs(15));
        assert_eq!(config.hold_dwell(), Duration::from_millis(200));
    }

    #[test]
    fn stated_timings_override_the_defaults() {
        let text = file("lease_ttl_ms = 9000\nhold_dwell_ms = 50");
        let config = Config::parse(&text).expect("the file parses");
        config.validate().expect("the file validates");
        assert_eq!(config.lease_ttl(), Duration::from_secs(9));
        assert_eq!(config.hold_dwell(), Duration::from_millis(50));
    }

    /// A key nothing reads is a refusal, not a no-op: a misspelled `pod` is a
    /// daemon obeying nobody's intents with a file that looks right.
    #[test]
    fn an_unknown_key_is_refused() {
        let text = file("poll_hz = 5");
        let error = Config::parse(&text).expect_err("an unknown key is refused");
        assert!(error.to_string().contains("poll_hz"), "{error}");
    }

    #[test]
    fn a_missing_required_key_is_refused() {
        let text = file("").replace("pod = \"reachy00\"\n", "");
        let error = Config::parse(&text).expect_err("a missing key is refused");
        assert!(error.to_string().contains("pod"), "{error}");
    }

    #[test]
    fn an_untransportable_channel_is_refused() {
        let config = with(
            "channel = \"brenn:reachy.presence\"",
            "channel = \"local:reachy.presence\"",
        );
        let message = config.validate().expect_err("a local: channel is refused");
        assert!(message.contains("local:reachy.presence"), "{message}");
    }

    #[test]
    fn a_channel_that_is_only_the_prefix_is_refused() {
        let config = with(
            "channel = \"brenn:reachy.presence\"",
            "channel = \"brenn:\"",
        );
        let message = config.validate().expect_err("a bare prefix is refused");
        assert!(message.contains("names nothing"), "{message}");
    }

    #[test]
    fn an_empty_pod_is_refused() {
        let config = with("pod = \"reachy00\"", "pod = \"\"");
        let message = config.validate().expect_err("an empty pod is refused");
        assert!(message.contains("pod is empty"), "{message}");
    }

    #[test]
    fn an_empty_motion_config_is_refused() {
        let config = with(
            "motion_config = \"/run/brenn-app/conf/reachy-bench.toml\"",
            "motion_config = \"\"",
        );
        let message = config
            .validate()
            .expect_err("an empty motion_config is refused");
        assert!(message.contains("motion_config is empty"), "{message}");
    }

    #[test]
    fn a_zero_lease_term_is_refused() {
        let text = file("lease_ttl_ms = 0");
        let config = Config::parse(&text).expect("the file parses");
        let message = config.validate().expect_err("a zero term is refused");
        assert!(message.contains("lease_ttl_ms"), "{message}");
    }

    #[test]
    fn a_zero_dwell_is_refused() {
        let text = file("hold_dwell_ms = 0");
        let config = Config::parse(&text).expect("the file parses");
        let message = config.validate().expect_err("a zero dwell is refused");
        assert!(message.contains("hold_dwell_ms"), "{message}");
    }

    /// A term inside one dwell can lapse between two polls, so the loop would
    /// never observe a lease held and the head would never rise.
    #[test]
    fn a_term_no_longer_than_a_dwell_is_refused() {
        let text = file("lease_ttl_ms = 200\nhold_dwell_ms = 200");
        let config = Config::parse(&text).expect("the file parses");
        let message = config
            .validate()
            .expect_err("a term inside a dwell is refused");
        assert!(message.contains("must exceed hold_dwell_ms"), "{message}");
    }

    /// The nested table is validated by its own owner, through this one call, so
    /// a bad bridge is a startup refusal here rather than a dial that fails
    /// later.
    #[test]
    fn the_nested_bridge_table_is_validated() {
        let config = with(
            "server_url = \"wss://bus.example/remote/reachy/ws\"",
            "server_url = \"ws://bus.example/remote/reachy/ws\"",
        );
        let message = config.validate().expect_err("a cleartext URL is refused");
        assert!(message.contains("wss://"), "{message}");
    }

    /// The shipped example is a working file, and the defaults it writes out
    /// really are the defaults.
    ///
    /// The example is what an operator copies, so a key renamed here without it
    /// would hand them a file the daemon refuses at startup on a machine at the
    /// bench — and `deny_unknown_fields` makes a stale key in it a refusal too.
    /// The values are compared against the minimal file's rather than only
    /// parsed, because a comment claiming a default beside a number that is not
    /// one is the failure a file of this shape invites.
    ///
    /// The comparison is of the whole value, not a list of keys: the file's own
    /// premise is that this vocabulary grows, and an enumerated comparison would
    /// pass over the next defaulted key written out at whatever the author
    /// typed. Only the five keys the example's header names as mandatory are
    /// taken from the example itself; everything else has to be the default.
    #[test]
    fn the_shipped_example_parses_validates_and_states_the_defaults() {
        let text = include_str!("../reachy-motiond.example.toml");
        let example = Config::parse(text).expect("the example parses");
        example.validate().expect("the example validates");

        let defaults = minimal();
        assert_eq!(
            example,
            Config {
                motion_config: example.motion_config.clone(),
                pod: example.pod.clone(),
                channel: example.channel.clone(),
                bridge: brenn_bridge::Config {
                    server_url: example.bridge.server_url.clone(),
                    token_file: example.bridge.token_file.clone(),
                    ..defaults.bridge.clone()
                },
                ..defaults
            },
        );
    }

    #[test]
    fn a_missing_file_names_itself() {
        let path = std::env::temp_dir().join("reachy-motiond-absent.toml");
        let error = Config::load(&path).expect_err("a file that is not there is refused");
        assert!(matches!(error, ConfigError::Read { .. }), "{error}");
        assert!(
            error.to_string().contains("reachy-motiond-absent"),
            "{error}"
        );
    }
}
