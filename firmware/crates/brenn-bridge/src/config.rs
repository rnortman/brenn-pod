//! Startup configuration: the TOML file that points the bridge at a bus and the
//! bearer token it authenticates with.
//!
//! `deny_unknown_fields` on every table makes a typo fatal at startup rather
//! than a silent no-op, and `server_url` is required and must be `wss://`: the
//! backend terminates no TLS of its own, the token is a long-lived credential,
//! and a cleartext URL is refused here — before the connector's own guard could
//! fire — so the refusal names the config line an operator can fix.

use std::fmt;
use std::path::{Path, PathBuf};

use brenn_attach_client::conn::ConnConfig;
use serde::Deserialize;

/// The only URL scheme this bridge will dial.
const WSS_SCHEME: &str = "wss://";

/// The prefix a channel name must carry to be transportable over the bus.
///
/// A `local:` address is delivered inside the process that minted it and never
/// reaches this attachment, so a subscription to one waits forever and a publish
/// to one is refused by the subscription plane.
pub const CHANNEL_PREFIX: &str = "brenn:";

/// Whether a channel name is one this attachment can carry.
///
/// The rule belongs to the bridge rather than to its embedders: it is this
/// crate's transport that a `local:` address never crosses. Embedders put the
/// configuration key in front of the message this answers with, so the same
/// refusal reads correctly wherever a channel is configured.
pub fn validate_channel_name(channel: &str) -> Result<(), String> {
    let Some(rest) = channel.strip_prefix(CHANNEL_PREFIX) else {
        return Err(format!(
            "{channel:?} must name a transportable channel (a {CHANNEL_PREFIX:?} prefix); \
             a local: address never crosses the wire"
        ));
    };
    if rest.is_empty() {
        return Err(format!(
            "{channel:?} names nothing after the {CHANNEL_PREFIX:?} prefix"
        ));
    }
    Ok(())
}

/// Parsed bridge configuration.
///
/// Comparable whole, so an embedder shipping an example file can assert that the
/// values it writes out under a "this is the default" comment really are the
/// defaults — and that a field added here is covered by that assertion without
/// anyone remembering to extend it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The fully-formed websocket URL of the remote route, path included —
    /// `wss://<host>/remote/<slug>/ws`. The bridge appends nothing: which host
    /// the pod reaches the reverse proxy on, and under which slug, is operator
    /// topology, and the attachment protocol carries no opinion about either.
    pub server_url: String,
    /// Path to the bearer token file. Mode-checked and read once, when the
    /// bridge is built ([`crate::Bridge::new`] calls [`Token::load`]) — never
    /// re-read on a reconnect, so a rotated token file takes effect at the next
    /// process start and not before.
    pub token_file: PathBuf,
    /// Free-form build identifier put on this end's handshake, for the server's
    /// logs. Never parsed by either end.
    #[serde(default = "default_ident")]
    pub ident: String,
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

/// Reconnect and liveness timings.
///
/// Every field has a default sized for a countertop appliance on a home LAN:
/// reconnect briskly, cap the backoff well under a human's patience, and treat
/// three missed heartbeats as a dead peer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    /// Delay before the first reconnect attempt after a drop. Jittered by the
    /// connection layer, so a fleet restarting together does not re-dial in
    /// lockstep.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Ceiling the backoff doubles up to.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// How long one connect-plus-handshake may take before the attempt is
    /// abandoned and backed off.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Multiples of the server's advertised heartbeat interval of inbound
    /// silence that mark the attachment dead.
    #[serde(default = "default_liveness_multiplier")]
    pub liveness_multiplier: u32,
    /// How many consecutive futile attachments end the process.
    ///
    /// An attachment is futile when this bridge sent something on it and the
    /// peer answered nothing before the socket died — the shape a rejected
    /// frame produces, since the peer's answer to an illegal frame is a closed
    /// socket and not a refusal frame. A network drop on an idle attachment is
    /// not futile: nothing was sent, so nothing went unanswered.
    #[serde(default = "default_max_futile_attachments")]
    pub max_futile_attachments: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            connect_timeout_ms: default_connect_timeout_ms(),
            liveness_multiplier: default_liveness_multiplier(),
            max_futile_attachments: default_max_futile_attachments(),
        }
    }
}

impl Config {
    /// Read, parse, and validate the TOML config at `path`. Read, parse, and
    /// validation errors all carry the path and precise context.
    ///
    /// The token file is *not* read here — [`crate::Bridge::new`] does that
    /// through [`Token::load`], once, before the first dial — so a config can be
    /// parsed and asserted on without a secret on disk.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config = Config::parse(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate().map_err(|message| ConfigError::Invalid {
            path: path.to_path_buf(),
            message,
        })?;
        Ok(config)
    }

    /// Parse config from an in-memory TOML string (path-free; [`Config::load`]
    /// wraps it). Semantic validation is separate — see [`Config::validate`].
    pub fn parse(text: &str) -> Result<Config, toml::de::Error> {
        toml::from_str(text)
    }

    /// Semantic checks the TOML grammar cannot express, so a misconfiguration is
    /// a precise startup error rather than a silent runtime hazard.
    pub fn validate(&self) -> Result<(), String> {
        let Some(authority) = self.server_url.strip_prefix(WSS_SCHEME) else {
            return Err(format!(
                "server_url {:?} is not {WSS_SCHEME}; the bearer token is a long-lived \
                 credential and this link carries it in a header",
                self.server_url
            ));
        };
        if authority.is_empty() {
            return Err(format!("server_url {:?} names no host", self.server_url));
        }
        if self.token_file.as_os_str().is_empty() {
            return Err("token_file is empty; name the file holding the bearer token".to_string());
        }
        if self.ident.is_empty() {
            return Err("ident is empty; it is what the server's logs name this build".to_string());
        }
        self.reconnect.validate()
    }

    /// The connection parameters this config lowers to.
    ///
    /// The backoff jitter seed is derived from `server_url` rather than read
    /// from entropy: the seed only has to differ between attachers so a fleet
    /// decorrelates after a server restart, and every pod's URL already differs
    /// in its slug. Deriving it keeps a restarted pod's reconnect schedule
    /// reproducible, which a fixed constant would also do but identically for
    /// every pod — the one property the seed exists to avoid.
    pub fn conn_config(&self) -> ConnConfig {
        ConnConfig {
            url: self.server_url.clone(),
            ident: self.ident.clone(),
            initial_backoff: std::time::Duration::from_millis(self.reconnect.initial_backoff_ms),
            max_backoff: std::time::Duration::from_millis(self.reconnect.max_backoff_ms),
            connect_timeout: std::time::Duration::from_millis(self.reconnect.connect_timeout_ms),
            liveness_multiplier: self.reconnect.liveness_multiplier,
            backoff_jitter_seed: jitter_seed(&self.server_url),
            // TODO(bridge-violation-close-code): the remote route closes on a
            // rejected frame without a code that says so, so there is no close
            // the bridge can single out as terminal. The futile-attachment
            // budget infers that shape instead — see
            // `ReconnectConfig::max_futile_attachments`.
            terminal_close_code: None,
        }
    }
}

impl ReconnectConfig {
    /// Semantic checks over the timings. Each rejects a value whose runtime
    /// behaviour would be a wedge rather than a slow link.
    pub fn validate(&self) -> Result<(), String> {
        if self.initial_backoff_ms == 0 {
            return Err(
                "reconnect.initial_backoff_ms must be at least 1 (0 re-dials in a spin loop)"
                    .to_string(),
            );
        }
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err(format!(
                "reconnect.max_backoff_ms {} is below initial_backoff_ms {}",
                self.max_backoff_ms, self.initial_backoff_ms
            ));
        }
        if self.connect_timeout_ms == 0 {
            return Err(
                "reconnect.connect_timeout_ms must be at least 1 (0 abandons every attempt)"
                    .to_string(),
            );
        }
        if self.liveness_multiplier == 0 {
            return Err(
                "reconnect.liveness_multiplier must be at least 1 (0 tolerates no silence and \
                 reaps every attachment on its first tick)"
                    .to_string(),
            );
        }
        if self.max_futile_attachments == 0 {
            return Err(
                "reconnect.max_futile_attachments must be at least 1 (0 ends the process before \
                 the first attachment)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// A bearer token read off disk.
///
/// A distinct type rather than a `String` so the credential cannot reach a log
/// through a derived `Debug` on something holding it: this one prints a byte
/// count and nothing else, matching the pod's posture on PSK material.
#[derive(Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    /// Read, mode-check, and trim the token at `path`.
    ///
    /// Refuses a file any other local account can read — the host's one secrets
    /// posture, `pod_secrets`, which the PSK table is held to as well. Trailing
    /// whitespace is trimmed — an editor's newline is not part of the credential
    /// — and an empty result is a refusal, since an empty token would
    /// authenticate nothing while looking configured.
    pub fn load(path: &Path) -> Result<Token, ConfigError> {
        if let Some(message) = pod_secrets::mode_error(path, "token file") {
            return Err(ConfigError::TokenMode {
                path: path.to_path_buf(),
                message,
            });
        }
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::TokenRead {
            path: path.to_path_buf(),
            source,
        })?;
        let token = text.trim().to_string();
        if token.is_empty() {
            return Err(ConfigError::TokenEmpty {
                path: path.to_path_buf(),
            });
        }
        Ok(Token(token))
    }

    /// The token as the connector wants it: the bare credential, no scheme.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Token {{ bytes: {} }}", self.0.len())
    }
}

/// FNV-1a over the URL bytes. Any spreading function would do; this one is four
/// lines and has no dependency behind it.
fn jitter_seed(url: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in url.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// A failure loading configuration or the token file it names, carrying the
/// offending path.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid {path}: {message}")]
    Invalid { path: PathBuf, message: String },
    /// A [`Config`] that failed [`Config::validate`] with no file behind it —
    /// one an embedder composed in memory, or parsed out of a larger document
    /// through [`Config::parse`].
    #[error("invalid bridge configuration: {message}")]
    Rejected { message: String },
    #[error("failed to read token file {path}: {source}")]
    TokenRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("refusing token file {path}: {message}")]
    TokenMode { path: PathBuf, message: String },
    #[error("token file {path} is empty")]
    TokenEmpty { path: PathBuf },
}

fn default_ident() -> String {
    concat!("brenn-bridge/", env!("CARGO_PKG_VERSION")).to_string()
}
fn default_initial_backoff_ms() -> u64 {
    500
}
fn default_max_backoff_ms() -> u64 {
    30_000
}
fn default_connect_timeout_ms() -> u64 {
    15_000
}
fn default_liveness_multiplier() -> u32 {
    3
}
fn default_max_futile_attachments() -> u32 {
    3
}

#[cfg(test)]
mod tests;
