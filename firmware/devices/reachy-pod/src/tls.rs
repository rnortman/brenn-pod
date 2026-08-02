//! The pod's link to the audio host, built from its configuration.
//!
//! Connection mechanics live in `psk-link`; this module supplies only the
//! pod-specific inputs: which identity and which key to present, and to whom.

use psk_link::link::LinkPlatform;

use crate::config::Config;

/// Build the streamer's platform from a loaded configuration.
///
/// `pod_id` is the host name rather than a configured value — it is both the
/// `Hello` id and the TLS-PSK identity, and the host keys its table by it.
///
/// The platform captures the key for the life of the streamer thread: a reconnect
/// must not depend on re-reading a file that may have been replaced mid-run by a
/// half-written one.
pub fn platform(pod_id: String, config: &Config) -> LinkPlatform {
    LinkPlatform::new(pod_id, config.addr, config.psk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pod_streamer::run::StreamerPlatform;

    /// A configuration whose address and key are distinguishable from defaults.
    fn config() -> Config {
        Config::parse(&format!(
            "ADDR=198.51.100.7:5555\nPSK={}\n",
            "ab".repeat(psk_link::PSK_LEN)
        ))
        .expect("parse")
    }

    #[test]
    fn the_platform_presents_the_configured_host_and_the_given_identity() {
        let platform = platform("reachy00".to_string(), &config());
        assert_eq!(platform.pod_id(), "reachy00");
        assert_eq!(
            platform.peer(),
            "198.51.100.7:5555".parse().expect("peer address"),
            "the streamer dials the address the file named, not a default"
        );
    }
}
