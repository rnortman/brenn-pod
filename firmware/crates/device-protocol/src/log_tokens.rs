//! Log-line tokens shared between the pod firmware's `log::info!`/`log::warn!` emit
//! sites and the `hil-host` evals that match/parse those lines.
//!
//! These are the cross-boundary contract: every token here is either a line prefix the
//! host filters on or a field name the host parses a value out of. Fields the host never
//! reads stay literal at the emit site — there is no contract to bind.
//!
//! `log::info!`/`log::warn!` require a literal format string, so emit sites must pass
//! these consts as **arguments** through `{}` slots — never re-inline the string.
//!
//! The host's field parse substring-searches the whole line, so a new literal (unbound) field
//! name added to a line carrying bound tokens must not contain any bound token — otherwise the
//! parse reads the wrong field silently.

/// WiFi `StaDisconnected` event callback. Emitted with the reason code appended.
/// Firmware: `devices/respeaker-pod/src/main.rs`. Host: `eval_wifi_reassociation_pass`,
/// `eval_gateway_probe_gate_unreachable`.
pub const WIFI_DISCONNECTED: &str = "wifi: disconnected reason=";

/// WiFi `StaConnected` event callback. Firmware: `devices/respeaker-pod/src/main.rs`.
/// Host: `eval_wifi_reassociation_pass`, `eval_gateway_probe_gate_unreachable`.
pub const WIFI_CONNECTED: &str = "wifi: connected";

/// DHCP lease acquired (`IpEvent::DhcpIpAssigned`). Also the prefix of the lease-lost
/// warn (`"<this> lost"`), which therefore satisfies a bare lease match.
/// Firmware: `devices/respeaker-pod/src/main.rs`. Host: `eval_wifi_reassociation_pass`,
/// `eval_gateway_probe_gate_unreachable`.
pub const WIFI_DHCP_LEASE: &str = "wifi: dhcp lease";

/// WiFi supervisor completed a re-association. Firmware: `devices/respeaker-pod/src/wifi.rs`.
/// Host: `eval_wifi_reassociation_pass`, `eval_gateway_probe_gate_unreachable`.
pub const WIFI_REASSOCIATED: &str = "wifi-supervisor: re-associated";

/// WiFi supervisor thread's one-time-per-boot startup line. This is a **negative**
/// assertion's anchor: `BootAssociationRetry` asserts this line is absent from a window to
/// prove no reboot occurred. Routing it through this registry (rather than a raw string
/// literal duplicated at the host call site) means a firmware rewording breaks the pairwise
/// non-containment test or the build, instead of the negative assertion silently passing
/// forever against a line the firmware no longer emits. Firmware:
/// `devices/respeaker-pod/src/wifi.rs`. Host: `eval_boot_association_retry_failures`,
/// `eval_boot_association_retry_recovery`.
pub const WIFI_SUPERVISOR_STARTED: &str = "wifi-supervisor: started";

/// WiFi supervisor parked because NVS holds no credentials. Charges no backoff and blocks
/// on the doorbell until provisioning arrives. Firmware:
/// `devices/respeaker-pod/src/wifi.rs`. Host: the `NoCredentialsPark` hil-host step.
pub const WIFI_PARKED_NO_CREDS: &str = "wifi-supervisor: no credentials — parked";

/// WiFi supervisor logged a failed re-association attempt (RF/AP failure, backoff charged).
/// Firmware: `devices/respeaker-pod/src/wifi.rs`. Host: the `NoCredentialsPark` step's
/// negative retry-spam assertion.
pub const WIFI_REASSOC_ATTEMPT_FAILED: &str = "wifi-supervisor: re-association attempt failed";

/// WiFi supervisor is about to attempt a re-association, logged with the attempt counter
/// immediately before the blocking connect call — the exact instant the supervisor's
/// wait-spacing guarantee is anchored at. Firmware: `devices/respeaker-pod/src/wifi.rs`.
/// Host: `BootAssociationRetry`'s backoff-spacing assertion, which measures gaps between
/// these lines rather than between `WIFI_REASSOC_ATTEMPT_FAILED` lines (the latter are
/// logged at attempt *end*, after a connect duration that varies attempt to attempt, so
/// their spacing is not the guaranteed quantity).
pub const WIFI_REASSOC_ATTEMPT_START: &str = "wifi-supervisor: re-association attempt starting";

/// WiFi supervisor entered the slow lane after repeated association failures.
/// Firmware: `devices/respeaker-pod/src/wifi.rs`. Host: the `NoCredentialsPark` step's
/// negative retry-spam assertion.
pub const WIFI_CONSECUTIVE_FAILURES: &str =
    "wifi-supervisor: many consecutive association failures";

/// A temporary (RAM-only) WiFi config override was applied. Logged with the ssid only —
/// never the passphrase, matching the `handle_provision_wifi` ssid-only logging.
/// Firmware: `devices/respeaker-pod/src/wifi.rs`.
pub const WIFI_TEMP_CONFIG_APPLIED: &str = "wifi: temporary config applied ssid=";

/// A temporary (RAM-only) WiFi config override was cleared (it was present). Firmware:
/// `devices/respeaker-pod/src/wifi.rs`.
pub const WIFI_TEMP_CONFIG_CLEARED: &str = "wifi: temporary config cleared";

/// Error-detail substring marking "NVS holds no usable WiFi credentials". Not a log line:
/// it is carried in a `TestReport` detail and matched by the supervisor's park arm and by
/// the host's `eval_no_credentials_park`. Firmware: `devices/respeaker-pod/src/wifi.rs`.
pub const NO_NVS_CREDENTIALS: &str = "no NVS credentials";

/// Capture-thread periodic summary line 1 prefix. Carries the HIL-parsed cross-check
/// tokens and must fit a 200-char `heapless::String` budget — do NOT add tokens to this
/// line; they can truncate the `rx_window_us` divisor. Firmware:
/// `devices/respeaker-pod/src/capture.rs`. Host: `eval_capture_periodic_line`,
/// `eval_playback_drain_rate`.
pub const CAPTURE_TX_LINE: &str = "capture: playback tx ";

/// Capture-thread periodic summary line 2 prefix — human-observability counters plus the
/// `rx_deficit=` mic-RX-loss telemetry, which lives here rather than on
/// [`CAPTURE_TX_LINE`] because that line is at its 200-char heapless budget.
/// Firmware: `devices/respeaker-pod/src/capture.rs`. Host: `eval_full_duplex_rx_integrity`.
pub const CAPTURE_OBS_LINE: &str = "capture: playback obs ";

/// Write-units drained this window, on [`CAPTURE_TX_LINE`].
pub const CHUNKS: &str = "chunks=";

/// Exact wall-time the window spanned in µs, on [`CAPTURE_TX_LINE`].
pub const RX_WINDOW_US: &str = "rx_window_us=";

/// First-poll-non-empty outer-pass count, on [`CAPTURE_TX_LINE`].
pub const NONEMPTY_POLLS: &str = "nonempty_polls=";

/// First-poll-empty outer-pass count, on [`CAPTURE_TX_LINE`]. Deliberately not spelled
/// `empty_polls=`, which is a substring of [`NONEMPTY_POLLS`] and made the host's
/// substring parse depend on emit order.
pub const POLL_EMPTY: &str = "poll_empty=";

/// Window-measurement validity, on [`CAPTURE_OBS_LINE`]: `1` = rx_deficit is a real
/// measurement; `0` = the window ran a tone test, mic RX was not drained, and
/// rx_deficit was suppressed to 0. Always emitted.
pub const RX_WIN_OK: &str = "rx_win_ok=";

/// Mic-RX frame-loss counter, on [`CAPTURE_OBS_LINE`].
pub const RX_DEFICIT: &str = "rx_deficit=";

/// First-poll-empty outer passes absorbed by the playback pre-roll gate this window, on
/// [`CAPTURE_OBS_LINE`]. Nonzero means the consumer was parked waiting for the ring to
/// refill rather than draining, so the window's `chunks=` is depressed for a reason that is
/// not a drain regression. Host: `eval_playback_drain_rate` (saturated-window exclusion).
pub const PREROLL_WAITS: &str = "preroll_waits=";

/// Mid-stream pre-roll re-arms this window, on [`CAPTURE_OBS_LINE`]. Nonzero means an
/// underrun re-armed the gate inside the window, so part of it was spent not draining even
/// if no first poll ever found the ring empty. Host: `eval_playback_drain_rate`.
pub const PREROLL_REARMS: &str = "preroll_rearms=";

/// Capture-thread core affinity, on [`CAPTURE_OBS_LINE`].
pub const CORE: &str = "core=";

/// Capture-thread FreeRTOS priority, on [`CAPTURE_OBS_LINE`].
pub const PRIO: &str = "prio=";

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_pairwise_non_containing(tokens: &[(&str, &str)], what: &str) {
        for (a_name, a) in tokens {
            for (b_name, b) in tokens {
                if a_name == b_name {
                    continue;
                }
                assert!(
                    !a.contains(b),
                    "{what}: {a_name} ({a:?}) contains {b_name} ({b:?}) — \
                     substring match would misread"
                );
            }
        }
    }

    const FIELD_NAMES: &[(&str, &str)] = &[
        ("CHUNKS", CHUNKS),
        ("RX_WINDOW_US", RX_WINDOW_US),
        ("NONEMPTY_POLLS", NONEMPTY_POLLS),
        ("POLL_EMPTY", POLL_EMPTY),
        ("RX_WIN_OK", RX_WIN_OK),
        ("RX_DEFICIT", RX_DEFICIT),
        ("PREROLL_WAITS", PREROLL_WAITS),
        ("PREROLL_REARMS", PREROLL_REARMS),
        ("CORE", CORE),
        ("PRIO", PRIO),
    ];

    const LINE_PREFIXES: &[(&str, &str)] = &[
        ("CAPTURE_TX_LINE", CAPTURE_TX_LINE),
        ("CAPTURE_OBS_LINE", CAPTURE_OBS_LINE),
        ("WIFI_DISCONNECTED", WIFI_DISCONNECTED),
        ("WIFI_CONNECTED", WIFI_CONNECTED),
        ("WIFI_DHCP_LEASE", WIFI_DHCP_LEASE),
        ("WIFI_REASSOCIATED", WIFI_REASSOCIATED),
        ("WIFI_SUPERVISOR_STARTED", WIFI_SUPERVISOR_STARTED),
        ("WIFI_PARKED_NO_CREDS", WIFI_PARKED_NO_CREDS),
        ("WIFI_REASSOC_ATTEMPT_FAILED", WIFI_REASSOC_ATTEMPT_FAILED),
        ("WIFI_REASSOC_ATTEMPT_START", WIFI_REASSOC_ATTEMPT_START),
        ("WIFI_CONSECUTIVE_FAILURES", WIFI_CONSECUTIVE_FAILURES),
        ("WIFI_TEMP_CONFIG_APPLIED", WIFI_TEMP_CONFIG_APPLIED),
        ("WIFI_TEMP_CONFIG_CLEARED", WIFI_TEMP_CONFIG_CLEARED),
        // A TestReport detail rather than a log line, but consumed by the same
        // host-side substring search, so it carries the same misparse hazard.
        ("NO_NVS_CREDENTIALS", NO_NVS_CREDENTIALS),
    ];

    /// The host parses these with a substring search, so any containment between field
    /// names is a silent-misparse hazard (the class the `empty_polls=` rename removed).
    #[test]
    fn field_name_tokens_are_pairwise_non_containing() {
        assert_pairwise_non_containing(FIELD_NAMES, "field names");
    }

    /// Host evals filter lines with a bare `line.contains(prefix)`, so containment between
    /// prefixes would make one eval collect another's lines. Same hazard class as the field
    /// names above. Note the lease-lost warn is emitted as `"<WIFI_DHCP_LEASE> lost"` — that
    /// intentional sharing is between a const and a literal, which this guard does not cover.
    #[test]
    fn line_prefix_tokens_are_pairwise_non_containing() {
        assert_pairwise_non_containing(LINE_PREFIXES, "line prefixes");
    }

    /// The host filters a line by prefix and then substring-searches that same line for
    /// field names, so containment across the two sets is the same misparse hazard.
    #[test]
    fn field_names_and_line_prefixes_are_non_containing_across_sets() {
        let all: Vec<(&str, &str)> = FIELD_NAMES
            .iter()
            .chain(LINE_PREFIXES.iter())
            .copied()
            .collect();
        assert_pairwise_non_containing(&all, "all tokens");
    }
}
