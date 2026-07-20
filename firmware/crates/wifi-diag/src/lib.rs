//! WiFi diagnostic snapshot type and render helpers.
//!
//! This crate is host-buildable (no ESP-IDF dependencies) so the render helpers
//! can be unit-tested in the host lane without the Xtensa toolchain.
//!
//! `WifiSnapshot` carries the result of a single demand-read of local WiFi/IP state.
//! Every field is `Option` — a sub-read that fails leaves that field `None` without
//! aborting the snapshot. `snapshot_wifi_state()` (in `respeaker-pod/src/main.rs`,
//! not here) acquires `WIFI_STACK` via `try_lock`, fills a `WifiSnapshot`, drops the
//! guard, and returns the plain value type. The render helpers here are then called
//! with no lock held.

#![no_std]

use core::fmt;

use heapless::String;

/// Plain, owned snapshot of local WiFi/IP state for diagnostic logging.
///
/// Every field is best-effort: a failed sub-read leaves it `None`, never panics.
/// RSSI is reported raw — suspect values (`0`, `<= -80`) are NOT suppressed
/// because a suspect value is itself diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WifiSnapshot {
    /// Whether `is_up()` returned `true`. `None` if the read failed or the lock was busy.
    pub up: Option<bool>,
    /// Local IP address octets. `None` if `get_ip_info()` failed or lock was busy.
    pub ip: Option<[u8; 4]>,
    /// Default gateway octets. `None` if `get_ip_info()` failed or lock was busy.
    pub gw: Option<[u8; 4]>,
    /// Raw RSSI in dBm. `None` if `get_rssi()` failed or lock was busy.
    /// Suspect values (0, <= -80) are included, not suppressed.
    pub rssi: Option<i32>,
    /// Raw `wifi_ps_type_t` modem power-save mode read via `esp_wifi_get_ps`.
    /// `None` if the read failed or the lock was busy. 0 = `WIFI_PS_NONE` (the
    /// expected production value), 1 = `MIN_MODEM`, 2 = `MAX_MODEM`. Raw, not bool,
    /// mirroring the RSSI policy: a suspect value is itself diagnostic — power save
    /// silently on (`ps=1`) is the host→device playback-dropout incident signature.
    pub ps_mode: Option<u32>,
}

/// Format a 4-byte IPv4 address as a dotted-decimal `heapless::String<16>`.
///
/// "255.255.255.255" is 15 bytes, so `String::<16>` fits every `[u8; 4]` input;
/// overflow is unreachable at this signature and the truncating formatter is
/// defense-in-depth against future signature drift only. The plain (unmarked)
/// variant is used because the `[u8; 4]` signature makes the sentinel physically
/// unable to fire — a marker that can never appear buys nothing here. Any future
/// widening that makes overflow reachable (IPv6, CIDR suffix, a larger buffer)
/// must revisit this: switch to `format_truncating_marked` so a cut address shows
/// a visible cue instead of a plausible-but-wrong one. `respeaker-pod/src/main.rs`
/// imports this via `use wifi_diag::fmt_ipv4`.
pub fn fmt_ipv4(a: [u8; 4]) -> String<16> {
    truncfmt::format_truncating::<16>(format_args!("{}", Ipv4(a)))
}

/// `Display` for a 4-byte IPv4 address as dotted-decimal.
struct Ipv4([u8; 4]);

impl fmt::Display for Ipv4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a = self.0;
        write!(f, "{}.{}.{}.{}", a[0], a[1], a[2], a[3])
    }
}

/// `Display` for an `Option<T>`: the inner value, or `?` when `None`.
struct OptField<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for OptField<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(v) => write!(f, "{v}"),
            None => f.write_str("?"),
        }
    }
}

/// `Display` for an optional IPv4 address: dotted-decimal, or `?` when `None`.
struct OptIpv4(Option<[u8; 4]>);

impl fmt::Display for OptIpv4 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(a) => write!(f, "{}", Ipv4(a)),
            None => f.write_str("?"),
        }
    }
}

/// Render a `WifiSnapshot` into a `String<N>` using a single truncating format.
///
/// Generic over capacity solely so tests can exercise overflow at a small `N`;
/// production callers use [`fmt_wifi_snapshot`] at `N = 64`.
fn fmt_snapshot_into<const N: usize>(snap: &WifiSnapshot) -> String<N> {
    truncfmt::format_truncating_marked::<N>(
        format_args!(
            "up={} ip={} gw={} rssi={} ps={}",
            OptField(snap.up),
            OptIpv4(snap.ip),
            OptIpv4(snap.gw),
            OptField(snap.rssi),
            OptField(snap.ps_mode)
        ),
        truncfmt::TRUNCATION_SENTINEL,
    )
}

/// Render a `WifiSnapshot` into the `up=<v> ip=<v> gw=<v> rssi=<v> ps=<v>` substring
/// used in enriched log lines. `None` fields render as `?`.
///
/// The returned string is `heapless::String<80>` — large enough for the five
/// fields with their labels:
/// - `up=false` (8) + ` ip=255.255.255.255` (19) + ` gw=255.255.255.255` (19)
///   + ` rssi=-2147483648` (17) + ` ps=4294967295` (14) = 77 bytes worst case, inside 80.
///
/// If a future field addition ever exceeds the cap the line truncates gracefully
/// at a UTF-8 char boundary with a visible `…` sentinel, never to empty/garbage.
pub fn fmt_wifi_snapshot(snap: &WifiSnapshot) -> String<80> {
    fmt_snapshot_into::<80>(snap)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── fmt_ipv4 ──────────────────────────────────────────────────────────────

    #[test]
    fn fmt_ipv4_typical() {
        assert_eq!(fmt_ipv4([192, 168, 1, 100]).as_str(), "192.168.1.100");
    }

    #[test]
    fn fmt_ipv4_max() {
        assert_eq!(fmt_ipv4([255, 255, 255, 255]).as_str(), "255.255.255.255");
    }

    #[test]
    fn fmt_ipv4_zeros() {
        assert_eq!(fmt_ipv4([0, 0, 0, 0]).as_str(), "0.0.0.0");
    }

    // ── fmt_wifi_snapshot ─────────────────────────────────────────────────────

    #[test]
    fn valid_snapshot() {
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([192, 168, 1, 50]),
            gw: Some([192, 168, 1, 1]),
            rssi: Some(-55),
            ps_mode: Some(0),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=192.168.1.50 gw=192.168.1.1 rssi=-55 ps=0"
        );
    }

    #[test]
    fn all_none_snapshot() {
        let snap = WifiSnapshot {
            up: None,
            ip: None,
            gw: None,
            rssi: None,
            ps_mode: None,
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=? ip=? gw=? rssi=? ps=?"
        );
    }

    #[test]
    fn zero_ip_zero_gateway() {
        // 0.0.0.0 IP and gateway are visibly distinguishable from a valid address.
        let snap = WifiSnapshot {
            up: Some(false),
            ip: Some([0, 0, 0, 0]),
            gw: Some([0, 0, 0, 0]),
            rssi: Some(-60),
            ps_mode: Some(0),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=false ip=0.0.0.0 gw=0.0.0.0 rssi=-60 ps=0"
        );
    }

    #[test]
    fn loopback_ip() {
        // 127.x address is visibly distinguishable from a valid routable address.
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([127, 0, 0, 1]),
            gw: Some([0, 0, 0, 0]),
            rssi: Some(-70),
            ps_mode: Some(0),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=127.0.0.1 gw=0.0.0.0 rssi=-70 ps=0"
        );
    }

    #[test]
    fn suspect_rssi_zero() {
        // rssi=0 is suspect but must be reported verbatim (not suppressed).
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([10, 0, 0, 5]),
            gw: Some([10, 0, 0, 1]),
            rssi: Some(0),
            ps_mode: Some(0),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=10.0.0.5 gw=10.0.0.1 rssi=0 ps=0"
        );
    }

    #[test]
    fn suspect_rssi_very_weak() {
        // rssi=-83 (below the existing -80 reject threshold) is suspect but reported.
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([10, 0, 0, 5]),
            gw: Some([10, 0, 0, 1]),
            rssi: Some(-83),
            ps_mode: Some(0),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=10.0.0.5 gw=10.0.0.1 rssi=-83 ps=0"
        );
    }

    #[test]
    fn partial_none_fields() {
        // Some fields readable, some not — each renders independently.
        let snap = WifiSnapshot {
            up: Some(true),
            ip: None,
            gw: Some([192, 168, 0, 1]),
            rssi: None,
            ps_mode: None,
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=? gw=192.168.0.1 rssi=? ps=?"
        );
    }

    #[test]
    fn power_save_silently_on() {
        // ps=1 (MIN_MODEM) is the host→device playback-dropout incident signature:
        // power save silently on despite the forced PS_NONE. Reported verbatim.
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([10, 0, 0, 5]),
            gw: Some([10, 0, 0, 1]),
            rssi: Some(-55),
            ps_mode: Some(1),
        };
        assert_eq!(
            fmt_wifi_snapshot(&snap).as_str(),
            "up=true ip=10.0.0.5 gw=10.0.0.1 rssi=-55 ps=1"
        );
    }

    // ── graceful truncation (drift foot-gun guard) ────────────────────────────

    #[test]
    fn snapshot_overflow_truncates_gracefully() {
        // A capacity far too small for the rendered line must yield a non-empty,
        // valid-UTF-8 verbatim prefix ending in the sentinel — never empty/garbage.
        let snap = WifiSnapshot {
            up: Some(true),
            ip: Some([255, 255, 255, 255]),
            gw: Some([255, 255, 255, 255]),
            rssi: Some(-2147483648),
            ps_mode: Some(4294967295),
        };
        let full = fmt_snapshot_into::<80>(&snap);
        let out = fmt_snapshot_into::<16>(&snap);

        assert!(!out.is_empty(), "overflow must not drop to empty");
        assert!(out.len() <= 16, "result must fit the cap");
        assert!(
            out.ends_with(truncfmt::TRUNCATION_SENTINEL),
            "genuine overflow is marked with the sentinel"
        );
        let prefix = &out[..out.len() - truncfmt::TRUNCATION_SENTINEL.len()];
        assert!(
            full.as_str().starts_with(prefix),
            "the marked output is a verbatim prefix of the full line"
        );
    }

    #[test]
    fn snapshot_exact_fit_is_unmarked() {
        // A capacity that exactly holds the rendered line gets no sentinel —
        // in-bounds output stays byte-identical to the untruncated form.
        let snap = WifiSnapshot {
            up: None,
            ip: None,
            gw: None,
            rssi: None,
            ps_mode: None,
        };
        let rendered = "up=? ip=? gw=? rssi=? ps=?";
        let out = fmt_snapshot_into::<26>(&snap); // 26 == rendered.len()
        assert_eq!(out.as_str(), rendered);
        assert!(!out.ends_with(truncfmt::TRUNCATION_SENTINEL));
    }
}
