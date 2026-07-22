//! Shared TLS-PSK parameters for the pod↔host audio link.
//!
//! One source of truth for what this link speaks — protocol version,
//! ciphersuite, identity encoding, key encoding — so the server, `replay-pod`,
//! and every test peer cannot drift from each other. A drifted test peer is the
//! worst case: it keeps passing against a configuration production no longer
//! speaks.

use std::path::Path;

use openssl::error::ErrorStack;
use openssl::ssl::{SslContext, SslContextBuilder, SslMethod, SslVersion};

/// Bytes in an audio-link pre-shared key.
pub const PSK_LEN: usize = 32;

/// Characters in a [`PSK_LEN`] key written as hex — the on-disk form.
pub const PSK_HEX_LEN: usize = PSK_LEN * 2;

/// Longest PSK identity (a pod id) this link accepts, matching the device's
/// `Hello.pod_id` capacity.
pub const MAX_IDENTITY_LEN: usize = 32;

/// The one ciphersuite this link speaks: ECDHE for forward secrecy (a leaked pod
/// key must not decrypt yesterday's audio), PSK for peer authentication, and
/// ChaCha20-Poly1305 because the pod's ESP32-S3 has no AES acceleration. Plain
/// `PSK-*` suites are excluded by naming this one alone.
pub const PSK_CIPHERSUITE: &str = "ECDHE-PSK-CHACHA20-POLY1305";

/// Pin the link's TLS parameters: TLS 1.2 exactly (both ends pinned there —
/// esp-tls's PSK support is the 1.2 `psk_hint_key` path) and the single PSK
/// suite. Every context on this wire, server or client, goes through here.
pub fn pin_link_params(builder: &mut SslContextBuilder) -> Result<(), ErrorStack> {
    builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
    builder.set_max_proto_version(Some(SslVersion::TLS1_2))?;
    builder.set_cipher_list(PSK_CIPHERSUITE)?;
    Ok(())
}

/// A client context presenting `pod_id` as the PSK identity with `key` — the
/// client half of what the pod's `tls_link` speaks, used by `replay-pod` and by
/// every test peer.
pub fn client_context(pod_id: &str, key: [u8; PSK_LEN]) -> Result<SslContext, String> {
    if pod_id.is_empty() || pod_id.len() > MAX_IDENTITY_LEN {
        return Err(format!(
            "pod id must be 1..={MAX_IDENTITY_LEN} bytes; got {}",
            pod_id.len()
        ));
    }
    let identity = pod_id.to_string();
    let mut builder =
        SslContext::builder(SslMethod::tls_client()).map_err(|e| format!("ssl context: {e}"))?;
    pin_link_params(&mut builder).map_err(|e| format!("tls parameters: {e}"))?;
    builder.set_psk_client_callback(move |_ssl, _hint, identity_out, secret| {
        // The C API wants a NUL-terminated identity, so the buffer holds the
        // bytes plus a terminator.
        let bytes = identity.as_bytes();
        identity_out[..bytes.len()].copy_from_slice(bytes);
        identity_out[bytes.len()] = 0;
        secret[..key.len()].copy_from_slice(&key);
        Ok(key.len())
    });
    Ok(builder.build())
}

/// Decode one [`PSK_HEX_LEN`]-character key. `label` names the offending entry
/// (a pod id, or a file path) in the error; key material never renders, so a
/// message can be logged as-is.
pub fn parse_psk_hex(label: &str, hex: &str) -> Result<[u8; PSK_LEN], String> {
    let hex = hex.trim();
    if hex.len() != PSK_HEX_LEN {
        return Err(format!(
            "psk for {label:?} is {} characters; expected {PSK_HEX_LEN} ({PSK_LEN} bytes, hex)",
            hex.len()
        ));
    }
    // Byte pairs, not `str` slicing: the length check counts bytes, so a
    // multibyte character can straddle an even offset and `&hex[i*2..]` would
    // panic on the char boundary instead of naming the entry.
    let bytes = hex.as_bytes();
    let mut key = [0u8; PSK_LEN];
    for (i, byte) in key.iter_mut().enumerate() {
        let hi = (bytes[i * 2] as char).to_digit(16);
        let lo = (bytes[i * 2 + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => *byte = (hi * 16 + lo) as u8,
            _ => return Err(format!("psk for {label:?} is not hexadecimal")),
        }
    }
    Ok(key)
}

/// Encode a key as the [`PSK_HEX_LEN`] characters the secrets file and
/// `replay-pod --psk-file` hold.
pub fn psk_hex(key: &[u8; PSK_LEN]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// Write a file holding key material with owner-only permissions — the writer
/// counterpart of the startup mode check in [`crate::config`], which rejects any
/// secrets file readable by another local account.
pub fn write_secret_file(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    // Owner-only from the moment the file exists: creating it at the umask
    // default and tightening afterwards leaves a window in which another local
    // account can open the keys, and an fd opened in that window survives the
    // chmod.
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    #[cfg(unix)]
    {
        // `mode` applies at creation only, so an already-existing looser file
        // keeps its permissions; tighten it explicitly.
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    f.write_all(contents.as_bytes())?;
    f.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let key = [0x5au8; PSK_LEN];
        let hex = psk_hex(&key);
        assert_eq!(hex.len(), PSK_HEX_LEN);
        assert_eq!(parse_psk_hex("pod-x", &hex).expect("parse"), key);
    }

    #[test]
    fn wrong_length_and_non_hex_are_named_without_key_material() {
        let short = parse_psk_hex("pod-x", "5a5a").unwrap_err();
        assert!(short.contains("pod-x") && short.contains('4'), "{short}");
        let bad = parse_psk_hex("pod-x", &"zz".repeat(PSK_LEN)).unwrap_err();
        assert!(bad.contains("not hexadecimal"), "{bad}");
        // Exactly 64 bytes, with a two-byte character starting at an odd offset so
        // it straddles a hex pair. Must be named, not a panic.
        let multibyte = format!("{}é{}", "a".repeat(61), "a");
        assert_eq!(multibyte.len(), PSK_HEX_LEN);
        let err = parse_psk_hex("pod-x", &multibyte).unwrap_err();
        assert!(err.contains("pod-x"), "{err}");
    }

    #[test]
    fn client_context_rejects_unusable_identities() {
        assert!(client_context("", [0u8; PSK_LEN]).is_err());
        assert!(client_context(&"p".repeat(MAX_IDENTITY_LEN + 1), [0u8; PSK_LEN]).is_err());
        assert!(client_context("pod-x", [0u8; PSK_LEN]).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("psk.toml");
        write_secret_file(&path, "pod-x = \"aa\"\n").expect("write");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
