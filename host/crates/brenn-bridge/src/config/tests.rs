//! Config parsing, validation, and the token file's on-disk posture.

use super::*;

const MINIMAL: &str = r#"
server_url = "wss://brenn.example.net/remote/pod-kitchen/ws"
token_file = "/etc/brenn/secrets/remote-pod-kitchen.token"
"#;

fn parsed(text: &str) -> Config {
    Config::parse(text).expect("the fixture parses")
}

#[test]
fn a_minimal_config_defaults_every_optional_field() {
    let config = parsed(MINIMAL);
    config.validate().expect("the fixture validates");

    assert_eq!(config.reconnect, ReconnectConfig::default());
    assert_eq!(config.reconnect.initial_backoff_ms, 500);
    assert_eq!(config.reconnect.max_backoff_ms, 30_000);
    assert_eq!(config.reconnect.liveness_multiplier, 3);
    assert_eq!(config.reconnect.max_futile_attachments, 3);
    assert!(
        config.ident.starts_with("brenn-bridge/"),
        "the default ident names this crate and its version: {}",
        config.ident
    );
}

#[test]
fn every_reconnect_field_is_settable() {
    let config = parsed(
        r#"
server_url = "wss://brenn.example.net/remote/pod-kitchen/ws"
token_file = "/etc/brenn/token"
ident = "pod-kitchen/2026.08"

[reconnect]
initial_backoff_ms = 100
max_backoff_ms = 5000
connect_timeout_ms = 2000
liveness_multiplier = 5
max_futile_attachments = 2
"#,
    );
    config.validate().expect("the fixture validates");
    assert_eq!(config.ident, "pod-kitchen/2026.08");
    assert_eq!(config.reconnect.initial_backoff_ms, 100);
    assert_eq!(config.reconnect.max_backoff_ms, 5000);
    assert_eq!(config.reconnect.connect_timeout_ms, 2000);
    assert_eq!(config.reconnect.liveness_multiplier, 5);
    assert_eq!(config.reconnect.max_futile_attachments, 2);
}

#[test]
fn an_unknown_field_is_a_parse_error_at_both_levels() {
    let top = Config::parse(&format!("{MINIMAL}\nserver_urll = \"typo\"\n"));
    assert!(top.is_err(), "an unknown top-level key must be refused");

    let nested = Config::parse(&format!("{MINIMAL}\n[reconnect]\nbackoff_ms = 1\n"));
    assert!(nested.is_err(), "an unknown reconnect key must be refused");
}

#[test]
fn a_cleartext_url_is_refused_before_anything_dials() {
    for url in [
        "ws://brenn.example.net/remote/pod-kitchen/ws",
        "ws://127.0.0.1:8080/remote/pod-kitchen/ws",
        "https://brenn.example.net/remote/pod-kitchen/ws",
        "brenn.example.net/remote/pod-kitchen/ws",
    ] {
        let config = parsed(&format!(
            "server_url = {url:?}\ntoken_file = \"/etc/brenn/token\"\n"
        ));
        let message = config
            .validate()
            .expect_err("a non-wss url must be refused");
        assert!(
            message.contains("wss://"),
            "the refusal names the scheme it wanted: {message}"
        );
    }
}

#[test]
fn a_url_with_no_authority_is_refused() {
    let config = parsed("server_url = \"wss://\"\ntoken_file = \"/etc/brenn/token\"\n");
    let message = config.validate().expect_err("a hostless url is refused");
    assert!(message.contains("no host"), "{message}");
}

#[test]
fn an_empty_token_path_is_refused() {
    let config = parsed("server_url = \"wss://host/remote/a/ws\"\ntoken_file = \"\"\n");
    let message = config.validate().expect_err("an empty path is refused");
    assert!(message.contains("token_file"), "{message}");
}

#[test]
fn an_empty_ident_is_refused() {
    let config = parsed(&format!("{MINIMAL}\nident = \"\"\n"));
    let message = config.validate().expect_err("an empty ident is refused");
    assert!(
        message.contains("ident"),
        "the refusal names the field an operator has to fill in: {message}"
    );
}

#[test]
fn the_timings_reject_the_values_that_would_wedge() {
    let cases: [(&str, &str); 5] = [
        ("initial_backoff_ms = 0", "initial_backoff_ms"),
        (
            "initial_backoff_ms = 1000\nmax_backoff_ms = 500",
            "max_backoff_ms",
        ),
        ("connect_timeout_ms = 0", "connect_timeout_ms"),
        ("liveness_multiplier = 0", "liveness_multiplier"),
        ("max_futile_attachments = 0", "max_futile_attachments"),
    ];
    for (body, expected) in cases {
        let config = parsed(&format!("{MINIMAL}\n[reconnect]\n{body}\n"));
        let message = config
            .validate()
            .expect_err("the fixture states an unusable timing");
        assert!(
            message.contains(expected),
            "the refusal names the offending field: {message}"
        );
    }
}

#[test]
fn the_conn_config_carries_the_configured_timings() {
    let config = parsed(&format!(
        "{MINIMAL}\n[reconnect]\ninitial_backoff_ms = 250\nmax_backoff_ms = 4000\nconnect_timeout_ms = 1500\nliveness_multiplier = 4\n"
    ));
    let conn = config.conn_config();
    assert_eq!(conn.url, config.server_url);
    assert_eq!(conn.ident, config.ident);
    assert_eq!(conn.initial_backoff, std::time::Duration::from_millis(250));
    assert_eq!(conn.max_backoff, std::time::Duration::from_millis(4000));
    assert_eq!(conn.connect_timeout, std::time::Duration::from_millis(1500));
    assert_eq!(conn.liveness_multiplier, 4);
    assert_eq!(
        conn.backoff_jitter_seed,
        jitter_seed(&config.server_url),
        "the derived seed has to reach the connection layer, or every pod lowers to the same one"
    );
    // The remote route signals a refusal by closing without a code, so no close
    // code can be singled out as terminal.
    assert_eq!(conn.terminal_close_code, None);
}

#[test]
fn the_jitter_seed_is_stable_per_url_and_differs_across_pods() {
    let kitchen = "wss://brenn.example.net/remote/pod-kitchen/ws";
    let study = "wss://brenn.example.net/remote/pod-study/ws";
    assert_eq!(jitter_seed(kitchen), jitter_seed(kitchen));
    assert_ne!(
        jitter_seed(kitchen),
        jitter_seed(study),
        "two pods on one server must not back off in lockstep"
    );

    // And through the lowering, which is the only path that matters: two pods
    // differing in nothing but their URL must not re-dial in lockstep after a
    // server restart.
    let lowered = |url: &str| {
        parsed(&format!(
            "server_url = {url:?}\ntoken_file = \"/etc/brenn/token\"\n"
        ))
        .conn_config()
        .backoff_jitter_seed
    };
    assert_ne!(lowered(kitchen), lowered(study));
}

// ── the token file ────────────────────────────────────────────────────────

#[cfg(unix)]
fn write_token(dir: &tempfile::TempDir, name: &str, contents: &str, mode: u32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.path().join(name);
    std::fs::write(&path, contents).expect("the fixture writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
        .expect("the fixture chmods");
    path
}

#[cfg(unix)]
#[test]
fn a_private_token_file_loads_trimmed() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = write_token(&dir, "token", "  s3cret-token\n", 0o600);
    let token = Token::load(&path).expect("a 0600 file loads");
    assert_eq!(token.clone().into_inner(), "s3cret-token");
    assert_eq!(
        format!("{token:?}"),
        "Token { bytes: 12 }",
        "the credential must never render"
    );
}

#[cfg(unix)]
#[test]
fn a_group_or_world_readable_token_file_is_refused() {
    let dir = tempfile::tempdir().expect("a temp dir");
    for mode in [0o640, 0o604, 0o644, 0o660] {
        let path = write_token(&dir, &format!("token-{mode:o}"), "s3cret", mode);
        let error = Token::load(&path).expect_err("a readable-by-others token is refused");
        assert!(
            matches!(error, ConfigError::TokenMode { .. }),
            "expected a mode refusal, got {error:?}"
        );
        assert!(
            error.to_string().contains("chmod 600"),
            "the refusal says how to fix it: {error}"
        );
    }
}

#[cfg(unix)]
#[test]
fn an_empty_token_file_is_refused_as_empty_not_as_a_credential() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = write_token(&dir, "token", "\n  \n", 0o600);
    let error = Token::load(&path).expect_err("whitespace is not a credential");
    assert!(
        matches!(error, ConfigError::TokenEmpty { .. }),
        "expected an empty refusal, got {error:?}"
    );
}

#[test]
fn a_missing_token_file_reports_as_a_read_failure() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("absent");
    let error = Token::load(&path).expect_err("a missing token is refused");
    assert!(
        matches!(error, ConfigError::TokenRead { .. }),
        "a file that is not there has no mode to complain about: {error:?}"
    );
}

#[test]
fn load_reports_the_path_on_a_missing_config() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("absent.toml");
    let error = Config::load(&path).expect_err("a missing config is refused");
    assert!(
        error.to_string().contains("absent.toml"),
        "the error names the file: {error}"
    );
}

#[test]
fn load_validates_as_well_as_parses() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("bridge.toml");
    std::fs::write(
        &path,
        "server_url = \"ws://host/remote/a/ws\"\ntoken_file = \"/etc/brenn/token\"\n",
    )
    .expect("the fixture writes");
    let error = Config::load(&path).expect_err("a parseable but invalid config is refused");
    assert!(
        matches!(error, ConfigError::Invalid { .. }),
        "expected a validation refusal, got {error:?}"
    );
}
