/// Dep-graph invariant guard (design §4.3).
///
/// The schema layer's default-feature build must carry ZERO postcard / transport /
/// encoding dependencies. This test shells `cargo tree` and fails if postcard appears
/// in the `--no-default-features` graph, so the invariant cannot silently regress.
///
/// Two checks:
/// 1. `device-protocol --no-default-features` contains no postcard (crate-level guard).
/// 2. `build-id` (the schema-only consumer) contains no postcard in its resolved graph
///    (consumer-level guard — catches `framing` accidentally becoming a default feature).
///
/// Run as part of `make check-host` / `cargo test -p device-protocol`.
#[test]
fn default_features_have_no_postcard() {
    // Locate the workspace root from this file's known position relative to it.
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("Cargo.toml")
        .canonicalize()
        .expect("Cargo.toml not found");
    let manifest_str = manifest
        .to_str()
        .expect("CARGO_MANIFEST_DIR contains non-UTF-8 — rename the workspace path");

    // ── Check 1: device-protocol default-feature build ────────────────────────
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--manifest-path",
            manifest_str,
            "-p",
            "device-protocol",
            "--no-default-features",
            "--edges",
            "normal", // `normal` shows only runtime deps — excludes dev AND build deps
        ])
        .output()
        .expect("failed to run `cargo tree` for device-protocol");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "`cargo tree -p device-protocol` failed:\n{stderr}"
    );
    assert!(
        !stdout.contains("postcard"),
        "postcard appeared in `cargo tree -p device-protocol --no-default-features`.\n\
         The schema layer must carry zero transport/encoding dependencies in its \
         default-feature build (design §4.3).\n\
         Tree output:\n{stdout}"
    );

    // ── Check 2: build-id consumer graph ─────────────────────────────────────
    // Verifies that the schema-only consumer does not transitively acquire postcard.
    // This catches the most likely regression: `framing` accidentally becoming a
    // default feature (Check 1 would still pass because it uses --no-default-features,
    // but Check 2 catches the consumer-side impact).
    let output2 = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--manifest-path",
            manifest_str,
            "-p",
            "build-id",
            "--edges",
            "normal",
        ])
        .output()
        .expect("failed to run `cargo tree` for build-id");

    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    let stderr2 = String::from_utf8_lossy(&output2.stderr);

    assert!(
        output2.status.success(),
        "`cargo tree -p build-id` failed:\n{stderr2}"
    );
    assert!(
        !stdout2.contains("postcard"),
        "postcard appeared in `cargo tree -p build-id`.\n\
         The schema-only consumer must not transitively acquire a postcard dependency.\n\
         Check whether `framing` was accidentally made a default feature in device-protocol.\n\
         Tree output:\n{stdout2}"
    );
}
