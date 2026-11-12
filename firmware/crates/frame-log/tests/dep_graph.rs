//! The reader's dependency closure is an exact allow-list.
//!
//! Whoever reads frame logs links this crate, so its closure must hold no
//! brenn-pod crate with a build script and no crate that links a native
//! library. Holding the package names to an exact list makes every new
//! dependency a conscious edit of this file.

use std::collections::BTreeSet;

const ALLOWED: &[&str] = &[
    "audio-pipeline",
    "byteorder",
    "cobs",
    "frame-log",
    "hash32",
    "heapless",
    "itoa",
    "log",
    "memchr",
    "postcard",
    "postcard-derive",
    "proc-macro2",
    "quote",
    "serde",
    "serde_core",
    "serde_derive",
    "serde_json",
    "stable_deref_trait",
    "syn",
    "thiserror",
    "thiserror-impl",
    "unicode-ident",
    "zmij",
];

const FORBIDDEN: &[&str] = &["build-id", "psk-link", "brenn-bridge"];

#[test]
fn closure_is_exactly_the_allow_list() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let output = std::process::Command::new(env!("CARGO"))
        .arg("tree")
        .arg("--manifest-path")
        .arg(&manifest)
        .args([
            "-p",
            "frame-log",
            "--edges",
            "normal,build",
            "--prefix",
            "none",
        ])
        .output()
        .expect("failed to run `cargo tree` for frame-log");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "`cargo tree -p frame-log` failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let names: BTreeSet<&str> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split_whitespace().next().expect("non-empty line"))
        .collect();

    for name in &names {
        assert!(
            !FORBIDDEN.contains(name) && !name.ends_with("-sys"),
            "`{name}` is in frame-log's closure and must never be.\nTree:\n{stdout}"
        );
    }
    let allowed: BTreeSet<&str> = ALLOWED.iter().copied().collect();
    assert_eq!(
        names, allowed,
        "frame-log's closure differs from the allow-list.\nTree:\n{stdout}"
    );
}
