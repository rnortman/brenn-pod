//! The recorder links libasound and no other system library, and none of the
//! voice pod's USB, TLS or bridge code.

const FORBIDDEN: &[&str] = &[
    "rusb",
    "libusb1-sys",
    "openssl",
    "openssl-sys",
    "native-tls",
    "psk-link",
    "brenn-bridge",
    "reachy-pod",
];

#[test]
fn closure_holds_no_usb_tls_or_voice_pod_crate_and_only_alsa_sys() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let output = std::process::Command::new(env!("CARGO"))
        .arg("tree")
        .arg("--manifest-path")
        .arg(&manifest)
        .args([
            "-p",
            "pod-capture",
            "--edges",
            "normal,build",
            "--prefix",
            "none",
        ])
        .output()
        .expect("failed to run `cargo tree` for pod-capture");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "`cargo tree -p pod-capture` failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let names: std::collections::BTreeSet<&str> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split_whitespace().next().expect("non-empty line"))
        .collect();

    for name in &names {
        assert!(
            !FORBIDDEN.contains(name),
            "`{name}` is in pod-capture's closure and must never be.\nTree:\n{stdout}"
        );
    }
    let sys: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| n.ends_with("-sys"))
        .collect();
    assert_eq!(
        sys,
        ["alsa-sys"],
        "pod-capture links a system library other than libasound.\nTree:\n{stdout}"
    );
}
