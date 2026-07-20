/// Dep-graph invariant guard.
///
/// `pod-ingest` is a sans-I/O session library: it must carry ZERO async-runtime
/// or socket dependencies in its runtime graph. This test shells `cargo tree`
/// and fails if any of tokio / mio / socket2 / async-std / smol appear in the
/// `--edges normal` graph, so the boundary cannot silently regress.
///
/// Run as part of `make check-host-arch` / `cargo test -p pod-ingest`.
#[test]
fn sans_io_graph_has_no_runtime_or_socket_deps() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("Cargo.toml")
        .canonicalize()
        .expect("Cargo.toml not found");
    let manifest_str = manifest
        .to_str()
        .expect("CARGO_MANIFEST_DIR contains non-UTF-8 — rename the workspace path");

    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--manifest-path",
            manifest_str,
            "-p",
            "pod-ingest",
            "--edges",
            "normal", // `normal` shows only runtime deps — excludes dev AND build deps
        ])
        .output()
        .expect("failed to run `cargo tree` for pod-ingest");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "`cargo tree -p pod-ingest` failed:\n{stderr}"
    );

    for forbidden in ["tokio", "mio", "socket2", "async-std", "smol"] {
        assert!(
            !stdout.contains(forbidden),
            "`{forbidden}` appeared in `cargo tree -p pod-ingest --edges normal`.\n\
             pod-ingest is sans-I/O: it must carry no async-runtime or socket \
             dependency in its runtime graph.\n\
             Tree output:\n{stdout}"
        );
    }
}
