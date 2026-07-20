# host/Makefile — dev inner loop for the host-architecture workspace.
#
# Run from host/ (workspace root). host/rust-toolchain.toml pins stable, so every
# target runs under stable Rust with no espup required. host/ is a standalone
# workspace beside firmware/; the cross-workspace edge points one way only
# (host consumes firmware/crates/audio-pipeline by path). firmware/Makefile's
# check delegates here so a firmware-side change that breaks host is still caught.

.DEFAULT_GOAL := check

# Full host-workspace lint + tests. All three crates are default members, so
# --workspace and no-flag invocations cover the same set.
#
# fetch-models is a real prerequisite, not a convenience: the speech-pipeline
# tests load the openWakeWord weights and panic on their absence, so a check on a
# fresh clone reds without them. Declaring the edge here fixes every caller at
# once — direct invocation, firmware's check-host-arch, the pre-commit host lane
# and CI. The fetch is idempotent and sha256-verified, so a warm run is a
# sub-second no-op that touches no network.
.PHONY: check
check: fetch-models
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace

# Auto-fix — fmt (all members) + clippy fix (full workspace).
# --allow-dirty/--allow-staged so this is usable mid-edit; review the diff before committing.
# Scope mirrors `check` so `make fix && make check` is always a clean cycle.
.PHONY: fix
fix:
	cargo fmt --all
	cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets

# Fetch the openWakeWord weights this repo does not redistribute (CC BY-NC-SA
# 4.0, NonCommercial — see host/models/oww/README.md). Silero VAD is committed
# (MIT). Idempotent; sha256-verifies each file.
.PHONY: fetch-models
fetch-models:
	./models/fetch.sh
