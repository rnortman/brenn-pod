# firmware/Makefile — dev inner loop for the firmware workspace.
#
# Run from firmware/ (workspace root). The root rust-toolchain.toml pins stable,
# so host targets run under stable Rust with no espup required.
#
# cargo's default-members = ["crates/device-protocol", "crates/build-id"] ensures
# no-flag invocations build/test only host crates. See
# docs/adr/2026/06/05-rust-toolchain-milestone0/ for the toolchain-scoping rationale.

.DEFAULT_GOAL := check

# Release ELF audited by check-realign and hashed by the flash guard. Single definition so
# the audit and the flash examine the same file — a path change stays in one place.
ELF := target/xtensa-esp32s3-espidf/release/respeaker-pod

# Default inner loop — full workspace lint + tests (stable, no espup) + device lint
# (esp channel, ~1s warm).  Device clippy runs from the device crate dir so crate-scoped
# rust-toolchain.toml and .cargo/config.toml take effect.  Requires espup but NOT an
# attached board.  Use check-host if espup is not installed.
#
# --workspace covers every crate (podctl, pod-transport, hil-host, audio-receiver,
# device-protocol, build-id) so cross-crate API breaks are caught here instead of hiding
# until hil-test or a manual build.  respeaker-pod is included: run from the workspace root
# it builds under its host cfg (esp-idf items gated `cfg(target_os = "espidf")`), so its
# unit tests execute here.  The two clippy runs therefore cover both halves of every cfg
# split — root clippy the host view, the device-dir run below the device view.
.PHONY: check
check: check-host-arch
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
# This lane runs only the awk-parser fixture self-test, NOT `check-realign` against a
# freshly-built device ELF. That is deliberate: pre-commit stays fast and toolchain-optional
# (the host lane needs no espup). The real-ELF audit runs manually before flash via
# `make check-realign` (a prerequisite of `make flash`); allowlist drift is caught there as a
# ship-gate finding, not at commit time. This is the accepted arrangement.
	tools/check-realign-args.test.sh
	cd devices/respeaker-pod && cargo clippy --all-targets -- -D warnings

# Auto-fix — fmt (all members) + clippy fix (full host workspace) + device clippy fix.
# --allow-dirty/--allow-staged so this is usable mid-edit; review the diff before committing.
# Scope mirrors `check`/`check-host` so `make fix && make check` is always a clean cycle.
.PHONY: fix
fix:
	cargo fmt --all
	cargo clippy --fix --allow-dirty --allow-staged --workspace
	cd devices/respeaker-pod && cargo clippy --all-targets --fix --allow-dirty --allow-staged
	$(MAKE) -C ../host fix

# Optimized firmware image — requires espup. Run from the device crate dir.
# Only needed before flashing; NOT in the default check loop (release build is slow).
.PHONY: build-firmware
build-firmware:
	cd devices/respeaker-pod && cargo build --release

# Host-only lane — no espup required. Use in environments without the esp toolchain.
# Covers fmt, full workspace clippy, and host tests — including the device crate's host cfg
# view, so respeaker-pod's device-logic unit tests run in this lane too (no espup needed).
# It does NOT cover the device cfg view; that is the device-dir clippy in `check`.
.PHONY: check-host
check-host: check-host-arch
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
	tools/check-realign-args.test.sh

# Host-architecture workspace (host/) — stable toolchain, no espup.
# Standalone workspace beside firmware/; check and check-host both depend on this
# so the default inner loop and the no-espup lane each cover host/. Delegates to
# host/Makefile so host's command list has a single source of truth.
.PHONY: check-host-arch
check-host-arch:
	$(MAKE) -C ../host check

# HIL test — enumerate the attached ESP32-S3, assert it runs the current build,
# run the self-test suite. Requires an attached device running the current firmware.
# NEVER invoked by check/check-host/pre-commit/CI (AC6).
.PHONY: hil-test
hil-test:
	cargo run -p hil-host --release

# Build-time guard against the xtensa frame-realign / stack-arg miscompile
# (TODO(xtensa-realign-stack-args)). The device toolchain is the stock esp channel, which
# still carries the miscompile; this gate is the primary guard that no realigned Rust
# function in the image takes stack-passed arguments. Runs before every HIL flash so a
# regression (e.g. a realigned function that spills arguments) fails loudly instead of
# miscompiling silently. Builds the release ELF first, then audits it.
.PHONY: check-realign
check-realign: build-firmware
	tools/check-realign-args.sh $(ELF)

# Flash release firmware to the attached device. No --monitor: monitor is interactive
# and headless-hostile (see docs/setup.md:186). cargo espflash flash subsumes the build.
#
# check-realign builds and audits the release ELF; this recipe then runs `cargo espflash
# flash --release`, which performs its OWN incremental build (cargo-espflash has no
# flash-a-prebuilt-ELF mode) and flashes whatever that build leaves at the ELF path. The
# audited image and the flashed image coincide only while both builds resolve identically
# — same crate dir, profile, and toolchain via .cargo/config.toml + rust-toolchain.toml —
# which holds today (the second build is an up-to-date no-op).
#
# The hash guard below enforces that invariant instead of assuming it: hash the audited
# ELF before espflash, hash it again after, and fail loudly on mismatch. A divergence
# (flag/env drift making espflash rebuild a different ELF, or a concurrent cargo run
# mutating the file) then fails `make flash` — and `flash-and-test` with it — so HIL tests
# never run against an image the realign gate never saw.
#
# Residual windows the guard does NOT cover: (1) a mutation between the audit's own read of
# the ELF and the first hash below is baked into both hashes and passes undetected; (2) on
# a real divergence espflash has already written the divergent image to the device before
# the post-flash hash catches it — the guard cannot prevent that first flash (only flashing
# a prebuilt ELF could), it converts a silent audit bypass into a loud, blocking one and
# tells the operator to re-flash and investigate. Note: invoking espflash outside
# `make flash` bypasses the audit entirely.
.PHONY: flash
flash: check-realign
	@set -e; \
	audited=$$(sha256sum $(ELF) | cut -d' ' -f1); \
	[ -n "$$audited" ] || { echo "flash: FATAL: could not hash audited ELF $(ELF)" >&2; exit 1; }; \
	(cd devices/respeaker-pod && cargo espflash flash --release); \
	flashed=$$(sha256sum $(ELF) | cut -d' ' -f1); \
	[ -n "$$flashed" ] || { echo "flash: FATAL: could not hash flashed ELF $(ELF)" >&2; exit 1; }; \
	echo "flash: ELF sha256: $$flashed"; \
	if [ "$$audited" != "$$flashed" ]; then \
	  echo "flash: FATAL: flashed ELF differs from audited ELF" >&2; \
	  echo "flash:   audited $$audited" >&2; \
	  echo "flash:   flashed $$flashed" >&2; \
	  echo "flash: the device now runs an image the realign audit never saw — re-run 'make flash' and investigate the build divergence before trusting it" >&2; \
	  exit 1; \
	fi

# Flash current firmware then immediately run HIL tests against the freshly-flashed device.
# Convenience target; hil-test alone is the canonical test-only target.
.PHONY: flash-and-test
flash-and-test: flash hil-test

# Fast-lane CLI unit tests — explicit-only (not wired into check/check-host/CI).
# Runs only pod-transport and podctl tests as a quick sanity-check during active
# CLI development.  `check-host` covers the full workspace (strict superset).
.PHONY: check-fast
check-fast:
	cargo test -p pod-transport
	cargo test -p podctl

# Build the podctl CLI binary. Requires no attached device.
.PHONY: podctl
podctl:
	cargo build -p podctl --release

# Wire the repo-local git hooks directory so the pre-commit hook travels with the repo.
# Run once per clone: make install-hooks
#
# The repo-root `make setup-hooks` does this and also checks for the scrub
# tooling the hooks invoke; prefer it. This target stays because the HIL docs
# name it, and delegating upward would make firmware/ depend on the root.
.PHONY: install-hooks
install-hooks:
	git config core.hooksPath .githooks
