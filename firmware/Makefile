# firmware/Makefile — dev inner loop for the firmware workspace.
#
# Run from firmware/ (workspace root). The root rust-toolchain.toml pins an exact
# stable release, so host targets run under that Rust with no espup required and
# the gate's verdict does not move with the machine's rustup default.
#
# cargo's default-members ensures no-flag invocations build/test only host crates.
#
# System prerequisites for the host lanes (check-host builds every workspace member,
# default-member or not):
#   libssl-dev         — psk-link links openssl (the audio link's TLS-PSK parameters).
#   libusb-1.0-0-dev   — reachy-pod links libusb (the XVF3800 USB control transport).
#   libasound2-dev     — reachy-pod links libasound (the board's ALSA capture and
#                        playback ends).
# The device lanes (check-device, build-firmware, flash) additionally need espup.
# The reachy lanes (reachy-pod and the deploy targets below) need
# podman, an aarch64 binfmt_misc registration on a non-arm64 workstation, and
# ssh + rsync.

.DEFAULT_GOAL := check

# Release ELF audited by check-realign and hashed by the flash guard. Single definition so
# the audit and the flash examine the same file — a path change stays in one place.
ELF := target/xtensa-esp32s3-espidf/release/respeaker-pod

# Default inner loop — the host lane plus the device-crate lint (esp channel, ~1s warm).
# Requires espup but NOT an attached board.  Use check-host if espup is not installed.
#
# The two lanes' clippy runs cover both halves of every cfg split — check-host the host
# view of every crate, check-device the `cfg(target_os = "espidf")` view of the device
# crate.
.PHONY: check
check: check-host check-device

# Device-crate clippy under the esp toolchain — the one step in `check` that check-host
# lacks.  Runs from the device crate dir so its crate-scoped rust-toolchain.toml
# (channel = esp) and .cargo/config.toml (xtensa-esp32s3-espidf target) take effect.
# Requires espup; no attached board.  Public CI runs this in its device-clippy job.
.PHONY: check-device
check-device:
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
# Covers fmt, full workspace clippy, and host tests.
#
# --workspace covers every crate (podctl, pod-transport, hil-host, audio-receiver,
# device-protocol, build-id) so cross-crate API breaks are caught here instead of hiding
# until hil-test or a manual build.  respeaker-pod is included: run from the workspace root
# it builds under its host cfg (esp-idf items gated `cfg(target_os = "espidf")`), so its
# device-logic unit tests execute in this lane too (no espup needed).  It does NOT cover
# the device cfg view; that is check-device.
.PHONY: check-host
check-host: check-host-arch
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace
# This lane runs only the awk-parser fixture self-test, NOT `check-realign` against a
# freshly-built device ELF. That is deliberate: pre-commit stays fast and toolchain-optional
# (the host lane needs no espup). The real-ELF audit runs manually before flash via
# `make check-realign` (a prerequisite of `make flash`); allowlist drift is caught there as a
# ship-gate finding, not at commit time. This is the accepted arrangement.
	tools/check-realign-args.test.sh
# The provisioning tool's host-only half — its TOML reader, its validation gates and its
# reuse-or-generate-a-key branch — with the device stubbed. Everything it decides before it
# touches a pod is text processing, and a wrong answer there mis-files or duplicates a key
# that exists nowhere else. No toolchain, no device, no network.
	tools/provision-reachy-pod.test.sh
# The payload's deploy path, with ssh and rsync stubbed. The sharp one is the refusal that
# keeps a pod-only payload off a robot: the store slot is shared, so an activation there
# would replace a five-app stack with a binary that cannot move the machine.
	tools/deploy-reachy-pod.test.sh
# The readiness check, against a fixture filesystem. It is trusted instead of a bench run, so
# a check that quietly stopped being asked would read exactly like a check that passed.
	tools/reachy-status.test.sh
# The recovery sequence, planned with `make -n` and never run. `reachy-up` is nothing but an
# ordering, and reordered it still exits zero on a healthy robot — so the order is asserted
# here rather than discovered on the device it exists to bring back.
	tools/reachy-up.test.sh
# Lint for this whole directory, which had none. Skips visibly where shellcheck is absent.
	tools/check-shell.sh

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

# ── The Reachy pod: build, deploy, watch, self-test ──────────────────────────
#
# The Linux voice node is not flashed, it is deployed: a payload tree is pushed
# into a tmpfs store on a running device and the application is restarted onto
# it. No reboot, no flash write, and the same contract check a payload served by
# an application server would get. `make reachy-pod` alone needs no device.
#
# Device targeting is REACHY_HOST, the unit's hostname or address. The
# environment (or the make command line) is consulted first and the gitignored
# .local/reachy.conf second, so a workstation that always talks to the same unit
# says so once:
#
#     echo 'REACHY_HOST=reachy00' > .local/reachy.conf
#
# The file is only read when the variable is not already set, so an override for
# one invocation stays an override for one invocation.
ifeq ($(origin REACHY_HOST), undefined)
-include .local/reachy.conf
endif

# Refuse before doing anything rather than ssh to nowhere. A prerequisite of
# every target that talks to a device, listed first so the refusal comes before
# a container build.
.PHONY: reachy-host
reachy-host:
	@[ -n "$(REACHY_HOST)" ] || { \
	    echo "REACHY_HOST is not set, so there is no device to talk to." >&2; \
	    echo "Name the unit for this invocation:" >&2; \
	    echo "    make $(MAKECMDGOALS) REACHY_HOST=<hostname>" >&2; \
	    echo "or once, in the gitignored firmware/.local/reachy.conf:" >&2; \
	    echo "    REACHY_HOST=<hostname>" >&2; \
	    exit 1; \
	}

# The payload: an aarch64 binary built in the pinned container, plus the tree
# and the tarball the device takes it as. Needs podman; needs no device.
.PHONY: reachy-pod
reachy-pod:
	tools/build-reachy-pod.sh

# The speech daemon's configuration — the source of truth for the address a pod
# dials and the key table it is filed in. Whatever `speech-surface` is actually
# started with is what belongs here: the rung examples under host/config are the
# default because they are what a fresh checkout can run, and a workstation
# running a config from elsewhere says so once in .local/reachy.conf. The two
# drifting apart is invisible until a pod cannot connect.
SPEECH_CONFIG ?= host/config/parrot.toml

# Give the unit its configuration: the address it dials and the key it presents,
# derived from the speech daemon's config and pushed into the same tmpfs the
# payload lands in. Needs no payload and no build, so it can run before or after
# a deploy; idempotent, and a reboot is answered by `make reachy-up`, which
# docs/runbooks/reachy-end-to-end.md walks through.
.PHONY: reachy-provision
reachy-provision: reachy-host
	tools/provision-reachy-pod.sh $(REACHY_HOST) $(SPEECH_CONFIG)

# Build, push into the payload store, and activate — which checks the contract,
# switches the current symlink and restarts the application onto it.
.PHONY: reachy-deploy
reachy-deploy: reachy-host reachy-pod
	tools/deploy-reachy-pod.sh $(REACHY_HOST) --activate

# Build, push, and run the bring-up registry out of the pushed release as the
# account the application runs as — without activating it, so a device with no
# payload stays without one. Refuses while the application is running: it holds
# the sound card and the USB control interface.
.PHONY: reachy-selftest
reachy-selftest: reachy-host reachy-pod
	tools/deploy-reachy-pod.sh $(REACHY_HOST) --selftest

# The same, for the registry's manual cases: the bench run that establishes
# which capture channel carries which of the chip's beams. It prompts, and you
# speak into the array when it does — so run this one from a terminal at the
# bench, not from CI.
.PHONY: reachy-bench
reachy-bench: reachy-host reachy-pod
	tools/deploy-reachy-pod.sh $(REACHY_HOST) --bench

# Follow what the deployed payload is saying. The journal is volatile, so this
# is where its output is read. Through the deploy script like everything else
# that talks to a device, so the unit's name is spelled in one place.
.PHONY: reachy-logs
reachy-logs: reachy-host
	tools/deploy-reachy-pod.sh $(REACHY_HOST) --logs

# ── The pod on a unit: one command up, one command to ask ───────────────────
#
# A reboot clears every device path this repository writes to — the payload
# store and the account's home are both RAM, by design — and recovering used to
# take a command per file, each failure discovered only after the previous one
# was fixed. This is that sequence, in order, with each step already idempotent
# on its own.
#
# The order is the dependency order: the payload first (nothing else works
# without it), then the configuration it reads, then the readiness check that
# says whether the two arrived.
#
# Sub-makes rather than prerequisites: prerequisites may be satisfied in any
# order, and this one is not any order.
#
# This is the pod's half of a robot. The motion stack on the same unit — its
# payload, the machine's own configuration, and its own bring-up sequence — is
# brenn-reachy's, and is brought back from that clone.
#
# The prose version, with what each step restores and what to do when one of
# them refuses, is docs/runbooks/reachy-end-to-end.md.
.PHONY: reachy-up
reachy-up: reachy-host
	$(MAKE) reachy-deploy
	$(MAKE) reachy-provision
	$(MAKE) reachy-status

# Is this unit's pod ready? Read-only, one ssh, and it touches nothing — which
# is the point: every refusal in this stack was correct and none of them was
# reachable without deploying something first. It answers for the audio half; a
# robot's motion half answers to brenn-reachy's own tooling.
.PHONY: reachy-status
reachy-status: reachy-host
	tools/reachy-status.sh $(REACHY_HOST)

# Wire the repo-local git hooks directory so the pre-commit hook travels with the repo.
# Run once per clone: make install-hooks
#
# The repo-root `make setup-hooks` does this and also checks for the scrub
# tooling the hooks invoke; prefer it. This target stays because the HIL docs
# name it, and delegating upward would make firmware/ depend on the root.
.PHONY: install-hooks
install-hooks:
	git config core.hooksPath .githooks
