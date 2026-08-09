# firmware/Makefile — dev inner loop for the firmware workspace.
#
# Run from firmware/ (workspace root). The root rust-toolchain.toml pins stable,
# so host targets run under stable Rust with no espup required.
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
# The reachy lanes (reachy-pod, reachy-motiond and the deploy targets below) need
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
# The motion daemon's deploy path, with ssh and rsync stubbed, plus the overlay mount every
# container build in this workspace currently depends on. The sharp ones are the exit-status
# passthrough — the daemon's verdict is the whole of what says a machine faulted — and the
# unit text, which is where the restart policy and the conditions that keep a rebooted device
# quiet are written down.
	tools/deploy-reachy-motiond.test.sh
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

# ── The head-presence daemon: build, provision, deploy ───────────────────────
#
# A second Linux binary for the same unit, built in the same container and
# pushed into the same executable store, and — like the payload — run as a
# systemd service with no terminal attached anywhere. The daemon rests with the
# motors unpowered, a wake word engages it, and every ending it has takes torque
# off again, so nobody needs to be watching for the machine to end up safe.
#
# reachy-motiond-run is the other form: a supervised foreground run for bench
# work, which is what the deploy target used to be. It refuses while the service
# is up, and the servo port's flock refuses underneath it either way.
#
# The machine's own configuration is authored in brenn-reachy, not here — the
# crank datum and the envelope describe one unit and have one source of truth.
# reachy-bench-config pushes that clone's copy, so bringing a rebooted robot
# back is one command in one repository.

# The daemon's own configuration for this unit, and the bearer token it presents
# to the bus. Both gitignored: one names the unit's identity and channel, the
# other is a credential.
MOTIOND_CONFIG ?= .local/reachy-motiond.toml
MOTIOND_TOKEN ?= .local/motiond-token

# The machine's own configuration. Empty by default, which is not "none": the
# deploy script then takes the sibling brenn-reachy clone's gitignored
# .local/reachy-bench.toml, the reviewed copy for this unit. Name a file here to
# push another.
BENCH_CONFIG ?=

# The aarch64 binary, built in the pinned container. Needs podman; needs no
# device. While the workspace's [patch] table redirects the motion crates at a
# clone beside this repo, this needs that clone too — the build script mounts it
# into the container and says so if it is not there.
.PHONY: reachy-motiond
reachy-motiond:
	tools/build-reachy-motiond.sh

# Give the unit the daemon's configuration, and the token it attaches with.
# Needs no build, so either can run before or after one; idempotent, and a
# reboot is answered by `make reachy-up` — the device's copies are in RAM.
#
# Neither restarts anything: the daemon reads its configuration once, so a
# changed value takes effect at the next `make reachy-motiond-deploy`. The knob
# map in docs/runbooks/reachy-end-to-end.md says which file owns which timing.
.PHONY: reachy-motiond-config
reachy-motiond-config: reachy-host
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --config $(MOTIOND_CONFIG)

.PHONY: reachy-motiond-token
reachy-motiond-token: reachy-host
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --token $(MOTIOND_TOKEN)

# Push the machine's own configuration, out of the brenn-reachy clone that
# authors it. Here rather than only there so one repository's `reachy-up` can
# bring a rebooted robot all the way back.
.PHONY: reachy-bench-config
reachy-bench-config: reachy-host
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --bench-config $(BENCH_CONFIG)

# Build, push into the executable store, install the unit and restart it — then
# return. The daemon is left running under systemd; no terminal is holding it up
# and none has to stay open.
.PHONY: reachy-motiond-deploy
reachy-motiond-deploy: reachy-host reachy-motiond
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --deploy

# The supervised form: build, push, and run it in the foreground at the bench.
# ARGS is passed to the daemon verbatim; with none it is given the configuration
# reachy-motiond-config pushed. Refuses while the service is running — one
# process at a time on the servo bus, and the port's flock says so underneath.
#
# ^C ends it, and so does `kill` or `systemctl stop`: SIGINT and SIGTERM are the
# same signal to this daemon, and both stow the head and take torque off. The
# terminal carries the narration only; the daemon's JSONL is captured on the
# device — the run prints where, and how to fetch it — because the pty this
# needs for ^C would otherwise merge the two streams into one.
.PHONY: reachy-motiond-run
reachy-motiond-run: reachy-host reachy-motiond
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --run $(ARGS)

# Follow what the deployed daemon is saying. Its JSONL and its narration both
# go to the journal in service mode; the capture file is the supervised run's
# arrangement and exists only there.
.PHONY: reachy-motiond-logs
reachy-motiond-logs: reachy-host
	tools/deploy-reachy-motiond.sh $(REACHY_HOST) --logs

# ── The whole robot: one command up, one command to ask ──────────────────────
#
# A reboot clears every device path this repository writes to — the payload
# store and the account's home are both RAM, by design — and recovering used to
# take six commands across two repositories, each failure discovered only after
# the previous one was fixed. This is that sequence, in order, with each step
# already idempotent on its own.
#
# The order is the dependency order: the payload first (nothing else works
# without it), then the three configurations, then the daemon's binary and unit
# last, so the service starts with everything it reads already in place.
#
# Sub-makes rather than prerequisites: prerequisites may be satisfied in any
# order, and this one is not any order.
#
# The prose version, with what each step restores and what to do when one of
# them refuses, is docs/runbooks/reachy-end-to-end.md.
.PHONY: reachy-up
reachy-up: reachy-host
	$(MAKE) reachy-deploy
	$(MAKE) reachy-provision
	$(MAKE) reachy-bench-config
	$(MAKE) reachy-motiond-config
	$(MAKE) reachy-motiond-token
	$(MAKE) reachy-motiond-deploy
	$(MAKE) reachy-status

# Is this robot ready? Read-only, one ssh, and it touches no servo — which is
# the point: every refusal in this stack was correct and none of them was
# reachable without torquing something first.
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
