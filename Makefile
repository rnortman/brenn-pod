# Repo-root Makefile — the scrub gate and the union check gate.
#
# This repo has no root Cargo workspace: firmware/ and host/ are two standalone
# workspaces with their own Makefiles, and the build/lint/test inner loop lives
# there (`make -C firmware check`, `make -C host check`). The pre-commit router
# dispatches to those per-workspace targets by staged path.
#
# What is genuinely repo-wide lives here: the scrub gate, which scans tracked
# content and knows nothing about workspaces, and `check` — the union of every
# lane CI's check job runs. `check` is not a new inner loop; it exists so the CI
# workflow stays dumb and that job is reproducible on a fresh clone. CI's other
# job, scrub, is not part of `check` — see scrub-tree.

.DEFAULT_GOAL := help

# List the targets this Makefile owns and point at the workspace ones.
.PHONY: help
help:
	@echo "Repo-root targets:"
	@echo "  make check         everything CI's check job runs (both workspaces + shell lanes)"
	@echo "  make setup-hooks   wire git at .githooks, check scrub tooling (once per clone)"
	@echo "  make scrub-tree    whole-tree scrub sweep — the local stand-in for CI's scrub job"
	@echo ""
	@echo "The per-area inner loop lives in the two workspaces:"
	@echo "  make -C firmware check"
	@echo "  make -C host check"

# Everything CI's check job runs, in one locally reproducible target.
#
# `firmware check-host` transitively runs `make -C host check` (check-host-arch),
# which pulls in the openWakeWord weights its tests need, so one invocation
# covers both workspaces with no duplicated work. It is the espup-free lane: no
# esp toolchain, no attached board. HIL is not reachable from here and never runs
# in CI.
#
# The last two steps invoke the same runners the pre-commit router's githooks and
# scripts lanes invoke — one definition each, shared rather than copied, so the
# local hook and the public gate cannot drift apart.
#
# Not covered: CI's other job, scrub. `make scrub-tree` is that half.
.PHONY: check
check:
	$(MAKE) -C firmware check-host
	.githooks/self-test.sh
	scripts/check.sh

# Wire git at the tracked hooks dir and report any missing scrub tooling.
# Idempotent; run once per clone. Subsumes `make -C firmware install-hooks`.
#
# The scrub crate lives in the brenn repo, so this target checks for the binary
# rather than installing it: nothing in this tree can build brenn-scrub.
.PHONY: setup-hooks
setup-hooks:
	git config core.hooksPath .githooks
	@rm -f .git/hooks/pre-commit
	@command -v brenn-scrub >/dev/null 2>&1 || { \
	    echo "brenn-scrub not found on PATH."; \
	    echo "Install it from the brenn repo: cargo install --path scrub"; \
	}
	@command -v gitleaks >/dev/null 2>&1 || { \
	    echo "gitleaks not found on PATH."; \
	    echo "Install the pinned release from https://github.com/gitleaks/gitleaks/releases"; \
	    echo "(version pin: see PINNED_VERSION in brenn's scrub/src/gitleaks.rs)"; \
	}
	@echo "setup-hooks: done."

.PHONY: scrub-tree
scrub-tree:
	brenn-scrub tree
