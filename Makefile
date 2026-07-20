# Repo-root Makefile — scrub-gate targets only.
#
# This repo has no root Cargo workspace: firmware/ and host/ are two standalone
# workspaces with their own Makefiles, and the build/lint/test inner loop lives
# there (`make -C firmware check`, `make -C host check`). The pre-commit router
# dispatches to those per-workspace targets by staged path.
#
# What is genuinely repo-wide is the scrub gate, which scans tracked content and
# knows nothing about workspaces. Those two targets live here.

.DEFAULT_GOAL := help

# List the two targets this Makefile owns and point at the workspace ones.
.PHONY: help
help:
	@echo "Repo-root targets (scrub gate only):"
	@echo "  make setup-hooks   wire git at .githooks, check scrub tooling (once per clone)"
	@echo "  make scrub-tree    whole-tree scrub sweep over tracked files"
	@echo ""
	@echo "Build, lint and test live in the two workspaces:"
	@echo "  make -C firmware check"
	@echo "  make -C host check"

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
