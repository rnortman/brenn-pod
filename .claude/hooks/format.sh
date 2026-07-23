#!/bin/bash
# Claude Code PostToolUse: format the one file just written. Silent on success.
# Enforcement lives in `make check`; this is convergence assistance only, so a
# parse error in mid-edit code exits 0.
set -u

file=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))' 2>/dev/null)

# The edition is hardcoded because standalone rustfmt does not read manifests. The
# source of truth is the `edition` key in the `[workspace.package]` table of
# firmware/Cargo.toml and host/Cargo.toml; scripts/check-edition.sh fails the build
# if this flag and those two keys ever disagree.
case "$file" in
    *.rs) rustfmt --edition 2024 "$file" >/dev/null 2>&1 ;;
esac

exit 0
