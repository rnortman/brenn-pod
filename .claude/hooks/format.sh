#!/bin/bash
# Claude Code PostToolUse: format the one file just written. Silent on success.
# Enforcement lives in `make check`; this is convergence assistance only, so a
# parse error in mid-edit code exits 0.
set -u

file=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))' 2>/dev/null)

case "$file" in
    *.rs) rustfmt --edition 2024 "$file" >/dev/null 2>&1 ;;
esac

exit 0
