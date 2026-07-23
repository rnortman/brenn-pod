#!/usr/bin/env bash
# Edition drift guard. The repo declares the Rust edition in exactly three places:
# the `[workspace.package]` table of each of the two workspace roots, and the
# rustfmt edition flag in .claude/hooks/format.sh. This fails when they
# disagree — the split that let files be formatted under one edition and gated
# under another.
#
# It also holds the member manifests to inheritance: a literal `edition = "..."`
# key re-opens the drift, and an omitted key silently means edition 2015.
set -euo pipefail

cd "$(dirname "$0")/.."

ROOTS=(firmware/Cargo.toml host/Cargo.toml)
HOOK=.claude/hooks/format.sh

fail=0
err() {
    echo "check-edition.sh: $*" >&2
    fail=1
}

declared=()

for manifest in "${ROOTS[@]}"; do
    if [ ! -r "$manifest" ]; then
        err "$manifest: not readable — cannot check the declared edition"
        continue
    fi
    hits=$(grep -c '^edition = "' "$manifest" || true)
    if [ "$hits" -ne 1 ]; then
        err "$manifest: expected exactly one '^edition = \"NNNN\"' line, found $hits"
        continue
    fi
    value=$(sed -n 's/^edition = "\([0-9]\{4\}\)"$/\1/p' "$manifest")
    if [ -z "$value" ]; then
        err "$manifest: edition value is not a four-digit year"
        continue
    fi
    declared+=("$value")
done

if [ ! -r "$HOOK" ]; then
    err "$HOOK: not readable — cannot check the rustfmt edition flag"
else
    # Comment lines are stripped and the pattern is anchored on the command name, so
    # documenting the flag in prose does not read as a second declaration.
    hook_code=$(grep -v '^[[:space:]]*#' "$HOOK" || true)
    hits=$(printf '%s\n' "$hook_code" | grep -c -- 'rustfmt --edition [0-9]\{4\}' || true)
    if [ "$hits" -ne 1 ]; then
        err "$HOOK: expected exactly one 'rustfmt --edition NNNN' invocation, found $hits"
    else
        declared+=("$(printf '%s\n' "$hook_code" |
            sed -n 's/.*rustfmt --edition \([0-9]\{4\}\).*/\1/p')")
    fi
fi

if [ "${#declared[@]}" -eq 3 ]; then
    distinct=$(printf '%s\n' "${declared[@]}" | sort -u | tr '\n' ' ')
    if [ "$(printf '%s\n' "${declared[@]}" | sort -u | wc -l)" -ne 1 ]; then
        err "editions disagree across ${ROOTS[*]} and $HOOK: $distinct"
    else
        echo "scripts/check-edition.sh: edition ${declared[0]} agrees across ${ROOTS[*]} and $HOOK"
    fi
fi

# Member manifests must inherit. Vendored trees are committed here, so they are
# excluded by pathspec rather than by tracked-ness: a third-party manifest is not
# ours to hold to this repo's inheritance convention.
#
# The listing is captured before the loop so a git failure is fatal — read from a
# process substitution it would leave the loop body unrun and the guard green.
manifests=$(git ls-files -- '*Cargo.toml' ':!:*vendor/*')
if [ -z "$manifests" ]; then
    err "git ls-files found no Cargo.toml — the member-manifest check ran on nothing"
else
    for root in "${ROOTS[@]}"; do
        printf '%s\n' "$manifests" | grep -qxF -- "$root" ||
            err "git ls-files output is missing $root — the manifest list is not trustworthy"
    done
    while IFS= read -r manifest; do
        for root in "${ROOTS[@]}"; do
            if [ "$manifest" = "$root" ]; then
                continue 2
            fi
        done
        if grep -q '^edition = "' "$manifest"; then
            err "$manifest: literal edition key — use 'edition.workspace = true'"
        fi
        if ! grep -q '^edition\.workspace = true$' "$manifest"; then
            err "$manifest: missing 'edition.workspace = true' (Cargo defaults to edition 2015)"
        fi
    done <<<"$manifests"
fi

exit "$fail"
