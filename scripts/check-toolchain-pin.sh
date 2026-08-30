#!/usr/bin/env bash
# Toolchain pin drift guard. The Rust release this repo's gate runs is written in
# three places: `channel` in each of the two workspace rust-toolchain.toml files,
# and the version CI's "Install Rust" step installs. This fails when they
# disagree.
#
# Why it matters: rustup resolves a workspace's toolchain from its
# rust-toolchain.toml, so a bump to the two workspace files with ci.yml left
# behind makes CI install components for a release nothing then uses. rustup
# auto-installs the pinned one instead, without rustfmt and clippy — the
# auto-install behaviour the explicit step exists to remove — and the gate reds
# on a missing component or lints under a compiler nobody chose. So the
# components are checked here too: they are why the step names a version at all.
#
# `firmware/devices/respeaker-pod/rust-toolchain.toml` is deliberately not read.
# It says `channel = "esp"`, a different compiler by design, and CI pins its
# version separately as ESP_TOOLCHAIN_VERSION.
set -euo pipefail

cd "$(dirname "$0")/.."

TOOLCHAINS=(firmware/rust-toolchain.toml host/rust-toolchain.toml)
CI=.github/workflows/ci.yml

fail=0
err() {
    echo "check-toolchain-pin.sh: $*" >&2
    fail=1
}

declared=()

for file in "${TOOLCHAINS[@]}"; do
    if [ ! -r "$file" ]; then
        err "$file: not readable — cannot check the pinned toolchain"
        continue
    fi
    hits=$(grep -c '^channel = "' "$file" || true)
    if [ "$hits" -ne 1 ]; then
        err "$file: expected exactly one '^channel = \"...\"' line, found $hits"
        continue
    fi
    value=$(sed -n 's/^channel = "\([^"]*\)"$/\1/p' "$file")
    case "$value" in
    [0-9]*.[0-9]*)
        declared+=("$value")
        ;;
    *)
        err "$file: channel is '$value' — the workspaces pin an exact release, not an alias"
        ;;
    esac
done

# Comment lines are stripped so the prose above the step, which names the version
# too, does not read as a second declaration.
if [ ! -r "$CI" ]; then
    err "$CI: not readable — cannot check the installed toolchain"
else
    ci_code=$(grep -v '^[[:space:]]*#' "$CI" || true)
    install_line=$(printf '%s\n' "$ci_code" | grep -- 'rustup toolchain install ' || true)
    hits=$(printf '%s\n' "$install_line" | grep -c . || true)
    if [ "$hits" -ne 1 ]; then
        err "$CI: expected exactly one 'rustup toolchain install ...' step, found $hits"
    else
        declared+=("$(printf '%s\n' "$install_line" |
            sed -n 's/.*rustup toolchain install \([^ ]*\).*/\1/p')")
        for component in rustfmt clippy; do
            printf '%s\n' "$install_line" | grep -q -- "--component ${component}" ||
                err "$CI: the install step does not name --component ${component}, which \`make check\` runs"
        done
    fi
fi

if [ "${#declared[@]}" -eq 3 ]; then
    distinct=$(printf '%s\n' "${declared[@]}" | sort -u)
    if [ "$(printf '%s\n' "$distinct" | wc -l)" -ne 1 ]; then
        err "toolchains disagree across ${TOOLCHAINS[*]} and $CI: $(printf '%s ' "$distinct" | tr '\n' ' ')"
    else
        echo "scripts/check-toolchain-pin.sh: toolchain ${declared[0]} agrees across ${TOOLCHAINS[*]} and $CI"
    fi
fi

exit "$fail"
