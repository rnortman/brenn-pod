#!/usr/bin/env bash
# Fetch the openWakeWord model weights this repository does NOT redistribute.
#
# Why not committed: openWakeWord's pre-trained models are CC BY-NC-SA 4.0
# (NonCommercial), which is incompatible with this repo's Apache-2.0 license, so
# the weights are fetched from upstream instead of being redistributed here.
# See host/models/oww/README.md. (Silero VAD IS committed — it is MIT.)
#
# Idempotent: a file already present with the correct sha256 is left untouched.
set -euo pipefail

BASE="https://github.com/dscripka/openWakeWord/releases/download/v0.5.1"
DIR="$(cd "$(dirname "$0")/oww" && pwd)"

# name  sha256  (kept in sync with the table in host/models/oww/README.md)
MODELS="
melspectrogram.onnx ba2b0e0f8b7b875369a2c89cb13360ff53bac436f2895cced9f479fa65eb176f
embedding_model.onnx 70d164290c1d095d1d4ee149bc5e00543250a7316b59f31d056cff7bd3075c1f
hey_jarvis_v0.1.onnx 94a13cfe60075b132f6a472e7e462e8123ee70861bc3fb58434a73712ee0d2cb
"

while read -r name sha; do
    [ -z "$name" ] && continue
    dest="$DIR/$name"
    if [ -f "$dest" ] && printf '%s  %s\n' "$sha" "$dest" | sha256sum -c - >/dev/null 2>&1; then
        echo "ok (cached): $name"
        continue
    fi
    echo "fetching: $name"
    curl -fsSL "$BASE/$name" -o "$dest"
    printf '%s  %s\n' "$sha" "$dest" | sha256sum -c -
done <<< "$MODELS"

echo "openWakeWord models present and verified in $DIR"
