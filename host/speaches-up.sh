#!/usr/bin/env bash
# Launch the CPU-only speaches container (STT/TTS for parrot mode, rung 3)
# and make sure the models named in config/parrot.toml are downloaded.
#
# Idempotent: safe to re-run; restarts the container if it already exists.
set -euo pipefail

IMAGE="ghcr.io/speaches-ai/speaches:latest-cpu"
NAME="speaches"
PORT=8000
STT_MODEL="Systran/faster-whisper-small"
TTS_MODEL="speaches-ai/Kokoro-82M-v1.0-ONNX"

# Remove any previous instance (running or stopped), then start fresh.
podman rm -f "$NAME" >/dev/null 2>&1 || true

podman run -d --name "$NAME" -p "$PORT:8000" \
  -v speaches-cache:/home/ubuntu/.cache/huggingface \
  "$IMAGE"

# Wait for the API to come up.
echo -n "waiting for speaches on :$PORT "
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 ||
     curl -sf "http://127.0.0.1:$PORT/v1/models" >/dev/null 2>&1; then
    echo "up"
    break
  fi
  echo -n .
  sleep 1
done

# Pull the models parrot.toml expects (no-op if already cached).
echo "ensuring models are downloaded (first run downloads ~1 GB)..."
curl -sf -X POST "http://127.0.0.1:$PORT/v1/models/$STT_MODEL" >/dev/null || true
curl -sf -X POST "http://127.0.0.1:$PORT/v1/models/$TTS_MODEL" >/dev/null || true

echo "speaches ready at http://127.0.0.1:$PORT"
echo "stop with: podman stop $NAME"
