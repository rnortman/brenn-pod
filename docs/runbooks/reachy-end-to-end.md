# Runbook: the Reachy pod end to end

Take a Reachy Mini running brenn-os from "nothing configured" to "you speak at it and it
reads your own sentence back in a synthesized voice", from a workstation, with no cloud
accounts — everything runs locally.

Assumes a checkout of this repo, a Reachy Mini on the same LAN with brenn-os on it, and
SSH-as-root to that unit.

## Scope: the standalone arrangement, with the daemon on your workstation

Everything below runs `speech-surface` as a foreground process on your machine, with the
pod dialling it across the LAN. That is one of two arrangements, and it is the one for
bringing a pod up on its own — an ESP unit, or a Reachy Mini whose head is not in play.

The other runs the whole speech pipeline **on the robot**, in the same launcher as the
motion stack, with the pod dialling loopback. Nothing here describes it, and following
these steps for a robot leaves the daemon on the wrong machine. Its procedure — assembling
the configuration, deploying it, and running a supervised session — is the speech-run
section of brenn-reachy's `docs/bench-runbook.md`.

The one piece this repository still owns in that arrangement is the pod's own
configuration, which the on-robot procedure sends you back here for:

```
make -C firmware reachy-provision ON_UNIT=1 SPEECH_CONFIG=<the robot's speech config>
```

`ON_UNIT=1` says the daemon runs on the unit, which is what makes the loopback address in
that config the right answer rather than a link that never comes up. The key table that
config names is written beside it, so both sides of the link keep deriving from one
directory.

Steps 1–5 are the whole runbook, and they are the whole of what this repository puts on a
unit. The head is the other half of a robot and it is another repository's: brenn-reachy
owns the motion stack, its payload, its deploy tooling and its runbook
(`docs/bench-runbook.md` there). Nothing here brings it up, and nothing here reports on
it — a `ready` from `reachy-status` says the pod is up and says nothing about the head. If
what you are doing is recovering a unit that already worked, skip to
["Recovering a rebooted pod"](#recovering-a-rebooted-pod) — that is one command.

## Doctrine first: everything lands in RAM

brenn-os never takes a flash write in normal operation — the eMMC is soldered down and has
a finite write budget, and nothing in this runbook touches it. The payload store,
`/run/brenn-app`, is a tmpfs: the application is rsynced (or fetched) into RAM, and its
per-unit configuration is pushed into RAM beside it, under `/run/brenn-app/conf`. A reboot
clears both; you deploy and provision again — each is one command.

The order never matters: a pod that comes up before its configuration is in place does not
exit — it parks and re-reads every 5 seconds, so a post-reboot re-run of the provision
command is all it takes to bring it back.

## What the pieces are

**The pod** is the Reachy Mini. It runs one payload — `reachy-pod` — as the `app` account:
captures from the XVF3800 mic array, lets the chip's own speech-energy telemetry decide
when someone is talking, streams those segments to the host over TLS-PSK, and plays back
whatever audio the host sends down the same connection.

**The host** is your workstation, running `speech-surface`: a plain foreground dev process
in this repo, not a service and not a Brenn server. It listens for pod connections, cuts
the incoming audio into utterances (wake word + endpointer), hands each utterance to a
*brain*, and sends the brain's audio answer back down to the pod.

**The motion stack** is brenn-reachy's, and on a robot it is deployed from that clone as
one payload under one launcher — the pod binary among its applications. It owns the servo
bus and nothing else does. This repository ships the libraries the voice pipeline is built
from and the pod that hears and speaks; it deploys neither the head nor, on a robot, the
pod. `deploy-reachy-pod.sh` refuses a unit carrying that payload for exactly this reason:
activating a pod-only payload there would replace the whole stack with a binary that
cannot move the machine.

**The brain** here is `mode = "echo"` — the parrot: transcribe what you said and read it
back. STT and TTS come from a local `speaches` container. (`mode = "wav"` — answer every
utterance with one fixed clip, no container needed — exists as a fault-isolation fallback,
not as a step on the way; see the last section. `mode = "llm"` is not a thing yet; the
config parser rejects it.)

Host configs live under `host/config/`, which is gitignored (`.gitignore`:
`host/config/*.toml`) — they hold a LAN address and a path to a secrets file, so they stay
on your machine. Full contents to paste are in step 1.

## Prerequisites — idempotent, and all no-ops on a workstation that already runs parrot mode (e.g. against the ESP pod)

- `make -C host fetch-models` — the openWakeWord weights (not redistributed in-tree).
  sha256-verified; a warm run is a no-op.
- `cd host && ./speaches-up.sh` — the local STT/TTS container
  (`ghcr.io/speaches-ai/speaches:latest-cpu`, podman, port 8000). Idempotent; only a
  first-ever run pulls the image and models.
- Tell the tooling which unit is yours, once — every `reachy-*` make target reads it:

  ```bash
  echo 'REACHY_HOST=reachy00' > firmware/.local/reachy.conf
  ```

- Your workstation's LAN IP — the address the pod will dial. Not `127.0.0.1`, not
  `0.0.0.0`; the pod is a different machine.

  ```bash
  ip -4 addr show scope global | grep inet
  ```

## Step 1 — write the host config

Already have a `host/config/parrot.toml` from running parrot mode against the ESP pod?
Skip this step — the provision command in step 2 reads that same file.

From `host/`, create `config/parrot.toml`. Substitute your own LAN IP and your own home
directory in `pod_psk_file` — both must be literal (no `~`, no hostname):

```toml
listen_addr = "<your workstation's LAN IP>:7380"
pod_psk_file = "/home/<you>/.config/brenn-pod/pod-psk.toml"

[record]
enabled = true
dir = "./framelogs"

[wake]
mode = "oww"
melspectrogram = "models/oww/melspectrogram.onnx"
embedding      = "models/oww/embedding_model.onnx"
model          = "models/oww/hey_jarvis_v0.1.onnx"
threshold      = 0.5

[endpointer]
model = "models/silero/silero_vad.onnx"

[brain]
mode = "echo"

[stt]
backend = "http"
url = "http://127.0.0.1:8000"
model = "Systran/faster-whisper-small"
language = "en"

[tts]
backend = "http"
url = "http://127.0.0.1:8000"
model = "speaches-ai/Kokoro-82M-v1.0-ONNX"
voice = "af_heart"
```

What each table is doing:

- `listen_addr` is required and must name a concrete interface — the daemon refuses
  `0.0.0.0` rather than guess. It is also where step 2 gets the address the pod dials.
- `pod_psk_file` is the listener's key table — one line per pod, every pod type in the
  one file, mode 0600. You never write it by hand: step 2 creates and updates it (the
  same table `podctl provision-audio-psk` maintains for ESP pods). A pod absent from it
  simply cannot connect — the listener speaks only TLS-PSK.
- `[record]` drops every captured segment under `host/framelogs/` — your evidence if
  something goes wrong.
- `[wake]` and `[endpointer]` together build the continuous listener — the thing that
  hears "Hey Jarvis", carves the utterance out of the stream, and hands it to the brain.
  Both tables are required for it, and `[wake]`'s three model paths are the weights
  `fetch-models` fetched; the Silero endpointer model is committed in-tree. `mode` has
  exactly one value, `"oww"` — `"bypass"` is rejected at startup as an unknown mode.
  Dropping either table is legal and gives a recording-only daemon: segments still arrive
  and are recorded, but no utterance is ever minted and the brain never answers — the
  daemon says so at startup with a loud `listener_absent` line.
- `[brain] mode = "echo"` with `[stt]`/`[tts]` pointing at the `speaches` container the
  prerequisites started; the two model names are the ones it pulls. Paths are relative to
  `host/`.

Unknown keys are fatal at startup rather than silently ignored, so a typo here shows up
immediately as a parse error naming the field.

## Step 2 — provision the pod: one command

```bash
make -C firmware reachy-provision
```

Everything is derived; there is nothing to type in. The command:

1. asks the device for its **hostname** — `reachy00` on the dev unit — which is also its
   TLS-PSK identity, so the id comes from the one place it is authoritative;
2. reads `listen_addr` and `pod_psk_file` from `host/config/parrot.toml`;
3. reuses the pod's key from the table if it is already there, or generates a fresh
   32-byte key and files it under the pod's hostname (file mode 0600);
4. composes the pod's `audio.conf` (`ADDR=<listen_addr>`, `PSK=<key>`) and pushes it over
   ssh **stdin** into `/run/brenn-app/conf/audio.conf` — tmpfs, RAM only — owned by
   `app`, mode 600.

The key is never printed, never part of a command line, and never lands in shell history
or either machine's process table. The command is idempotent: re-run it after any reboot
of the unit and the pod reconnects. It is true about `audio.conf` and about nothing else,
though — a reboot clears every device file this repository writes, and
["Recovering a rebooted robot"](#recovering-a-rebooted-robot) is the command that pushes
all of them. To rotate a key deliberately, delete the pod's line from the key table and
re-run.

`ADDR` is a **literal IP and port** — the pod refuses names on purpose (it may have no
clock and no resolver at boot, and a link that waits on DNS is a link that is down for
reasons nothing on the pod can report). Optional tuning keys — `CHANNEL`,
`VAD_THRESHOLD`, `VAD_HANGOVER_MS` — all have working defaults; to set one, put its
`KEY=VALUE` line in the gitignored `firmware/.local/audio.conf.extra` and re-run — the
push appends that fragment verbatim.

## Step 3 — start the host daemon

From `host/`:

```bash
cargo run -p speech-surface --bin speech-surface -- --config config/parrot.toml
```

It stays in the foreground and logs to the console; it also writes structured JSONL, and
with `[record] enabled = true` it drops captured segments under `./framelogs`. Ctrl-C
stops it. Nothing is installed, no service is created. Leave it running in its own
terminal.

## Step 4 — self-test, then deploy the pod

The self-tests and the running application are mutually exclusive — both want the sound
card and the USB control interface — so run the bring-up registry **first**, while nothing
is activated:

```bash
make -C firmware reachy-selftest
```

Seven cases, all of which should pass. If the array or the USB control plane is unhappy,
this is where it says so, before any of the network path is in play.

Then deploy and start the application:

```bash
make -C firmware reachy-deploy
```

This builds the aarch64 payload in the pinned container, rsyncs it into the device's
payload store — `/run/brenn-app/releases`, a tmpfs: RAM, not flash — activates it
(contract check, symlink switch) and restarts `brenn-app.service`.

Watch it in a third terminal:

```bash
make -C firmware reachy-logs
```

You want to see the pod read its config, resolve its identity as `reachy00`, and connect
to your workstation. The host's console should show the connection from the same moment.
If the pod says it cannot read `/run/brenn-app/conf/audio.conf`, the unit has rebooted
since step 2 — re-run the provision command.

## Step 5 — say the wake phrase

Stand near the array and say **"Hey Jarvis"**, then a short sentence.

What should happen, in order:

1. The pod's chip-side gate opens; the pod streams a segment (`reachy-logs` shows the
   segment opening).
2. The host wakes on "Hey Jarvis", endpoints the utterance, and hands it to the brain
   (its console and JSONL show the wake and the utterance).
3. The host transcribes, synthesizes, and sends the audio down; the pod reads your own
   sentence back to you out of its speaker.

Hearing your sentence back is the end-to-end pass: both directions, wake, endpointing,
STT and TTS have all run over one TLS-PSK connection. Record the outcome — pass or
failure transcript — in
`~/src/brenn-ops/docs/adr/2026/07/31-multi--reachy-xvf3800-usb-audio-pipeline/bench-observations.md`.

## Recovering a rebooted pod

A reboot clears everything: `/run/brenn-app` and `/var/lib/brenn-app` are both RAM.
Nothing this repository pushes touches the eMMC, in dev iteration, ever — that is the
brenn-os design and not an accident to be fixed by persisting something.

So recovery is one command, and it is the same one as first-time bring-up:

```bash
make -C firmware reachy-up
```

In order, each step idempotent on its own:

1. `reachy-deploy` — the audio payload; `brenn-app` is dead without it
2. `reachy-provision` — the pod's `audio.conf`
3. `reachy-status` — the answer

There is no self-test step: no record gates anything that moves. The individual targets
still exist for the case where one file is the only thing that changed.

On a robot this is not the recovery: the pod there is one application of brenn-reachy's
payload, and that clone's own deploy brings the whole stack back in one command.

## The knob map

Every timing this repository owns that decides when the head is asked to move, and what
pushes it. This is what makes "tweak the presence timings without code changes" true in
practice.

**The host's schedule** — when the head goes up and when it is told to come down. Lives in
whichever `speech-surface` config the daemon is actually started with (`SPEECH_CONFIG` in
`firmware/.local/reachy.conf` should name that same file). Pushed by restarting
`speech-surface`; nothing reaches the device.

| Key | In | Default | Decides |
|---|---|---|---|
| `presence_channel` | `[brenn]` | unset | The channel scripts are published on. Unset means no scripts and a head that never moves (`presence_absent` at startup) |
| `presence_refresh_ms` | `[brenn]` | 5000 | How often the standing script is said again. A script lost in transit is repaired within one of these |
| `presence_linger_ms` | `[brenn]` | 8000 | How long the head stays up after a turn that asked to keep listening, and after a wake that produced no turn at all |
| `presence_max_engaged_ms` | `[brenn]` | 30000 | The floor under the timeout every script carries: the daemon stows this long after receipt whatever else happens. The bound that matters while the brain is still thinking. A turn whose speech reaches further carries a timeout sized from its own timeline instead — a script's timeout is a ceiling on that timeline, never shorter than it. Capped at 600000 (the protocol's own ceiling); a config past that is refused at startup |
| `presence_stow_margin_ms` | `[brenn]` | 500 | How long after the estimated end of the speech the head starts down. Absorbs playback jitter |

**The head's timings** are brenn-reachy's — the machine's own file and the motion stack's
configuration, both authored and pushed from that clone. They are documented there,
because a second copy of a duration is a copy that goes stale.

## When it does not work

Capture these before changing anything — they are what an investigation needs:

- **The pod's journal**: `make -C firmware reachy-logs`, from before the attempt through
  the failure. Volatile, so capture it while it is there.
- **`make -C firmware reachy-status`**, which says in one screen which of the four things
  a reboot clears is missing.
- **The host daemon's console output**, and its JSONL file.
- **The recorded segments** under `host/framelogs/`, if the segment reached the host at
  all.

To isolate a failure, simplify the host side — the pod side never changes:

- **Take STT/TTS out of play:** switch `[brain]` to `mode = "wav"` with
  `clip = "testdata/wake/wake-phrase.wav"` and drop the `[stt]`/`[tts]` tables. A clip
  played back on a wake proves the whole pod loop with zero external services.
- **Take the listener out of play:** drop the `[wake]`, `[endpointer]` and `[brain]`
  tables. The daemon is recording-only — expect the loud `listener_absent` startup line —
  and `host/framelogs/` answers whether segments are arriving at all.

Common shapes:

| Symptom | Look at |
|---|---|
| Pod logs a config error every 5 s | The device config is missing (a reboot cleared it) — re-run `make -C firmware reachy-provision`. A hand-edited device file is overwritten by the next provision run, on purpose: the workstation's key table and `parrot.toml` are the sources of truth |
| Pod connects and is dropped | The pod's key-table line and the pushed key disagree — someone edited one side by hand. Re-run the provision command; to rotate both sides, delete the pod's line first |
| Nothing connects at all | `listen_addr` on a loopback address; a firewall on the workstation's `:7380` |
| Selftest refuses to run (exit 3) | `brenn-app.service` is running; the registry will not share the hardware with it |
| Segments arrive, nothing comes back | Whether `speaches-up.sh` finished and `:8000` answers; in the `wav` fallback, the brain clip path |
| Daemon dies at startup on a model load | `make -C host fetch-models` never ran, or a `[wake]`/`[endpointer]` model path is wrong — they are relative to `host/`. A missing model is fatal, never a quietly deaf listener |
| Daemon rejects the config naming `mode` | `mode = "bypass"` from an old config — the mode is gone; the only value is `"oww"` |
| Nothing wakes | The wake threshold is too high for the room, you are too far from the array, or that is not the phrase the configured model listens for |

The head is not in this table. A robot whose head is wrong is brenn-reachy's runbook:
nothing this repository deploys or reports on can tell you anything about it.

A failure here is a finding worth writing down, not a step to retry until it passes.
