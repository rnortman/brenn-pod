# Runbook: the Reachy pod end to end

Take a Reachy Mini running brenn-os from "nothing configured" to "you speak at it and it
reads your own sentence back in a synthesized voice", from a workstation, with no cloud
accounts — everything runs locally.

Assumes a checkout of this repo, a Reachy Mini on the same LAN with brenn-os on it, and
SSH-as-root to that unit.

Steps 1–5 are the audio half, and they are the whole runbook for a unit whose head never
moves. The head is a second daemon and a second rung: it wants the Brenn bus, and it is
["The other half: the head"](#the-other-half-the-head) below. If what you are doing is
recovering a unit that already worked, skip to
["Recovering a rebooted robot"](#recovering-a-rebooted-robot) — that is one command.

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

**The motion daemon** is `reachy-motiond`, a second Linux binary on the same unit, run as
its own systemd service. It owns the servo bus and nothing else does: the head rests with
the motors unpowered, a wake word engages it in tens of milliseconds, and every ending it
has — a finished conversation, a timeout, a shutdown, a fault — takes torque off again. It
takes no orders from the audio path directly; it obeys motion scripts published on the
Brenn bus, which is why the head is a later rung than the parrot. Its half of this runbook
starts at ["The other half: the head"](#the-other-half-the-head).

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

## The other half: the head

Steps 1–5 leave a unit that hears and speaks and never moves. The head is
`reachy-motiond`: a second daemon, a second service, and the servo bus.

**Read the fault doctrine before touching anything that arms, disarms or handles a motion
fault**: `docs/fault-management.md` in the sibling brenn-reachy checkout. The short form
is the shape of everything below — the Minimum Risk Condition is *stowed and de-torqued*,
a fault de-torques the motors, nothing ever gates de-torquing, and holding torque is never
a fault response.

### What it needs, and where each piece is authored

Three files reach the device, from two repositories, and they answer different questions:

| File on the device | What it describes | Authored in |
|---|---|---|
| `/var/lib/brenn-app/reachy-bench.toml` | **the machine** — servo map, bus node, the crank datum a human measured, the envelope, the step bounds, the tick rate | brenn-reachy, `.local/reachy-bench.toml` (gitignored: it is one unit's calibration) |
| `/var/lib/brenn-app/reachy-motiond.toml` | **the policy** — which pod this is, which channel its scripts arrive on, the rest and dwell timings, optional move-duration overrides | this repo, `firmware/.local/reachy-motiond.toml` |
| `/var/lib/brenn-app/motiond-token` | the bearer token the daemon attaches to the bus with | this repo, `firmware/.local/motiond-token` |

The split is deliberate: the machine's truth has one source and it is not presence policy.
Start the daemon's config from
`firmware/devices/reachy-motiond/reachy-motiond.example.toml`, which writes every
defaulted key at its default and documents what omitting it means.

The head also needs the bus. `speech-surface` publishes motion scripts on
`[brenn] presence_channel`, so the host config has to be a `mode = "brenn"` one with a
`[brenn.bridge]` attachment — the parrot rung in step 1 has neither, and with
`presence_channel` unset the daemon's startup says `presence_absent` and the head never
moves. Both ends must name the same channel, and the remote's ACLs on the bus have to
allow it.

### Bring it up

```bash
make -C firmware reachy-up
```

That is the whole robot, and it is the same command whether this is the first time or the
morning after a reboot — see the next section for why it is one command.

To ask whether it worked, or whether it is still true:

```bash
make -C firmware reachy-status
```

Read-only, one ssh, and it touches no servo. It prints eleven checks — the payload store,
the payload, `brenn-app.service`, the pod's `audio.conf`, the three motion files, the
motion binary, `reachy-motiond.service`, the servo bus node the machine's own
configuration names, and what the motion daemon says it is doing — each `OK` or `MISSING`,
and exits nonzero if anything is missing. Ten of the eleven have the same fix, which is
`make reachy-up`. The self-test record is reported and never counted: nothing gates on it.

The eleventh is the daemon's own state, and it is the one whose fix is not a push. A
daemon whose machine faulted **parks**: torque comes off, it commands nothing further, and
it deliberately does not exit — a fault is never auto-cleared, and a process that exited
would let systemd's restart policy re-torque a machine nobody has looked at. So
`systemctl is-active` calls a parked daemon running. It writes `starting`, `resting`,
`active`, `parked` or `stopping` to `/run/reachy-motiond/state` — its unit's
`RuntimeDirectory`, so the file goes away with the service and can never be stale — along
with whether its pre-torque sweeps are answering. `reachy-status` reads that file and says
so:

- `resting` or `active`, sweeps answering → `OK`.
- `parked` → `MISSING`, printing the fault's stage and detail. Read
  `make -C firmware reachy-motiond-logs`, then restart the unit. `make reachy-up` does not
  clear a fault and the report says as much.
- sweeps failing → `MISSING`. The machine is limp and safe, and no wake will raise the
  head until the servo bus answers again; it recovers by itself when it does.
- no file while the unit is running → `MISSING`: the daemon has only just started, or the
  deployed binary predates the state file. Re-run, then redeploy if it persists.

Then watch the daemon:

```bash
make -C firmware reachy-motiond-logs
```

### Say the wake phrase again

1. The head comes up within about half a second of the wake — that is the acknowledgement,
   and it is the whole point of the raise being quick.
2. It stays up while the brain thinks and while the answer plays.
3. It starts down about half a second after the speech ends — not at some later timeout.
   The host sends the whole timeline in one message when playback starts, so the stow is
   scheduled from how long the audio is rather than reacted to afterwards.
4. Five seconds later (`rest_delay_ms`) torque comes off and the machine goes limp at
   stow. A wake inside that window turns the head straight back up with no release in
   between.

The antennas take the **inboard** arc, crossing over the head rather than sweeping out to
the sides — that is the smaller exterior envelope and it is deliberate. An antenna already
pointing sideways takes the short way down instead.

### The supervised form

`make -C firmware reachy-motiond-deploy` leaves the daemon running under systemd with no
terminal attached. For bench work there is the other form:

```bash
make -C firmware reachy-motiond-run
```

It builds, pushes and runs the daemon in the foreground with a pty, and refuses while the
service is up — one process at a time on the servo bus, with the port's `flock` refusing
underneath either way. `^C` ends it, and so do `kill` and `systemctl stop`: SIGINT and
SIGTERM are the same signal to this daemon, and all of them stow the head and take torque
off. brenn-reachy's own bench targets — the operator tool that moves the machine by hand —
refuse while the service is running, for the same reason.

## Recovering a rebooted robot

A reboot clears everything: `/run/brenn-app` and `/var/lib/brenn-app` are both RAM, and so
is `/run/systemd/system`, where the motion daemon's unit is written. Nothing this
repository pushes touches the eMMC, in dev iteration, ever — that is the brenn-os design
and not an accident to be fixed by persisting something.

So recovery is one command, and it is the same one as first-time bring-up:

```bash
make -C firmware reachy-up
```

In order, each step idempotent on its own:

1. `reachy-deploy` — the audio payload; `brenn-app` is dead without it
2. `reachy-provision` — the pod's `audio.conf`
3. `reachy-bench-config` — the machine's configuration, out of the brenn-reachy clone
4. `reachy-motiond-config` — the motion daemon's configuration
5. `reachy-motiond-token` — its bus token
6. `reachy-motiond-deploy` — its binary and its unit, last, so the service starts with
   everything it reads already in place
7. `reachy-status` — the answer

There is no self-test step: no record gates anything that moves. The individual targets
still exist for the case where one file is the only thing that changed.

## The knob map

Every timing that decides how the head behaves, what owns it, and what pushes it. This is
what makes "tweak the presence timings without code changes" true in practice.

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

**The daemon's policy** — `firmware/.local/reachy-motiond.toml`. Pushed by
`make -C firmware reachy-motiond-config`, which does *not* restart anything; the daemon
reads its configuration once, so follow it with `make -C firmware reachy-motiond-deploy`.

| Key | Default | Decides |
|---|---|---|
| `hold_dwell_ms` | 200 | The longest the motion loop watches the machine before consulting the schedule again. A ceiling, not a period — a step at an arbitrary offset lands on the script's own clock |
| `rest_poll_ms` | 100 | How often the resting watch sweeps a limp machine. Bounds how stale the pose an engage plans from can be, and how long a script asking for the head up waits |
| `rest_delay_ms` | 5000 | How long the head holds at stow, still torqued, before torque comes off. The quick-follow-up window. Refused above 60000 |
| `up_duration_s` | unset | Overrides the machine's own raise duration for this daemon alone |
| `stow_duration_s` | unset | The same for the fold |
| `antenna_duration_s` | 1.5 in the example | The antennas' own clock. They are mechanically independent of the head group, which is why a quick lift is not floored by an antenna arc |

**The machine** — brenn-reachy `.local/reachy-bench.toml`. Pushed by
`make -C firmware reachy-bench-config` (or brenn-reachy's own `make bench-config`), and
read by both the daemon and the operator tool, so the two cannot describe different
platforms.

| Key | In | Decides |
|---|---|---|
| `up_duration_s`, `stow_duration_s`, `move_duration_s` | `[motion]` | The head group's clocks — the head pose and the body yaw, which the six legs follow through the IK. What the daemon governs unless it overrides them |
| `antenna_duration_s` | `[motion]` | The antennas' clock; absent means they ride whichever head-group clock the move is using |
| `max_step_legs_rad`, `max_step_body_yaw_rad`, `max_step_antennas_rad` | `[motion]` | Not timings — the per-tick bounds a duration has to clear. A move whose span will not fit at the duration asked for runs on a clock stretched to fit; the guard past that is a **fault**, never a clamp, and a fault takes torque off |
| `tick_hz` | `[motion]` | The control period those bounds are per |

Duration floors are the thing to read before shortening a move. Both example TOMLs carry
them with the arithmetic: a shaped move's peak rate is 1.875 times its average, so for a
joint moving linearly the floor is `1.875 × span ÷ (bound × tick_hz)`. At the shipped
bounds and 50 Hz that is 1.07 s for the head group, 0.79 s for the body yaw from the
60° cap (1.571 s cap to cap), and 0.81/1.21/1.57 s for the antennas depending on the arc.
A duration under its floor is a move the library right-sizes before it commands it: the
path is unchanged and traversed more slowly, and a `motion_clock_stretched` line in the
capture carries what was asked for beside what it ran on. So a value set too low costs a
slower move and a line of output. The case this exists for is the startup fold out of a
machine a hand left most of a turn round, which no configured clock can be sized for.

## When it does not work

Capture these before changing anything — they are what an investigation needs:

- **The pod's journal**: `make -C firmware reachy-logs`, from before the attempt through
  the failure. Volatile, so capture it while it is there.
- **The motion daemon's journal**: `make -C firmware reachy-motiond-logs`. Its JSONL goes
  to the journal too, in service mode — script receipts and refusals, every engage with
  its wall clock, every release with its verdict, and every fault.
- **`make -C firmware reachy-status`**, which says in one screen which of the ten things
  a reboot clears is missing, and what the motion daemon says it is doing.
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

And the motion half:

| Symptom | Look at |
|---|---|
| Anything at all is wrong after a reboot | `make -C firmware reachy-status` first, then `make -C firmware reachy-up`. Between them they cover every file a reboot cleared — do not go looking one refusal at a time |
| The head never moves, and everything else works | `presence_channel` unset in the host config, or set to a channel the daemon does not name. The host says `presence_absent` at startup; the daemon reports every script it receives, so if it reports none the message is not reaching it |
| Wakes do nothing, the head never moves, and `reachy-status` says the daemon is **parked** | The machine faulted. Torque is off and the daemon commands nothing until a person acts — that is the doctrine, not a bug. The status line names the stage and the fault; read `make -C firmware reachy-motiond-logs` for the run that produced it, decide whether the cause is addressed, then `ssh root@<host> systemctl restart reachy-motiond.service`. A restart is the only way out, and nothing restarts it for you |
| `reachy-status` says the daemon **cannot read the machine** | The servo bus stopped answering the resting sweeps. The machine is limp and safe, and the daemon keeps sweeping and recovers on its own — but no wake raises the head until it does. Check the serial cabling and the bus node; nothing needs restarting |
| `reachy-motiond.service` is installed but not running | Its `ConditionPathExists` lines are unmet — one of the three motion files is missing, which is what keeps a half-provisioned device quiet instead of crash-looping. `reachy-status` names which one |
| The service exits and systemd does not restart it | Exit 6 is a fault: the machine is limp at stow and restarting is noise until someone looks at the journal. Exit 7 is a futile bus attachment — a token, a URL or an ACL. Neither is restarted on purpose |
| The head stops partway through a move and goes limp | A per-tick step bound faulted the move; the journal names the joint. Almost always a duration under its floor — see the knob map. This is the sanctioned outcome, not a malfunction: the alternative is a clamp that lies about where the machine is |
| The head comes down late, at a flat ~30 s | That is a hold script running out its `presence_max_engaged_ms`, which means the closing script never went out. The host's JSONL says whether one was published. Two consecutive losses are needed for this now: the host sends the stow once more at the first refresh past it |
| The daemon refuses a script and says the timeline outruns its timeout, or the timeout outruns 600000 ms | The publisher is not our scripter, or its arithmetic slipped past the host-side clamp. The script standing before it — and that script's timeout — still governs, so the head is still bounded. The host's `script_horizon_clamped` line names a stow instant that was cut back; its absence points at another publisher on the channel |
| The head stays up with torque on for a long time after a turn | `rest_delay_ms`. It is the only knob that holds the pinch posture, which is why it is refused above 60 s |
| A bench command refuses (exit 3 or 4) | The other daemon has the bus. Exit 3 is `brenn-app.service`, exit 4 is `reachy-motiond.service`; stop the one you do not want, or use `make -C firmware reachy-motiond-run`, which refuses for the same reason |
| The daemon refuses to start on the machine's configuration | The bench file is missing or has no crank datum. The datum is calibration a human measured, not a gate that can be waived |

A failure here is a finding worth writing down, not a step to retry until it passes.
