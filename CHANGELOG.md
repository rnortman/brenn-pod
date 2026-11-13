# Changelog

All notable changes to brenn-pod are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [Unreleased]

### Added

- **The Linux pod can record a capture device to a frame log that says where
  it lost audio.** The new `pod-capture` program records every channel of one
  ALSA capture device, at the rate and sample format asked for and never
  resampled, until it is stopped, into a frame log: the same format the voice
  host records a pod's connection into. Each block carries its position and a
  reading of both the monotonic and wall clocks; overruns, suspends and a
  destination that falls behind are recorded as losses at their positions, and
  the log ends saying why it stopped. `frame-log summary` describes a capture's
  log or the voice host's, and `frame-log export-wav` turns either into a WAV
  with holes as silence plus a map of them. `pod-capture` needs only
  libasound: no configuration, network or key. The capture's additions to the
  frame format (a 32-bit sample format, described channels, a capture frame
  the pods never send) leave the audio protocol version at 4, so no pod needs
  reflashing. Format:
  `docs/reference/frame-log.md`; the program: `docs/reference/pod-capture.md`;
  procedure and the reSpeaker Flex hardware tests
  (`make -C firmware capture-hil`): `docs/runbooks/pod-capture.md`.
- A build outside a git checkout can state its commit in
  `BRENN_POD_BUILD_COMMIT`; inside a checkout git decides, and a declared
  commit that disagrees fails the build.
- **Every pod reports its health and its room once a second, whether or not
  anyone is speaking.** The pod sends a small status frame over the
  connection it already holds: its capture position, 100 ms room-level blocks
  measured on the raw microphone (a rolling 30 s history, each block marked
  when the pod was playing), and counters and gauges — reconnects, segments
  opened and dropped, capture and playback underruns and stalls, Wi-Fi signal,
  free heap, the streamer's least free stack. The voice host writes a
  `pod_status` line on the first frame of each connection, whenever a counter
  moves or the speech gate opens or closes, and every 10 s otherwise; a pod
  that sends nothing for 10 s gets a `pod_silent` line and its connection is
  closed. A counter a pod does not measure is `null`, never 0. This is protocol
  version 4: **update the voice host before or with the pods** — an older host
  refuses a version-4 pod. The new host still reads version-3 pods and
  recorded frame logs, without status. Hardware self-tests
  (`LevelMeterMatchesRing`, `LevelBlocksFlagPlayback`, `PodStatusIdle`) guard
  the meter, the playing flag and the idle frame; `PodStatusIdle` needs one
  more port open to the HIL host (`hil-host --print-ports` lists it, and
  `scripts/hil-firewall.sh` opens it).
- **The echo brain can listen after its replies.** A new `[echo]
  listen_window_ms` makes the parrot end every turn open, so the follow-up
  window can be exercised end to end with no language model. Refused under
  any brain other than echo.
- **Spoken replies now have a volume control, up to answering at the volume
  you spoke.** Every sound the pod plays — brain replies, announcements, the
  offline "can't hear you" clip — passes through one gain stage, set in
  decibels (perceived loudness, not a linear multiplier). Layers, all optional:
  - a new `[volume]` table: `gain_db` (baseline, also settable per pod), and
    `min_db` / `max_db`, hard bounds nothing can exceed;
  - the brenn brain can send a `<volume/>` marker: `db` for one reply,
    `hold` to keep a level until changed (or the voice host restarts), and
    `auto`, the automatic gain's tuning knobs and its swing limits;
  - automatic gain (`[volume.auto]`, off by default): louder in a noisy room,
    quieter in a quiet one, with a conversion factor, a reference room level
    and min/max swing (see "Automatic volume follows the room" under Changed).
  Each played reply logs a `reply_gain` line saying what was applied and why;
  brain changes log `volume_set` or `volume_refused`. The brain also sees the
  pod's current volume state and how loudly it was addressed on every
  utterance. Without the new keys and markers nothing changes.
- **The reSpeaker pod now streams two audio channels.** Next to the processed
  speech beam, the pod routes one raw microphone (no AGC, noise suppression
  or beamforming) to the second I2S slot at boot and streams both. The host
  uses the raw channel to measure speech and room levels for automatic gain;
  speech recognition still uses the beam only. This also corrects our
  reference docs: the stock I2S firmware does give raw-mic access. New
  hardware self-tests (`Xvf3800OutputRouting`, `I2sRightSlotSanity`) guard
  the routing.
- **The transcriber can be primed for the wake word.** New `[stt] prompt` and
  `[stt] hotwords` keys are passed verbatim to the speech-to-text service —
  useful with the `keep` wake-word mode, where the wake phrase stays in the
  audio. A prompt can occasionally make Whisper echo it on near-silent clips;
  shorten it if that happens.
- **A process composing the voice server can choose where the head looks when
  the wake word is heard** — the hook behind "look at the talker". The new
  `Sinks.gaze` seam (a `WakeGaze`) is asked once per wake with the microphone array's direction readings from the end of
  the phrase, and names a direction — a bearing and an elevation in
  milliradians, the motion wire's look — for the head to face. The chosen look
  is also what the head answers from, in place of `presence_turn_pose`, until
  the head stows or the next wake. A direction no motion script may carry is
  refused with a `gaze_refused` line (carrying the direction) and the wake
  takes the configured pose; a seam supplied to a run with no scripter is
  reported once as `gaze_seam_unused`. Without the seam nothing changes.
- **`presence_wake_pose = "keep"` freezes the head where it is on a wake**
  instead of moving it to a pose. If nothing follows, the head stows at
  `presence_max_engaged_ms`. `presence_turn_pose` may not be `keep`, and a
  `presence_wake_move_ms` has no effect alongside it.
- **The voice host now starts on a robot deployed from a brenn-os payload.**
  brenn-os unpacks a payload (the read-only bundle of binaries, configs and
  keys it fetches onto the device) world-readable, so the host refused its
  own pod key file ("psk file mode 0644 is group/world-accessible") and would
  not start. Set the new top-level `secrets_posture = "payload"` on such a host:
  the pod key file and the bus token file may then be world-readable, and are
  refused only if another account could write them. The default,
  `"owner-only"`, keeps the existing rule that these files be mode 0600.
- **The robot now answers when it can't hear you.** If the speech-to-text
  service (the transcriber) is unreachable, times out or errors after a wake
  word, the pod plays a clip you configure — e.g. "I can't hear you right
  now" — instead of going silent. Set it with the new `[stt] unreachable_clip`
  key (a 16 kHz mono clip); it needs a `[brain]` table, and a configuration
  that sets it without one is refused at startup. Without the key nothing
  changes. Each such reply is logged as `stt_unreachable_reply`.
- **The pod's link configuration can now travel with its launcher.**
  `reachy-pod run --config PATH` reads the link configuration from `PATH`
  instead of the compiled-in default, and `provision-reachy-pod.sh --emit
  <pod-id>` prints that file instead of pushing it over ssh, so a build can
  stage it. Pods provisioned with `make reachy-provision` work as before.
- **An LLM reply can now drive the robot's head.** The brain's response may
  embed `<pose name="..." speed="..."/>` and `<motion name="..." speed="..."/>`
  markers. A pose retargets the head for the rest of the reply; a motion plays
  once over the current pose, and a second motion interrupts the first. Names
  are resolved against a local copy of the daemon's pose/motion library
  (configured via `[brenn] library_names`); an unknown or out-of-range cue is
  refused with a `cue_refused` log line and never reaches the wire. The help
  document sent to the brain now lists every pose and motion it may name.
- **`<listen/>` keeps the microphone open after a reply.** A reply ending in
  `<listen/>` opens a bounded capture window (duration = `presence_linger_ms`)
  so the person can respond without saying the wake word. The head stays up for
  as long as the window is open, so it never starts down while a follow-up is in
  flight. Follow-ups are confidence-gated the same way wake and barge carves
  are.
- **Echo mute mode.** `[barge] mode = "mute"` suppresses all capture while the
  pod's own playback is active and for a short tail after it drops, so the
  robot never hears its own reply. The cost: under mute, no voice interruption
  is possible at all, wake word included. The default remains `"wake"`.
- **The `wake_detected` line carries `doa_count` and `doa_last`**: how many of
  the microphone array's direction readings arrived during the end of the
  phrase, and the last of them.
- **The reachy pod's self-test now checks the mic array's fixed-beam mode.**
  `reachy-pod selftest` gains `ctrl_fixedbeams`, which points both of the
  array's focused beams straight ahead, checks the settings read back exactly
  and turns the mode off again. `selftest --manual` gains
  `fixed_beams_broadside`, which asks you to speak from straight in front of the
  array while the beams are fixed and checks it places you within 10° of
  straight ahead. Nothing outside the self-test uses fixed-beam mode.

### Changed

- **Automatic volume follows the room, both up and down, and nothing else.**
  The automatic term is now `k_ambient` dB per dB the room is above
  `ambient_ref_dbfs`, and the same per dB *below* it, within the swing bounds:
  set the reference to the room level you call typical, set `gain_db` for
  that room, and replies get louder in a louder room and quieter in a quieter
  one. How loudly you spoke no longer moves the volume (a quiet voice next to
  the pod and a loud one across the room read alike). The room is measured
  from the pod's own level blocks: the median of the last 10 s of blocks
  before you spoke, leaving out blocks while the pod was playing and just
  after, and your own earlier requests. A reading counts fully for 30 s and
  not at all from 5 min on (`stale` on the `reply_gain` line); the
  utterance's `level` and the brain's view of it now carry the room reading's
  block count, age and whether it was carried over from earlier. Default
  `k_ambient` is 1.0. **To migrate:**
  - delete `k_speech` and `speech_ref_dbfs` from `[volume.auto]`: the voice
    host refuses a config that names either, naming the key. A brain that
    still writes `k_speech` or `speech_ref` in `<volume/>` has them ignored
    beside a live attribute, and reported as an unknown marker alone;
  - re-set `ambient_ref_dbfs` before running this build. A value tuned when
    the room could only boost (a very low reference, with `gain_db` turned
    down to compensate) now turns every reply *down* in any room quieter than
    it, by `k_ambient` dB per dB, as far as the swing floor. Choose the room
    level you call typical: the `pod_status` lines' `levels.p50_dbfs` shows
    the room continuously. On the bench a quiet room read about −77 dBFS,
    and about −74 on the loudest of three takes. The swing (`min_db` / `max_db`) now bounds
    attenuation as well as boost.
- **The HIL speaker tone is about half as loud** (amplitude 0.16, was 0.5;
  `SpeakerOutput` reports `amp=16`).
- **`[wake] wake_tail_ms` defaults to 500 (was 1500).** Short commands ("hey
  cogsworth, tell me a joke") were held as a bare wake. A command under about
  half a second said in one breath with the wake phrase is still held.
- **The provisioning tool's status lines go to stderr.** The
  `provisioned — … points at …` and `appended … verbatim` lines join the key
  messages there, so stdout carries nothing but a composed file.
- **Noise inside a `<listen/>` window no longer kills the conversation.** A
  cough, the television, or a half-word during a capture window used to close
  it permanently, forcing the person to say the wake word again to be heard.
  The window now survives any candidate the gate declines — at its original
  deadline, never re-dated — and a new `listen_restored` line records the
  recovery. The head tracks the window rather than a fixed timer: it stays up
  while the window is open (bounded by `presence_max_engaged_ms` as a fault
  ceiling) and stows when the window expires, instead of folding
  `presence_linger_ms` on each speech report. Because the listener's sample
  clock only advances when audio arrives, a wall-clock `listen_released` backs
  the expiry in a quiet room. New log lines: `listen_restored` (with
  `at_sample`), `listen_released`.
- **An utterance that transcribes to nothing is declined before any brain.**
  The gate makes "does this carry usable text?" its first test, so no brain is
  called for speech that said nothing. A wake-gated empty is now
  `brain_wake_command_absent {reason: "empty"}` under every brain mode (was
  `brain_no_transcript` under `mode = "brenn"`), and `mode = "wav"` needs an
  `[stt]` table to answer anything. The gate also classifies a candidate's
  provenance once, so a carve that began inside a window and went on to cut the
  reply is declined as a barge rather than as a follow-up.
- **`continuation_timeout_ms` default raised from 10 s to 30 s**, matching a
  realistic cloud round-trip budget. The help document renders the configured
  value so it cannot contradict the running config. The wake-deafness cost of a
  long continuation wait is now stated in the key's doc comment.
- **`<listen/>` is no longer reported as unsupported.** The
  `LinkListenUnsupported` event, its counter, and the `TODO(brenn-brain-listen)`
  marker are removed.

- **The head's poses are named per event.** Five new optional `[brenn]` keys:
  `presence_wake_pose` (where the head goes on a wake word and on a barge —
  listening) and `presence_turn_pose` (where it goes when an utterance is
  dispatched — answering), both defaulting to `neutral`; and
  `presence_wake_move_ms`, `presence_turn_move_ms` and `presence_stow_move_ms`,
  which state how fast each of those moves goes. A pace left unset leaves the
  move at the pace the daemon's own library holds for that pose. A deployment
  that names none of the five behaves exactly as before.
- **Wake-word barge-in is now the default.** A new `[barge] mode` switch
  controls what may cut an audible reply. Under `"wake"` (the default), only a
  wake-word detection interrupts; sustained speech over a reply is ignored, since
  the echo canceller cannot reliably distinguish the robot's own voice once the
  head moves. `"speech"` restores the previous sustained-speech rule alongside
  the wake rule. A reply whose text contains the wake phrase is automatically
  exempt so the robot does not cut itself off by saying its own name. The
  `barge_in` log line now carries a `cause` field (`wake` or `speech`).
- **New required config key `[wake] phrase`.**  Existing deployments with a
  listener must add `phrase = "..."` (the words the wake model listens for)
  before their next restart; the daemon refuses to start without it.
- **`reachy-pod reboot-chip` subcommand.** The audio-chip reset is now a
  separate step so a launcher can finish the reboot before any other process
  starts. `run --chip-rebooted` then attaches without resetting again. A bare
  `run` still reboots the chip as before.
- `ScriptTiming` has a `Default` impl matching the compiled-in presence
  durations, and a regression test pins that a closing script does not stow
  before the estimated audible end plus the stow margin.
- **A pod can forward every utterance, wake word or not.** `[wake] policy =
  "bypass"` sends every utterance to speech-to-text and to the brain with no
  wake word required — a recording or labelling run, where saying the wake word
  before each label is the thing in the way. The default, `"gated"`, is today's
  behaviour: an utterance reaches speech-to-text only when an armed wake covers
  it. The wake gate is still built and scored under either policy, so wake
  detections are still reported, and the startup header says when a daemon is
  bypassing. Bypassing alongside the bus brain is refused at startup: that pair
  would publish every word said in the room to the harness.
- **The wake word now waits for its command.** Saying "Hey Jarvis", pausing, and
  then speaking used to send the wake word alone to speech-to-text and drop the
  command that followed — the utterance closed a second after the wake word, and
  the command arrived with no wake to gate it. An utterance that holds nothing
  but the wake word is now kept back, and the next thing said within eight seconds
  is transcribed together with it as one utterance — including when the pause was
  long enough that the microphone stopped sending in the middle of it. Endpointing
  everywhere else is unchanged, so a conversational reply is no slower. Two new
  `[wake]` knobs tune it: `wake_tail_ms` (how much speech after the wake word
  makes it a command, default 1500) and `command_wait_ms` (how long to wait,
  default 8000; `0` restores the old behaviour). A wake that is never followed is
  still reported as a wake with no command, and a new `wake_held` log line records
  every wait. The head does not wait out a quiet room with it: a wake nothing
  follows settles the head once the wait is up on the host's own clock, whether or
  not the room has made another sound, and a `wake_hold_released` line records it.
- **Wake-word trim is now a config switch.** `[stt] wake_word = "trim"` (the
  default, preserving today's behaviour) cuts the wake word out of the clip
  before transcription; `"keep"` sends the whole carve. The `utterance` log
  line now reports both the listener's trim boundary and the sample offset
  actually sent, so an offline comparison can re-transcribe the other variant
  from the same recording.
- `transcribe_pcm` is now a public helper in `speech-pipeline`, so downstream
  tools can drain a transcriber stream without reimplementing the final-event
  settle logic.
- Reachy robot voice pipeline support! You can now run the speech in/out on a
  Reachy Mini Wireless robot.
- Reachy acknowledges the wake word by raising its head, then stows it when the
  interaction is over.
- **The Reachy pod now brings up the XVF3800 mic-array chip at startup.** The pod
  reads the chip's identity, reboots it to clear adaptive state, then routes the
  right output channel to the ASR signal — the beamformed, echo-cancelled path
  that bypasses the chip's noise suppressor and AGC. The noise suppressor was
  learning the robot's servo noise and suppressing speech along with it, which
  caused most second-and-later utterances in a conversation to be declined by the
  STT confidence gate. The new `chip` module (`reachy-pod`) and expanded register
  surface in `xvf3800-ctrl` are tested off the device.
- **A chip state line while someone is speaking.** While the VAD gate is open the
  pod reads eleven post-processing registers and prints them periodically, so a
  fetched console says what the adaptive stages were doing during the utterance
  they degraded.
- **The pod's startup line carries `build=`**, the first twelve hex digits of the
  commit it was compiled from (plus `+dirty` when the tree had tracked edits).
  The build script writes a `.build` sidecar beside the payload binary with the
  full revision and a SHA-256 digest, so a downstream consumer can verify that
  the artifact it stages matches the source it compiled.
- **Connection announcements on the pipeline.** Each pod connection sends a
  `Connected` item carrying its room and frame-log path before its first segment
  closes, so the first utterance of a session is attributed to the right room and
  the right recording — previously it read `unmapped` until a segment closed.
- **`segment_closed` carries `base_sample`** — the absolute sample index the
  tracking line's offsets are relative to, previously unstated on the console.
- **`stt_configured` carries the confidence-gate thresholds** (`no_speech_max`,
  `avg_logprob_min`), so a declined transcript in a fetched log is interpretable
  without knowing the build's defaults.
- **An announcement seam on `speech-surface`**: `announce_seam`,
  `Server::with_announcements` and `Config::carries_announcements()`. A
  composing process puts a sentence on the queue and the robot says it out loud
  on every pod that is connected — one non-interruptible `Text` command per pod,
  answering no turn, so an announcement authors no head motion and opens no
  barge floor. New lines: `announcement_spoken`, `announcement_unheard`,
  `announce_seam_unused`, `announce_task_exited` — the first two carrying the
  sentence itself, so the spoken half of an alert joins its `alert_handed_off`
  half by what was said.
- **An alert or a sentence still queued when its drain ends is named** —
  `alert_seam_ended` for the run shutting down and for a bridge that is gone,
  `announcement_unheard` for the run shutting down and for a router that is
  gone — rather than dropped with the drain. A seam whose queue has been taken
  also refuses anything handed over afterwards, so a late alert is reported to
  the composing process instead of vanishing into a receiver about to close.
- **Boot-path labels on reported heap samples**, so a heap figure can be
  attributed to the boot that produced it. `TestData::DeviceHealth` gains a
  `reset_reason` field — a wire-schema change — surfacing as `rr=` in report
  details.
- **`wsub=` on the `StreamRealtimeDuplex` report**, counting TLS poll-direction
  substitutions.
- **Wake-detection oracle script** (`host/tools/oww-oracle/capture_scores.py`).
  Drives upstream openWakeWord's Python API with the same deterministic seed
  noise the Rust stream uses, producing per-step scores that the Rust regression
  tests pin against. A model change invalidates the pins; re-running the script
  regenerates them.

### Changed

- **Motion scripts name a pose instead of one of two postures.** A base step
  now carries `"pose": "<name>"` — a name the daemon resolves against its
  deployed pose library — and may carry `"move_ms"`, the pace of the move to
  it; the old `"posture": "up" | "stow"` spelling is gone and a body carrying
  it is refused. This side's `motion-proto` pin moves to the brenn-reachy
  revision that publishes that contract, so a daemon older than it cannot
  execute what this scripter now sends.
- **Playback events now distinguish "written to the device" from "heard to the
  end".** The old `playback_finished` fired when the last frame was handed to
  the device, up to a second before the audio played out. A new
  `playback_written` marks that handoff; `playback_finished` now fires at the
  pacer's estimate of the audible end. A third event, `playback_audible`, names
  the job the speaker is playing right now (or silence), and the listener's
  barge-in floor follows it directly instead of a timer. Barge-in at the tail of
  a reply now cuts the audio instead of being rejected as stale, and the ledger
  settles each job at the right instant.
- **Speech heard over the pod's own playback is gated on STT confidence.** A
  reply's residual leaking back through the mic used to reach the brain under
  `[wake] policy = "bypass"`, because the echo never tripped the barge guard
  and had no wake to gate it. The listener now latches whether any chunk of an
  utterance overlapped the pod's playback floor, and the confidence gate
  declines such a carve the same way it declines a hallucinated wake or barge.
  New event: `echo_declined`; new counter: `echo_declined` in `stage_health`.
- **The brenn bridge speaks wire version 4**, following the bus server's move
  there. A pod built before this bump cannot attach to a v4 server at all.
- **Losing the bus no longer stops a voice pod.** A bridge that ends terminally
  — a version skew, a protocol error — is reported loudly and the pipeline keeps
  waking, endpointing and transcribing; each turn then speaks the configured
  `failure_message` instead of the whole daemon exiting. The `brenn_bridge_exit`
  line's `fatal` field is now `unexpected`, which is what it always measured.
- **`speech-surface` no longer exports `DriverTokens`.** `BridgeDriver::new` takes
  the driver's teardown token directly; the one-field wrapper is gone.
- **Rust edition 2024 pinned across both workspaces**, inherited per crate from
  `[workspace.package]`. Resolves a drift where the editor format hook ran
  `rustfmt --edition 2024` against edition-2021 manifests. `scripts/check-edition.sh`
  guards against recurrence.
- **TLS-PSK connect timeout cut from 10s to 3s.** These devices are LAN-only; a longer
  wait only delays an inevitable failure.
- `barge_command_absent` and `echo_declined` now appear on the console alongside
  `wake_command_absent`, so all three confidence-gate declines are visible during
  a bench session.
- `FeedPermit` and `reserve_marker` on the listener are crate-private again;
  they were only used internally by the reliable-marker path inside `FeedSender`.
- The floor-close generation timer in the playback fan-out is removed, replaced
  by the pacer's own `Audible` event.

### Fixed

- **`<listen/>` now actually listens.** The window after a reply was dated
  from the last audio the pod had sent, so after a reply it had usually
  expired before it opened and the follow-up was ignored. It is now dated
  from the pod's own capture position, which the status frame keeps current
  while nothing is streaming; the playback mute tail and a window restored
  after a declined candidate are dated the same way. A window nobody speaks
  into ends one to two seconds after its deadline, on the pod's clock, with
  `listen_expired`. A window asked for before a new connection has reported
  its position opens when it does. `listen_opened` gains `at_sample`.
- **Automatic volume now reads the room on the ESP pod.** The "playing" mark
  on the pod's room-level blocks followed the DAC's mute state, which is only
  released by an end-of-audio mark. A stream that ended without one left
  every block marked from then on, so automatic volume had no room reading
  and every reply logged `auto: "no_level"`. The mark now follows audio
  reaching the speaker and clears 200 ms after it stops. The voice host also
  sets aside its own playback from the room by its own record, writes a
  `playing_flag_disputed` line (shown loud on the console) when the pod's
  mark and that record disagree, and every `level` object — on
  `wake_detected`, `reply_gain`, the utterance line and what the brain
  receives — now carries `room`: how many readings were on hand and how many
  were set aside because the pod was speaking, someone was addressing it, or
  the microphone was dead. A new hardware self-test,
  `LevelBlocksFlagStreamReset`, guards the mark across a reconnect with audio
  still queued. Flash the pod with the host update. Now that the room is
  actually read, a `[volume.auto] ambient_ref_dbfs` set well above the room's
  typical level turns replies down, as far as the swing floor or the
  `[volume] min_db` floor, whichever is reached first: set it to a typical
  room level, as the migration note under **Changed** describes, before
  judging this fix by ear.
- **After the wake word, the robot no longer holds its head up while ignoring
  what you say.** Sometimes the head stayed in its listening pose for a long
  time after a wake while every repeat of the command was dropped — when the
  wake phrase itself wasn't cleanly picked out of the audio, or when the
  command was heard but judged too unclear to act on. Now a wake stays open
  for commands for `[wake] command_wait_ms` after the wake word; if a command
  is judged too unclear you can simply repeat it within that time, without
  saying the wake word again; and the head comes down as soon as that window
  closes. Log changes: new `wake_restored` and `wake_expired` lines;
  `wake_held` is logged on every wake and gains `speaking`; `arm_expired`
  gains `candidate_minted` and `cause`. No configuration changes.

- **Under `[barge] mode = "wake"`, a wake word spoken over a reply no longer
  raises the head twice.** The second raise could retarget a head already
  moving.
- **A command is answered once.** Speech that resumed within a second of a
  dispatched command was re-transcribed and answered a second time. Dispatch
  now ends the utterance, and speech after it needs the wake word again. A
  resumed carve that raced the dispatch is logged as `carve_after_dispatch`.
- **The head no longer drops while you are still talking after the wake
  word.** The wall-clock release of a held wake (`wake_hold_released`) waits
  while speech is being heard, and fires at its own time again if a gap in the
  audio stream drops that speech.
- **An `[endpointer] onset_thresh` above 0.60 (or `onset_chunks` above 8) is
  refused at startup** with a message naming the key, instead of crashing on
  the first pod connection.
- **The wake-word detector no longer false-fires on silence after a stream
  reset.** Certain wake heads scored near 1.0 on the first chunk of every
  segment because the embedding window cold-started from zeros, an
  out-of-distribution input upstream openWakeWord never produces. The embedding
  history is now seeded from the models with low-amplitude noise embeddings
  (matching upstream's own initialisation), and the warm-up suppresses five
  predictions instead of waiting for sixteen real embeddings. Wake detection
  begins 0.48 s into each segment instead of 1.28 s, so every chunk that
  touches the phrase the device VAD opened on is now scored. A compile-time
  assertion ties the readiness window to the device preroll so the relationship
  cannot regress silently.
- **Doorbell rings arriving during WiFi backoff are no longer dropped**, so the wake one
  asks for actually happens.
- **First HIL attempt after a cold boot could fail (AC9).** The host now retries Identify,
  and a re-send write error no longer aborts the whole wait.
- The audio reference now warns that the reSpeaker Flex's USB audio image
  can play nothing, while accepting every frame without error, when the
  board's XIAO ESP32-S3 module runs firmware that configures the XVF3800.
  Erasing the module's firmware restored playback. Which firmware does this is
  not yet known; a tracker entry follows it up.

## [0.1.0] - 2026-07-23

First tagged release. This inaugural entry records the release milestone and the
notable recent work rather than reconstructing the full pre-release bring-up
history; earlier platform work is summarized at a high level.

### Added

- **TLS-PSK mutual authentication on all pod links.** TLS 1.2 ECDHE-PSK with a
  per-pod key; the PSK identity is the pod id, bound to the `Hello` frame. No
  plaintext fallback in production — the pod streamer always connects over
  TLS-PSK, and the host runs every accepted socket through the handshake before
  reading a frame.
- **HIL self-tests over TLS-PSK with a volatile session key.** The hardware-in-the-loop
  network fixtures run over the production TLS-PSK path instead of plaintext TCP.
  The test key lives in a RAM-only session store, zeroized at session end, so a
  HIL run performs zero NVS writes and never clobbers a production pod's key.
- **Pod provisioning CLI (`podctl`)** over USB-serial: writes WiFi credentials, the
  audio receiver address, and a generated 32-byte audio PSK into device NVS, and in
  the same step records the matching key in the host-side PSK secrets file.
- **Host-side voice surface (`speech-surface`)**: TLS-PSK audio ingest, wake
  detection, speech-to-text, brain dispatch, and text-to-speech playback, with
  per-pod room mapping.
- **reSpeaker Flex voice-node bring-up**: mic capture and XVF3800
  direction-of-arrival, TLS audio streaming to the homelab, and playback — brought
  up HIL-first, with each hardware discovery baked into a permanent regression
  self-test.

### Notes

- Two internal-RAM heap floors (`HEAP_MIN_EVER_FLOOR`, `RTD_HEAP_LOW_FLOOR`) were
  re-baked to account for the TLS-PSK duplex heap cost. Each new value was
  human-reviewed under the project's hardware bring-up guardrail — an unexpected
  reading gets reviewed before a test is made to pass — rather than adjusted to
  force a green test.
