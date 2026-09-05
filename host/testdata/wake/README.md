# Wake-phrase test fixtures

Both clips are TTS-generated, never a personal voice recording, and a synthetic
rendition is legitimate signal for both consumers: openWakeWord is trained on
synthetic speech, and Silero's VAD classifies speech energy, which espeak's
output carries. No human voice enters the tree.

Each is checked in as opaque audio (the `.wav` is the asset, not the recipe),
but the recipes are recorded for reproducibility.

## `wake-phrase.wav`

A synthetic "Hey Jarvis" rendition, 16 kHz mono S16, ~1.3 s. It scores above the
default 0.5 threshold through the committed `models/oww` graph — the pin
asserted by `speech-pipeline`'s `wake_phrase_fixture_scores_above_threshold`
test.

```
espeak-ng -v en-us -s 150 -w hj_raw.wav "Hey Jarvis"
ffmpeg -y -i hj_raw.wav -ar 16000 -ac 1 -sample_fmt s16 wake-phrase.wav
```

sha256 (`wake-phrase.wav`): `cef5108f4acbfea5654519daba9c5546468222c40a9669e91bc8b4551587b017`

## `command-phrase.wav`

A synthetic "this is a test one two three", 16 kHz mono S16, ~2.1 s. It is not
the wake phrase and arms nothing; it stands for the command a speaker says after
waking the robot. `speech-surface`'s `listener_replay` cases splice it after
`wake-phrase.wav` and a stretch of digital silence to drive the wake-command
hold end to end — the pause the hold exists to cover.

```
espeak-ng -v en-us -s 150 -w cmd_raw.wav "this is a test one two three"
ffmpeg -y -i cmd_raw.wav -ar 16000 -ac 1 -sample_fmt s16 command-phrase.wav
```

sha256 (`command-phrase.wav`): `0402d60f99aff56b2ccbfec4e08279edf0464e4b85e1190d80fa4575cd8b11b1`
