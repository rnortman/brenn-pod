# Wake-phrase test fixture

`wake-phrase.wav` — a synthetic "Hey Jarvis" rendition, 16 kHz mono S16, ~1.3 s.
TTS-generated (not a personal voice recording): openWakeWord is trained on
synthetic speech, so a TTS rendition is legitimate trigger signal. It scores
above the default 0.5 threshold through the committed `models/oww` graph — the
pin asserted by `speech-pipeline`'s `wake_phrase_fixture_scores_above_threshold`
test.

It is checked in as opaque audio (the `.wav` is the asset, not the recipe), but
the recipe is recorded for reproducibility:

```
espeak-ng -v en-us -s 150 -w hj_raw.wav "Hey Jarvis"
ffmpeg -y -i hj_raw.wav -ar 16000 -ac 1 -sample_fmt s16 wake-phrase.wav
```

sha256 (`wake-phrase.wav`): `cef5108f4acbfea5654519daba9c5546468222c40a9669e91bc8b4551587b017`
