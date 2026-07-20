//! The host endpointer: a hangover state machine over Silero P(speech) that
//! carves utterance boundaries out of the live stream, replacing the device
//! VAD-release-as-utterance-boundary coupling.
//!
//! The machine is pure over `(P(speech), sample_index)` — it emits *spans*
//! ([`EndpointEvent`]) in the absolute sample-index domain, and the listener
//! thread turns a span into a `CarvedUtterance` by slicing the PCM ring. Keeping
//! the FSM free of the ring and the wake state makes it unit-testable against
//! synthetic probability sequences, both directions of Silero failure included.
//!
//! States:
//!
//! ```text
//! Idle --P>=onset for onset_chunks--> Speech(start = onset - preroll_pad)
//! Speech --P<release for soft_hangover--> SoftEndpointed (emit SoftEndpoint)
//! Speech --length >= max_utterance--> SoftEndpointed (emit SoftEndpoint, Capped)
//! SoftEndpointed --P>=onset within continuation_window--> Speech (emit Superseded)
//! SoftEndpointed --continuation_window elapses--> Idle (emit UtteranceClosed)
//! ```
//!
//! The device's `SegmentClosed(VadRelease)` is the authoritative outer boundary,
//! fed via [`Endpointer::on_device_release`]: it forces an endpoint on a
//! Silero-missed release, and carves the wake-armed fallback on a Silero-missed
//! onset. Host endpointing only ever makes us faster; it never loses audio.
//!
//! Alongside the carve-relevant [`EndpointEvent`], the FSM records every
//! structural state transition ([`EndpointTransition`]) into an internal buffer
//! the caller drains with [`Endpointer::drain_transitions`]. This is pure
//! observability data — no clock, no I/O — so the FSM stays replay-testable; the
//! listener runtime stamps each drained transition with the pod, epoch, and turns
//! it into an `endpointer_transition` line.

use serde::Serialize;

pub use crate::types::EndpointCause;

/// A boundary decision in the absolute sample-index domain. The listener carves
/// `[start_sample, end_sample)` from the PCM ring to build the utterance PCM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointEvent {
    /// An utterance ended (STT may start). Spans `[start_sample, end_sample)`.
    SoftEndpoint {
        start_sample: u64,
        end_sample: u64,
        cause: EndpointCause,
    },
    /// Speech resumed inside the continuation window: abort the in-flight STT; the
    /// same utterance keeps accumulating and a later `SoftEndpoint` carries the
    /// whole concatenation.
    Superseded,
    /// The continuation window elapsed with no resume — the utterance is final.
    UtteranceClosed,
}

/// The endpointer FSM's structural states, surfaced with every transition for
/// timing observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointState {
    Idle,
    Speech,
    SoftEndpointed,
}

/// What triggered an endpointer state transition — the observability twin of the
/// carve-relevant [`EndpointEvent`]. Every FSM edge maps to exactly one cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionCause {
    /// `Idle → Speech`: `onset_chunks` consecutive onset chunks crossed.
    Onset,
    /// `Speech → SoftEndpointed`: `soft_hangover_chunks` consecutive release chunks
    /// (a natural pause that may still resume).
    SoftEndpoint,
    /// `Speech → Idle`: the `max_utterance` length cap forced a hard boundary.
    Capped,
    /// `SoftEndpointed → Speech`: speech resumed inside the continuation window.
    Continuation,
    /// `SoftEndpointed → Idle`: the continuation window elapsed with no resume.
    Closed,
    /// `Speech → Idle`: the device VAD released while speech was still open (a
    /// Silero-missed release).
    DeviceRelease,
    /// `SoftEndpointed → Idle`: the device VAD released inside the continuation
    /// window, finalizing the utterance.
    DeviceReleaseClosed,
    /// `Idle → Idle`: the device VAD released with a wake armed but Silero never
    /// onset — the missed-onset fallback carve.
    MissedOnsetCarve,
    /// Any non-idle state `→ Idle`: a discontinuity/reconnect dropped the
    /// in-progress utterance.
    Reset,
}

/// One FSM state transition, surfaced purely for observability (no PCM payload).
/// `sample_offset` is the absolute sample index at which the transition occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EndpointTransition {
    pub from: EndpointState,
    pub to: EndpointState,
    pub cause: TransitionCause,
    pub sample_offset: u64,
}

/// Endpointer timing/threshold knobs. Chunk-count fields are in Silero chunks
/// (512 samples / 32 ms); sample fields are absolute-index deltas. Design-time
/// defaults ([`Default`]) are tuned on framelog replay.
#[derive(Debug, Clone, Copy)]
pub struct EndpointerConfig {
    /// P(speech) at/above which a chunk counts toward onset (and resume).
    pub onset_thresh: f32,
    /// P(speech) below which a chunk counts toward release.
    pub release_thresh: f32,
    /// Consecutive onset chunks required to enter `Speech`.
    pub onset_chunks: u32,
    /// Consecutive release chunks required to soft-endpoint.
    pub soft_hangover_chunks: u32,
    /// Chunks after a soft endpoint during which a resume is a continuation of the
    /// same utterance rather than a new one.
    pub continuation_chunks: u32,
    /// Samples of lead prepended to an utterance start (`start = onset - this`),
    /// so the first phoneme isn't clipped.
    pub preroll_pad_samples: u64,
    /// Maximum utterance length before a forced `Capped` endpoint.
    pub max_utterance_samples: u64,
}

/// Silero chunk length in samples — the quantum the chunk-count knobs are in.
const CHUNK_SAMPLES: u64 = 512;

impl Default for EndpointerConfig {
    fn default() -> EndpointerConfig {
        // Tuned defaults (framelog replay rig): onset 0.5, release 0.35, onset
        // ~96 ms, soft hangover 250 ms, continuation 1000 ms, preroll 500 ms,
        // max utterance 60 s (the shipped segment cap). ms→chunk at 32 ms/chunk.
        EndpointerConfig {
            onset_thresh: 0.5,
            release_thresh: 0.35,
            onset_chunks: 3,
            soft_hangover_chunks: 8,
            continuation_chunks: 31,
            preroll_pad_samples: 8_000,
            max_utterance_samples: 60 * 16_000,
        }
    }
}

/// Internal FSM state. `start`/`speech_end` are absolute sample indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// No utterance in progress; counting consecutive onset chunks.
    Idle { onset_run: u32, onset_start: u64 },
    /// An utterance is open; counting consecutive release chunks.
    Speech {
        start: u64,
        speech_end: u64,
        release_run: u32,
    },
    /// Soft-endpointed; counting the continuation window down.
    SoftEndpointed { start: u64, cont_run: u32 },
}

impl State {
    /// The public [`EndpointState`] label for this internal state.
    fn label(&self) -> EndpointState {
        match self {
            State::Idle { .. } => EndpointState::Idle,
            State::Speech { .. } => EndpointState::Speech,
            State::SoftEndpointed { .. } => EndpointState::SoftEndpointed,
        }
    }
}

/// One pod's endpointer. Drive it with [`push`](Endpointer::push) per Silero
/// chunk, [`on_device_release`](Endpointer::on_device_release) at segment close,
/// and [`reset`](Endpointer::reset) on a discontinuity.
pub struct Endpointer {
    config: EndpointerConfig,
    state: State,
    /// Structural transitions recorded since the last drain. Pure observability
    /// data — no clock, no I/O — so the FSM stays replay-testable; the listener
    /// runtime drains and stamps these into `endpointer_transition` events.
    transitions: Vec<EndpointTransition>,
}

impl Endpointer {
    /// A fresh endpointer in `Idle`.
    pub fn new(config: EndpointerConfig) -> Endpointer {
        Endpointer {
            config,
            state: State::Idle {
                onset_run: 0,
                onset_start: 0,
            },
            transitions: Vec::new(),
        }
    }

    /// Reset to `Idle`, dropping any in-progress utterance. Called on a pod
    /// reconnect or sample-index discontinuity; `at_sample` is the absolute index
    /// the stream re-anchors to. A reset out of a non-idle state records a `Reset`
    /// transition (a reset already in `Idle` is a no-op, so nothing is recorded).
    pub fn reset(&mut self, at_sample: u64) {
        let from = self.state.label();
        self.state = State::Idle {
            onset_run: 0,
            onset_start: 0,
        };
        if from != EndpointState::Idle {
            self.record(from, EndpointState::Idle, TransitionCause::Reset, at_sample);
        }
    }

    /// Record a structural transition for the caller to drain.
    fn record(
        &mut self,
        from: EndpointState,
        to: EndpointState,
        cause: TransitionCause,
        sample_offset: u64,
    ) {
        self.transitions.push(EndpointTransition {
            from,
            to,
            cause,
            sample_offset,
        });
    }

    /// Drain the transitions recorded since the last call. The listener runtime
    /// pulls these after each `push`/`on_device_release`/`reset` and turns each
    /// into an `endpointer_transition` observability event.
    pub fn drain_transitions(&mut self) -> Vec<EndpointTransition> {
        std::mem::take(&mut self.transitions)
    }

    /// Whether an utterance (open or soft-endpointed-awaiting-continuation) is
    /// currently in progress. The listener consults this to decide whether a
    /// device release is a missed-release fallback vs. a missed-onset carve.
    pub fn utterance_in_progress(&self) -> bool {
        !matches!(self.state, State::Idle { .. })
    }

    /// Feed one Silero chunk. `p` is P(speech); `chunk_end_sample` is the absolute
    /// index one past the chunk's last sample. Returns a boundary event if this
    /// chunk crossed one.
    pub fn push(&mut self, p: f32, chunk_end_sample: u64) -> Option<EndpointEvent> {
        let chunk_start = chunk_end_sample.saturating_sub(CHUNK_SAMPLES);
        match self.state {
            State::Idle {
                mut onset_run,
                onset_start,
            } => {
                if p >= self.config.onset_thresh {
                    // First onset chunk pins the onset anchor.
                    let anchor = if onset_run == 0 {
                        chunk_start
                    } else {
                        onset_start
                    };
                    onset_run += 1;
                    if onset_run >= self.config.onset_chunks {
                        let start = anchor.saturating_sub(self.config.preroll_pad_samples);
                        self.state = State::Speech {
                            start,
                            speech_end: chunk_end_sample,
                            release_run: 0,
                        };
                        self.record(
                            EndpointState::Idle,
                            EndpointState::Speech,
                            TransitionCause::Onset,
                            chunk_end_sample,
                        );
                    } else {
                        self.state = State::Idle {
                            onset_run,
                            onset_start: anchor,
                        };
                    }
                } else {
                    self.state = State::Idle {
                        onset_run: 0,
                        onset_start: 0,
                    };
                }
                None
            }
            State::Speech {
                start,
                mut speech_end,
                mut release_run,
            } => {
                // Length cap forces an endpoint regardless of the probability.
                // Unlike a natural soft endpoint (a pause that might resume the
                // same sentence), the cap fires while speech is ongoing, so it is
                // a hard boundary: return to `Idle`, and sustained speech re-onsets
                // as a fresh utterance. The new utterance's preroll backfills the
                // onset-detection gap from the ring, so no audio is lost, and the
                // length accounting resets — no immediate re-cap churn.
                if chunk_end_sample.saturating_sub(start) >= self.config.max_utterance_samples {
                    self.state = State::Idle {
                        onset_run: 0,
                        onset_start: 0,
                    };
                    self.record(
                        EndpointState::Speech,
                        EndpointState::Idle,
                        TransitionCause::Capped,
                        chunk_end_sample,
                    );
                    return Some(EndpointEvent::SoftEndpoint {
                        start_sample: start,
                        end_sample: chunk_end_sample,
                        cause: EndpointCause::Capped,
                    });
                }
                if p < self.config.release_thresh {
                    release_run += 1;
                    if release_run >= self.config.soft_hangover_chunks {
                        self.state = State::SoftEndpointed { start, cont_run: 0 };
                        self.record(
                            EndpointState::Speech,
                            EndpointState::SoftEndpointed,
                            TransitionCause::SoftEndpoint,
                            chunk_end_sample,
                        );
                        return Some(EndpointEvent::SoftEndpoint {
                            start_sample: start,
                            end_sample: speech_end,
                            cause: EndpointCause::SoftEndpoint,
                        });
                    }
                } else {
                    release_run = 0;
                    speech_end = chunk_end_sample;
                }
                self.state = State::Speech {
                    start,
                    speech_end,
                    release_run,
                };
                None
            }
            State::SoftEndpointed {
                start,
                mut cont_run,
            } => {
                if p >= self.config.onset_thresh {
                    // Resume: same utterance continues accumulating from `start`.
                    self.state = State::Speech {
                        start,
                        speech_end: chunk_end_sample,
                        release_run: 0,
                    };
                    self.record(
                        EndpointState::SoftEndpointed,
                        EndpointState::Speech,
                        TransitionCause::Continuation,
                        chunk_end_sample,
                    );
                    Some(EndpointEvent::Superseded)
                } else {
                    cont_run += 1;
                    if cont_run >= self.config.continuation_chunks {
                        self.state = State::Idle {
                            onset_run: 0,
                            onset_start: 0,
                        };
                        self.record(
                            EndpointState::SoftEndpointed,
                            EndpointState::Idle,
                            TransitionCause::Closed,
                            chunk_end_sample,
                        );
                        Some(EndpointEvent::UtteranceClosed)
                    } else {
                        self.state = State::SoftEndpointed { start, cont_run };
                        None
                    }
                }
            }
        }
    }

    /// The device closed the transport segment (VAD release) at `close_sample` —
    /// the authoritative outer boundary. `armed_wake_end` is the absolute sample
    /// index of an unconsumed armed wake, if any (the listener's state, passed in
    /// so the FSM stays pure). Covers both Silero failure directions:
    ///
    /// - **Missed release** (still in `Speech`): force a `SoftEndpoint` on the
    ///   in-progress utterance, spanning to `close_sample`.
    /// - **Missed onset** (`Idle` with a wake armed): carve
    ///   `[wake_end - preroll_pad, close_sample]` as a fallback utterance, so the
    ///   command reaches STT as today's batch path would have.
    ///
    /// Returns to `Idle` either way. A soft-endpointed utterance awaiting
    /// continuation is finalized (`UtteranceClosed`); an idle stream with no armed
    /// wake yields nothing.
    pub fn on_device_release(
        &mut self,
        close_sample: u64,
        armed_wake_end: Option<u64>,
    ) -> Option<EndpointEvent> {
        let event = match self.state {
            State::Speech { start, .. } => {
                self.record(
                    EndpointState::Speech,
                    EndpointState::Idle,
                    TransitionCause::DeviceRelease,
                    close_sample,
                );
                Some(EndpointEvent::SoftEndpoint {
                    start_sample: start,
                    end_sample: close_sample,
                    cause: EndpointCause::DeviceVadRelease,
                })
            }
            State::SoftEndpointed { .. } => {
                self.record(
                    EndpointState::SoftEndpointed,
                    EndpointState::Idle,
                    TransitionCause::DeviceReleaseClosed,
                    close_sample,
                );
                Some(EndpointEvent::UtteranceClosed)
            }
            State::Idle { .. } => armed_wake_end.and_then(|wake_end| {
                // Clamp the fallback start so a stale arm on a very long segment
                // (music holding the device VAD open for minutes) can't carve an
                // hours-long, mostly-silence span — the length cap bounds this
                // path exactly as it bounds the `Speech` path. Drop an
                // empty/inverted span (a stale arm surviving a ring reset).
                let start = wake_end
                    .saturating_sub(self.config.preroll_pad_samples)
                    .max(close_sample.saturating_sub(self.config.max_utterance_samples));
                (start < close_sample).then(|| {
                    self.record(
                        EndpointState::Idle,
                        EndpointState::Idle,
                        TransitionCause::MissedOnsetCarve,
                        close_sample,
                    );
                    EndpointEvent::SoftEndpoint {
                        start_sample: start,
                        end_sample: close_sample,
                        cause: EndpointCause::DeviceVadRelease,
                    }
                })
            }),
        };
        self.state = State::Idle {
            onset_run: 0,
            onset_start: 0,
        };
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config with short windows so tests stay compact: onset 2 chunks, soft
    /// hangover 3, continuation 4, preroll 100 samples, cap 20 chunks.
    fn test_config() -> EndpointerConfig {
        EndpointerConfig {
            onset_thresh: 0.5,
            release_thresh: 0.35,
            onset_chunks: 2,
            soft_hangover_chunks: 3,
            continuation_chunks: 4,
            preroll_pad_samples: 100,
            max_utterance_samples: 20 * CHUNK_SAMPLES,
        }
    }

    /// Feed a probability sequence starting at chunk index 0, returning every
    /// event emitted alongside the chunk that produced it.
    fn feed(ep: &mut Endpointer, probs: &[f32]) -> Vec<(usize, EndpointEvent)> {
        let mut out = Vec::new();
        for (i, &p) in probs.iter().enumerate() {
            let end = (i as u64 + 1) * CHUNK_SAMPLES;
            if let Some(ev) = ep.push(p, end) {
                out.push((i, ev));
            }
        }
        out
    }

    /// Onset requires `onset_chunks` consecutive high chunks; a low chunk resets
    /// the run. The utterance start backs off by `preroll_pad`.
    #[test]
    fn onset_then_soft_endpoint() {
        let mut ep = Endpointer::new(test_config());
        // chunk 0 high, chunk 1 low resets, chunks 2-3 high → Speech at chunk 3,
        // anchored at chunk 2's start (sample 1024), start = 1024 - 100 = 924.
        // chunks 4-6 low → soft endpoint at chunk 6, end = chunk 3's end (2048).
        let events = feed(&mut ep, &[0.9, 0.1, 0.9, 0.9, 0.1, 0.1, 0.1]);
        assert_eq!(events.len(), 1, "exactly one soft endpoint: {events:?}");
        let (i, ev) = events[0];
        assert_eq!(i, 6, "soft endpoint fires on the third release chunk");
        assert_eq!(
            ev,
            EndpointEvent::SoftEndpoint {
                start_sample: 2 * CHUNK_SAMPLES - 100,
                end_sample: 4 * CHUNK_SAMPLES,
                cause: EndpointCause::SoftEndpoint,
            }
        );
        // A natural soft endpoint stays in progress, awaiting continuation.
        assert!(ep.utterance_in_progress());
    }

    /// A release run interrupted by a high chunk does not endpoint; `speech_end`
    /// advances to the latest speech chunk.
    #[test]
    fn release_run_resets_on_speech() {
        let mut ep = Endpointer::new(test_config());
        // Speech by chunk 1; low,low,high,low,low,low → the first low run breaks,
        // endpoint only after the final three lows.
        let events = feed(&mut ep, &[0.9, 0.9, 0.1, 0.1, 0.9, 0.1, 0.1, 0.1]);
        assert_eq!(events.len(), 1);
        let (i, ev) = events[0];
        assert_eq!(i, 7);
        if let EndpointEvent::SoftEndpoint { end_sample, .. } = ev {
            // Latest speech chunk was index 4 → end = 5 * 512.
            assert_eq!(end_sample, 5 * CHUNK_SAMPLES);
        } else {
            panic!("expected soft endpoint, got {ev:?}");
        }
    }

    /// Speech longer than `max_utterance` forces a `Capped` endpoint that is a hard
    /// boundary: it spans exactly `[onset, onset + max_utterance)`, returns the FSM
    /// to `Idle`, and sustained speech re-onsets as a fresh utterance with reset
    /// length accounting — so caps recur one `max_utterance` apart, never churning
    /// per-chunk.
    #[test]
    fn length_cap_forces_capped_endpoint() {
        let mut ep = Endpointer::new(test_config());
        let cap = 20 * CHUNK_SAMPLES; // test_config max_utterance
        let mut caps = Vec::new();
        // 45 chunks of sustained, never-releasing speech at contiguous indices.
        for i in 0..45u64 {
            let end = (i + 1) * CHUNK_SAMPLES;
            if let Some(ev) = ep.push(0.9, end) {
                caps.push(ev);
                assert!(
                    !ep.utterance_in_progress(),
                    "a cap at {end} returns the FSM to Idle"
                );
            }
        }
        assert_eq!(caps.len(), 2, "exactly one re-onset, no churn: {caps:?}");
        // Onset at chunk 0 (start 0); first cap exactly max_utterance later.
        assert_eq!(
            caps[0],
            EndpointEvent::SoftEndpoint {
                start_sample: 0,
                end_sample: cap,
                cause: EndpointCause::Capped,
            }
        );
        // The re-onset's cap is anchored one preroll before the fresh onset at
        // `cap`, proving length accounting reset after the first cap.
        assert_eq!(
            caps[1],
            EndpointEvent::SoftEndpoint {
                start_sample: cap - 100,
                end_sample: 2 * cap,
                cause: EndpointCause::Capped,
            }
        );
    }

    /// A resume inside the continuation window supersedes and keeps the same
    /// utterance start; the eventual endpoint spans from the original start.
    #[test]
    fn continuation_supersedes_and_keeps_start() {
        let mut ep = Endpointer::new(test_config());
        // Speech, soft endpoint, resume within window, speech, soft endpoint.
        let probs = [
            0.9, 0.9, // onset → Speech, start anchored at chunk 0 (sample 0-100→0)
            0.1, 0.1, 0.1, // soft endpoint at chunk 4
            0.9, // resume within continuation window → Superseded
            0.9, 0.1, 0.1, 0.1, // soft endpoint again
        ];
        let events = feed(&mut ep, &probs);
        let kinds: Vec<_> = events.iter().map(|(_, e)| *e).collect();
        assert!(
            matches!(
                kinds[0],
                EndpointEvent::SoftEndpoint {
                    start_sample: 0,
                    ..
                }
            ),
            "first endpoint from start 0: {:?}",
            kinds[0]
        );
        assert_eq!(kinds[1], EndpointEvent::Superseded);
        assert!(
            matches!(
                kinds[2],
                EndpointEvent::SoftEndpoint {
                    start_sample: 0,
                    ..
                }
            ),
            "second endpoint still from the original start: {:?}",
            kinds[2]
        );
    }

    /// The continuation window elapsing with no resume closes the utterance.
    #[test]
    fn continuation_window_elapses_to_closed() {
        let mut ep = Endpointer::new(test_config());
        let probs = [
            0.9, 0.9, // Speech
            0.1, 0.1, 0.1, // soft endpoint
            0.1, 0.1, 0.1, 0.1, // continuation window (4 chunks) elapses
        ];
        let events = feed(&mut ep, &probs);
        let kinds: Vec<_> = events.iter().map(|(_, e)| *e).collect();
        assert!(matches!(kinds[0], EndpointEvent::SoftEndpoint { .. }));
        assert_eq!(kinds[1], EndpointEvent::UtteranceClosed);
        assert!(!ep.utterance_in_progress());
    }

    /// Device release while in `Speech` forces a `DeviceVadRelease` endpoint to
    /// the segment close (Silero missed the release).
    #[test]
    fn device_release_forces_endpoint_when_speech_open() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.9, 0.9, 0.9]); // open Speech
        let ev = ep.on_device_release(100_000, None).unwrap();
        assert_eq!(
            ev,
            EndpointEvent::SoftEndpoint {
                start_sample: 0,
                end_sample: 100_000,
                cause: EndpointCause::DeviceVadRelease,
            }
        );
        assert!(!ep.utterance_in_progress());
    }

    /// Device release while `Idle` with an armed wake carves the missed-onset
    /// fallback (Silero never saw the onset).
    #[test]
    fn device_release_carves_missed_onset_with_armed_wake() {
        let mut ep = Endpointer::new(test_config());
        // Idle throughout — Silero missed the onset. Close is within the length
        // cap of the wake, so the carve spans exactly `[wake_end − preroll, close]`.
        feed(&mut ep, &[0.1, 0.1]);
        let ev = ep.on_device_release(35_000, Some(30_000)).unwrap();
        assert_eq!(
            ev,
            EndpointEvent::SoftEndpoint {
                start_sample: 30_000 - 100,
                end_sample: 35_000,
                cause: EndpointCause::DeviceVadRelease,
            }
        );
    }

    /// A missed-onset fallback whose `[wake_end − preroll, close]` span exceeds the
    /// length cap (a stale arm on a long noisy segment) clamps its start to
    /// `close − max_utterance`, so the carve can never allocate an unbounded span.
    #[test]
    fn device_release_missed_onset_clamps_to_max_utterance() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.1, 0.1]);
        // Wake armed long before close: 50_000 − 100 preroll = 49_900, but the cap
        // (20 chunks = 10_240) bounds the start to 100_000 − 10_240 = 89_760.
        let ev = ep.on_device_release(100_000, Some(50_000)).unwrap();
        assert_eq!(
            ev,
            EndpointEvent::SoftEndpoint {
                start_sample: 100_000 - 20 * CHUNK_SAMPLES,
                end_sample: 100_000,
                cause: EndpointCause::DeviceVadRelease,
            }
        );
    }

    /// A stale arm whose `wake_end` sits at or past the close (e.g. surviving a
    /// backward ring reset) yields no event rather than an inverted span.
    #[test]
    fn device_release_missed_onset_drops_inverted_span() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.1, 0.1]);
        assert_eq!(ep.on_device_release(1_000, Some(5_000)), None);
    }

    /// Device release while soft-endpointed-awaiting-continuation finalizes the
    /// utterance (`UtteranceClosed`) and returns to `Idle` — the VadRelease landed
    /// inside the continuation window.
    #[test]
    fn device_release_closes_soft_endpointed_utterance() {
        let mut ep = Endpointer::new(test_config());
        // Onset → Speech → soft endpoint, then a device release inside the window.
        feed(&mut ep, &[0.9, 0.9, 0.1, 0.1, 0.1]);
        assert!(
            ep.utterance_in_progress(),
            "soft-endpointed, awaiting cont."
        );
        let ev = ep.on_device_release(9_999, None).unwrap();
        assert_eq!(ev, EndpointEvent::UtteranceClosed);
        assert!(!ep.utterance_in_progress(), "returns to Idle");
    }

    /// Device release while `Idle` with no armed wake yields nothing.
    #[test]
    fn device_release_idle_no_wake_is_silent() {
        let mut ep = Endpointer::new(test_config());
        assert_eq!(ep.on_device_release(50_000, None), None);
    }

    /// Reset drops an in-progress utterance back to `Idle`.
    #[test]
    fn reset_drops_in_progress_utterance() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.9, 0.9]);
        assert!(ep.utterance_in_progress());
        ep.reset(1_234);
        assert!(!ep.utterance_in_progress());
        // A subsequent onset opens a fresh utterance, no bleed.
        let events = feed(&mut ep, &[0.9, 0.9, 0.1, 0.1, 0.1]);
        assert_eq!(events.len(), 1);
    }

    /// Every FSM edge records a drainable transition: onset (`Idle → Speech`) and
    /// the natural soft endpoint (`Speech → SoftEndpointed`), each stamped with the
    /// chunk-end sample at which it fired. A second drain is empty (the buffer is
    /// consumed).
    #[test]
    fn transitions_cover_onset_and_soft_endpoint() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.9, 0.9, 0.1, 0.1, 0.1]);
        let t = ep.drain_transitions();
        assert_eq!(t.len(), 2, "onset + soft endpoint: {t:?}");
        assert_eq!(
            t[0],
            EndpointTransition {
                from: EndpointState::Idle,
                to: EndpointState::Speech,
                cause: TransitionCause::Onset,
                sample_offset: 2 * CHUNK_SAMPLES,
            }
        );
        assert_eq!(
            t[1],
            EndpointTransition {
                from: EndpointState::Speech,
                to: EndpointState::SoftEndpointed,
                cause: TransitionCause::SoftEndpoint,
                sample_offset: 5 * CHUNK_SAMPLES,
            }
        );
        assert!(
            ep.drain_transitions().is_empty(),
            "the buffer is consumed by the drain"
        );
    }

    /// The length cap records a `Capped` hard boundary (`Speech → Idle`), distinct
    /// from the natural `SoftEndpoint` cause.
    #[test]
    fn transitions_cover_cap_as_hard_boundary() {
        let mut ep = Endpointer::new(test_config());
        // 22 chunks of sustained speech: onset, then a cap at 20 chunks.
        for i in 0..22u64 {
            ep.push(0.9, (i + 1) * CHUNK_SAMPLES);
        }
        let t = ep.drain_transitions();
        assert!(
            t.iter().any(|x| x.cause == TransitionCause::Capped
                && x.from == EndpointState::Speech
                && x.to == EndpointState::Idle),
            "a cap is a Speech→Idle Capped transition: {t:?}"
        );
    }

    /// A resume records `Continuation` (`SoftEndpointed → Speech`) and a later
    /// window elapse records `Closed` (`SoftEndpointed → Idle`).
    #[test]
    fn transitions_cover_continuation_and_close() {
        let mut ep = Endpointer::new(test_config());
        feed(
            &mut ep,
            &[
                0.9, 0.9, // onset
                0.1, 0.1, 0.1, // soft endpoint
                0.9, // resume (continuation)
                0.9, 0.1, 0.1, 0.1, // soft endpoint again
                0.1, 0.1, 0.1, 0.1, // continuation window elapses → close
            ],
        );
        let causes: Vec<_> = ep.drain_transitions().iter().map(|t| t.cause).collect();
        assert!(
            causes.contains(&TransitionCause::Continuation),
            "a resume is a Continuation: {causes:?}"
        );
        assert!(
            causes.contains(&TransitionCause::Closed),
            "the window elapse is a Closed: {causes:?}"
        );
    }

    /// The device-release directions each record a distinct transition: a missed
    /// release (`Speech → Idle`, `DeviceRelease`), a release inside the window
    /// (`SoftEndpointed → Idle`, `DeviceReleaseClosed`), a missed-onset carve
    /// (`Idle → Idle`, `MissedOnsetCarve`), and an idle release with no armed wake
    /// records nothing.
    #[test]
    fn transitions_cover_device_release_paths() {
        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.9, 0.9, 0.9]);
        ep.drain_transitions(); // discard the onset
        ep.on_device_release(100_000, None);
        let t = ep.drain_transitions();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].cause, TransitionCause::DeviceRelease);
        assert_eq!(t[0].sample_offset, 100_000);

        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.1, 0.1]);
        ep.on_device_release(35_000, Some(30_000));
        let t = ep.drain_transitions();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].cause, TransitionCause::MissedOnsetCarve);
        assert_eq!(
            (t[0].from, t[0].to),
            (EndpointState::Idle, EndpointState::Idle)
        );

        let mut ep = Endpointer::new(test_config());
        feed(&mut ep, &[0.9, 0.9, 0.1, 0.1, 0.1]);
        ep.drain_transitions();
        ep.on_device_release(9_999, None);
        let t = ep.drain_transitions();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].cause, TransitionCause::DeviceReleaseClosed);

        let mut ep = Endpointer::new(test_config());
        ep.on_device_release(50_000, None);
        assert!(
            ep.drain_transitions().is_empty(),
            "an idle release with no armed wake records nothing"
        );
    }

    /// A reset out of a non-idle state records a `Reset` at the re-anchor sample;
    /// a reset already in `Idle` records nothing.
    #[test]
    fn reset_records_transition_only_from_nonidle() {
        let mut ep = Endpointer::new(test_config());
        ep.reset(500);
        assert!(
            ep.drain_transitions().is_empty(),
            "an idle reset is a no-op"
        );
        feed(&mut ep, &[0.9, 0.9]);
        ep.drain_transitions(); // discard the onset
        ep.reset(7_777);
        let t = ep.drain_transitions();
        assert_eq!(t.len(), 1);
        assert_eq!(
            t[0],
            EndpointTransition {
                from: EndpointState::Speech,
                to: EndpointState::Idle,
                cause: TransitionCause::Reset,
                sample_offset: 7_777,
            }
        );
    }
}
