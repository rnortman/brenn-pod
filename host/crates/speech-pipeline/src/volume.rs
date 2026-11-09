//! Per-pod reply volume state: the operator's hard bounds, each pod's held gain
//! and automatic-gain policy, and the rule that turns them into the gain applied
//! to one reply.
//!
//! Every gain is in decibels relative to the synthesizer's natural level
//! (0 = as produced, negative = quieter); every level is dBFS. The book is shared
//! as `Arc<Mutex<VolumeBook>>` between the playback router (reader at route
//! time), each turn's volume tap (writer) and the bus brain (reader, for the
//! state it reports with every utterance); the lock is never held across an
//! await.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::types::{LevelSource, PodId, UtteranceLevel};

/// Default floor of any applied gain, dB.
pub const DEFAULT_MIN_DB: f32 = -30.0;
/// Default ceiling of any applied gain, dB.
pub const DEFAULT_MAX_DB: f32 = 6.0;
/// Default baseline gain for every pod, dB.
pub const DEFAULT_GAIN_DB: f32 = 0.0;

/// Default: automatic gain is off until the config or the brain turns it on.
pub const DEFAULT_AUTO_ENABLED: bool = false;
/// Default dB of gain per dB of speech level above the reference.
pub const DEFAULT_AUTO_K_SPEECH: f32 = 1.0;
/// Default speech level at which the automatic term adds 0 dB, dBFS. The raw-mic
/// channel's reading of a normal speaking voice at about 0.8 m from the array:
/// four normal utterances in the acceptance session measured −51.5, −53.7, −52.4
/// and −50.1 (80th percentile of 20 ms frame rms of the wake phrase, top-16 units).
/// With `k_speech = 1` the same session's raised voice (−46.2) maps to +5.8 dB, 0.2 dB
/// under the +6 swing ceiling, and its whisper (−62.4) to −10.4 dB, 1.6 dB above the
/// −12 swing floor: the demonstrated 16.2 dB range sits inside the 18 dB swing,
/// unclipped at both ends.
pub const DEFAULT_AUTO_SPEECH_REF_DBFS: f32 = -52.0;
/// Default dB of gain per dB of ambient level above its reference.
pub const DEFAULT_AUTO_K_AMBIENT: f32 = 0.0;
/// Default ambient level above which the ambient term contributes, dBFS. The
/// raw-mic channel's quiet-room floor: 20th percentile of frame rms −76.5 … −79.8
/// across the acceptance session, rms ≈ 4 LSB (−78.3 dBFS) over eleven 250 ms device
/// windows. A louder room raises the term from here: the room's air purifier on
/// high read −75.6; pink noise at −20 dBFS digital from a speaker 1.2 m away read −64.
pub const DEFAULT_AUTO_AMBIENT_REF_DBFS: f32 = -78.0;
/// Default swing floor: the automatic term never subtracts more than this, dB.
pub const DEFAULT_AUTO_MIN_DB: f32 = -12.0;
/// Default swing ceiling: the automatic term never adds more than this, dB.
pub const DEFAULT_AUTO_MAX_DB: f32 = 6.0;

/// The operator's hard bounds on any applied gain. Constructed only through
/// [`OperatorBounds::new`], so a value of this type always has finite bounds with
/// `min_db <= max_db` — the precondition of every `f32::clamp` against it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct OperatorBounds {
    min_db: f32,
    max_db: f32,
}

impl OperatorBounds {
    /// `None` when either bound is non-finite or `min_db > max_db`.
    pub fn new(min_db: f32, max_db: f32) -> Option<Self> {
        (min_db.is_finite() && max_db.is_finite() && min_db <= max_db)
            .then_some(Self { min_db, max_db })
    }

    pub fn min_db(&self) -> f32 {
        self.min_db
    }

    pub fn max_db(&self) -> f32 {
        self.max_db
    }

    /// `db` clamped into `[min_db, max_db]`; a non-finite `db` (never produced by
    /// a validated path) clamps to the floor, the quiet side.
    pub fn clamp(&self, db: f32) -> f32 {
        if db.is_nan() {
            return self.min_db;
        }
        db.clamp(self.min_db, self.max_db)
    }
}

impl Default for OperatorBounds {
    fn default() -> Self {
        Self {
            min_db: DEFAULT_MIN_DB,
            max_db: DEFAULT_MAX_DB,
        }
    }
}

/// The configured starting gain of each pod: a default for every pod, and an
/// optional per-pod override keyed by pod id. A baseline need not lie inside the
/// operator's bounds; it is clamped when a pod's state is first created.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GainBaselines {
    pub default_db: f32,
    pub per_pod: HashMap<String, f32>,
}

impl GainBaselines {
    /// The configured baseline for `pod`: its own when mapped, else the default.
    pub fn for_pod(&self, pod: &PodId) -> f32 {
        self.per_pod.get(&pod.0).copied().unwrap_or(self.default_db)
    }
}

/// Which measured levels may drive automatic gain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LevelAdmission {
    /// Only a level read off an uncompressed raw microphone.
    RawMic,
    /// Any measured level, including one read off a processed beam.
    Any,
}

impl LevelAdmission {
    fn admits(self, source: LevelSource) -> bool {
        match self {
            LevelAdmission::Any => true,
            LevelAdmission::RawMic => source == LevelSource::RawMic,
        }
    }
}

/// One pod's automatic-gain policy: whether it runs, which levels it reads, the
/// linear map from levels to a gain term, and the swing bounds on that term.
///
/// The swing bounds limit the automatic term only; the operator's bounds clamp
/// the applied total afterwards, so the swing bounds need not lie inside them.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct AutoPolicy {
    pub enabled: bool,
    pub level_source: LevelAdmission,
    /// dB of gain per dB of speech level above `speech_ref_dbfs`.
    pub k_speech: f32,
    /// The speech level at which the speech term is 0, dBFS.
    pub speech_ref_dbfs: f32,
    /// dB of gain per dB of ambient level above `ambient_ref_dbfs`.
    pub k_ambient: f32,
    /// The ambient level above which the ambient term contributes, dBFS.
    pub ambient_ref_dbfs: f32,
    /// Swing floor of the automatic term, dB.
    pub min_db: f32,
    /// Swing ceiling of the automatic term, dB.
    pub max_db: f32,
}

impl Default for AutoPolicy {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_AUTO_ENABLED,
            level_source: LevelAdmission::RawMic,
            k_speech: DEFAULT_AUTO_K_SPEECH,
            speech_ref_dbfs: DEFAULT_AUTO_SPEECH_REF_DBFS,
            k_ambient: DEFAULT_AUTO_K_AMBIENT,
            ambient_ref_dbfs: DEFAULT_AUTO_AMBIENT_REF_DBFS,
            min_db: DEFAULT_AUTO_MIN_DB,
            max_db: DEFAULT_AUTO_MAX_DB,
        }
    }
}

impl AutoPolicy {
    /// Every numeric knob finite and `min_db <= max_db` — the shape the book
    /// keeps every pod's policy in.
    pub fn is_well_formed(&self) -> bool {
        [
            self.k_speech,
            self.speech_ref_dbfs,
            self.k_ambient,
            self.ambient_ref_dbfs,
            self.min_db,
            self.max_db,
        ]
        .iter()
        .all(|v| v.is_finite())
            && self.min_db <= self.max_db
    }

    /// The automatic term for a reply whose utterance measured `level`, and why
    /// it is what it is:
    ///
    /// `k_speech·(speech − speech_ref) + k_ambient·max(0, ambient − ambient_ref)`
    /// (the ambient part 0 when no ambient was measured), held inside the swing
    /// bounds. 0 when the policy is off, no level was measured, or the level's
    /// source is not admitted.
    ///
    /// Computed in `f64`: every input is a finite `f32` (the book keeps the
    /// policy well formed, and a measured level is floored), and no product or
    /// sum of finite `f32`s overflows an `f64`, so the term is always finite —
    /// even for a brain-set `k_speech` near `f32::MAX`, where `f32` arithmetic
    /// would reach infinity or NaN.
    // TODO(far-field-auto-volume-cue): the speech term under-drives a reply to a
    // speaker far from the pod; which cue serves a far-field speaker is undecided until
    // a far-field session is run.
    pub fn term(&self, level: Option<&UtteranceLevel>) -> (f32, AutoStatus) {
        if !self.enabled {
            return (0.0, AutoStatus::Off);
        }
        let Some(level) = level else {
            return (0.0, AutoStatus::NoLevel);
        };
        if !self.level_source.admits(level.source) {
            return (0.0, AutoStatus::SourceRejected);
        }
        let f = f64::from;
        let speech = f(self.k_speech) * (f(level.speech_dbfs) - f(self.speech_ref_dbfs));
        let ambient = level.ambient_dbfs.map_or(0.0, |a| {
            f(self.k_ambient) * (f(a) - f(self.ambient_ref_dbfs)).max(0.0)
        });
        let raw = speech + ambient;
        debug_assert!(
            raw.is_finite(),
            "auto term {raw} from a non-finite input: policy {self:?}, level {level:?}"
        );
        // `max` then `min` rather than `clamp`: never panics, whatever the bounds.
        // The result lies inside the `f32` swing bounds, so narrowing is exact
        // enough and finite.
        #[allow(clippy::cast_possible_truncation)]
        let term = raw.max(f(self.min_db)).min(f(self.max_db)) as f32;
        (term, AutoStatus::Applied)
    }
}

/// Why the automatic term of one reply is what it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoStatus {
    /// The policy ran on the utterance's level.
    Applied,
    /// The pod's automatic gain is disabled.
    Off,
    /// No usable level was measured for the utterance being answered.
    NoLevel,
    /// A level was measured, on a source the policy does not admit.
    SourceRejected,
    /// The reply carried its own gain, which replaces the held and automatic
    /// terms.
    Overridden,
}

/// The persistent part of one `<volume/>` marker: what it asks the pod's state
/// to become. Every field is optional; an absent field leaves that part of the
/// state alone. Serialized with the marker's own attribute names, as the
/// `asked` object of `volume_set` / `volume_refused`.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize)]
pub struct VolumeCommand {
    #[serde(rename = "hold", skip_serializing_if = "Option::is_none")]
    pub hold_db: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k_speech: Option<f32>,
    #[serde(rename = "speech_ref", skip_serializing_if = "Option::is_none")]
    pub speech_ref_dbfs: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k_ambient: Option<f32>,
    #[serde(rename = "ambient_ref", skip_serializing_if = "Option::is_none")]
    pub ambient_ref_dbfs: Option<f32>,
    #[serde(rename = "min", skip_serializing_if = "Option::is_none")]
    pub auto_min_db: Option<f32>,
    #[serde(rename = "max", skip_serializing_if = "Option::is_none")]
    pub auto_max_db: Option<f32>,
}

impl VolumeCommand {
    /// Whether the command asks for no change at all.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn numbers(&self) -> [Option<f32>; 7] {
        [
            self.hold_db,
            self.k_speech,
            self.speech_ref_dbfs,
            self.k_ambient,
            self.ambient_ref_dbfs,
            self.auto_min_db,
            self.auto_max_db,
        ]
    }
}

/// Why a [`VolumeCommand`] was refused whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeRefusal {
    /// The result would leave the swing floor above the swing ceiling.
    MinGtMax,
    /// A value was not a finite number, whatever produced the command.
    NonFinite,
}

/// A pod's volume state as the brain sees it on every utterance: the held gain,
/// the automatic policy, and the operator's bounds that clamp everything.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct VolumeState {
    pub held_db: f32,
    pub auto: bool,
    pub level_source: LevelAdmission,
    pub k_speech: f32,
    pub speech_ref_dbfs: f32,
    pub k_ambient: f32,
    pub ambient_ref_dbfs: f32,
    pub auto_min_db: f32,
    pub auto_max_db: f32,
    pub min_db: f32,
    pub max_db: f32,
}

impl Default for VolumeState {
    /// The state of any pod in a book of config defaults.
    fn default() -> Self {
        VolumeBook::default().state(&PodId(String::new()))
    }
}

/// One pod's mutable volume state.
#[derive(Debug, Clone, Copy, PartialEq)]
struct PodVolume {
    /// The pod's held gain. Always inside the operator's bounds.
    held_db: f32,
    /// The pod's automatic policy. Always well-formed.
    auto: AutoPolicy,
}

/// How the gain for one reply was decided, by the [`VolumeBook`] at the moment
/// the reply's command was queued, for the router to apply and report.
///
/// Constructible only by [`VolumeBook::resolve`], so every gain that reaches a
/// writer was bounded by the book.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct GainResolution {
    held_db: f32,
    auto_db: f32,
    auto: AutoStatus,
    override_db: Option<f32>,
    applied_db: f32,
}

impl GainResolution {
    /// The pod's held gain when the command was queued.
    pub fn held_db(&self) -> f32 {
        self.held_db
    }

    /// The automatic term added to `held_db`, inside the pod's swing bounds.
    pub fn auto_db(&self) -> f32 {
        self.auto_db
    }

    /// Why `auto_db` is what it is.
    pub fn auto(&self) -> AutoStatus {
        self.auto
    }

    /// The reply's own override, as asked (before clamping), when it carried
    /// one.
    pub fn override_db(&self) -> Option<f32> {
        self.override_db
    }

    /// The gain applied to the samples, inside the operator's bounds.
    pub fn applied_db(&self) -> f32 {
        self.applied_db
    }
}

/// Per-pod volume state under the operator's bounds.
///
/// Invariants, upheld by every method: each pod's `held_db` lies in
/// `[bounds.min_db, bounds.max_db]`, and each pod's automatic policy is
/// well-formed (finite, swing floor not above swing ceiling). Pod state is
/// created lazily from the pod's baseline and the auto defaults the first time
/// the pod is touched; it lives in memory only, so a restart returns every pod
/// to its configured state.
#[derive(Debug, Clone, Default)]
pub struct VolumeBook {
    bounds: OperatorBounds,
    baselines: GainBaselines,
    auto_defaults: AutoPolicy,
    pods: HashMap<PodId, PodVolume>,
}

impl VolumeBook {
    /// A book over the operator's bounds, the per-pod baselines and the default
    /// automatic policy. A malformed `auto_defaults` (the config validates it)
    /// falls back to [`AutoPolicy::default`] so the invariant holds regardless.
    pub fn new(
        bounds: OperatorBounds,
        baselines: GainBaselines,
        auto_defaults: AutoPolicy,
    ) -> Self {
        let auto_defaults = if auto_defaults.is_well_formed() {
            auto_defaults
        } else {
            AutoPolicy::default()
        };
        Self {
            bounds,
            baselines,
            auto_defaults,
            pods: HashMap::new(),
        }
    }

    pub fn bounds(&self) -> OperatorBounds {
        self.bounds
    }

    pub fn baselines(&self) -> &GainBaselines {
        &self.baselines
    }

    pub fn auto_defaults(&self) -> AutoPolicy {
        self.auto_defaults
    }

    fn pod_mut(&mut self, pod: &PodId) -> &mut PodVolume {
        let initial = self.initial(pod);
        self.pods.entry(pod.clone()).or_insert(initial)
    }

    /// The pod's state, without creating it.
    fn pod(&self, pod: &PodId) -> PodVolume {
        self.pods
            .get(pod)
            .copied()
            .unwrap_or_else(|| self.initial(pod))
    }

    /// The pod's state as it would be with nothing changed yet.
    fn initial(&self, pod: &PodId) -> PodVolume {
        PodVolume {
            held_db: self.bounds.clamp(self.baselines.for_pod(pod)),
            auto: self.auto_defaults,
        }
    }

    /// The pod's held gain, creating its state from the baseline if needed.
    pub fn held_db(&mut self, pod: &PodId) -> f32 {
        self.pod_mut(pod).held_db
    }

    /// The pod's current state, without creating it: an untouched pod reports
    /// its configured state.
    pub fn state(&self, pod: &PodId) -> VolumeState {
        let p = self.pod(pod);
        VolumeState {
            held_db: p.held_db,
            auto: p.auto.enabled,
            level_source: p.auto.level_source,
            k_speech: p.auto.k_speech,
            speech_ref_dbfs: p.auto.speech_ref_dbfs,
            k_ambient: p.auto.k_ambient,
            ambient_ref_dbfs: p.auto.ambient_ref_dbfs,
            auto_min_db: p.auto.min_db,
            auto_max_db: p.auto.max_db,
            min_db: self.bounds.min_db,
            max_db: self.bounds.max_db,
        }
    }

    /// Apply a marker's persistent attributes to `pod` as one transaction.
    ///
    /// A hold is clamped into the operator's bounds; the policy knobs and swing
    /// bounds are taken as given. When the result would not be well-formed —
    /// the swing floor above the ceiling, or a non-finite value — nothing is
    /// applied and the pod's state is unchanged. Returns the state after.
    pub fn apply(
        &mut self,
        pod: &PodId,
        cmd: &VolumeCommand,
    ) -> Result<VolumeState, VolumeRefusal> {
        if cmd.numbers().iter().flatten().any(|v| !v.is_finite()) {
            return Err(VolumeRefusal::NonFinite);
        }
        let bounds = self.bounds;
        let current = *self.pod_mut(pod);
        let mut next = current;
        if let Some(hold) = cmd.hold_db {
            next.held_db = bounds.clamp(hold);
        }
        let a = &mut next.auto;
        a.enabled = cmd.auto.unwrap_or(a.enabled);
        a.k_speech = cmd.k_speech.unwrap_or(a.k_speech);
        a.speech_ref_dbfs = cmd.speech_ref_dbfs.unwrap_or(a.speech_ref_dbfs);
        a.k_ambient = cmd.k_ambient.unwrap_or(a.k_ambient);
        a.ambient_ref_dbfs = cmd.ambient_ref_dbfs.unwrap_or(a.ambient_ref_dbfs);
        a.min_db = cmd.auto_min_db.unwrap_or(a.min_db);
        a.max_db = cmd.auto_max_db.unwrap_or(a.max_db);
        if a.min_db > a.max_db {
            return Err(VolumeRefusal::MinGtMax);
        }
        *self.pod_mut(pod) = next;
        Ok(self.state(pod))
    }

    /// The gain for one reply to `pod` answering an utterance that measured
    /// `level`:
    ///
    /// - with an override, the override clamped into the operator's bounds;
    /// - otherwise the held gain plus the automatic term (already inside the
    ///   pod's swing bounds), clamped into the operator's bounds.
    ///
    /// A read: an untouched pod resolves from its configured state and stays
    /// untouched. Never panics: the bounds are ordered by construction.
    pub fn resolve(
        &self,
        pod: &PodId,
        override_db: Option<f32>,
        level: Option<&UtteranceLevel>,
    ) -> GainResolution {
        let bounds = self.bounds;
        let p = self.pod(pod);
        let (auto_db, auto, applied_db) = match override_db {
            Some(db) => (0.0, AutoStatus::Overridden, bounds.clamp(db)),
            None => {
                let (auto_db, auto) = p.auto.term(level);
                (auto_db, auto, bounds.clamp(p.held_db + auto_db))
            }
        };
        GainResolution {
            held_db: p.held_db,
            auto_db,
            auto,
            override_db,
            applied_db,
        }
    }
}

/// [`VolumeBook::resolve`] on a shared book: what every producer of a
/// `SpeakCmd` calls at the moment it queues the command.
///
/// The lock guards plain in-memory state and is released before this returns,
/// so it is never held across an await. A poisoned lock means a writer panicked
/// mid-update; the book's invariants hold between every statement, so its state
/// is used.
pub fn resolve_shared(
    book: &Mutex<VolumeBook>,
    pod: &PodId,
    override_db: Option<f32>,
    level: Option<&UtteranceLevel>,
) -> GainResolution {
    book.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .resolve(pod, override_db, level)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(id: &str) -> PodId {
        PodId(id.to_string())
    }

    fn book(min: f32, max: f32, default_db: f32, per_pod: &[(&str, f32)]) -> VolumeBook {
        VolumeBook::new(
            OperatorBounds::new(min, max).unwrap(),
            GainBaselines {
                default_db,
                per_pod: per_pod
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), *v))
                    .collect(),
            },
            // References fixed so the arithmetic in the tests reads against round numbers.
            AutoPolicy {
                speech_ref_dbfs: -30.0,
                ambient_ref_dbfs: -55.0,
                ..AutoPolicy::default()
            },
        )
    }

    fn level(speech: f32, ambient: Option<f32>, source: LevelSource) -> UtteranceLevel {
        UtteranceLevel {
            speech_dbfs: speech,
            ambient_dbfs: ambient,
            source,
        }
    }

    fn auto_on() -> VolumeCommand {
        VolumeCommand {
            auto: Some(true),
            ..VolumeCommand::default()
        }
    }

    #[test]
    fn bounds_reject_disorder_and_non_finite() {
        assert!(OperatorBounds::new(1.0, 0.0).is_none());
        assert!(OperatorBounds::new(f32::NAN, 0.0).is_none());
        assert!(OperatorBounds::new(-1.0, f32::INFINITY).is_none());
        assert!(OperatorBounds::new(0.0, 0.0).is_some());
    }

    #[test]
    fn default_book_matches_config_defaults() {
        let mut b = VolumeBook::default();
        assert_eq!(b.bounds().min_db(), DEFAULT_MIN_DB);
        assert_eq!(b.bounds().max_db(), DEFAULT_MAX_DB);
        assert_eq!(b.held_db(&pod("p")), DEFAULT_GAIN_DB);
        let s = VolumeState::default();
        assert!(!s.auto);
        assert_eq!((s.auto_min_db, s.auto_max_db), (-12.0, 6.0));
        assert_eq!((s.min_db, s.max_db), (-30.0, 6.0));
        assert_eq!(s.level_source, LevelAdmission::RawMic);
    }

    /// The auto-policy defaults are the bench values, and the acceptance session's
    /// whisper and raised voice resolve inside the swing under them, unclipped.
    #[test]
    fn the_auto_defaults_are_the_bench_values() {
        // Pinned so a retune is visible in test output rather than only in a diff.
        assert_eq!(DEFAULT_AUTO_SPEECH_REF_DBFS, -52.0);
        assert_eq!(DEFAULT_AUTO_AMBIENT_REF_DBFS, -78.0);
        assert_eq!(DEFAULT_AUTO_K_SPEECH, 1.0);
        assert_eq!(DEFAULT_AUTO_K_AMBIENT, 0.0);

        // The one test that reads the defaults: built from `AutoPolicy::default()`, not
        // through `book()`, whose references are fixed.
        let b = VolumeBook::new(
            OperatorBounds::default(),
            GainBaselines::default(),
            AutoPolicy {
                enabled: true,
                ..AutoPolicy::default()
            },
        );
        let resolve = |speech: f32, ambient: f32| {
            b.resolve(
                &pod("p"),
                None,
                Some(&level(speech, Some(ambient), LevelSource::RawMic)),
            )
        };
        let whisper = resolve(-62.4, -76.5);
        let raised = resolve(-46.2, -77.1);
        for (r, expected) in [(&whisper, -10.4), (&raised, 5.8)] {
            assert_eq!(r.auto, AutoStatus::Applied);
            assert!((r.auto_db - expected).abs() < 1e-4, "{r:?} vs {expected}");
            assert!(r.auto_db > DEFAULT_AUTO_MIN_DB && r.auto_db < DEFAULT_AUTO_MAX_DB);
            assert_eq!(r.applied_db, r.auto_db);
        }
    }

    #[test]
    fn baseline_is_per_pod_with_default_fallback() {
        let mut b = book(-30.0, 6.0, -3.0, &[("kitchen", -9.0)]);
        assert_eq!(b.held_db(&pod("kitchen")), -9.0);
        assert_eq!(b.held_db(&pod("unmapped")), -3.0);
    }

    #[test]
    fn baseline_outside_bounds_is_clamped_into_held() {
        let mut b = book(-10.0, 0.0, 5.0, &[("quiet", -50.0)]);
        assert_eq!(b.held_db(&pod("loud")), 0.0);
        assert_eq!(b.held_db(&pod("quiet")), -10.0);
    }

    #[test]
    fn resolve_uses_held_without_override() {
        let b = book(-30.0, 6.0, -4.0, &[]);
        let r = b.resolve(&pod("p"), None, None);
        assert_eq!(
            r,
            GainResolution {
                held_db: -4.0,
                auto_db: 0.0,
                auto: AutoStatus::Off,
                override_db: None,
                applied_db: -4.0,
            }
        );
    }

    #[test]
    fn resolve_override_wins_and_is_clamped_both_ways() {
        let b = book(-30.0, 6.0, -4.0, &[]);
        let r = b.resolve(&pod("p"), Some(-12.0), None);
        assert_eq!(r.applied_db, -12.0);
        assert_eq!(r.held_db, -4.0);
        assert_eq!(r.auto, AutoStatus::Overridden);
        assert_eq!(b.resolve(&pod("p"), Some(60.0), None).applied_db, 6.0);
        let low = b.resolve(&pod("p"), Some(-90.0), None);
        assert_eq!(low.applied_db, -30.0);
        assert_eq!(
            low.override_db,
            Some(-90.0),
            "the line shows what was asked"
        );
    }

    #[test]
    fn resolve_never_panics_at_degenerate_bounds() {
        let mut b = book(0.0, 0.0, 12.0, &[]);
        b.apply(&pod("p"), &auto_on()).unwrap();
        let loud = level(0.0, Some(0.0), LevelSource::RawMic);
        for o in [None, Some(f32::NAN), Some(-1e30), Some(1e30), Some(0.0)] {
            for l in [None, Some(&loud)] {
                assert_eq!(b.resolve(&pod("p"), o, l).applied_db, 0.0);
            }
        }
    }

    #[test]
    fn auto_term_follows_the_speech_level_inside_the_swing() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(&pod("p"), &auto_on()).unwrap();
        // 4 dB louder than the -30 reference with k_speech 1 adds 4 dB.
        let r = b.resolve(
            &pod("p"),
            None,
            Some(&level(-26.0, None, LevelSource::RawMic)),
        );
        assert_eq!(
            (r.auto_db, r.auto, r.applied_db),
            (4.0, AutoStatus::Applied, 4.0)
        );
        // A whisper 30 dB under the reference is held at the -12 swing floor.
        let r = b.resolve(
            &pod("p"),
            None,
            Some(&level(-60.0, None, LevelSource::RawMic)),
        );
        assert_eq!(r.auto_db, -12.0);
        // A shout is held at the +6 swing ceiling.
        let r = b.resolve(
            &pod("p"),
            None,
            Some(&level(0.0, None, LevelSource::RawMic)),
        );
        assert_eq!(r.auto_db, 6.0);
    }

    #[test]
    fn auto_ambient_term_adds_only_above_its_reference() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(
            &pod("p"),
            &VolumeCommand {
                auto: Some(true),
                k_ambient: Some(0.5),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        let at_ref = level(-30.0, Some(-70.0), LevelSource::RawMic);
        assert_eq!(b.resolve(&pod("p"), None, Some(&at_ref)).auto_db, 0.0);
        let noisy = level(-30.0, Some(-45.0), LevelSource::RawMic);
        assert_eq!(b.resolve(&pod("p"), None, Some(&noisy)).auto_db, 5.0);
    }

    #[test]
    fn an_extreme_policy_saturates_at_the_swing_bounds() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(
            &pod("p"),
            &VolumeCommand {
                auto: Some(true),
                k_speech: Some(f32::MAX),
                k_ambient: Some(-f32::MAX),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        // In `f32` the first reading overflows the speech term to +inf.
        let loud = level(-10.0, None, LevelSource::RawMic);
        let r = b.resolve(&pod("p"), None, Some(&loud));
        assert_eq!((r.auto_db, r.auto), (6.0, AutoStatus::Applied));
        // In `f32` the speech term is +inf and the ambient term -inf, summing to
        // NaN; in `f64` the term is -25 × `f32::MAX`.
        let loud_in_noise = level(-10.0, Some(-10.0), LevelSource::RawMic);
        let r = b.resolve(&pod("p"), None, Some(&loud_in_noise));
        assert_eq!((r.auto_db, r.auto), (-12.0, AutoStatus::Applied));
        // In `f32` both terms overflow to -inf.
        let quiet_in_noise = level(-50.0, Some(-10.0), LevelSource::RawMic);
        let r = b.resolve(&pod("p"), None, Some(&quiet_in_noise));
        assert_eq!((r.auto_db, r.auto), (-12.0, AutoStatus::Applied));
    }

    #[test]
    fn auto_contributes_nothing_off_without_level_or_from_a_rejected_source() {
        let mut b = book(-30.0, 6.0, -2.0, &[]);
        let beam = level(-10.0, None, LevelSource::Beam);
        let r = b.resolve(&pod("p"), None, Some(&beam));
        assert_eq!((r.auto_db, r.auto), (0.0, AutoStatus::Off));
        b.apply(&pod("p"), &auto_on()).unwrap();
        let r = b.resolve(&pod("p"), None, None);
        assert_eq!(
            (r.auto_db, r.auto, r.applied_db),
            (0.0, AutoStatus::NoLevel, -2.0)
        );
        let r = b.resolve(&pod("p"), None, Some(&beam));
        assert_eq!((r.auto_db, r.auto), (0.0, AutoStatus::SourceRejected));
        // `any` admits the beam.
        let any = VolumeBook::new(
            OperatorBounds::default(),
            GainBaselines::default(),
            AutoPolicy {
                enabled: true,
                level_source: LevelAdmission::Any,
                ..AutoPolicy::default()
            },
        );
        assert_eq!(
            any.resolve(&pod("p"), None, Some(&beam)).auto,
            AutoStatus::Applied
        );
    }

    #[test]
    fn a_swing_ceiling_above_the_operator_ceiling_still_applies_the_operator_ceiling() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(
            &pod("p"),
            &VolumeCommand {
                auto: Some(true),
                auto_max_db: Some(60.0),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        let r = b.resolve(
            &pod("p"),
            None,
            Some(&level(0.0, None, LevelSource::RawMic)),
        );
        assert_eq!(r.auto_db, 30.0);
        assert_eq!(r.applied_db, 6.0);
    }

    #[test]
    fn a_hold_beyond_the_operator_bounds_is_clamped() {
        let mut b = VolumeBook::default();
        let s = b
            .apply(
                &pod("p"),
                &VolumeCommand {
                    hold_db: Some(60.0),
                    auto_max_db: Some(60.0),
                    ..VolumeCommand::default()
                },
            )
            .unwrap();
        assert_eq!(s.held_db, 6.0);
        assert_eq!(s.auto_max_db, 60.0, "swing bounds are taken as given");
        assert_eq!(b.held_db(&pod("p")), 6.0);
    }

    #[test]
    fn a_swing_floor_above_the_ceiling_is_refused_whole() {
        let mut b = VolumeBook::default();
        let before = b.state(&pod("p"));
        let cmd = VolumeCommand {
            hold_db: Some(-10.0),
            auto: Some(true),
            auto_min_db: Some(10.0),
            ..VolumeCommand::default()
        };
        assert_eq!(b.apply(&pod("p"), &cmd), Err(VolumeRefusal::MinGtMax));
        assert_eq!(
            b.state(&pod("p")),
            before,
            "nothing applied, not even the hold"
        );
        let both = VolumeCommand {
            auto_min_db: Some(-20.0),
            auto_max_db: Some(-25.0),
            ..VolumeCommand::default()
        };
        assert_eq!(b.apply(&pod("p"), &both), Err(VolumeRefusal::MinGtMax));
        assert_eq!(b.state(&pod("p")), before);
    }

    #[test]
    fn swing_bounds_moved_together_apply() {
        let mut b = VolumeBook::default();
        let s = b
            .apply(
                &pod("p"),
                &VolumeCommand {
                    auto_min_db: Some(-5.0),
                    auto_max_db: Some(5.0),
                    ..VolumeCommand::default()
                },
            )
            .unwrap();
        assert_eq!((s.auto_min_db, s.auto_max_db), (-5.0, 5.0));
    }

    #[test]
    fn a_non_finite_value_is_refused() {
        let mut b = VolumeBook::default();
        let cmd = VolumeCommand {
            k_speech: Some(f32::INFINITY),
            ..VolumeCommand::default()
        };
        assert_eq!(b.apply(&pod("p"), &cmd), Err(VolumeRefusal::NonFinite));
    }

    #[test]
    fn state_reports_an_untouched_pod_without_creating_it() {
        let b = book(-30.0, 6.0, -3.0, &[("k", 12.0)]);
        assert_eq!(b.state(&pod("k")).held_db, 6.0);
        assert_eq!(b.state(&pod("other")).held_db, -3.0);
        assert!(b.pods.is_empty());
    }

    #[test]
    fn resolve_reads_an_untouched_pod_without_creating_it() {
        let b = book(-30.0, 6.0, -3.0, &[("k", 12.0)]);
        assert_eq!(b.resolve(&pod("k"), None, None).held_db, 6.0);
        assert_eq!(b.resolve(&pod("other"), Some(-8.0), None).held_db, -3.0);
        assert!(b.pods.is_empty());
        assert_eq!(b.state(&pod("k")).held_db, 6.0);
    }

    #[test]
    fn a_fresh_book_reports_the_baseline_after_a_hold_elsewhere() {
        let mut b = VolumeBook::default();
        b.apply(
            &pod("p"),
            &VolumeCommand {
                hold_db: Some(-10.0),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        assert_eq!(b.state(&pod("p")).held_db, -10.0);
        assert_eq!(VolumeBook::default().state(&pod("p")).held_db, 0.0);
    }

    #[test]
    fn the_asked_object_uses_the_marker_attribute_names() {
        let cmd = VolumeCommand {
            hold_db: Some(-10.0),
            auto: Some(true),
            speech_ref_dbfs: Some(-28.0),
            auto_min_db: Some(-6.0),
            ..VolumeCommand::default()
        };
        assert_eq!(
            serde_json::to_value(cmd).unwrap(),
            serde_json::json!({"hold": -10.0, "auto": true, "speech_ref": -28.0, "min": -6.0})
        );
        assert!(VolumeCommand::default().is_empty());
        assert!(!cmd.is_empty());
    }

    /// Random markers against random operator bounds never break the book's
    /// invariants, and resolving at the resulting state never panics.
    #[test]
    fn random_markers_never_break_the_invariants() {
        // xorshift32: deterministic, dependency-free.
        let mut seed: u32 = 0x9e37_79b9;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed
        };
        let value = |next: &mut dyn FnMut() -> u32| -> Option<f32> {
            match next() % 8 {
                0..=3 => None,
                4 => Some(f32::NAN),
                _ => Some((next() % 2001) as f32 / 10.0 - 100.0),
            }
        };
        for _ in 0..200 {
            let a = (next() % 61) as f32 - 40.0;
            let c = (next() % 61) as f32 - 40.0;
            let (lo, hi) = if a <= c { (a, c) } else { (c, a) };
            let mut b = book(lo, hi, (next() % 81) as f32 - 40.0, &[]);
            for _ in 0..50 {
                let cmd = VolumeCommand {
                    hold_db: value(&mut next),
                    auto: (next() % 3 == 0).then_some(next() % 2 == 0),
                    k_speech: value(&mut next),
                    speech_ref_dbfs: value(&mut next),
                    k_ambient: value(&mut next),
                    ambient_ref_dbfs: value(&mut next),
                    auto_min_db: value(&mut next),
                    auto_max_db: value(&mut next),
                };
                let _ = b.apply(&pod("p"), &cmd);
                let s = b.state(&pod("p"));
                assert!(s.held_db >= lo && s.held_db <= hi, "{s:?}");
                assert!(s.auto_min_db <= s.auto_max_db, "{s:?}");
                // A measured level is finite by construction (`FrameLevel`
                // floors it); markers are where non-finite values can arrive.
                let measured =
                    |next: &mut dyn FnMut() -> u32| value(next).filter(|v| v.is_finite());
                let l = level(
                    measured(&mut next).unwrap_or(-30.0),
                    measured(&mut next),
                    LevelSource::RawMic,
                );
                let r = b.resolve(&pod("p"), value(&mut next), Some(&l));
                assert!(r.applied_db >= lo && r.applied_db <= hi, "{r:?}");
            }
        }
    }
}
