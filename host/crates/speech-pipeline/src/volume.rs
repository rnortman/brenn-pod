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
/// Default dB of gain per dB the room is above its reference, and of attenuation
/// per dB it is below.
pub const DEFAULT_AUTO_K_AMBIENT: f32 = 1.0;
/// Default room level at which a reply plays at the held volume, dBFS: the
/// raw-mic channel's quiet-room floor, so by default a quiet room plays at the
/// held volume and a louder one boosts. The room reading is the median of
/// 100 ms raw-mic blocks over the 10 s before the speech. On pod-fbe2f8 on
/// 2026-11-09, in a room that was not controlled, a quiet room read −77.0
/// (three takes, −77.5 … −74.25); pink noise at −30 and −20 dBFS digital from a
/// speaker 1.2 m away read −71.5 and −62.5. These are one evening in one room,
/// not a property of the meter.
pub const DEFAULT_AUTO_AMBIENT_REF_DBFS: f32 = -78.0;
/// Default swing floor: the automatic term never subtracts more than this, dB.
pub const DEFAULT_AUTO_MIN_DB: f32 = -12.0;
/// Default swing ceiling: the automatic term never adds more than this, dB.
pub const DEFAULT_AUTO_MAX_DB: f32 = 6.0;

/// An ambient reading up to this old is the room, at full weight.
pub const AMBIENT_FRESH_MS: u32 = 30_000;
/// An ambient reading this old or older is not evidence of the room: weight 0.
/// Between [`AMBIENT_FRESH_MS`] and this the weight falls linearly from 1 to 0.
pub const AMBIENT_STALE_MS: u32 = 300_000;

/// How much an ambient reading `age_ms` old counts, 0…1.
pub fn ambient_weight(age_ms: u32) -> f32 {
    if age_ms <= AMBIENT_FRESH_MS {
        1.0
    } else if age_ms >= AMBIENT_STALE_MS {
        0.0
    } else {
        // Both spans are under 2^24, so the `f32` division is exact enough.
        #[allow(clippy::cast_precision_loss)]
        let w = (AMBIENT_STALE_MS - age_ms) as f32 / (AMBIENT_STALE_MS - AMBIENT_FRESH_MS) as f32;
        w
    }
}

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
/// linear map from the room's level to a gain term, and the swing bounds on that
/// term.
///
/// The swing bounds limit the automatic term only; the operator's bounds clamp
/// the applied total afterwards, so the swing bounds need not lie inside them.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct AutoPolicy {
    pub enabled: bool,
    pub level_source: LevelAdmission,
    /// dB of gain per dB the room is above `ambient_ref_dbfs`, and of
    /// attenuation per dB it is below.
    pub k_ambient: f32,
    /// The room level at which a reply plays at the held volume, dBFS.
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
            self.k_ambient,
            self.ambient_ref_dbfs,
            self.min_db,
            self.max_db,
        ]
        .iter()
        .all(|v| v.is_finite())
            && self.min_db <= self.max_db
    }

    /// The automatic term for a reply whose utterance measured `level`, the
    /// weight its ambient reading carried, and why the term is what it is:
    ///
    /// `k_ambient · (ambient − ambient_ref) · weight(age)`, held inside the swing
    /// bounds — a room louder than the reference boosts, a quieter one
    /// attenuates. 0 when the policy is off, no ambient reading exists, the
    /// level's source is not admitted, or the reading is too old to count
    /// ([`ambient_weight`] 0). The weight is `Some` exactly when a reading was
    /// weighed.
    ///
    /// Computed in `f64`: every input is a finite `f32` (the book keeps the
    /// policy well formed, and a measured level is floored), and no product of
    /// finite `f32`s overflows an `f64`, so the term is always finite — even for
    /// a brain-set `k_ambient` near `f32::MAX`, where `f32` arithmetic would
    /// reach infinity.
    pub fn term(&self, level: Option<&UtteranceLevel>) -> (f32, Option<f32>, AutoStatus) {
        if !self.enabled {
            return (0.0, None, AutoStatus::Off);
        }
        let Some(level) = level else {
            return (0.0, None, AutoStatus::NoLevel);
        };
        if !self.level_source.admits(level.source) {
            return (0.0, None, AutoStatus::SourceRejected);
        }
        let Some(ambient) = level.ambient else {
            return (0.0, None, AutoStatus::NoLevel);
        };
        let weight = ambient_weight(ambient.age_ms);
        if weight == 0.0 {
            return (0.0, Some(weight), AutoStatus::Stale);
        }
        let f = f64::from;
        let raw = f(self.k_ambient) * (f(ambient.dbfs) - f(self.ambient_ref_dbfs)) * f(weight);
        debug_assert!(
            raw.is_finite(),
            "auto term {raw} from a non-finite input: policy {self:?}, level {level:?}"
        );
        // `max` then `min` rather than `clamp`: never panics, whatever the bounds.
        // The result lies inside the `f32` swing bounds, so narrowing is exact
        // enough and finite.
        #[allow(clippy::cast_possible_truncation)]
        let term = raw.max(f(self.min_db)).min(f(self.max_db)) as f32;
        (term, Some(weight), AutoStatus::Applied)
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
    /// No ambient reading, measured or carried, exists for the utterance being
    /// answered.
    NoLevel,
    /// An ambient reading exists and is too old to count: its weight is 0.
    Stale,
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

    fn numbers(&self) -> [Option<f32>; 5] {
        [
            self.hold_db,
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
    ambient_weight: Option<f32>,
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

    /// The weight (0…1) the ambient reading's age gave it, when a reading was
    /// weighed.
    pub fn ambient_weight(&self) -> Option<f32> {
        self.ambient_weight
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
// TODO(volume-state-lost-on-restart): what the brain sets by voice does not outlive
// the daemon.
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
        let (auto_db, ambient_weight, auto, applied_db) = match override_db {
            Some(db) => (0.0, None, AutoStatus::Overridden, bounds.clamp(db)),
            None => {
                let (auto_db, weight, auto) = p.auto.term(level);
                (auto_db, weight, auto, bounds.clamp(p.held_db + auto_db))
            }
        };
        GainResolution {
            held_db: p.held_db,
            auto_db,
            auto,
            ambient_weight,
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
    use crate::types::AmbientReading;

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
            // The reference fixed so the arithmetic in the tests reads against round
            // numbers.
            AutoPolicy {
                ambient_ref_dbfs: -55.0,
                ..AutoPolicy::default()
            },
        )
    }

    /// A fresh ambient reading at `dbfs`, with no speech reading.
    fn room(dbfs: f32, source: LevelSource) -> UtteranceLevel {
        aged(dbfs, 500, source)
    }

    fn aged(dbfs: f32, age_ms: u32, source: LevelSource) -> UtteranceLevel {
        UtteranceLevel {
            speech_dbfs: None,
            ambient: Some(AmbientReading {
                dbfs,
                blocks: 100,
                age_ms,
                carried: false,
            }),
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

    /// The auto-policy defaults, pinned so a retune is visible in test output
    /// rather than only in a diff; under them the room drives the term both ways.
    #[test]
    fn the_auto_defaults_are_pinned() {
        assert_eq!(DEFAULT_AUTO_AMBIENT_REF_DBFS, -78.0);
        assert_eq!(DEFAULT_AUTO_K_AMBIENT, 1.0);
        assert_eq!((AMBIENT_FRESH_MS, AMBIENT_STALE_MS), (30_000, 300_000));

        let b = VolumeBook::new(
            OperatorBounds::default(),
            GainBaselines::default(),
            AutoPolicy {
                enabled: true,
                ..AutoPolicy::default()
            },
        );
        let resolve =
            |dbfs: f32| b.resolve(&pod("p"), None, Some(&room(dbfs, LevelSource::RawMic)));
        assert_eq!(
            resolve(-78.0).auto_db,
            0.0,
            "the quiet room plays at the held volume"
        );
        assert_eq!(resolve(-74.0).auto_db, 4.0);
        assert_eq!(resolve(-81.0).auto_db, -3.0);
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
                ambient_weight: None,
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
        assert_eq!(r.ambient_weight, None);
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
        let loud = room(0.0, LevelSource::RawMic);
        for o in [None, Some(f32::NAN), Some(-1e30), Some(1e30), Some(0.0)] {
            for l in [None, Some(&loud)] {
                assert_eq!(b.resolve(&pod("p"), o, l).applied_db, 0.0);
            }
        }
    }

    /// The term is symmetric about the reference: a quieter room attenuates and a
    /// louder one boosts, each inside the swing.
    #[test]
    fn the_room_drives_the_term_both_ways_inside_the_swing() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(&pod("p"), &auto_on()).unwrap();
        let resolve = |b: &VolumeBook, dbfs: f32| {
            b.resolve(&pod("p"), None, Some(&room(dbfs, LevelSource::RawMic)))
        };
        let r = resolve(&b, -63.0);
        assert_eq!(
            (r.auto_db, r.auto, r.applied_db, r.ambient_weight),
            (-8.0, AutoStatus::Applied, -8.0, Some(1.0)),
            "8 dB below the reference gives -8"
        );
        assert_eq!(
            resolve(&b, -47.0).auto_db,
            6.0,
            "8 dB above hits the +6 ceiling"
        );
        assert_eq!(
            resolve(&b, -85.0).auto_db,
            -12.0,
            "the -12 floor is reachable"
        );
        assert_eq!(resolve(&b, -55.0).auto_db, 0.0);
        b.apply(
            &pod("p"),
            &VolumeCommand {
                k_ambient: Some(0.5),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        assert_eq!(resolve(&b, -45.0).auto_db, 5.0);
        assert_eq!(resolve(&b, -65.0).auto_db, -5.0);
    }

    /// A reading counts fully up to 30 s, half at 165 s, and not at all at 300 s,
    /// where the status says so.
    #[test]
    fn an_old_reading_eases_toward_no_adjustment() {
        assert_eq!(ambient_weight(0), 1.0);
        assert_eq!(ambient_weight(30_000), 1.0);
        assert!((ambient_weight(165_000) - 0.5).abs() < 1e-6);
        assert_eq!(ambient_weight(300_000), 0.0);
        assert_eq!(ambient_weight(u32::MAX), 0.0);

        let mut b = book(-30.0, 6.0, -2.0, &[]);
        b.apply(&pod("p"), &auto_on()).unwrap();
        let at = |age_ms: u32| {
            b.resolve(
                &pod("p"),
                None,
                Some(&aged(-63.0, age_ms, LevelSource::RawMic)),
            )
        };
        let fresh = at(30_000);
        assert_eq!((fresh.auto_db, fresh.auto), (-8.0, AutoStatus::Applied));
        let half = at(165_000);
        assert!((half.auto_db - -4.0).abs() < 1e-4, "{half:?}");
        assert_eq!(half.auto, AutoStatus::Applied);
        let stale = at(300_000);
        assert_eq!(
            (
                stale.auto_db,
                stale.auto,
                stale.ambient_weight,
                stale.applied_db
            ),
            (0.0, AutoStatus::Stale, Some(0.0), -2.0)
        );
    }

    #[test]
    fn an_extreme_policy_saturates_at_the_swing_bounds() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(
            &pod("p"),
            &VolumeCommand {
                auto: Some(true),
                k_ambient: Some(f32::MAX),
                ..VolumeCommand::default()
            },
        )
        .unwrap();
        // In `f32` both products overflow to ±inf.
        let loud = room(-10.0, LevelSource::RawMic);
        let r = b.resolve(&pod("p"), None, Some(&loud));
        assert_eq!((r.auto_db, r.auto), (6.0, AutoStatus::Applied));
        let quiet = room(-90.0, LevelSource::RawMic);
        let r = b.resolve(&pod("p"), None, Some(&quiet));
        assert_eq!((r.auto_db, r.auto), (-12.0, AutoStatus::Applied));
    }

    #[test]
    fn auto_contributes_nothing_off_without_ambient_or_from_a_rejected_source() {
        let mut b = book(-30.0, 6.0, -2.0, &[]);
        let beam = room(-10.0, LevelSource::Beam);
        let r = b.resolve(&pod("p"), None, Some(&beam));
        assert_eq!((r.auto_db, r.auto), (0.0, AutoStatus::Off));
        b.apply(&pod("p"), &auto_on()).unwrap();
        let r = b.resolve(&pod("p"), None, None);
        assert_eq!(
            (r.auto_db, r.auto, r.applied_db),
            (0.0, AutoStatus::NoLevel, -2.0)
        );
        // A speech reading alone does not drive the volume.
        let speech_only = UtteranceLevel {
            speech_dbfs: Some(-20.0),
            ambient: None,
            source: LevelSource::RawMic,
        };
        let r = b.resolve(&pod("p"), None, Some(&speech_only));
        assert_eq!((r.auto_db, r.auto), (0.0, AutoStatus::NoLevel));
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

    /// The speech level is information only: two readings that differ only in it
    /// resolve alike.
    #[test]
    fn the_speech_level_does_not_move_the_term() {
        let mut b = book(-30.0, 6.0, 0.0, &[]);
        b.apply(&pod("p"), &auto_on()).unwrap();
        let mut whisper = room(-60.0, LevelSource::RawMic);
        whisper.speech_dbfs = Some(-70.0);
        let mut shout = whisper;
        shout.speech_dbfs = Some(-5.0);
        assert_eq!(
            b.resolve(&pod("p"), None, Some(&whisper)),
            b.resolve(&pod("p"), None, Some(&shout))
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
        let r = b.resolve(&pod("p"), None, Some(&room(-25.0, LevelSource::RawMic)));
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
            k_ambient: Some(f32::INFINITY),
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
            ambient_ref_dbfs: Some(-60.0),
            auto_min_db: Some(-6.0),
            ..VolumeCommand::default()
        };
        assert_eq!(
            serde_json::to_value(cmd).unwrap(),
            serde_json::json!({"hold": -10.0, "auto": true, "ambient_ref": -60.0, "min": -6.0})
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
                let l = aged(
                    measured(&mut next).unwrap_or(-60.0),
                    next() % 400_000,
                    LevelSource::RawMic,
                );
                let r = b.resolve(&pod("p"), value(&mut next), Some(&l));
                assert!(r.applied_db >= lo && r.applied_db <= hi, "{r:?}");
            }
        }
    }
}
