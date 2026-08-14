//! The motion library: the clips and sequences a script's `play` steps name.
//!
//! One directory of JSON documents, read once at startup and never again. What
//! comes out is a set of motions addressable by name, which is the whole of what
//! the wire carries — the bus moves invocations, never assets, so nothing here
//! is reachable from the network.
//!
//! The reading is this module's; the deciding is not. Validation, resolution and
//! flattening all live in `reachy-clips` beside the importer that writes these
//! files, so a document the importer produced and a document an author wrote by
//! hand are judged by the same code and cannot drift. What is added here is the
//! directory walk, the report, and the acceptance screen the bus thread runs a
//! script through before the schedule ever sees it.
//!
//! A document that does not validate is named and skipped and the rest of the
//! library loads: one bad file must not cost the machine its whole vocabulary,
//! and a script naming the skipped motion is refused by name anyway.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use motion_proto::{MotionScript, OverlayError, Play, PlayWindow};
use reachy_clips::{ClipLimits, Library, Motion};
use serde_json::json;
use thiserror::Error;

use crate::report::Sink;

/// The slack a speed comparison gets against a motion's own ceiling.
///
/// A sequence's ceiling is a product and a quotient of entry speeds, so an
/// invocation at exactly the number an operator computed need not land on it in
/// binary. Refusing a script over the last bit of a `f64` would be refusing
/// arithmetic; the ceiling itself is derived with a 20% margin against the step
/// bounds, which is what actually protects the machine.
const SPEED_EPS: f64 = 1e-9;

/// Why the daemon could not read the library its configuration named.
///
/// Not a per-document refusal — those are skips, reported and survived. This is
/// the directory itself, and it is a startup refusal: an operator who named a
/// `clip_dir` asked for a machine with a vocabulary, and one that came up
/// silently posture-only would answer every emote with "no such motion" until
/// somebody read the log.
#[derive(Debug, Error)]
#[error("clip_dir {path:?}: {source}")]
pub struct LibraryError {
    /// The directory that could not be read.
    pub path: PathBuf,
    /// What the filesystem said.
    pub source: io::Error,
}

/// Why a script's overlays cannot run against this library.
///
/// Answered at delivery, ahead of the schedule, so a script that fails one of
/// these never replaces the timeline already running and never advances the
/// sequence high-water mark.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum Unplayable {
    /// A name that does not resolve, or more overlays at once than the machine
    /// composes. Both are the protocol crate's own arithmetic over the
    /// durations this library supplies.
    #[error(transparent)]
    Overlay(#[from] OverlayError),

    /// An invocation faster than the motion's own ceiling. Refused rather than
    /// slowed to it: a clip played at a speed nobody asked for is not the motion
    /// the script requested, and silently obeying half of an instruction is the
    /// failure mode this whole path exists to avoid.
    #[error(
        "step {index} plays `{name}` at {speed}x; that motion may not be played above \
         {max_speed}x"
    )]
    TooFast {
        /// Which step named it.
        index: usize,
        /// The motion.
        name: String,
        /// What the script asked for.
        speed: f64,
        /// What the library allows.
        max_speed: f64,
    },
}

impl Unplayable {
    /// The slug this refusal is reported and searched for under.
    ///
    /// One word per distinct thing an operator would go and change: a name that
    /// is not in the library, a script asking for more layers than the machine
    /// composes, and a speed above a motion's own ceiling are three different
    /// fixes.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Overlay(OverlayError::UnknownMotion { .. }) => "no_such_motion",
            Self::Overlay(OverlayError::TooManyOverlays { .. }) => "too_many_overlays",
            Self::TooFast { .. } => "over_speed",
        }
    }
}

/// The motions this daemon holds, and the questions the rest of it asks of them.
///
/// Cheap to clone and shared between the two threads: the bus thread screens
/// arriving scripts against it, and the motion thread plays out of it. Nothing
/// mutates after startup — a library is a deployment, not a session.
#[derive(Debug, Clone)]
pub struct Motions {
    library: Arc<Library>,
}

impl Default for Motions {
    fn default() -> Self {
        Self::none()
    }
}

impl Motions {
    /// The library of a daemon whose configuration named no `clip_dir`.
    ///
    /// Not a degraded mode: this is the posture-only daemon, and every `play`
    /// step is refused for naming a motion nothing holds.
    #[must_use]
    pub fn none() -> Self {
        Self {
            library: Arc::new(Library::empty()),
        }
    }

    /// Read every document in `dir` and report what came of it.
    ///
    /// `limits` are the machine's own bounds, so the ramps and speed ceilings
    /// this daemon derives are the ones its envelope and step bounds imply
    /// rather than the library defaults. Every `*.json` is offered; anything
    /// else in the directory — the sound sidecars the importer copies through —
    /// is left alone.
    pub fn load(dir: &Path, limits: &ClipLimits, sink: &dyn Sink) -> Result<Self, LibraryError> {
        let documents = reachy_clips::documents(dir).map_err(|source| LibraryError {
            path: dir.to_path_buf(),
            source,
        })?;
        let unreadable: Vec<(String, io::Error)> = documents
            .iter()
            .filter_map(|(source, outcome)| {
                outcome.as_ref().err().map(|error| {
                    (
                        source.clone(),
                        io::Error::new(error.kind(), error.to_string()),
                    )
                })
            })
            .collect();
        let readable: Vec<(&str, &str)> = documents
            .iter()
            .filter_map(|(source, outcome)| {
                outcome
                    .as_ref()
                    .ok()
                    .map(|text| (source.as_str(), text.as_str()))
            })
            .collect();

        let (library, skips) = Library::load(readable, limits);

        for (source, error) in &unreadable {
            report_skip(sink, source, None, &error.to_string());
        }
        for skip in &skips {
            report_skip(
                sink,
                &skip.source,
                skip.name.as_deref(),
                &skip.error.to_string(),
            );
        }
        for note in library.notes() {
            sink.line(&format!("library: {note}"));
            sink.event(
                "motion_asset_note",
                &json!({
                    "source": note.source,
                    "name": note.name,
                    "note": note.note.to_string(),
                }),
            );
        }

        let skipped = unreadable.len() + skips.len();
        sink.line(&format!(
            "library: {} from {} ({} skipped)",
            plural(library.len(), "motion", "motions"),
            dir.display(),
            skipped
        ));
        sink.event(
            "motion_library",
            &json!({
                "dir": dir.display().to_string(),
                "motions": library.len(),
                "skipped": skipped,
                "names": library.names().collect::<Vec<_>>(),
            }),
        );

        Ok(Self {
            library: Arc::new(library),
        })
    }

    /// The motion `name` addresses, if this library holds one.
    #[must_use]
    pub fn motion(&self, name: &str) -> Option<&Arc<Motion>> {
        self.library.motion(name)
    }

    /// How many motions are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.library.len()
    }

    /// Whether the daemon holds no motions at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.library.is_empty()
    }

    /// How long an invocation of `play` occupies a script's timeline.
    ///
    /// The protocol crate does the window arithmetic and this answers the two
    /// numbers it cannot know. `None` for a name nothing holds, which is what
    /// turns into the unknown-motion refusal.
    #[must_use]
    pub fn window(&self, play: &Play) -> Option<PlayWindow> {
        let motion = self.library.motion(&play.name)?;
        Some(PlayWindow {
            duration_ms: ms_ceil(motion.duration_s()),
            blend_out_ms: u64::from(motion.blend_out_ms()),
        })
    }

    /// The screen an arriving script passes before the schedule sees it.
    ///
    /// Three questions, all of which need the library the publisher may not
    /// have: does every name resolve, is every invocation inside its motion's
    /// own ceiling, and does the timeline ever run more overlays at once than
    /// the machine composes. The speed check comes first because it is per-step
    /// and names the step, where the concurrency refusal names an instant.
    pub fn screen(&self, script: &MotionScript) -> Result<(), Unplayable> {
        for (index, step) in script.steps().iter().enumerate() {
            let Some(play) = step.action.play() else {
                continue;
            };
            // A name that does not resolve is left to `check_overlays`, which
            // refuses it with the step it came from; answering it twice in two
            // wordings would be two refusals for one fault.
            let Some(motion) = self.library.motion(&play.name) else {
                continue;
            };
            if play.speed > motion.max_speed() + SPEED_EPS {
                return Err(Unplayable::TooFast {
                    index,
                    name: play.name.clone(),
                    speed: play.speed,
                    max_speed: motion.max_speed(),
                });
            }
        }
        script.check_overlays(|play| self.window(play))?;
        Ok(())
    }
}

/// One skipped document, said both ways.
fn report_skip(sink: &dyn Sink, source: &str, name: Option<&str>, error: &str) {
    let named = name.map_or_else(String::new, |name| format!(" ({name})"));
    sink.line(&format!("library: skipped {source}{named}: {error}"));
    sink.event(
        "motion_asset_skipped",
        &json!({ "source": source, "name": name, "error": error }),
    );
}

/// Seconds as whole milliseconds, rounded up.
///
/// Up, so a window closes no earlier than the last frame the player has to
/// produce: a motion truncated by its own window arithmetic would drop its final
/// frames and its fade with them.
fn ms_ceil(seconds: f64) -> u64 {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a validated motion's duration, in seconds, is finite and positive"
    )]
    let ms = (seconds * 1000.0).ceil() as u64;
    ms
}

/// `n` with the word for it, so a line reads as English at one and at none.
fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {many}")
    }
}

impl fmt::Display for Motions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} held", plural(self.len(), "motion", "motions"))
    }
}

/// Motion documents on disk, for the tests on both sides of the delivery path.
#[cfg(test)]
pub(crate) mod fixtures {
    use std::path::Path;

    use reachy_clips::{ClipLimits, DEFAULT_BLEND_MS, STEP_MARGIN};
    use reachy_motion::FLOOR_TICK_HZ;
    use tempfile::TempDir;

    use super::Motions;
    use crate::report::Sink;

    /// The bounds a test derives against: the machine's defaults, since no
    /// bench configuration is resolved without a machine.
    pub fn limits() -> ClipLimits {
        ClipLimits::default()
    }

    /// An antennas-only clip of `frames` frames deriving exactly `max_speed`.
    ///
    /// The frames alternate between the neutral angle and one step off it, so
    /// the per-frame delta the loader measures is the whole of what bounds the
    /// speed, and the number in the document is the number that comes back out
    /// of the library.
    pub fn clip(name: &str, frames: usize, max_speed: f64) -> String {
        clip_with(name, frames, max_speed, fixture_blend_ms(frames))
    }

    /// The blend fields a document carries, rendered for splicing into one.
    ///
    /// `None` writes no fields at all, which is how a fixture reaches the
    /// loader's own defaulting rather than a number of its own.
    fn blend_fields(blend: Option<u64>) -> String {
        blend.map_or_else(String::new, |blend| {
            format!(r#""blend_in_ms": {blend}, "blend_out_ms": {blend},"#)
        })
    }

    /// The same clip authoring `blend` at either end rather than the ramp the
    /// fixtures default to.
    ///
    /// The ramp is a parameter rather than something a caller patches into the
    /// rendered text: a substitution that stops matching leaves a clip whose
    /// blend is the default while the test goes on asserting about the number
    /// it thought it wrote.
    pub fn clip_with(name: &str, frames: usize, max_speed: f64, blend: u64) -> String {
        let step = limits().max_step.antennas * STEP_MARGIN / max_speed;
        let track: Vec<String> = (0..frames)
            .map(|index| {
                let angle = f64::from(u8::try_from(index % 2).expect("0 or 1")) * step;
                format!("{{\"antennas\": [{angle}, 0.0]}}")
            })
            .collect();
        format!(
            r#"{{"version": 1, "kind": "clip", "name": "{name}",
                 "channels": ["antennas"], "frame_hz": {FLOOR_TICK_HZ},
                 "max_speed": {max_speed}, {}
                 "frames": [{}]}}"#,
            blend_fields(Some(blend)),
            track.join(",")
        )
    }

    /// The ramp a fixture clip of `frames` frames authors at either end.
    ///
    /// A ramp longer than its own clip is refused at load, and these fixtures
    /// run to a handful of frames, so the library's own default is taken only
    /// by a clip long enough to hold it. The same answer the loader reaches for
    /// an omitted ramp, held to it by
    /// `the_fixture_ramp_is_the_clips_own_span_capped_at_the_library_default`.
    pub fn fixture_blend_ms(frames: usize) -> u64 {
        let frames = u32::try_from(frames).expect("a small count");
        let span_ms = f64::from(frames) * 1000.0 / FLOOR_TICK_HZ;
        (span_ms as u64).min(u64::from(DEFAULT_BLEND_MS))
    }

    /// A head-only clip of `frames` frames lifting the head `dz` metres on
    /// alternate frames.
    ///
    /// The other half of [`clip`]: what a motion that says nothing at all about
    /// the antennas or the body does to a composition, and the mask that makes
    /// it say nothing.
    pub fn head_clip(name: &str, frames: usize, dz: f64) -> String {
        head_clip_with(name, frames, dz, Some(fixture_blend_ms(frames)))
    }

    /// The same head clip authoring `blend` at either end, or — at `None` —
    /// saying nothing about its ramps, so the loader's own default and the cap
    /// on it are what the document ends up with.
    pub fn head_clip_with(name: &str, frames: usize, dz: f64, blend: Option<u64>) -> String {
        let track: Vec<String> = (0..frames)
            .map(|index| {
                let lift = f64::from(u8::try_from(index % 2).expect("0 or 1")) * dz;
                format!("{{\"dt\": [0.0, 0.0, {lift}], \"dq\": [1.0, 0.0, 0.0, 0.0]}}")
            })
            .collect();
        format!(
            r#"{{"version": 1, "kind": "clip", "name": "{name}",
                 "channels": ["head"], "frame_hz": {FLOOR_TICK_HZ},
                 "max_speed": 1.0, {}
                 "frames": [{}]}}"#,
            blend_fields(blend),
            track.join(",")
        )
    }

    /// A directory holding `documents`, and the library loaded out of it.
    ///
    /// The real walk over real files, because what the daemon does at startup
    /// is read a directory and a test that skipped that would not be testing
    /// the startup.
    pub fn loaded(documents: &[(&str, String)], sink: &dyn Sink) -> (TempDir, Motions) {
        let dir = written(documents);
        let motions = Motions::load(dir.path(), &limits(), sink).expect("the directory reads");
        (dir, motions)
    }

    /// A directory holding `documents`, each written under its own file name.
    pub fn written(documents: &[(&str, String)]) -> TempDir {
        let dir = tempfile::tempdir().expect("a temporary directory");
        for (file, text) in documents {
            write(dir.path(), file, text);
        }
        dir
    }

    /// One file in `dir`.
    pub fn write(dir: &Path, file: &str, text: &str) {
        std::fs::write(dir.join(file), text).expect("the file writes");
    }
}

#[cfg(test)]
mod tests {
    use motion_proto::{MAX_MOTION_NAME_LEN, MAX_SPEED, MIN_SPEED, Play};

    use reachy_clips::DEFAULT_BLEND_MS;

    use super::fixtures::{
        clip, clip_with, fixture_blend_ms, head_clip_with, limits, loaded, write, written,
    };
    use super::*;
    use crate::report::Collect;

    /// The two crates hold the same global speed bounds and the same name
    /// bound, on opposite sides of the repository seam.
    ///
    /// `motion-proto` is the authoritative side — it is the wire, and both ends
    /// of the wire enforce it — and `reachy-clips` mirrors it because the two
    /// cannot cheaply share a dependency. This daemon is the one crate that
    /// depends on both, so it is the only place the mirror can be held to its
    /// original, and the commit that carries a changed constant across the seam
    /// is the commit that fails here if only one side moved.
    #[test]
    fn the_speed_and_name_bounds_are_the_same_on_both_sides_of_the_seam() {
        assert!((MIN_SPEED - reachy_clips::MIN_SPEED).abs() < f64::EPSILON);
        assert!((MAX_SPEED - reachy_clips::MAX_SPEED).abs() < f64::EPSILON);
        assert_eq!(MAX_MOTION_NAME_LEN, reachy_clips::MAX_MOTION_NAME_LEN);
    }

    /// A daemon whose configuration named no directory holds nothing, and says
    /// so to every name.
    #[test]
    fn a_daemon_with_no_clip_dir_holds_no_motions() {
        let motions = Motions::none();
        assert!(motions.is_empty());
        assert_eq!(motions.len(), 0);
        assert!(motions.window(&Play::new("pod/nod")).is_none());
        assert_eq!(motions.to_string(), "0 motions held");
    }

    /// The walk takes the documents and leaves everything else, and it reports
    /// what it loaded.
    #[test]
    fn the_directory_walk_loads_the_documents_and_leaves_the_sidecars() {
        let sink = Collect::default();
        let (dir, motions) = loaded(
            &[
                ("nod.json", clip("pod/nod", 8, 1.5)),
                ("wiggle.json", clip("pod/wiggle", 4, 2.0)),
            ],
            &sink,
        );
        // The importer copies a recording's sound through beside it; reading
        // one as a motion document would skip it noisily every startup.
        write(dir.path(), "nod.wav", "not a motion document");

        let reloaded =
            Motions::load(dir.path(), &limits(), &Collect::default()).expect("the directory reads");
        assert_eq!(motions.len(), 2);
        assert_eq!(reloaded.len(), 2);
        assert!(motions.motion("pod/nod").is_some());
        let fields = sink.fields("motion_library").expect("reported");
        assert_eq!(fields["motions"], json!(2));
        assert_eq!(fields["skipped"], json!(0));
        assert_eq!(fields["names"], json!(["pod/nod", "pod/wiggle"]));
    }

    /// One document that does not validate costs the machine that motion and
    /// nothing else.
    #[test]
    fn a_document_that_does_not_validate_is_named_and_skipped() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(
            &[
                ("good.json", clip("pod/nod", 8, 1.5)),
                ("bad.json", r#"{"version": 99, "kind": "clip"}"#.to_owned()),
            ],
            &sink,
        );

        assert_eq!(motions.len(), 1);
        assert!(motions.motion("pod/nod").is_some());
        let fields = sink.fields("motion_asset_skipped").expect("reported");
        assert!(
            fields["source"]
                .as_str()
                .expect("a source")
                .ends_with("bad.json"),
            "{fields}"
        );
        assert_eq!(
            sink.fields("motion_library").expect("reported")["skipped"],
            json!(1)
        );
    }

    /// A ramp the author wrote longer than the clip it fades is a content
    /// fault the library refuses, and it reaches this daemon as the same
    /// per-document skip a malformed file does.
    ///
    /// Held here rather than left to the library's own tests because the skip
    /// is where the two crates meet: the daemon's contract is that one bad file
    /// costs the machine that motion and nothing else, and a refusal class the
    /// library later promoted to a whole-load failure would break that contract
    /// here, silently.
    #[test]
    fn a_ramp_longer_than_its_own_clip_is_named_and_skipped() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(
            &[
                ("good.json", clip("pod/nod", 8, 1.5)),
                // Four frames is 80 ms of motion under a minute of fade — the
                // hundredfold typo the ceiling exists for.
                ("slow.json", clip_with("pod/slow", 4, 1.0, 60_000)),
            ],
            &sink,
        );

        assert_eq!(motions.len(), 1);
        assert!(motions.motion("pod/nod").is_some());
        assert!(motions.motion("pod/slow").is_none());
        let fields = sink.fields("motion_asset_skipped").expect("reported");
        assert!(
            fields["source"]
                .as_str()
                .expect("a source")
                .ends_with("slow.json"),
            "{fields}"
        );
        let error = fields["error"].as_str().expect("an error");
        assert!(error.contains("60000"), "{error}");
        assert!(error.contains("80"), "{error}");
        assert_eq!(
            sink.fields("motion_library").expect("reported")["skipped"],
            json!(1)
        );
    }

    /// A document that says nothing about its ramps gets the library's default,
    /// capped at the clip's own span — and that capped number is what the
    /// window this daemon computes carries.
    ///
    /// The fixtures all author their ramps, so without this the defaulting path
    /// the wire's timeline arithmetic depends on is exercised nowhere in the
    /// crate.
    #[test]
    fn an_omitted_ramp_takes_the_library_default_capped_at_the_clip() {
        let sink = Collect::default();
        // A small enough lift that no derived floor stretches either ramp, so
        // what comes back out is the cap and nothing else.
        let (_dir, motions) = loaded(
            &[
                ("short.json", head_clip_with("pod/short", 4, 0.0002, None)),
                ("long.json", head_clip_with("pod/long", 40, 0.0002, None)),
            ],
            &sink,
        );

        let short = motions.window(&Play::new("pod/short")).expect("held");
        assert_eq!(short.duration_ms, 80);
        assert_eq!(short.blend_out_ms, 80);
        let long = motions.window(&Play::new("pod/long")).expect("held");
        assert_eq!(long.duration_ms, 800);
        assert_eq!(long.blend_out_ms, u64::from(DEFAULT_BLEND_MS));
    }

    /// The ramp the fixtures author is the same answer the library reaches for
    /// a document that authors none: the clip's own span, capped at the default.
    ///
    /// `fixture_blend_ms` re-derives the library's clip duration and its default
    /// on this side of the seam, so the two can drift and leave every fixture
    /// authoring a ramp its test's comment no longer describes. This holds the
    /// local formula to the loaded clip's own numbers.
    #[test]
    fn the_fixture_ramp_is_the_clips_own_span_capped_at_the_library_default() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(
            &[
                ("short.json", head_clip_with("pod/short", 4, 0.0002, None)),
                ("long.json", head_clip_with("pod/long", 40, 0.0002, None)),
            ],
            &sink,
        );

        for (name, frames) in [("pod/short", 4), ("pod/long", 40)] {
            let motion = motions.motion(name).expect("held");
            let span_ms = ms_ceil(motion.duration_s());
            assert_eq!(
                fixture_blend_ms(frames),
                span_ms.min(u64::from(DEFAULT_BLEND_MS)),
                "{name}"
            );
            // The loader's own defaulting reaches the same number, which is
            // what makes the fixtures' ramps lawful under the ceiling.
            assert_eq!(u64::from(motion.blend_out_ms()), fixture_blend_ms(frames));
        }
    }

    /// What a load changed about an asset it accepted is said too: a ramp the
    /// document asked for and the machine's step bounds would not allow is a
    /// difference between the file and what plays.
    #[test]
    fn a_stretched_ramp_is_reported_against_the_asset() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(&[("brisk.json", clip_with("pod/brisk", 8, 2.0, 1))], &sink);

        assert_eq!(motions.len(), 1);
        let fields = sink.fields("motion_asset_note").expect("reported");
        assert_eq!(fields["name"], json!("pod/brisk"));
        assert!(
            fields["note"]
                .as_str()
                .expect("a note")
                .contains("stretched"),
            "{fields}"
        );
    }

    /// A directory the configuration names and the filesystem does not have is
    /// a startup refusal, not a quietly empty library.
    #[test]
    fn a_clip_dir_that_is_not_there_refuses() {
        let dir = written(&[]);
        let missing = dir.path().join("no-such-directory");
        let error = Motions::load(&missing, &limits(), &Collect::default())
            .expect_err("a directory that is not there");
        assert!(error.to_string().contains("no-such-directory"), "{error}");
    }

    /// A file the walk selected and cannot read is a skip like a malformed one:
    /// the rest of the library still loads.
    #[test]
    fn a_document_that_cannot_be_read_is_a_skip() {
        let sink = Collect::default();
        let dir = written(&[("good.json", clip("pod/nod", 8, 1.5))]);
        // A directory named `*.json` is selected by the walk and answers the
        // read with an error rather than text.
        std::fs::create_dir(dir.path().join("wat.json")).expect("a directory");

        let motions = Motions::load(dir.path(), &limits(), &sink).expect("the directory reads");
        assert_eq!(motions.len(), 1);
        assert_eq!(
            sink.fields("motion_library").expect("reported")["skipped"],
            json!(1)
        );
    }

    /// The window a play step occupies: the motion's own clock, and the fade
    /// that follows it.
    #[test]
    fn a_windows_two_numbers_are_the_duration_and_the_fade() {
        let sink = Collect::default();
        // Eight frames at the floor tick rate is 160 ms, and the fixture's fade
        // is that same span — the longest a clip this short may author.
        let (_dir, motions) = loaded(&[("nod.json", clip("pod/nod", 8, 1.5))], &sink);

        let window = motions.window(&Play::new("pod/nod")).expect("held");
        assert_eq!(window.duration_ms, 160);
        assert_eq!(window.blend_out_ms, 160);
        // The clock scales with the speed and the fade does not.
        assert_eq!(window.span_ms(1.0), 320);
        assert_eq!(window.span_ms(2.0), 240);
    }

    /// The screen a script passes before the schedule sees it.
    #[test]
    fn a_script_playing_a_held_motion_at_a_lawful_speed_passes() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(&[("nod.json", clip("pod/nod", 8, 1.5))], &sink);

        motions
            .screen(&playing(&[("pod/nod", 1.5, 100)]))
            .expect("held, in bounds, and one at a time");
    }

    /// A name nothing holds refuses the whole script, and names the step.
    #[test]
    fn a_name_the_library_does_not_hold_refuses_the_script() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(&[("nod.json", clip("pod/nod", 8, 1.5))], &sink);

        let error = motions
            .screen(&playing(&[("pod/nod", 1.0, 100), ("pod/absent", 1.0, 400)]))
            .expect_err("a name nothing holds");
        assert_eq!(error.reason(), "no_such_motion");
        assert!(error.to_string().contains("pod/absent"), "{error}");
    }

    /// An invocation above a motion's own ceiling is refused rather than
    /// slowed to it.
    #[test]
    fn a_speed_above_the_motions_ceiling_refuses_the_script() {
        let sink = Collect::default();
        let (_dir, motions) = loaded(&[("nod.json", clip("pod/nod", 8, 1.5))], &sink);

        let error = motions
            .screen(&playing(&[("pod/nod", 2.0, 100)]))
            .expect_err("faster than the clip may be played");
        assert_eq!(error.reason(), "over_speed");
        assert!(error.to_string().contains("pod/nod"), "{error}");
        // Its own ceiling exactly is lawful; the slack is for the arithmetic,
        // not for the machine.
        motions
            .screen(&playing(&[("pod/nod", 1.5, 100)]))
            .expect("at the ceiling");
    }

    /// More overlays at once than the machine composes is refused at the
    /// instant the count is reached.
    #[test]
    fn more_overlays_at_once_than_the_machine_composes_refuses_the_script() {
        let sink = Collect::default();
        // A long motion, so five started a tick apart all overlap.
        let (_dir, motions) = loaded(&[("long.json", clip("pod/long", 100, 1.0))], &sink);

        let plays: Vec<(&str, f64, u64)> = (0..5)
            .map(|index| ("pod/long", 1.0, 100 + index * 10))
            .collect();
        let error = motions
            .screen(&playing(&plays))
            .expect_err("five at once, where four is the cap");
        assert_eq!(error.reason(), "too_many_overlays");
    }

    /// A posture-only script is not the library's business, and passes a daemon
    /// that holds nothing.
    #[test]
    fn a_posture_only_script_passes_an_empty_library() {
        let script = MotionScript::new(
            "reachy00",
            1,
            vec![motion_proto::Step::new(0, motion_proto::Posture::Up)],
            5_000,
        )
        .expect("a valid script");
        Motions::none()
            .screen(&script)
            .expect("no overlays to screen");
    }

    /// A script that raises the head and then plays each named motion at the
    /// named speed and offset.
    fn playing(plays: &[(&str, f64, u64)]) -> MotionScript {
        let mut steps = vec![motion_proto::Step::new(0, motion_proto::Posture::Up)];
        for (name, speed, after_ms) in plays {
            steps.push(motion_proto::Step::play(
                *after_ms,
                Play::at_speed(*name, *speed),
            ));
        }
        MotionScript::new("reachy00", 1, steps, 60_000).expect("a valid script")
    }
}
