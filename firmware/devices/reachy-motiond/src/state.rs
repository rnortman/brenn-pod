//! The one file that says whether this daemon is alive or alive-but-dead.
//!
//! A parked daemon does not exit — a fault is never auto-cleared, and a process
//! that exited would let `Restart=on-failure` re-torque a machine nobody has
//! looked at. The cost of that decision is a service which `systemctl is-active`
//! calls running while it commands nothing at all, over a journal that has gone
//! silent. A robot in that state answering "ready" is the failure this module
//! exists to end.
//!
//! So the daemon states its state, in one file, and the status probe reads it:
//!
//! ```text
//! state=starting|resting|active|parked|stopping
//! watch=ok|failing
//! fault_stage=the motion loop     # parked only
//! fault_detail=servo 4: timed out # parked only
//! updated_unix=1765238400
//! ```
//!
//! Three properties, each load-bearing:
//!
//! - **Whole or not at all.** Every transition writes a temporary file beside
//!   the real one and renames it over the top, so a reader either sees the
//!   previous record or the next one and never half of either. A status command
//!   that read `state=par` would be worse than one that read nothing.
//! - **Nothing depends on it.** Every write is best-effort: a failure narrates
//!   once and the run carries on. This surface is downstream of the fault
//!   doctrine and never in front of it — a torque-off write is never gated,
//!   delayed or conditioned on a file getting written, and the fault path
//!   publishes here only once torque is already off.
//! - **It cannot go stale.** The file lives under the unit's
//!   `RuntimeDirectory`, which systemd creates on start and removes on stop, so
//!   a crashed or stopped service leaves no `parked` behind for the next run to
//!   be judged by. The path is a parameter rather than a constant reached for
//!   inside, so the decisions here are assertable against a temporary directory.
//!
//! A supervised foreground run has no `RuntimeDirectory`, so it usually cannot
//! write here at all. That is the best-effort case working as intended: the
//! operator is watching the narration, which is the surface this one stands in
//! for.

use std::cell::{Cell, RefCell};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bus::one_line;
use crate::cells::FaultStage;
use crate::report::Sink;

/// Where the daemon writes its state under systemd.
///
/// Under `/run` because it is tmpfs — nothing this daemon writes reaches the
/// device's flash — and under the unit's own `RuntimeDirectory` so the file's
/// lifetime is the service's.
pub const DEFAULT_PATH: &str = "/run/reachy-motiond/state";

/// What the replacement is staged as, beside the file it replaces.
///
/// The same directory, because a rename is only atomic within one filesystem.
/// One writer means one fixed name is enough.
const STAGING_SUFFIX: &str = ".new";

/// Where the daemon is, in the one word a probe judges on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Commissioning and the startup look: the machine has not been taken and
    /// the loop has not begun. A probe treats this as not-yet-ready rather than
    /// as either answer.
    Starting,
    /// The minimum risk condition, watched. Torque is off and a script asking
    /// for the head up is what ends it.
    Resting,
    /// Torque is on and the running script's timeline is being executed.
    Active,
    /// A fault has stopped the daemon commanding. Torque is off, the port is
    /// held, and nothing but an operator changes this.
    Parked,
    /// A stop has been asked for; the machine is being folded and released.
    Stopping,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Resting => "resting",
            Self::Active => "active",
            Self::Parked => "parked",
            Self::Stopping => "stopping",
        }
    }
}

/// Whether the pre-torque sweeps are answering.
///
/// Its own field rather than a phase, because it is orthogonal to one: a limp
/// machine nobody can read is still resting, and it is still safe. What it costs
/// is presence — no script will raise a head whose posture nobody has measured —
/// so it is a degraded state a probe has to be able to see without inferring it
/// from a silent journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Watching {
    /// Sweeps are answering.
    Ok,
    /// Sweeps have stopped answering, and are still being taken.
    Failing,
}

impl Watching {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failing => "failing",
        }
    }
}

/// What the file says, as the daemon holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stated {
    phase: Phase,
    watching: Watching,
    /// The fault, once there is one: present only while parked, because it is
    /// the one thing an operator needs before deciding anything.
    fault: Option<(FaultStage, String)>,
}

/// The state file, and the daemon's own copy of what is in it.
///
/// Not one of the [`crate::cells`]: those are where the two threads meet, and
/// this is written by the motion thread alone — the thread that owns the port
/// and therefore owns every transition worth reporting. Nothing here is `Sync`
/// and nothing here needs to be.
#[derive(Debug)]
pub struct Surface {
    path: PathBuf,
    /// What was last published, so a transition that changes nothing writes
    /// nothing. The resting loop passes through the same phase every
    /// `rest_poll`.
    ///
    /// `None` until the first transition: a surface holds no opinion about a
    /// process that has not said anything yet, which is what makes the very
    /// first `starting` a change like any other.
    stated: RefCell<Option<Stated>>,
    /// Whether a write failure has already been narrated. Once per run: a
    /// directory that is not there stays not there, and a line per transition
    /// would bury the narration this daemon is actually read for.
    complained: Cell<bool>,
}

impl Surface {
    /// A surface writing to `path`, which is not touched until something
    /// transitions.
    ///
    /// Nothing is written by construction: what the daemon is doing before it
    /// says `starting` is reading text files, and a file claiming a state the
    /// process has not reached is exactly the lie this surface must not tell.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            stated: RefCell::new(None),
            complained: Cell::new(false),
        }
    }

    /// A surface at `path` that has already said the daemon is starting.
    ///
    /// The opening move, and it belongs before the first thing that takes real
    /// time — commissioning — rather than beside the loop: a probe arriving
    /// during it should read `starting`, not find no file and have to guess
    /// between a daemon coming up and one too old to say anything. Written once
    /// and taking its path, so a test drives the same opening the binary does
    /// against a directory of its own; left in the binary it would be the one
    /// ordering nothing could check.
    pub fn opening(path: impl Into<PathBuf>, sink: &dyn Sink) -> Self {
        let surface = Self::at(path);
        surface.phase(Phase::Starting, sink);
        surface
    }

    /// The daemon has reached `phase`.
    pub fn phase(&self, phase: Phase, sink: &dyn Sink) {
        self.transition(sink, |stated| stated.phase = phase);
    }

    /// The pre-torque sweeps have stopped answering, or have come back.
    pub fn watching(&self, watching: Watching, sink: &dyn Sink) {
        self.transition(sink, |stated| stated.watching = watching);
    }

    /// The machine stopped taking commands and the daemon has parked.
    ///
    /// Called after torque is already off, never before: reaching the minimum
    /// risk condition is what happens first, and saying so is what happens
    /// after.
    pub fn parked(&self, stage: FaultStage, detail: &str, sink: &dyn Sink) {
        self.transition(sink, |stated| {
            stated.phase = Phase::Parked;
            stated.fault = Some((stage, one_line(detail)));
        });
    }

    /// Apply `change` to what is held, and publish it if it said anything new.
    fn transition(&self, sink: &dyn Sink, change: impl FnOnce(&mut Stated)) {
        let text = {
            let mut held = self.stated.borrow_mut();
            let before = held.clone();
            let stated = held.get_or_insert(Stated {
                phase: Phase::Starting,
                watching: Watching::Ok,
                fault: None,
            });
            change(stated);
            if before.as_ref() == Some(&*stated) {
                return;
            }
            render(stated, stamp())
        };
        self.publish(&text, sink);
    }

    /// Put the record into the file, whole, or say once that it could not be.
    fn publish(&self, text: &str, sink: &dyn Sink) {
        let Err(error) = replace(&self.path, text) else {
            return;
        };
        if self.complained.replace(true) {
            return;
        }
        sink.line(&format!(
            "the state file at {} cannot be written: {error}. nothing about the machine depends \
             on it, so this run carries on without one — but `reachy-status` cannot tell this \
             daemon's state from a dead one.",
            self.path.display()
        ));
    }
}

/// The record, as one whole file.
///
/// Separate from the write so the shape a probe parses is assertable without a
/// filesystem. `key=value`, one per line: the reader is a shell script, and this
/// is the one format it needs no tooling to read.
fn render(stated: &Stated, updated_unix: u64) -> String {
    let mut text = format!(
        "state={}\nwatch={}\n",
        stated.phase.as_str(),
        stated.watching.as_str()
    );
    if let Some((stage, detail)) = &stated.fault {
        // Through the narration sanitizer: a fault detail carries whatever the
        // motion libraries rendered, and a newline in it would forge a key in a
        // file whose reader splits on lines.
        text.push_str(&format!("fault_stage={stage}\nfault_detail={detail}\n"));
    }
    text.push_str(&format!("updated_unix={updated_unix}\n"));
    text
}

/// Wall-clock seconds, for a reader outside this process.
///
/// Not the monotonic clock the schedule runs on: this number's only job is to
/// let a human compare the file against a journal, and a monotonic instant means
/// nothing to either.
fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

/// Put `text` at `path` whole: stage it beside, then rename over the top.
///
/// A reader that opened the file mid-write would otherwise see a truncated
/// record, and `state=par` read as a state is worse than no file at all. A
/// rename cannot be seen half-done, so a reader gets the old record or the new
/// one.
fn replace(path: &Path, text: &str) -> io::Result<()> {
    let mut staged = path.as_os_str().to_owned();
    staged.push(STAGING_SUFFIX);
    let staged = PathBuf::from(staged);
    fs::write(&staged, text)?;
    if let Err(error) = fs::rename(&staged, path) {
        // The staged copy is not a record of anything; leaving it would be a
        // second file for a reader to find.
        let _ = fs::remove_file(&staged);
        return Err(error);
    }
    Ok(())
}

/// One key out of a written surface, for tests on both sides of the write.
#[cfg(test)]
pub(crate) fn value_of(path: &Path, key: &str) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .map(str::to_owned)
}

/// A directory that lasts as long as the value does.
///
/// The state file's decisions are about a filesystem, so the tests are too.
#[cfg(test)]
pub(crate) fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("a temporary directory")
}

/// The state file's path inside `dir`. The directory exists; the file does not
/// until something publishes.
#[cfg(test)]
pub(crate) fn state_in(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("state")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    use super::*;
    use crate::report::Collect;

    fn state(path: &Path) -> Option<String> {
        value_of(path, "state")
    }

    /// The ordinary sequence, and the property that makes the file worth
    /// reading: each transition replaces the whole record, so what is there is
    /// what the daemon is doing and not an accumulation of what it has done.
    #[test]
    fn each_transition_replaces_the_whole_record() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        assert_eq!(state(&path), None, "nothing is written by construction");

        surface.phase(Phase::Starting, &sink);
        assert_eq!(state(&path).as_deref(), Some("starting"));
        surface.phase(Phase::Resting, &sink);
        assert_eq!(state(&path).as_deref(), Some("resting"));
        surface.phase(Phase::Active, &sink);
        assert_eq!(state(&path).as_deref(), Some("active"));
        surface.phase(Phase::Stopping, &sink);
        assert_eq!(state(&path).as_deref(), Some("stopping"));

        assert_eq!(
            value_of(&path, "watch").as_deref(),
            Some("ok"),
            "the watch flag rides every record"
        );
        assert!(
            value_of(&path, "updated_unix").is_some_and(|value| value.parse::<u64>().is_ok()),
            "the stamp is seconds a reader can compare against a journal"
        );
        assert_eq!(
            value_of(&path, "fault_stage"),
            None,
            "a daemon that has not faulted says nothing about a fault"
        );
        assert!(
            sink.said().lines.is_empty(),
            "a write that worked is silent"
        );
    }

    /// Opening the surface says `starting` there and then.
    ///
    /// The whole point of the phase: it is published before commissioning, the
    /// first thing in a run that takes real time, so the window in which a probe
    /// finds no file at all is milliseconds rather than the whole ceremony. Move
    /// the publish after the slow part and the probe reports "written no state,
    /// redeploy if it persists" on every boot — the misleading answer for the
    /// one state that is entirely fine.
    #[test]
    fn opening_the_surface_says_the_daemon_is_starting() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();

        let surface = Surface::opening(&path, &sink);

        assert_eq!(state(&path).as_deref(), Some("starting"));
        surface.phase(Phase::Resting, &sink);
        assert_eq!(state(&path).as_deref(), Some("resting"));
    }

    /// The path the daemon writes and the path the tools read are one contract
    /// across two languages, joined by nothing but this literal. The shell half
    /// pins the same string against `lib.sh` in
    /// `firmware/tools/deploy-reachy-motiond.test.sh`.
    #[test]
    fn the_default_path_is_the_one_the_tools_read() {
        assert_eq!(DEFAULT_PATH, "/run/reachy-motiond/state");
    }

    /// The line that stops a dead robot answering ready: the stage and the
    /// detail an operator needs before deciding anything, in the file rather
    /// than only in a journal that has gone silent.
    #[test]
    fn a_parked_daemon_publishes_what_stopped_it() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.phase(Phase::Active, &sink);
        surface.parked(FaultStage::Motion, "servo 4: timed out", &sink);

        assert_eq!(state(&path).as_deref(), Some("parked"));
        assert_eq!(
            value_of(&path, "fault_stage").as_deref(),
            Some("the motion loop")
        );
        assert_eq!(
            value_of(&path, "fault_detail").as_deref(),
            Some("servo 4: timed out")
        );
    }

    /// The detail is whatever the motion libraries rendered, and this file's
    /// reader splits on lines: a newline in it would forge a key.
    #[test]
    fn a_fault_detail_cannot_forge_a_line() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.parked(FaultStage::Engage, "servo 4 said\nstate=resting", &sink);

        assert_eq!(state(&path).as_deref(), Some("parked"));
        assert_eq!(
            value_of(&path, "fault_detail").as_deref(),
            Some("servo 4 said state=resting"),
            "the newline became a space rather than a second record"
        );
    }

    /// A failing watch is a state of its own and not a phase: the machine is
    /// resting, safely, and cannot raise its head. Both facts are in one record.
    #[test]
    fn the_watch_flag_flips_without_disturbing_the_phase() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.phase(Phase::Resting, &sink);
        surface.watching(Watching::Failing, &sink);
        assert_eq!(state(&path).as_deref(), Some("resting"));
        assert_eq!(value_of(&path, "watch").as_deref(), Some("failing"));

        surface.watching(Watching::Ok, &sink);
        assert_eq!(state(&path).as_deref(), Some("resting"));
        assert_eq!(value_of(&path, "watch").as_deref(), Some("ok"));
    }

    /// The resting loop passes the same phase every `rest_poll`. A file rewritten
    /// ten times a second for the life of a quiet robot would be churn nobody
    /// asked for, so a transition that changes nothing is not one.
    #[test]
    fn a_transition_that_changes_nothing_writes_nothing() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.phase(Phase::Resting, &sink);
        fs::remove_file(&path).expect("the record that was written");
        surface.phase(Phase::Resting, &sink);

        assert_eq!(state(&path), None, "the same phase was published twice");
    }

    /// A reader is a status probe on another schedule entirely, and there is no
    /// lock between them. A rename cannot be observed half-done; a truncating
    /// write can, and `state=par` read as a state is worse than no file.
    #[test]
    fn a_reader_never_sees_half_a_record() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);
        surface.phase(Phase::Resting, &sink);

        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let path = path.clone();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut reads = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    let Ok(text) = fs::read_to_string(&path) else {
                        continue;
                    };
                    reads += 1;
                    let phase = text
                        .lines()
                        .find_map(|line| line.strip_prefix("state="))
                        .unwrap_or("");
                    assert!(
                        ["resting", "active"].contains(&phase),
                        "a partial record was read: {text:?}"
                    );
                    assert!(
                        text.contains("updated_unix="),
                        "a record was read without its last line: {text:?}"
                    );
                }
                reads
            })
        };

        for _ in 0..200 {
            surface.phase(Phase::Active, &sink);
            surface.phase(Phase::Resting, &sink);
        }
        stop.store(true, Ordering::Relaxed);

        let reads = reader.join().expect("the reader end runs");
        assert!(reads > 0, "the reader never saw the file at all");
    }

    /// A supervised run has no `RuntimeDirectory`, so this is the ordinary case
    /// there rather than an exceptional one: say it once, and carry on. The
    /// alternative — a daemon that will not run without somewhere to write a
    /// status file — is a surface gating motion, which is the one thing it may
    /// never do.
    #[test]
    fn nowhere_to_write_is_said_once_and_changes_nothing() {
        let dir = temp_dir();
        let path = state_in(&dir).join("no-such-directory").join("state");
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.phase(Phase::Resting, &sink);
        surface.watching(Watching::Failing, &sink);
        surface.phase(Phase::Active, &sink);
        surface.parked(FaultStage::Motion, "servo 4: timed out", &sink);

        let lines = sink.said().lines;
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("cannot be written"), "{lines:?}");
        assert!(!path.exists());
    }

    /// A staged copy left behind would be a second file for a reader to find,
    /// and the reader is a shell glob away from finding it.
    #[test]
    fn the_staging_copy_does_not_outlive_the_write() {
        let dir = temp_dir();
        let path = state_in(&dir);
        let sink = Collect::default();
        let surface = Surface::at(&path);

        surface.phase(Phase::Resting, &sink);

        let mut staged = path.as_os_str().to_owned();
        staged.push(STAGING_SUFFIX);
        assert!(!Path::new(&staged).exists());
    }

    /// The format a shell probe parses, pinned here rather than inferred from a
    /// filesystem: keys in a fixed order, one per line, and no fault keys unless
    /// there is a fault.
    #[test]
    fn the_record_is_key_equals_value_one_per_line() {
        let resting = Stated {
            phase: Phase::Resting,
            watching: Watching::Ok,
            fault: None,
        };
        assert_eq!(
            render(&resting, 1_765_238_400),
            "state=resting\nwatch=ok\nupdated_unix=1765238400\n"
        );

        let parked = Stated {
            phase: Phase::Parked,
            watching: Watching::Failing,
            fault: Some((FaultStage::Startup, "servo 4: timed out".to_owned())),
        };
        assert_eq!(
            render(&parked, 1_765_238_401),
            "state=parked\nwatch=failing\nfault_stage=startup normalisation\n\
             fault_detail=servo 4: timed out\nupdated_unix=1765238401\n"
        );
    }
}
