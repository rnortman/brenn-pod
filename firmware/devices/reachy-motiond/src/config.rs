//! The daemon's configuration file: which machine, whose scripts, and on what
//! bus.
//!
//! One TOML file, named on the command line, `deny_unknown_fields` throughout —
//! a mistyped key is a startup refusal naming the key rather than a daemon
//! quietly running on a default nobody chose.
//!
//! The machine half of the configuration is deliberately *not* here.
//! `motion_config` names the same bench TOML the operator tool reads on this
//! unit, so the crank datum, the envelope and the bus timing have exactly one
//! source of truth on the machine. Two files describing one platform is two
//! files to disagree, and the disagreement would be about the numbers that keep
//! the head out of the linkage's singular configurations.
//!
//! The move durations are the one exception, and they are an exception because
//! they are not a fact about the platform: how briskly a head acknowledges a
//! wake word is presence policy, tuned by whoever is living with the machine,
//! while the bench file's values are what an operator watching a single command
//! wants. The five optional `*_duration_s` keys here override the bench file's
//! for this daemon only; absent, the bench file governs, so there is still one
//! number and not two unless somebody deliberately wrote a second.
//!
//! What is here otherwise is only what the daemon adds: whose scripts to obey,
//! which channel they arrive on, how often the motion loop comes up for air, and
//! the bridge's own table nested whole.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

/// The longest the motion loop watches the machine before it looks at the
/// schedule again, when the file does not say.
///
/// A ceiling and not a period: a dwell is cut short by the running script's next
/// step or its expiry, so this is what bounds the wait when nothing is
/// scheduled — the reaction latency to a script arriving, and the cadence at
/// which an idle machine is monitored for a fault. Short enough that nobody
/// perceives the lag, long enough that the monitoring is not the dominant
/// traffic on the wire.
const fn default_hold_dwell_ms() -> u64 {
    200
}

/// How often the resting watch sweeps a limp machine, when the file does not
/// say.
///
/// Nine position reads, plus the supply and the error bits on the slower
/// cadence the bench configuration sets. It bounds two things: how stale the
/// pose an engage plans from can be when a hand has moved the head, and how
/// long a script asking for the head up waits before anything happens. Ten a
/// second is imperceptible on the second and a fraction of the wire's capacity
/// on the first.
const fn default_rest_poll_ms() -> u64 {
    100
}

/// How long the machine holds at stow before torque comes off, when the file
/// does not say.
///
/// The quick-follow-up window: a wake inside it retargets the head up with no
/// release and no engage in between. Long enough to cover the gap between one
/// turn's stow and the next turn's wake in a real conversation, short enough
/// that the machine is not left torqued at stow — this platform's only pinch
/// hazard — for any longer than that buys something.
const fn default_rest_delay_ms() -> u64 {
    5_000
}

/// The longest rest delay the daemon will run on.
///
/// Every other timing here is refused for being too small, because too small is
/// what wedges a loop. This one is refused for being too large, because what it
/// buys — a follow-up wake that costs no release and no engage — is spent within
/// a couple of seconds of a turn ending, while what it costs is the machine
/// sitting torqued at stow, which is this platform's only pinch hazard. A minute
/// is a dozen times the useful window and still well short of a
/// seconds-versus-milliseconds slip, which is what this exists to catch.
const MAX_REST_DELAY_MS: u64 = 60_000;

/// The daemon's configuration, as the file is written.
///
/// Comparable whole so the shipped example can be asserted against a minimal
/// file field by field rather than key by key: the example is what an operator
/// copies, and a key added here without a default it actually writes is the
/// failure a file of that shape invites.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The bench configuration for this unit — the one the operator tool reads,
    /// not a copy.
    pub motion_config: PathBuf,
    /// The pod identity whose scripts this daemon obeys. Bodies addressed to any
    /// other pod are reported and dropped — the channel is not assumed to carry
    /// one machine's traffic.
    pub pod: String,
    /// The channel motion scripts arrive on. No default: which channel a
    /// deployment uses is operator topology, and a name invented here would be
    /// a convention two ends could silently disagree about.
    pub channel: String,
    /// The longest the motion loop watches an engaged machine before it looks
    /// at the schedule again.
    #[serde(default = "default_hold_dwell_ms")]
    pub hold_dwell_ms: u64,
    /// How often the resting watch sweeps a limp machine.
    #[serde(default = "default_rest_poll_ms")]
    pub rest_poll_ms: u64,
    /// How long the machine holds at stow before torque comes off.
    #[serde(default = "default_rest_delay_ms")]
    pub rest_delay_ms: u64,
    /// How long the raise takes, seconds, head group. Absent leaves the bench
    /// configuration's value governing.
    #[serde(default)]
    pub up_duration_s: Option<f64>,
    /// How long the fold takes, seconds, head group. Absent leaves the bench
    /// configuration's value governing.
    #[serde(default)]
    pub stow_duration_s: Option<f64>,
    /// How long the antennas take on either move, seconds. Absent leaves the
    /// bench configuration's value governing — and if that says nothing either,
    /// the antennas run on whichever head-group clock the move is using.
    #[serde(default)]
    pub antenna_duration_s: Option<f64>,
    /// The right antenna's own clock, seconds, taking precedence over
    /// `antenna_duration_s` for that side alone.
    ///
    /// The pair is the one place on this machine where two joints can reach the
    /// same piece of air: their tips cross inboard, and a pair sweeping
    /// mirror-symmetrically meets there. Two clocks is how they are parted.
    #[serde(default)]
    pub antenna_duration_right_s: Option<f64>,
    /// The left antenna's own clock, seconds, taking precedence over
    /// `antenna_duration_s` for that side alone. See
    /// `antenna_duration_right_s`.
    #[serde(default)]
    pub antenna_duration_left_s: Option<f64>,
    /// The bus attachment. Nested whole so there is one description of a bridge
    /// and every embedder writes the same table.
    pub bridge: brenn_bridge::Config,
}

/// The move durations the daemon's file states, absent where it states nothing.
///
/// Answers and not numbers: what the file says is only half of a duration, the
/// other half being the bench configuration it overrides, and the resolution of
/// the two lives with the machine ([`crate::motion::Clocks`]) so that what a
/// move actually runs at is decided once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Overrides {
    /// The raise's head-group clock.
    pub up: Option<Duration>,
    /// The fold's head-group clock.
    pub stow: Option<Duration>,
    /// The antennas' shared clock, on either move.
    pub antennas: Option<Duration>,
    /// Each antenna's own clock, right then left, taking precedence over the
    /// shared one for that side.
    pub antenna_sides: [Option<Duration>; 2],
}

/// A duration a `*_duration_s` key states, or `None` where it states nothing.
///
/// A value no `Duration` can hold answers `None` as well. That case never
/// reaches a running daemon — [`Config::validate`] refuses it by name, and
/// [`Config::load`] validates — so the only caller that can see one is holding a
/// configuration that was parsed and never checked.
fn stated(secs: Option<f64>) -> Option<Duration> {
    Duration::try_from_secs_f64(secs?).ok()
}

/// Refuse a move duration nothing could run on.
///
/// Zero is refused here where it is lawful for the rest delay, and for the
/// opposite reason: a rest delay of nothing means let go at once, while a move of
/// nothing asks for the whole span inside one control period, which is not a move
/// anybody wants and almost always a units slip.
fn check_duration(key: &str, secs: Option<f64>) -> Result<(), String> {
    let Some(secs) = secs else { return Ok(()) };
    if !secs.is_finite() || secs <= 0.0 || Duration::try_from_secs_f64(secs).is_err() {
        return Err(format!(
            "{key} is {secs}; a move duration is a positive number of seconds, and omitting the \
             key leaves the bench configuration's value governing"
        ));
    }
    Ok(())
}

/// Why a configuration could not be used.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file is not there, or cannot be read as this account.
    #[error("cannot read the daemon configuration at {path}")]
    Read {
        /// The file named on the command line.
        path: PathBuf,
        /// What the read failed with.
        source: std::io::Error,
    },
    /// The file is not the TOML this daemon expects — a missing key, a key
    /// nothing reads, or a value of the wrong shape.
    #[error("cannot parse the daemon configuration at {path}")]
    Parse {
        /// The file named on the command line.
        path: PathBuf,
        /// What the parser refused, line and column included.
        source: toml::de::Error,
    },
    /// The file parsed and says something the daemon will not run on.
    #[error("{path}: {message}")]
    Invalid {
        /// The file named on the command line.
        path: PathBuf,
        /// Which value, and why it is refused.
        message: String,
    },
}

impl Config {
    /// Read, parse and validate the configuration at `path`.
    ///
    /// Nothing here opens a port, reads the bench configuration or touches the
    /// token file: this is the whole of what can be decided from text, and it is
    /// decided before the daemon acquires anything.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config = Self::parse(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate().map_err(|message| ConfigError::Invalid {
            path: path.to_path_buf(),
            message,
        })?;
        Ok(config)
    }

    /// Parse a configuration from TOML text. Semantic validation is separate —
    /// see [`Config::validate`].
    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    /// The checks the TOML grammar cannot express.
    ///
    /// Each one rejects a value whose runtime behaviour is a wedge rather than a
    /// slow daemon: a channel nothing can deliver on, an identity nothing can
    /// match, a dwell the loop would spin on. The rest delay is refused from the
    /// other end as well, because too long a one is not slow — it is the machine
    /// held torqued at its only pinch posture.
    pub fn validate(&self) -> Result<(), String> {
        if self.motion_config.as_os_str().is_empty() {
            return Err(
                "motion_config is empty; name the bench configuration for this unit".to_string(),
            );
        }
        if self.pod.is_empty() {
            return Err(
                "pod is empty; name the identity whose motion scripts this daemon obeys"
                    .to_string(),
            );
        }
        // The grammar is the bridge's — it is that transport a `local:` address
        // never crosses — so it is answered by the bridge rather than copied
        // here and left to drift from the other end's copy.
        brenn_bridge::validate_channel_name(&self.channel)
            .map_err(|refusal| format!("channel {refusal}"))?;
        if self.hold_dwell_ms == 0 {
            return Err(
                "hold_dwell_ms must be at least 1 (0 reads the schedule in a spin loop)"
                    .to_string(),
            );
        }
        if self.rest_poll_ms == 0 {
            return Err(
                "rest_poll_ms must be at least 1 (0 sweeps the servo bus as fast as it will \
                 answer)"
                    .to_string(),
            );
        }
        if self.rest_delay_ms > MAX_REST_DELAY_MS {
            return Err(format!(
                "rest_delay_ms is {}; at most {MAX_REST_DELAY_MS} (it holds the machine torqued \
                 at stow after every turn, which is this platform's only pinch hazard)",
                self.rest_delay_ms
            ));
        }
        // Positive and placeable, and nothing further. A duration under the
        // per-tick step bound's floor is a move that faults partway and
        // de-torques, which is bad tuning and not a wedge — the floors are
        // documented beside the keys in the example, and this daemon does not
        // hold the numbers they are derived from anyway: the step bounds and the
        // tick rate are the machine's file, read later and by somebody else.
        check_duration("up_duration_s", self.up_duration_s)?;
        check_duration("stow_duration_s", self.stow_duration_s)?;
        check_duration("antenna_duration_s", self.antenna_duration_s)?;
        check_duration("antenna_duration_right_s", self.antenna_duration_right_s)?;
        check_duration("antenna_duration_left_s", self.antenna_duration_left_s)?;
        self.bridge.validate()
    }

    /// The longest the motion loop watches an engaged machine before it looks
    /// at the schedule again.
    #[must_use]
    pub fn hold_dwell(&self) -> Duration {
        Duration::from_millis(self.hold_dwell_ms)
    }

    /// How often the resting watch sweeps a limp machine.
    #[must_use]
    pub fn rest_poll(&self) -> Duration {
        Duration::from_millis(self.rest_poll_ms)
    }

    /// How long the machine holds at stow before torque comes off.
    ///
    /// Zero is lawful: it means let go the moment the head is folded, which is
    /// a legitimate thing to want on a machine nobody is having a conversation
    /// with.
    #[must_use]
    pub fn rest_delay(&self) -> Duration {
        Duration::from_millis(self.rest_delay_ms)
    }

    /// The move durations this file states, absent where it states nothing.
    ///
    /// One value rather than an accessor apiece because they are resolved
    /// together against the machine's own file, and a caller that picked up
    /// some of them would run the rest at whatever the bench configuration says
    /// without anything saying so.
    #[must_use]
    pub fn durations(&self) -> Overrides {
        Overrides {
            up: stated(self.up_duration_s),
            stow: stated(self.stow_duration_s),
            antennas: stated(self.antenna_duration_s),
            antenna_sides: [
                stated(self.antenna_duration_right_s),
                stated(self.antenna_duration_left_s),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The top-level half of the smallest file that runs.
    const TOP: &str = "\
motion_config = \"/run/brenn-app/conf/reachy-bench.toml\"
pod = \"reachy00\"
channel = \"brenn:reachy.motion\"
";

    /// The nested bridge table, which every fixture ends with — TOML puts a
    /// bare key after a table header *into* that table, so the top-level keys a
    /// case adds have to go in ahead of this.
    const BRIDGE: &str = "\
[bridge]
server_url = \"wss://bus.example/remote/reachy/ws\"
token_file = \"/run/brenn-app/conf/bridge.token\"
";

    /// The smallest file that runs, plus whatever top-level lines a case adds.
    fn file(extra: &str) -> String {
        format!("{TOP}{extra}\n{BRIDGE}")
    }

    fn minimal() -> Config {
        let config = Config::parse(&file("")).expect("the minimal file parses");
        config.validate().expect("the minimal file validates");
        config
    }

    /// The same file with one line replaced, for the refusal cases.
    fn with(line: &str, replacement: &str) -> Config {
        let text = file("").replace(line, replacement);
        assert_ne!(text, file(""), "the line to replace is in the fixture");
        Config::parse(&text).expect("the edited file parses")
    }

    #[test]
    fn the_minimal_file_carries_the_defaults() {
        let config = minimal();
        assert_eq!(config.pod, "reachy00");
        assert_eq!(config.channel, "brenn:reachy.motion");
        assert_eq!(
            config.motion_config,
            PathBuf::from("/run/brenn-app/conf/reachy-bench.toml")
        );
        assert_eq!(config.hold_dwell(), Duration::from_millis(200));
        assert_eq!(config.rest_poll(), Duration::from_millis(100));
        // How long the head is left folded with torque still on, which is this
        // machine's one pinch hazard: a value nothing pins is a value an edit
        // can move without a red test.
        assert_eq!(config.rest_delay(), Duration::from_millis(5_000));
        // Nothing stated is nothing overridden: a file that says nothing about
        // the durations must leave all three to the machine's own configuration,
        // which is the only way the two files cannot come to disagree.
        assert_eq!(config.durations(), Overrides::default());
    }

    /// The one place presence pace is tunable without touching the machine's
    /// file — and it has to reach the motion loop as seconds, because that is
    /// what the file writes and milliseconds is what everything else here uses.
    #[test]
    fn the_stated_durations_are_read_as_seconds() {
        let text = file(
            "up_duration_s = 1.4\nstow_duration_s = 1.25\nantenna_duration_s = 1.5\n\
             antenna_duration_right_s = 0.7\nantenna_duration_left_s = 0.3",
        );
        let config = Config::parse(&text).expect("the file parses");
        config.validate().expect("the file validates");
        assert_eq!(
            config.durations(),
            Overrides {
                up: Some(Duration::from_millis(1_400)),
                stow: Some(Duration::from_millis(1_250)),
                antennas: Some(Duration::from_millis(1_500)),
                antenna_sides: [
                    Some(Duration::from_millis(700)),
                    Some(Duration::from_millis(300)),
                ],
            }
        );
    }

    /// Each duration is independent of the others: overriding the raise must
    /// not quietly pull the fold or either antenna off the bench file with it,
    /// and one antenna's own clock must not answer for the other side.
    #[test]
    fn one_stated_duration_leaves_the_others_to_the_machine() {
        for (line, expected) in [
            (
                "up_duration_s = 1.4",
                Overrides {
                    up: Some(Duration::from_millis(1_400)),
                    ..Overrides::default()
                },
            ),
            (
                "stow_duration_s = 1.4",
                Overrides {
                    stow: Some(Duration::from_millis(1_400)),
                    ..Overrides::default()
                },
            ),
            (
                "antenna_duration_s = 1.4",
                Overrides {
                    antennas: Some(Duration::from_millis(1_400)),
                    ..Overrides::default()
                },
            ),
            (
                "antenna_duration_right_s = 1.4",
                Overrides {
                    antenna_sides: [Some(Duration::from_millis(1_400)), None],
                    ..Overrides::default()
                },
            ),
            (
                "antenna_duration_left_s = 1.4",
                Overrides {
                    antenna_sides: [None, Some(Duration::from_millis(1_400))],
                    ..Overrides::default()
                },
            ),
        ] {
            let config = Config::parse(&file(line)).expect("the file parses");
            config.validate().expect("the file validates");
            assert_eq!(config.durations(), expected, "{line}");
        }
    }

    /// A move of no time at all is not a fast head: it is a per-tick step the
    /// guard faults on, and after the gate audit a fault takes torque off. Same
    /// for a negative one, and for a number of seconds no clock can hold.
    #[test]
    fn a_duration_nothing_could_move_in_is_refused_by_name() {
        for (key, value) in [
            ("up_duration_s", "0.0"),
            ("stow_duration_s", "-1.5"),
            ("antenna_duration_s", "nan"),
            ("up_duration_s", "inf"),
            ("stow_duration_s", "1e300"),
            ("antenna_duration_right_s", "0.0"),
            ("antenna_duration_left_s", "-0.3"),
        ] {
            let text = file(&format!("{key} = {value}"));
            let config = Config::parse(&text).expect("the file parses");
            let message = config.validate().expect_err("a duration nothing can run");
            assert!(message.contains(key), "{key} = {value}: {message}");
        }
    }

    #[test]
    fn a_stated_dwell_overrides_the_default() {
        let text = file("hold_dwell_ms = 50");
        let config = Config::parse(&text).expect("the file parses");
        config.validate().expect("the file validates");
        assert_eq!(config.hold_dwell(), Duration::from_millis(50));
    }

    /// The lease and its term are gone with the presence vocabulary, and the
    /// configuration is `deny_unknown_fields`: a file still carrying the key
    /// refuses at startup naming it, which is what sends the operator to the
    /// one edit that fixes it.
    #[test]
    fn the_retired_lease_term_is_refused_by_name() {
        let text = file("lease_ttl_ms = 15000");
        let error = Config::parse(&text).expect_err("a retired key is refused");
        assert!(error.to_string().contains("lease_ttl_ms"), "{error}");
    }

    /// A key nothing reads is a refusal, not a no-op: a misspelled `pod` is a
    /// daemon obeying nobody's scripts with a file that looks right.
    #[test]
    fn an_unknown_key_is_refused() {
        let text = file("poll_hz = 5");
        let error = Config::parse(&text).expect_err("an unknown key is refused");
        assert!(error.to_string().contains("poll_hz"), "{error}");
    }

    #[test]
    fn a_missing_required_key_is_refused() {
        let text = file("").replace("pod = \"reachy00\"\n", "");
        let error = Config::parse(&text).expect_err("a missing key is refused");
        assert!(error.to_string().contains("pod"), "{error}");
    }

    #[test]
    fn an_untransportable_channel_is_refused() {
        let config = with(
            "channel = \"brenn:reachy.motion\"",
            "channel = \"local:reachy.motion\"",
        );
        let message = config.validate().expect_err("a local: channel is refused");
        assert!(message.contains("local:reachy.motion"), "{message}");
    }

    #[test]
    fn a_channel_that_is_only_the_prefix_is_refused() {
        let config = with("channel = \"brenn:reachy.motion\"", "channel = \"brenn:\"");
        let message = config.validate().expect_err("a bare prefix is refused");
        assert!(message.contains("names nothing"), "{message}");
    }

    #[test]
    fn an_empty_pod_is_refused() {
        let config = with("pod = \"reachy00\"", "pod = \"\"");
        let message = config.validate().expect_err("an empty pod is refused");
        assert!(message.contains("pod is empty"), "{message}");
    }

    #[test]
    fn an_empty_motion_config_is_refused() {
        let config = with(
            "motion_config = \"/run/brenn-app/conf/reachy-bench.toml\"",
            "motion_config = \"\"",
        );
        let message = config
            .validate()
            .expect_err("an empty motion_config is refused");
        assert!(message.contains("motion_config is empty"), "{message}");
    }

    #[test]
    fn a_zero_dwell_is_refused() {
        let text = file("hold_dwell_ms = 0");
        let config = Config::parse(&text).expect("the file parses");
        let message = config.validate().expect_err("a zero dwell is refused");
        assert!(message.contains("hold_dwell_ms"), "{message}");
    }

    /// The resting sweep's period, which zero turns into a servo bus read as
    /// fast as nine servos will answer, for as long as the machine rests.
    #[test]
    fn a_zero_rest_poll_is_refused() {
        let text = file("rest_poll_ms = 0");
        let config = Config::parse(&text).expect("the file parses");
        let message = config.validate().expect_err("a zero rest poll is refused");
        assert!(message.contains("rest_poll_ms"), "{message}");
    }

    /// The one knob that decides how long the machine sits torqued at stow.
    /// Zero is lawful — let go the moment the head is folded — but a
    /// seconds-versus-milliseconds slip would hold this platform's only pinch
    /// posture for minutes after every turn, on a machine nobody is watching.
    #[test]
    fn a_rest_delay_past_the_ceiling_is_refused_and_the_ceiling_itself_is_not() {
        let text = file(&format!("rest_delay_ms = {}", MAX_REST_DELAY_MS + 1));
        let config = Config::parse(&text).expect("the file parses");
        let message = config.validate().expect_err("too long a rest delay");
        assert!(message.contains("rest_delay_ms"), "{message}");
        assert!(message.contains("pinch hazard"), "{message}");

        for lawful in [0, MAX_REST_DELAY_MS] {
            let text = file(&format!("rest_delay_ms = {lawful}"));
            Config::parse(&text)
                .expect("the file parses")
                .validate()
                .expect("a rest delay at or under the ceiling runs");
        }
    }

    /// The nested table is validated by its own owner, through this one call, so
    /// a bad bridge is a startup refusal here rather than a dial that fails
    /// later.
    #[test]
    fn the_nested_bridge_table_is_validated() {
        let config = with(
            "server_url = \"wss://bus.example/remote/reachy/ws\"",
            "server_url = \"ws://bus.example/remote/reachy/ws\"",
        );
        let message = config.validate().expect_err("a cleartext URL is refused");
        assert!(message.contains("wss://"), "{message}");
    }

    /// The shipped example is a working file, and the defaults it writes out
    /// really are the defaults.
    ///
    /// The example is what an operator copies, so a key renamed here without it
    /// would hand them a file the daemon refuses at startup on a machine at the
    /// bench — and `deny_unknown_fields` makes a stale key in it a refusal too.
    /// The values are compared against the minimal file's rather than only
    /// parsed, because a comment claiming a default beside a number that is not
    /// one is the failure a file of this shape invites.
    ///
    /// The comparison is of the whole value, not a list of keys: the file's own
    /// premise is that this vocabulary grows, and an enumerated comparison would
    /// pass over the next defaulted key written out at whatever the author
    /// typed. Only the five keys the example's header names as mandatory, and
    /// the one recommendation it deliberately ships, are taken from the example
    /// itself; everything else has to be the default.
    #[test]
    fn the_shipped_example_parses_validates_and_states_the_defaults() {
        let text = include_str!("../reachy-motiond.example.toml");
        let example = Config::parse(text).expect("the example parses");
        example.validate().expect("the example validates");

        let defaults = minimal();
        assert_eq!(
            example,
            Config {
                motion_config: example.motion_config.clone(),
                pod: example.pod.clone(),
                channel: example.channel.clone(),
                antenna_duration_s: example.antenna_duration_s,
                bridge: brenn_bridge::Config {
                    server_url: example.bridge.server_url.clone(),
                    token_file: example.bridge.token_file.clone(),
                    ..defaults.bridge.clone()
                },
                ..defaults
            },
        );
    }

    /// The one value the example states rather than restates, and the four it
    /// deliberately does not.
    ///
    /// The shared antenna clock is a recommendation the file can back: its floor
    /// is closed-form, and 1.5 s clears by real margin the 0.28 s worst case the
    /// daemon reaches re-stowing an antenna left inboard of sideways. The two
    /// head-group clocks stay commented out at the bench file's own values —
    /// read off that file's defaults below rather than transcribed, because a
    /// restated number is one that goes stale the first time the measured one
    /// moves, which is what happened to the raise when the machine was finally
    /// run at it. The two per-side clocks stay commented out because a stagger nobody chose
    /// is not one this file should ship: the pair is parted at its crossing by
    /// the resolver whatever these say, so what they change is the pace, and
    /// pace is the operator's.
    ///
    /// That every key is present as a line either way is this file's premise,
    /// and it is checked against the struct itself by the test below; what is
    /// checked here is the handful of values those lines carry.
    #[test]
    fn the_example_recommends_an_antenna_clock_and_no_other() {
        let text = include_str!("../reachy-motiond.example.toml");
        let example = Config::parse(text).expect("the example parses");

        assert_eq!(
            example.durations(),
            Overrides {
                antennas: Some(Duration::from_millis(1_500)),
                ..Overrides::default()
            }
        );
        // The head-group lines restate the machine's own file, and the per-side
        // lines restate the shared clock above them, so both are asked for as
        // what they restate rather than as literals this file would have to
        // chase.
        let machine = reachy_bench::config::MotionSection::default();
        let shared = example
            .durations()
            .antennas
            .expect("the file recommends a shared antenna clock")
            .as_secs_f64();
        for commented in [
            format!("\n# up_duration_s = {:.1}", machine.up_duration_s),
            format!("\n# stow_duration_s = {:.1}", machine.stow_duration_s),
            format!("\n# antenna_duration_right_s = {shared:.1}"),
            format!("\n# antenna_duration_left_s = {shared:.1}"),
        ] {
            assert!(
                text.contains(&commented),
                "the example no longer carries `{}` for an operator to uncomment",
                commented.trim()
            );
        }
    }

    /// Every key the daemon reads, out of the parser's own refusal.
    ///
    /// `deny_unknown_fields` makes serde name every field it knows when it meets
    /// one it does not, so the enumeration is the deserializer's and cannot drift
    /// from the struct — which is the whole point of asking it this way rather
    /// than keeping a list somebody has to remember to extend.
    fn keys_the_daemon_reads() -> Vec<String> {
        let refusal = Config::parse(&file("a_key_no_daemon_reads = 1"))
            .expect_err("a key nothing reads is refused")
            .to_string();
        let (_, listed) = refusal
            .split_once("expected one of ")
            .expect("the refusal names the fields it knows");
        let keys: Vec<String> = listed
            .split('`')
            .skip(1)
            .step_by(2)
            .map(str::to_owned)
            .collect();
        assert!(
            keys.len() > 5,
            "no key names were read out of the refusal, so whoever asked is checking nothing: \
             {refusal}"
        );
        keys
    }

    /// Every key the daemon reads is a line in the example, live or commented.
    ///
    /// The file's premise, asked of the struct rather than of a list somebody
    /// has to remember to extend. A key added to `Config` with no line here is
    /// a knob nobody copying this file can find, and it slips past everything
    /// else in this module by construction: an unstated optional key is `None`
    /// in the example and `None` in the default, so nothing compares unequal.
    #[test]
    fn every_key_the_daemon_reads_is_a_line_in_the_example() {
        let text = include_str!("../reachy-motiond.example.toml");
        for key in keys_the_daemon_reads() {
            // Live, commented out, or — for the table nested whole — its own
            // header. The leading newline is what keeps `duration_s` from
            // matching inside `antenna_duration_s`.
            let forms = [
                format!("\n{key} = "),
                format!("\n# {key} = "),
                format!("\n[{key}]"),
            ];
            assert!(
                forms.iter().any(|form| text.contains(form)),
                "the example carries no line for `{key}`, so it is a knob nobody copying the \
                 file can find"
            );
        }
    }

    /// The three antenna arcs the floors are quoted for, radians: the
    /// stow-to-neutral presence sweep, the re-stow of an antenna left just
    /// inboard of sideways, and the widest sweep a bench command can ask for.
    ///
    /// Computed from the library's constants rather than stated, because the arcs
    /// are the input to the antenna floors that two documents quote and nothing
    /// else would notice moving. Each arc assumes the sweep takes the way round
    /// that misses the outboard sideways point: stow to neutral is a full turn
    /// less the stow angle, re-stow from just inboard of sideways adds the
    /// sideways angle, and the widest is bounded by a full turn (the long way is
    /// always less than a full turn, so TAU over-approximates by at most 3 mrad /
    /// 4 µs of floor).
    fn antenna_arcs() -> [f64; 3] {
        let stow = reachy_motion::disarm::STOW_ANTENNAS[1].abs();
        let sideways = reachy_motion::ANTENNA_OUTBOARD[1].abs();
        let to_neutral = std::f64::consts::TAU - stow;
        [to_neutral, to_neutral + sideways, std::f64::consts::TAU]
    }

    /// Every floor this daemon's example quotes, derived where the derivation
    /// lives.
    struct Floors {
        /// The yaw cap in the units the prose quotes it in.
        yaw_cap_deg: f64,
        /// The head group's floor.
        head: f64,
        /// The yaw's, in the three spans the prose names: the cap, cap to cap,
        /// and the half turn a hand can leave the body at.
        yaw: [f64; 3],
        /// The arcs the antenna floors are for, radians.
        arcs: [f64; 3],
        /// The antennas', one per [`Floors::arcs`] entry.
        antennas: [f64; 3],
        /// The machine's own fold clock, which the prose claims carries the
        /// widest fold.
        stow: f64,
    }

    /// The floors, from the machine's configuration and the library's
    /// arithmetic.
    ///
    /// Nothing here is a literal that a document could also hold — a bound or
    /// the tick rate moving in the bench file is how these numbers go stale.
    fn floors() -> Floors {
        let motion = reachy_bench::config::MotionSection::default();
        let envelope = reachy_bench::config::EnvelopeSection::default();
        let tick_hz = f64::from(motion.tick_hz);
        let cap = envelope.body_yaw_limit_deg.to_radians();
        let yaw_span = [cap, 2.0 * cap, std::f64::consts::PI];
        let arcs = antenna_arcs();
        Floors {
            yaw_cap_deg: envelope.body_yaw_limit_deg,
            head: reachy_motion::HEAD_GROUP_FLOOR_S,
            yaw: yaw_span.map(|span| {
                reachy_motion::duration_floor_s(span, motion.max_step_body_yaw_rad, tick_hz)
            }),
            arcs,
            antennas: arcs.map(|span| {
                reachy_motion::duration_floor_s(span, motion.max_step_antennas_rad, tick_hz)
            }),
            stow: motion.stow_duration_s,
        }
    }

    /// The slice of `text` from `from` up to the next `to`.
    fn between<'a>(text: &'a str, from: &str, to: &str) -> &'a str {
        let start = text
            .find(from)
            .unwrap_or_else(|| panic!("the text no longer carries `{from}`"));
        let rest = &text[start..];
        let end = rest
            .find(to)
            .unwrap_or_else(|| panic!("the text no longer carries `{to}` after `{from}`"));
        &rest[..end]
    }

    /// One quoted figure, in the passage that owns it.
    fn quotes(text: &str, expected: &str, doc: &str) {
        assert!(
            text.contains(expected),
            "{doc} no longer says `{expected}`, so a figure it prints is not the one the \
             library derives from the shipped bounds"
        );
    }

    /// The FLOORS block quotes the derivation, not a number of its own.
    #[test]
    fn the_examples_floors_are_the_ones_the_library_derives() {
        let text = include_str!("../reachy-motiond.example.toml");
        let example = Config::parse(text).expect("the example parses");
        let f = floors();
        let doc = "the example's FLOORS block";

        let block = between(text, "# FLOORS.", "\n# The two head-group lines");
        // The argument these numbers come from has one home, and this block
        // points at it rather than restating it.
        quotes(
            block,
            "FLOORS comment of `crates/reachy-bench/reachy-bench.example.toml`",
            doc,
        );

        let head = between(block, "#   head group", "#   body yaw");
        quotes(head, &format!("{:.2} s", f.head), doc);

        let yaw = between(block, "#   body yaw", "#   antennas");
        quotes(
            yaw,
            &format!("{:.2} s from the {:.0}-degree cap", f.yaw[0], f.yaw_cap_deg),
            doc,
        );
        quotes(yaw, &format!("and {:.2} s", f.yaw[1]), doc);
        quotes(yaw, &format!("needs {:.2} s of yaw", f.yaw[2]), doc);
        quotes(yaw, &format!("`stow_duration_s = {:.1}`", f.stow), doc);

        let antennas = between(block, "#   antennas", "commands the first two");
        quotes(
            antennas,
            &format!("{:.2} s for the {:.2} rad", f.antennas[0], f.arcs[0]),
            doc,
        );
        quotes(antennas, &format!("{:.2} s to re-stow", f.antennas[1]), doc);
        quotes(
            antennas,
            &format!("and {:.2} s for the", f.antennas[2]),
            doc,
        );

        // The two claims the prose makes about shipped values, checked as
        // claims and not only as text: the calm fold carries the widest yaw a
        // hand can leave the body at, and the one duration this file
        // recommends clears the worst arc the daemon commands.
        assert!(
            f.stow >= f.yaw[2],
            "the {:.2} s fold no longer carries the {:.2} s half turn the block says it does",
            f.stow,
            f.yaw[2],
        );
        let shared = example
            .durations()
            .antennas
            .expect("the file recommends a shared antenna clock")
            .as_secs_f64();
        assert!(
            shared >= f.antennas[1],
            "the recommended {shared:.1} s antenna clock is under the {:.2} s worst case the \
             daemon reaches, which is the whole reason the key ships uncommented",
            f.antennas[1],
        );
        quotes(
            text,
            &format!("{shared:.1} s clears the {:.2} s worst", f.antennas[1]),
            doc,
        );
    }

    /// The operator runbook, read from the repository.
    ///
    /// It is the document an investigation reaches for first and the one nothing
    /// else in this repo touches. Read at run time rather than embedded, because
    /// it is not this crate's file and a device crate should not fail to *build*
    /// because a repo-root document moved.
    fn runbook() -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../docs/runbooks/reachy-end-to-end.md");
        std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!(
                "the operator runbook quoting this daemon's knobs and duration floors is \
                 unreadable at {}: {error}",
                path.display()
            )
        })
    }

    /// One value a knob's own row claims, against what the daemon runs on.
    fn claims(row: &str, expected: &str, key: &str, doc: &str) {
        assert!(
            row.contains(expected),
            "{doc}'s row for `{key}` no longer says `{expected}`, so it quotes a value this \
             daemon does not run on"
        );
    }

    /// The runbook table row that leads with `key`, up to the next row or the
    /// end of the table.
    fn row<'a>(table: &'a str, key: &str) -> &'a str {
        let start = table
            .find(&format!("`{key}`"))
            .unwrap_or_else(|| panic!("the table no longer carries a row for `{key}`"));
        let rest = &table[start..];
        &rest[..rest.find("\n|").unwrap_or(rest.len())]
    }

    /// Every knob the runbook's daemon-policy table is for has a row, and the
    /// values those rows quote are the ones the daemon runs on.
    ///
    /// The example file has a structural guard that every key the daemon reads is
    /// a line in it, so the table an operator reaches for *first* is the one place
    /// a knob can go unmentioned. The key set is the timing subset of the struct's
    /// own keys, taken by the unit its name ends in rather than listed, so the
    /// next key measured in seconds or milliseconds has to appear in the table
    /// too. The rest of the struct — the machine's file, the pod, the channel, the
    /// bridge's nested table — is deliberately not this table's business and is
    /// documented around it.
    #[test]
    fn the_runbooks_knob_table_carries_every_knob_it_is_for() {
        let text = runbook();
        let table = between(&text, "**The daemon's policy**", "**The machine**");
        let doc = "the runbook's daemon-policy table";

        let tuning: Vec<String> = keys_the_daemon_reads()
            .into_iter()
            .filter(|key| key.ends_with("_ms") || key.ends_with("_s"))
            .collect();
        assert!(
            tuning.len() > 5,
            "the suffix filter matched almost nothing, so this test is checking nothing: {tuning:?}"
        );
        for key in &tuning {
            assert!(
                table.contains(&format!("`{key}`")),
                "{doc} has no row for `{key}`, so it is a knob nobody reading the runbook can \
                 find"
            );
        }

        // The four optional clocks have no default at all, which is what their
        // rows say instead of a number.
        let defaults = minimal();
        for (key, value) in [
            ("hold_dwell_ms", defaults.hold_dwell_ms),
            ("rest_poll_ms", defaults.rest_poll_ms),
            ("rest_delay_ms", defaults.rest_delay_ms),
        ] {
            claims(row(table, key), &format!("| {value} |"), key, doc);
        }
        for (key, stated) in [
            ("up_duration_s", defaults.up_duration_s),
            ("stow_duration_s", defaults.stow_duration_s),
            (
                "antenna_duration_right_s",
                defaults.antenna_duration_right_s,
            ),
            ("antenna_duration_left_s", defaults.antenna_duration_left_s),
        ] {
            assert!(
                stated.is_none(),
                "`{key}` now has a default, so {doc} calling it unset is wrong"
            );
            claims(row(table, key), "| unset |", key, doc);
        }

        let shared = Config::parse(include_str!("../reachy-motiond.example.toml"))
            .expect("the example parses")
            .durations()
            .antennas
            .expect("the file recommends a shared antenna clock")
            .as_secs_f64();
        claims(
            row(table, "antenna_duration_s"),
            &format!("| {shared:.1} in the example |"),
            "antenna_duration_s",
            doc,
        );
    }

    #[test]
    fn a_missing_file_names_itself() {
        let path = std::env::temp_dir().join("reachy-motiond-absent.toml");
        let error = Config::load(&path).expect_err("a file that is not there is refused");
        assert!(matches!(error, ConfigError::Read { .. }), "{error}");
        assert!(
            error.to_string().contains("reachy-motiond-absent"),
            "{error}"
        );
    }
}
