//! `reachy-motiond` — the head-presence daemon: a bus attachment on one side, a
//! servo bus on the other, and the head moving between stow and neutral to match
//! the timed script it is running.
//!
//! The machine's motion libraries own no loop and no I/O by construction, so a
//! host has to supply the port, the clock and the loop. The bench binary is one
//! such host — one supervised command per process, an operator watching. This is
//! the other: a long-lived process that commissions the machine once and then
//! leaves it limp, taking hold of it only for as long as a script asks the head
//! to be up.
//!
//! Two threads, and the split is deliberate:
//!
//! - The **bus thread** runs a current-thread tokio runtime, owns the bridge and
//!   the motion subscription, and offers each delivery to the schedule. It never
//!   touches a servo.
//! - The **motion thread** blocks, owns the serial port and the control clock,
//!   and never awaits anything. It reads the schedule between dwells and at
//!   every control period of a move, so an arriving script turns a move already
//!   travelling around rather than waiting it out.
//!
//! They meet only in [`cells`]: the schedule, a shutdown flag, the engage
//! refusals owed an alert, a write-once fault, and the note that the machine is
//! no longer being touched. Nothing else crosses.
//!
//! What the daemon is told is a timed script, not a stream of commands: a
//! timeline of postures at offsets from the moment the message landed on this
//! machine's own monotonic clock, under a timeout after which the head goes back
//! down. The host knows how long its speech is, so the ordinary conversation is
//! one message. Everything that can fail — a dead scripter, a dropped bus, a
//! lost message — ends in the running script lapsing, which means the head folds
//! and torque comes off. That is resting on loss of instruction, and it is not
//! fault recovery: a motion fault takes torque off immediately and stops the
//! daemon commanding anything at all, leaving the machine at the minimum risk
//! condition until an operator restarts it.

#![forbid(unsafe_code)]

pub mod bus;
pub mod cells;
pub mod cli;
pub mod config;
pub mod motion;
pub mod report;

pub use bus::{Chore, Listener};
pub use cells::{Delivered, FaultReport, FaultStage, Shared, Stop};
pub use cli::{Invocation, exit_code};
pub use config::{Config, ConfigError, Overrides};
pub use motion::{
    Active, Clock, Clocks, Machine, Outcome, Refusal, Rest, Source, StartupError, Timing,
};
pub use report::{Sink, Streams};
