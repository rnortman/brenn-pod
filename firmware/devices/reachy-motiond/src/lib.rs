//! `reachy-motiond` — the head-presence daemon: a bus attachment on one side, a
//! servo bus on the other, and the head moving between stow and neutral to match
//! what it is told.
//!
//! The machine's motion libraries own no loop and no I/O by construction, so a
//! host has to supply the port, the clock and the loop. The bench binary is one
//! such host — one supervised command per process, an operator watching. This is
//! the other: a long-lived process that arms once, stows, and then holds the
//! machine while it waits to be told what posture to be in.
//!
//! Two threads, and the split is deliberate:
//!
//! - The **bus thread** runs a current-thread tokio runtime, owns the bridge and
//!   the presence subscription, and folds each delivery into the lease. It never
//!   touches a servo.
//! - The **motion thread** blocks, owns the serial port and the control clock,
//!   and never awaits anything. It reads the lease between moves.
//!
//! They meet at exactly three places, all of them in [`cells`]: the lease, a
//! shutdown flag, and a write-once fault. Nothing else crosses.
//!
//! What the daemon does with what it is told is a lease, not a command. An
//! *engaged* intent is valid for a bounded term from the moment it arrived on
//! this machine's own monotonic clock, and the publisher keeps it alive by
//! republishing; everything that can fail — a dead publisher, a dropped bus, a
//! lost message — ends in the term running out, which means idle, which means
//! the head is stowed. That is parking on loss of instruction, and it is not
//! fault recovery: a motion fault stops the daemon commanding anything at all,
//! with torque untouched and the machine holding, until an operator acts.

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
pub use config::{Config, ConfigError};
pub use motion::{Head, Machine, Outcome, Refusal, StartupError};
pub use report::{Sink, Streams};
