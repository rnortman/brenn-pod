//! `motion-proto` — timed motion scripts, and the executor state that runs one.
//!
//! One channel on the bus carries what a machine's head should be doing. The
//! publisher is whichever process can observe a speech interaction; the
//! consumer is the motion daemon on the machine. This crate is the piece they
//! share, and it is deliberately the only piece — no I/O, no clock of its own,
//! no async, so both ends can test it and neither end grows protocol logic of
//! its own.
//!
//! The unit of intent is a **script**: a timeline of postures at offsets from
//! the moment it arrives, under a timeout after which the head goes back down.
//! The timeout is an unconditional ceiling on the script's own timeline — every
//! step falls strictly inside it — so "the head is up for at most this long" is
//! readable off one field of one message, with no arithmetic and no second
//! number to check it against.
//! The host knows how long its speech is, so the ordinary conversation is one
//! message — up now, stow when the audio ends — rather than a stream of states
//! the daemon has to reduce. A new script can arrive at any moment and wholly
//! replaces the one running.
//!
//! Three parts:
//!
//! - **The script** ([`MotionScript`]): pod identity, an ordering number, the
//!   timeline, and the timeout, as JSON carrying a `"type"` discriminator.
//!   Decoding is tolerant of fields it does not know, because the schema is
//!   meant to grow richer intents, and a daemon built today must not choke on a
//!   script authored by something newer.
//! - **The schedule** ([`Schedule`]): the executor's state. Deliveries fold in;
//!   the motion side asks what posture is wanted now.
//! - **The sequence source** ([`SeqSource`]): the publisher's half of the
//!   ordering rule, kept here so both ends share one definition of it.
//!
//! Every script lapses, and that is the whole safety argument. A scripter that
//! crashes, a bus that drops, a daemon that restarts mid-conversation, a lost
//! closing script — each ends in a script lapsing, which means stow, which means
//! the machine goes back to rest. The lapse is at the timeout the script named
//! ([`MotionScript::expiry_ms`]), so the bound is finite, stated by the script
//! itself rather than assumed, and never larger than what the message says.
//! Two validation rules make that true and both are refusals, because a script
//! runs entirely or not at all: a timeline may not reach its own timeout, and no
//! timeout may exceed [`MAX_TIMEOUT_MS`] — the bound on a publisher whose two
//! numbers are wrong together. Nothing retained yesterday can raise a head
//! tonight, and no wall-clock comparison between two hosts is ever made: offsets
//! are measured on the consumer's own monotonic clock, which is the only clock
//! this crate ever sees.
//!
//! A body that does not decode, one that is not executable, and one addressed
//! to some other pod are all facts a caller reports and moves past. A daemon
//! that stopped executing because one message was malformed would have turned a
//! bad message into a stuck head; the script it was already running, and that
//! script's timeout, stand.

#![forbid(unsafe_code)]

pub mod schedule;
pub mod script;
pub mod seq;

pub use schedule::{Acceptance, Desired, Schedule};
pub use script::{
    DecodeError, MAX_TIMEOUT_MS, MOTION_SCRIPT_TYPE, MotionScript, Posture, ScriptError, Step,
};
pub use seq::{SeqSource, unix_millis};
