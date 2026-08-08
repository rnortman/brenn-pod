//! `presence-proto` — the head-presence vocabulary, and the lease that reduces
//! it to a desired posture.
//!
//! One channel on the bus carries what the machine's head should be doing:
//! *engaged*, meaning up and attending, or *idle*, meaning stowed. The publisher
//! is whichever process can observe a speech interaction; the consumer is the
//! motion daemon on the machine. This crate is the piece they share, and it is
//! deliberately the only piece — no I/O, no clock of its own, no async, so both
//! ends can test it and neither end grows protocol logic of its own.
//!
//! Two halves:
//!
//! - **The body** ([`PresenceBody`]): pod identity, desired state, and a
//!   sequence number, as JSON carrying a `"type"` discriminator. Decoding is
//!   tolerant of fields it does not know, because the channel is meant to grow
//!   more kinds of intent, and a consumer built today must not choke on a body
//!   authored by something newer.
//! - **The lease** ([`Lease`]): the reducer. An engaged intent is not a command
//!   that latches — it is a lease, valid for a bounded time from the moment it
//!   arrived, which the publisher keeps alive by republishing. An explicit idle
//!   applies at once.
//!
//! The lease is the whole safety argument. Every way this can fail — a publisher
//! that crashes, a bus that drops, a consumer that restarts mid-conversation, a
//! lost idle message — ends in the lease running out, which means idle, which
//! means the head is stowed. Nothing retained yesterday can raise a head
//! tonight, and no wall-clock comparison between two hosts is ever made: the
//! deadline is measured on the consumer's own monotonic clock, which is the only
//! clock this crate ever sees.
//!
//! Nothing here is an error that stops a consumer. A body that does not decode,
//! and a body addressed to some other pod, are facts a caller reports and moves
//! past — an intent stream is repaired by the next refresh, and a consumer that
//! stops reducing because one message was malformed has turned a bad message
//! into a stuck head.

#![forbid(unsafe_code)]

pub mod body;
pub mod lease;

pub use body::{DecodeError, PRESENCE_TYPE, PresenceBody, PresenceState};
pub use lease::{Lease, Reduction};
