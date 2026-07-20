//! Process exit codes shared across the offline tools (`segments-export`,
//! `replay-pod`), so the two cannot drift on what a given code means. The tools
//! map "worst thing that happened" onto the most severe applicable nonzero code,
//! ranked hard failure (1) > peer-closed (4) > missing input (3). The code
//! numbers are not the severity order.

/// A hard failure: unreadable input, corrupt record, refused connect, or a
/// CLI/usage error. Dominates the soft outcomes below.
pub const HARD_FAILURE: u8 = 1;

/// One or more inputs were absent (pruned or never existed) but every present
/// input was processed cleanly — distinct from a hard failure.
pub const MISSING_INPUT: u8 = 3;

/// One or more replays were cut short by the daemon closing the connection
/// (write error / reset), while everything else completed.
pub const PEER_CLOSED: u8 = 4;
