//! Process exit codes for a daemon built on this crate.
//!
//! The bridge library never exits a process — it answers with a
//! [`BridgeOutcome`](crate::BridgeOutcome), and the binary that owns `main`
//! decides. These are the codes [`BridgeOutcome::exit_code`] maps onto, kept in
//! one place so every bridge binary agrees on what a number means.
//!
//! The numbers are chosen not to collide with `speech-surface`'s offline-tool
//! codes (1 hard failure, 3 missing input, 4 peer closed): an operator reading
//! an exit code off a unit should never have to ask which binary produced it.

/// The attachment ended in a way that means this process is wrong: a protocol
/// error, a peer that closed on a code declared terminal, or a run of
/// attachments that achieved nothing (see
/// [`BridgeOutcome::Futile`](crate::BridgeOutcome::Futile)).
///
/// Better dead than wrong. A bridge that keeps reconnecting into a refusal is a
/// bridge hammering the peer with frames the peer has already judged illegal,
/// and the peer's answer to that is a ban. Dying is the loudest log.
pub const HARD_FAILURE: u8 = 1;

/// The two ends speak no wire version in common.
///
/// Distinct from a hard failure because the remedy is distinct: nothing is wrong
/// with either build, one of them is simply older than the other's support
/// window, and the fix is a deploy rather than a debug session. Retrying in
/// process cannot help — both version ranges are build constants — so the
/// restart supervisor's backoff is the retry loop, and the exit resolves the
/// moment either side is updated.
pub const VERSION_INCOMPATIBLE: u8 = 5;
