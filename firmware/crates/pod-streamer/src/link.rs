//! The byte-stream seam the streamer's event loop drives.
//!
//! A link is a pollable fd plus `Read`/`Write` plus two facts the loop cannot
//! derive itself: which poll directions serve an operation the caller wants, and
//! whether plaintext can sit in a transport-internal buffer that readiness
//! cannot reveal. TLS transports answer both differently from a bare socket, so
//! they answer them here rather than at every call site.
//!
//! [`Want`] and [`plan_poll_interest`] are the substitution rule every TLS
//! transport needs to answer the first question: a TLS read can be blocked on
//! writability and a write on readability, so the direction the caller armed is
//! not always the direction to wait on. The rule is subtle enough — and its
//! failure mode (a de-armed direction reinstated, turning backpressure into a
//! busy spin on a level-triggered fd) quiet enough — that both pods' transports
//! spend the same copy of it.

use std::io;
use std::os::fd::RawFd;

/// Poll directions to wait on for one wake.
///
/// Platform-neutral by construction: the numeric `POLLIN`/`POLLOUT` values are a
/// platform's business, and esp-idf's and libc's are not the same constants even
/// where they happen to agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollInterest {
    /// Wait for the transport to be readable.
    pub read: bool,
    /// Wait for the transport to be writable.
    pub write: bool,
}

impl PollInterest {
    /// Interest in neither direction — a wake that can only time out. Produced
    /// when the caller armed nothing, which is a legitimate state (both
    /// directions de-armed) rather than an error.
    pub const NONE: PollInterest = PollInterest {
        read: false,
        write: false,
    };

    /// Interest in readability alone: a wake driven by inbound bytes, or a TLS
    /// operation waiting on the peer.
    pub const READ: PollInterest = PollInterest {
        read: true,
        write: false,
    };

    /// Interest in writability alone, for a bounded `POLLOUT` wait.
    pub const WRITE: PollInterest = PollInterest {
        read: false,
        write: true,
    };

    /// Interest in both directions, for a wake whose blocking direction is not
    /// observable (a TLS handshake step).
    pub const BOTH: PollInterest = PollInterest {
        read: true,
        write: true,
    };
}

/// Which direction a TLS transport last said it was waiting on, for one
/// operation (a read's outstanding request and a write's are tracked
/// separately).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    /// The last call wanted read: poll for readability before retrying.
    Read,
    /// The last call wanted write: poll for writability before retrying.
    Write,
    /// The last call completed; no direction is outstanding.
    None,
}

/// Poll directions for one wake under the substitution rule, plus how many of
/// the armed directions were substituted (0..=2).
pub struct PollPlan {
    /// Directions the wake should wait on.
    pub interest: PollInterest,
    /// Armed directions whose poll direction was replaced by the one TLS asked
    /// for. Bookkeeping for a transport that wants to report how often its
    /// session inverted a direction; it never affects `interest`.
    pub substituted: u32,
}

/// Decide which directions to poll for a wake in which the caller armed reading
/// (`readable`) and/or writing (`writable`), given each direction's outstanding
/// [`Want`].
///
/// An armed read waits on writability when the last read wanted write, and an
/// armed write waits on readability when the last write wanted read —
/// *substitution*, not addition, so a de-armed direction is never reinstated. A
/// caller that de-armed writability (write backoff) or readability (inbound
/// backpressure) must not have it put back by the other direction's outstanding
/// request, or the level-triggered fd wakes the loop immediately and the de-arm
/// becomes a busy spin. Unarmed directions contribute nothing.
///
/// Pure, so a transport's [`LinkStream::poll_interest`] can return this plan's
/// interest verbatim and the truth table below pins the real stream's behavior.
pub fn plan_poll_interest(
    readable: bool,
    writable: bool,
    read_want: Want,
    write_want: Want,
) -> PollPlan {
    let mut plan = PollPlan {
        interest: PollInterest::NONE,
        substituted: 0,
    };
    if readable {
        if read_want == Want::Write {
            plan.interest.write = true;
            plan.substituted += 1;
        } else {
            plan.interest.read = true;
        }
    }
    if writable {
        if write_want == Want::Read {
            plan.interest.read = true;
            plan.substituted += 1;
        } else {
            plan.interest.write = true;
        }
    }
    plan
}

/// A byte stream the streamer's `poll`-driven event loop can drive.
///
/// Bundles the pollable fd, poll interest, and readiness-trust signal with
/// `Read`/`Write` so TLS poll discipline lives in the impl rather than at
/// every call site.
pub trait LinkStream: io::Read + io::Write {
    /// The fd to hand `poll()`.
    fn link_fd(&self) -> RawFd;

    /// Poll directions for a wake in which the caller is interested in reading
    /// (`readable`) and/or writing (`writable`). A direction the caller did not
    /// arm contributes nothing; an armed one contributes whichever direction the
    /// transport needs to make that operation progress — which for a TLS
    /// transport can be the opposite of the one asked for.
    fn poll_interest(&self, readable: bool, writable: bool) -> PollInterest;

    /// Whether decrypted bytes can sit in a transport-internal buffer that
    /// readiness cannot reveal. `true` obliges the caller to attempt a read
    /// every wake instead of only on readiness.
    fn buffers_plaintext(&self) -> bool;

    /// Reborrow as a plain reader, for helpers that need only `Read`.
    fn as_read(&mut self) -> &mut dyn io::Read;

    /// Reborrow as a plain writer, for helpers that need only `Write`.
    fn as_write(&mut self) -> &mut dyn io::Write;
}

impl LinkStream for std::net::TcpStream {
    fn link_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd as _;
        self.as_raw_fd()
    }

    fn poll_interest(&self, readable: bool, writable: bool) -> PollInterest {
        PollInterest {
            read: readable,
            write: writable,
        }
    }

    fn buffers_plaintext(&self) -> bool {
        false
    }

    fn as_read(&mut self) -> &mut dyn io::Read {
        self
    }

    fn as_write(&mut self) -> &mut dyn io::Write {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connected loopback pair, for the one impl in this module that needs a
    /// real socket.
    fn loopback_pair() -> (std::net::TcpStream, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let client = std::net::TcpStream::connect(addr).expect("connect");
        let (server, _) = listener.accept().expect("accept");
        (client, server)
    }

    /// A plain socket asks for exactly the directions the caller armed — no
    /// substitution, in either direction, in any combination.
    #[test]
    fn tcp_interest_is_what_the_caller_armed() {
        let (client, _server) = loopback_pair();
        for readable in [false, true] {
            for writable in [false, true] {
                assert_eq!(
                    client.poll_interest(readable, writable),
                    PollInterest {
                        read: readable,
                        write: writable
                    },
                    "armed r={readable} w={writable}"
                );
            }
        }
    }

    /// A bare socket holds no decrypted bytes, so readiness is the whole truth
    /// and the loop need not attempt a read every wake.
    #[test]
    fn tcp_does_not_buffer_plaintext() {
        let (client, _server) = loopback_pair();
        assert!(!client.buffers_plaintext());
        assert!(client.link_fd() >= 0, "a connected socket has a real fd");
    }

    // ── plan_poll_interest ────────────────────────────────────────────────

    /// Expected plan for one truth-table row: an armed read polls out iff its last read
    /// wanted write, an armed write polls in iff its last write wanted read, and each such
    /// flip is one substitution.
    ///
    /// A second spelling of the rule, not an independent oracle — it can only show that the
    /// implementation has not moved, which is what the truth table is for. The literal
    /// expected values live in the single-case tests below.
    fn expected(readable: bool, writable: bool, read_want: Want, write_want: Want) -> PollPlan {
        let read_flipped = readable && read_want == Want::Write;
        let write_flipped = writable && write_want == Want::Read;
        PollPlan {
            interest: PollInterest {
                read: (readable && !read_flipped) || write_flipped,
                write: (writable && !write_flipped) || read_flipped,
            },
            substituted: u32::from(read_flipped) + u32::from(write_flipped),
        }
    }

    /// Exhaustive over the whole input space (2 x 2 x 3 x 3), as a refactor lock: it pins
    /// every row, including that the substitution count is bookkeeping laid alongside an
    /// interest decided by the arming flags and the `Want`s alone. It asserts against a
    /// paraphrase of the rule, so the cases below are what say the rule itself is right.
    #[test]
    fn plan_poll_interest_truth_table() {
        let wants = [Want::Read, Want::Write, Want::None];
        for readable in [false, true] {
            for writable in [false, true] {
                for read_want in wants {
                    for write_want in wants {
                        let got = plan_poll_interest(readable, writable, read_want, write_want);
                        let want = expected(readable, writable, read_want, write_want);
                        assert_eq!(
                            (got.interest, got.substituted),
                            (want.interest, want.substituted),
                            "readable={readable} writable={writable} \
                             read_want={read_want:?} write_want={write_want:?}"
                        );
                    }
                }
            }
        }
    }

    /// An unarmed direction contributes nothing, whatever it last wanted — substitution
    /// replaces an armed direction's poll, it never reinstates a de-armed one (which would
    /// turn a backpressure de-arm into a busy spin on the level-triggered fd).
    #[test]
    fn plan_poll_interest_unarmed_directions_contribute_nothing() {
        for read_want in [Want::Read, Want::Write, Want::None] {
            for write_want in [Want::Read, Want::Write, Want::None] {
                let plan = plan_poll_interest(false, false, read_want, write_want);
                assert_eq!(plan.interest, PollInterest::NONE);
                assert_eq!(plan.substituted, 0);
            }
        }
    }

    /// The production shape the substitution counter exists to detect: the drain loop arms
    /// the read alone and the session answered `WANT_WRITE`. The read interest must come out
    /// **false** —
    /// the armed direction's poll is replaced, not joined, so a wake happens on the
    /// handshake flight the read is really blocked behind instead of on arriving bytes.
    #[test]
    fn plan_poll_interest_read_wanting_write_polls_out_alone() {
        let plan = plan_poll_interest(true, false, Want::Write, Want::None);
        assert!(
            !plan.interest.read,
            "the armed read's readability wait is replaced, not kept"
        );
        assert!(plan.interest.write);
        assert_eq!(plan.substituted, 1);
    }

    /// The mirror: a lone armed write whose last write wanted read polls in, not out.
    #[test]
    fn plan_poll_interest_write_wanting_read_polls_in_alone() {
        let plan = plan_poll_interest(false, true, Want::None, Want::Read);
        assert!(plan.interest.read);
        assert!(
            !plan.interest.write,
            "the armed write's writability wait is replaced, not kept"
        );
        assert_eq!(plan.substituted, 1);
    }

    /// Both directions armed and both cross-wanting is the only way to substitute twice —
    /// the read waits on write, the write waits on read, and the interest is still both.
    #[test]
    fn plan_poll_interest_counts_two_substitutions() {
        let plan = plan_poll_interest(true, true, Want::Write, Want::Read);
        assert_eq!(plan.interest, PollInterest::BOTH);
        assert_eq!(plan.substituted, 2);
    }

    /// The steady state: nothing outstanding, so each armed direction polls its own.
    #[test]
    fn plan_poll_interest_no_want_polls_armed_directions() {
        let plan = plan_poll_interest(true, true, Want::None, Want::None);
        assert_eq!(plan.interest, PollInterest::BOTH);
        assert_eq!(plan.substituted, 0);
    }
}
