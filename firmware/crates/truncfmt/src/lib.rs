//! Graceful char-boundary truncation for formatting into fixed
//! `heapless::String<N>` buffers.
//!
//! Lives in this crate — rather than the device/protocol crate — so its
//! truncation behaviour is host-unit-testable and so heapless-only consumers can
//! route through it without pulling a serde/protocol stack.

#![cfg_attr(not(test), no_std)]

// `alloc` is only needed in tests (they build over-capacity inputs with
// `alloc::string::String` / `alloc::format!`). Production (no_std) code paths do
// not allocate.
#[cfg(test)]
extern crate alloc;

/// A [`core::fmt::Write`] adapter that appends formatted output into a
/// `heapless::String<N>` char-by-char, silently stopping once the buffer is full.
///
/// This exists because `heapless::String`'s own `fmt::Write` impl is
/// all-or-nothing *per `write_str` slice*: when a single slice does not fit in
/// the remaining capacity it writes NOTHING and returns `Err`. A formatter that
/// emits the whole message as one over-capacity slice therefore drops the
/// *entire* message to empty — the silent-blank-log bug this adapter fixes (a
/// 202-byte `log::warn!` literal was being shipped as an empty `LogFrame`).
///
/// `heapless::String::push(ch)` is all-or-nothing *per char*, so pushing
/// char-by-char and stopping at the first char that does not fit truncates
/// cleanly at a UTF-8 char boundary: everything that fits is kept, no partial
/// code point is ever written, and `write_str` never reports an error.
/// Truncation is lossy-but-safe.
///
/// `core::fmt::write` delivers a multi-argument message as one `write_str` call
/// per format segment. Once the buffer fills mid-message, `full` latches so every
/// later segment is dropped whole rather than being partially admitted: without
/// the latch a *narrower* char in a later segment could slip into capacity that a
/// *wider* char in an earlier segment failed to fill, yielding output that is not
/// a prefix of the logical message. With the latch, the result is always a
/// verbatim prefix of the fully-formatted message, cut at a UTF-8 char boundary.
struct TruncatingWriter<'a, const N: usize> {
    buf: &'a mut heapless::String<N>,
    /// Set once a segment failed to fit whole; suppresses all later output.
    full: bool,
}

impl<const N: usize> core::fmt::Write for TruncatingWriter<'_, N> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        if self.full {
            return Ok(());
        }
        // Fast path: `push_str` is all-or-nothing per slice (a bulk
        // `extend_from_slice`), so when the whole segment fits it is one
        // bounds-check + one copy, and on overflow it leaves the buffer
        // untouched for the char-by-char fallback below.
        if self.buf.push_str(s).is_ok() {
            return Ok(());
        }
        // Segment did not fit whole: keep the chars that do, cut on the first
        // char that overflows (a clean UTF-8 boundary), then latch `full` so no
        // later segment can backfill the remaining bytes.
        for ch in s.chars() {
            if self.buf.push(ch).is_err() {
                break;
            }
        }
        self.full = true;
        Ok(())
    }
}

/// Format `args` into a `heapless::String<N>`, truncating gracefully at a UTF-8
/// char boundary if the formatted output exceeds `N` bytes.
///
/// Never errors, never panics, and — unlike formatting straight into a
/// `heapless::String` via `core::fmt::write` — never drops the whole message to
/// empty on overflow (see [`TruncatingWriter`]).
pub fn format_truncating<const N: usize>(args: core::fmt::Arguments) -> heapless::String<N> {
    format_truncating_inner::<N>(args).0
}

/// The marker spliced onto genuinely-truncated output by
/// [`format_truncating_marked`]. Three bytes: at the small caps these buffers use,
/// a longer marker would cost the diagnostic tail it exists to flag.
pub const TRUNCATION_SENTINEL: &str = "…";

/// Like [`format_truncating`], but when the formatted output genuinely overflows
/// `N` bytes the tail is replaced with `sentinel` so truncation is visible to the
/// reader. Output that fits exactly (no input dropped) gets no sentinel.
///
/// If `sentinel.len() > N` the splice is skipped and the plain truncated buffer is
/// returned, identical to [`format_truncating`]; a `debug_assert` catches that
/// misuse in host tests.
pub fn format_truncating_marked<const N: usize>(
    args: core::fmt::Arguments,
    sentinel: &str,
) -> heapless::String<N> {
    debug_assert!(sentinel.len() <= N, "sentinel must fit the capacity");
    let (mut buf, full) = format_truncating_inner::<N>(args);
    if !full || sentinel.len() > N {
        return buf;
    }
    // `pop` removes one whole char, so every intermediate state stays on a valid
    // UTF-8 boundary.
    while N - buf.len() < sentinel.len() {
        buf.pop();
    }
    // Cannot fail: the loop guaranteed capacity.
    let _ = buf.push_str(sentinel);
    buf
}

/// Shared core: returns the truncated buffer plus the writer's latched `full`
/// flag (true iff input bytes were genuinely dropped).
fn format_truncating_inner<const N: usize>(
    args: core::fmt::Arguments,
) -> (heapless::String<N>, bool) {
    let mut buf = heapless::String::<N>::new();
    // `TruncatingWriter::write_str` never returns `Err`, so the only way
    // `core::fmt::write` can return `Err` here is if an argument's own
    // `Display`/`Debug` impl returns `Err` (a hand-written `fmt` that bails
    // early — derived impls never do). In that case whatever was buffered
    // before the failing argument is shipped best-effort, which is the correct
    // response for a logger; hence the discard. It does mean a message could
    // still be empty if the very first argument's `fmt` errored.
    let mut writer = TruncatingWriter {
        buf: &mut buf,
        full: false,
    };
    let _ = core::fmt::write(&mut writer, args);
    let full = writer.full;
    (buf, full)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── format_truncating: graceful log-message truncation ────────────────────
    //
    // Regression guard for the silent-blank-log bug: an over-capacity message
    // formatted via heapless::String's all-or-nothing `write_str` was dropped to
    // empty and shipped as a blank Warn frame. `format_truncating` must instead
    // keep the prefix that fits and cut at a UTF-8 char boundary.

    /// A message well over the 200-byte cap, with a multi-byte char straddling
    /// the boundary, must truncate to a non-empty, valid, char-boundary prefix —
    /// never to empty and never to partial UTF-8.
    #[test]
    fn format_truncating_truncates_at_char_boundary() {
        // 199 ASCII bytes, then a 3-byte em-dash whose bytes span the 200-byte
        // cap (bytes 199..202), then filler. Byte 200 lands mid-char in the
        // input, so a naive byte-slice at 200 would split the em-dash.
        let mut input = alloc::string::String::new();
        for _ in 0..199 {
            input.push('a');
        }
        input.push('\u{2014}'); // em-dash, 3 bytes, straddles the cap
        for _ in 0..50 {
            input.push('b');
        }
        assert!(input.len() > 200, "test input must exceed the cap");
        assert!(
            !input.is_char_boundary(200),
            "byte 200 must fall mid-char so a naive cut would split UTF-8"
        );

        let out = format_truncating::<200>(format_args!("{input}"));

        assert!(!out.is_empty(), "over-long message must not drop to empty");
        assert!(out.len() <= 200, "result must fit the 200-byte cap");
        assert!(
            input.starts_with(out.as_str()),
            "result must be a verbatim prefix of the input"
        );
        assert!(
            input.is_char_boundary(out.len()),
            "result must end on a UTF-8 char boundary"
        );
        // The straddling em-dash does not fit whole, so it is dropped entirely —
        // no partial code point is ever written.
        assert!(
            !out.contains('\u{2014}'),
            "the straddling multi-byte char must be excluded whole, not split"
        );
        assert_eq!(out.len(), 199, "prefix is the 199 ASCII bytes that fit");
    }

    /// A multi-byte char that fits exactly up to the cap is kept whole.
    #[test]
    fn format_truncating_keeps_boundary_char_that_fits() {
        let mut input = alloc::string::String::new();
        for _ in 0..197 {
            input.push('a');
        }
        input.push('\u{2014}'); // 3 bytes → bytes 197..200, fits exactly at cap
        input.push('c'); // would overflow
        let out = format_truncating::<200>(format_args!("{input}"));
        assert_eq!(out.len(), 200, "exact-fit char must be retained whole");
        assert!(out.ends_with('\u{2014}'), "boundary char kept intact");
        assert!(input.starts_with(out.as_str()));
    }

    // ── format_truncating_marked: opt-in truncation sentinel ──────────────────

    /// Genuine overflow: output fits the cap, ends with the sentinel, and the
    /// pre-sentinel prefix is verbatim.
    #[test]
    fn format_truncating_marked_marks_overflow() {
        let input: alloc::string::String = "a".repeat(300);
        let out = format_truncating_marked::<200>(format_args!("{input}"), TRUNCATION_SENTINEL);
        assert!(out.len() <= 200);
        assert!(out.ends_with(TRUNCATION_SENTINEL));
        let prefix = &out[..out.len() - TRUNCATION_SENTINEL.len()];
        assert!(input.starts_with(prefix));
        assert!(!prefix.is_empty());
    }

    /// A fitting message gets no sentinel.
    #[test]
    fn format_truncating_marked_short_message_unmarked() {
        let out =
            format_truncating_marked::<200>(format_args!("hello {}", 42), TRUNCATION_SENTINEL);
        assert_eq!(out.as_str(), "hello 42");
    }

    /// Output that is exactly `N` bytes with nothing dropped gets no sentinel.
    #[test]
    fn format_truncating_marked_exact_fit_unmarked() {
        let input: alloc::string::String = "a".repeat(200);
        let out = format_truncating_marked::<200>(format_args!("{input}"), TRUNCATION_SENTINEL);
        assert_eq!(out.len(), 200);
        assert_eq!(out.as_str(), input.as_str());
    }

    /// A multi-byte char at the splice point is popped whole: valid UTF-8, no
    /// partial code point before the sentinel.
    #[test]
    fn format_truncating_marked_splices_on_char_boundary() {
        let mut input = alloc::string::String::new();
        for _ in 0..100 {
            input.push('\u{2014}'); // 3 bytes each → 300 bytes
        }
        let out = format_truncating_marked::<200>(format_args!("{input}"), TRUNCATION_SENTINEL);
        assert!(out.ends_with(TRUNCATION_SENTINEL));
        let prefix = &out[..out.len() - TRUNCATION_SENTINEL.len()];
        assert!(input.starts_with(prefix));
        assert!(prefix.chars().all(|c| c == '\u{2014}'));
    }

    /// A sentinel larger than the capacity is misuse; the debug_assert fires.
    /// The release-mode fallback (skip the splice, return the plain truncated
    /// buffer) cannot be exercised from a debug-assertions build.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "sentinel must fit the capacity")]
    fn format_truncating_marked_oversized_sentinel_asserts() {
        let _ = format_truncating_marked::<2>(format_args!("hello"), "[truncated]");
    }

    /// Sentinel exactly fills capacity: the pop loop must drain the buffer to
    /// empty and still terminate.
    #[test]
    fn format_truncating_marked_sentinel_fills_capacity() {
        let input = "a".repeat(50);
        let out = format_truncating_marked::<3>(format_args!("{input}"), TRUNCATION_SENTINEL);
        assert_eq!(out.as_str(), TRUNCATION_SENTINEL);
    }

    /// One byte of headroom over the sentinel: a single-char prefix survives.
    #[test]
    fn format_truncating_marked_minimal_prefix_survives() {
        let input = "a".repeat(50);
        let out = format_truncating_marked::<4>(format_args!("{input}"), TRUNCATION_SENTINEL);
        assert_eq!(out.as_str(), "a…");
    }

    /// A message that fits is passed through unchanged.
    #[test]
    fn format_truncating_passes_through_short_message() {
        let out = format_truncating::<200>(format_args!("hello {}", 42));
        assert_eq!(out.as_str(), "hello 42");
    }

    /// Empty format arguments must yield an empty string (the zero-write path
    /// through `TruncatingWriter`), not panic or produce garbage.
    #[test]
    fn format_truncating_empty_args_returns_empty() {
        let out = format_truncating::<200>(format_args!(""));
        assert!(out.is_empty());
    }

    /// Multi-argument message crossing the cap: the result must be a verbatim
    /// prefix of the *fully-formatted* message. `core::fmt::write` hands each
    /// argument to a separate `write_str`; without the `full` latch a narrow
    /// char from the second argument would backfill the byte a wide char in the
    /// first argument could not use, producing output that is NOT a prefix.
    #[test]
    fn format_truncating_multi_segment_is_verbatim_prefix() {
        // First arg: 199 'a' then a 3-byte em-dash (bytes 199..202) that will
        // not fit whole in the 200-byte cap. Second arg: all 'b' (1 byte each),
        // one of which WOULD fit in the single free byte if not latched out.
        let mut first = alloc::string::String::new();
        for _ in 0..199 {
            first.push('a');
        }
        first.push('\u{2014}'); // em-dash: cannot fit whole, must be dropped
        let second = "bbbbb";

        let out = format_truncating::<200>(format_args!("{first}{second}"));

        assert_eq!(out.len(), 199, "only the 199 'a' bytes fit before the cap");
        assert!(
            !out.contains('b'),
            "a later-segment char must not backfill capacity a wider earlier \
             char failed to fill (verbatim-prefix invariant)"
        );
        assert!(
            !out.contains('\u{2014}'),
            "the straddling char is dropped whole"
        );
        // The result is a prefix of the fully-formatted logical message.
        let full = alloc::format!("{first}{second}");
        assert!(
            full.starts_with(out.as_str()),
            "result must be a verbatim prefix"
        );
    }
}
