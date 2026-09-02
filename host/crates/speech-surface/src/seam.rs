//! The shape every composer-to-server queue in this crate has: a bounded,
//! dropping handoff whose sender needs no runtime and whose receiver lives
//! inside [`Server::run`](crate::server::Server::run).
//!
//! The two seams in [`Sinks`](crate::server::Sinks) are things the server
//! *calls*. These run the other way — the composing process has something to
//! say and the thing that could say it is inside the run — so what crosses is a
//! queue rather than a handle, and a refusal is a drop rather than a wait.
//!
//! One implementation rather than one per seam: the alert seam and the
//! announcement seam differ in what they carry and in what the server does with
//! it, and in nothing else. Two copies would have to grow every later property —
//! a drop counter, a capacity report, a non-panicking constructor — twice, and
//! would spell one refusal two ways the day they diverged.

use tokio::sync::mpsc;

/// Why an item did not reach a seam's queue. Both are drops: nothing is retried
/// and nothing is held, because only the sender knows whether the condition is
/// still worth carrying once there is room again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SeamRefused {
    /// The queue is full — the server's half is behind, or nothing is taking
    /// what it drains.
    #[error("the seam's queue is full")]
    Backlogged,
    /// The server's half is gone: the run ended, or it was never composed with
    /// this seam.
    #[error("the server's end of the seam is gone")]
    Gone,
}

impl SeamRefused {
    /// The fixed word a caller reports the refusal under, so two embedders' logs
    /// spell one condition the same way — and so the two seams a reporting
    /// surface carries cannot drift into two vocabularies for one failure.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            SeamRefused::Backlogged => "backlogged",
            SeamRefused::Gone => "gone",
        }
    }
}

/// The composing process's end. Cheap to clone, and callable from any thread —
/// handing an item over neither blocks nor needs a runtime.
#[derive(Debug)]
pub struct Handoff<T> {
    items: mpsc::Sender<T>,
}

// Hand written rather than derived: cloning a sender never depends on what it
// carries, and the derive would demand `T: Clone`.
impl<T> Clone for Handoff<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
        }
    }
}

impl<T> Handoff<T> {
    /// Queue one item. Returns once it is queued, never once the far end has
    /// acted on it.
    ///
    /// # Errors
    ///
    /// [`SeamRefused::Backlogged`] if the queue is full and [`SeamRefused::Gone`]
    /// if the server's end is closed. Either way this item is dropped.
    pub fn hand(&self, item: T) -> Result<(), SeamRefused> {
        self.items.try_send(item).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SeamRefused::Backlogged,
            mpsc::error::TrySendError::Closed(_) => SeamRefused::Gone,
        })
    }
}

/// The server's end, handed to the run that drains it.
#[derive(Debug)]
pub struct Inbox<T> {
    items: mpsc::Receiver<T>,
}

impl<T> Inbox<T> {
    /// The next item, or `None` once every handoff is dropped.
    pub(crate) async fn next(&mut self) -> Option<T> {
        self.items.recv().await
    }

    /// Whatever is already queued, taken without waiting.
    ///
    /// For a drain that is ending: what it holds at that moment is lost, and a
    /// dropped item nothing names is indistinguishable from one that was never
    /// handed over.
    ///
    /// Closes the queue before taking it, so an item handed over after this
    /// call is refused [`SeamRefused::Gone`] — which the composer names —
    /// rather than accepted into a buffer about to be dropped. Items already
    /// queued still come back: closing stops the senders, not this drain.
    pub(crate) fn take_queued(&mut self) -> Vec<T> {
        self.items.close();
        let mut queued = Vec::new();
        while let Ok(item) = self.items.try_recv() {
            queued.push(item);
        }
        queued
    }
}

/// Mint a seam `depth` items deep.
///
/// # Panics
///
/// If `depth` is zero. A zero-depth queue accepts nothing, which is a seam that
/// silently drops everything rather than a configuration.
#[must_use]
pub fn seam<T>(depth: usize) -> (Handoff<T>, Inbox<T>) {
    assert!(depth > 0, "a seam holds at least one item");
    let (tx, rx) = mpsc::channel(depth);
    (Handoff { items: tx }, Inbox { items: rx })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ordinary path, driven the way the robot's host drives both seams:
    /// off the blocking loop's own thread.
    #[tokio::test]
    async fn an_item_handed_over_off_a_runtime_reaches_the_inbox() {
        let (handoff, mut inbox) = seam::<u32>(2);
        let handed = std::thread::spawn(move || handoff.hand(7));
        handed
            .join()
            .expect("the handing thread")
            .expect("the queue has room");

        assert_eq!(inbox.next().await, Some(7));
    }

    /// Full is a drop, and the caller is told which drop it was.
    #[test]
    fn a_full_queue_refuses_without_blocking() {
        let (handoff, _inbox) = seam::<u32>(1);
        handoff.hand(1).expect("room");

        let refused = handoff.hand(2).expect_err("the queue is full");
        assert_eq!(refused, SeamRefused::Backlogged);
        assert_eq!(refused.reason(), "backlogged");
    }

    /// The server's end dropped — the run ended, or the composer never handed
    /// the inbox over. Handing says so instead of appearing to succeed.
    #[test]
    fn a_dropped_inbox_refuses_as_gone() {
        let (handoff, inbox) = seam::<u32>(1);
        drop(inbox);

        let refused = handoff.hand(1).expect_err("the inbox is gone");
        assert_eq!(refused, SeamRefused::Gone);
        assert_eq!(refused.reason(), "gone");
    }

    /// What a drain that is ending reports: everything still queued, at once,
    /// and nothing left behind.
    #[test]
    fn what_is_queued_is_taken_without_waiting() {
        let (handoff, mut inbox) = seam::<u32>(4);
        handoff.hand(1).expect("room");
        handoff.hand(2).expect("room");

        assert_eq!(inbox.take_queued(), vec![1, 2]);
        assert!(inbox.take_queued().is_empty(), "taken once");
    }

    /// The race the close is for: a drain takes what it holds and returns while
    /// the composer, on its own thread, is still handing items over. Anything
    /// handed after the take is refused rather than queued into a buffer that
    /// is about to be dropped, so the composer names it.
    #[test]
    fn nothing_handed_after_the_queue_is_taken_appears_to_succeed() {
        let (handoff, mut inbox) = seam::<u32>(4);
        handoff.hand(1).expect("room");

        assert_eq!(inbox.take_queued(), vec![1]);

        let refused = handoff.hand(2).expect_err("the drain has ended");
        assert_eq!(refused, SeamRefused::Gone);
        assert!(
            inbox.take_queued().is_empty(),
            "a refused item was never queued"
        );
    }

    #[test]
    #[should_panic(expected = "at least one item")]
    fn a_zero_depth_seam_is_refused() {
        let _ = seam::<u32>(0);
    }
}
