//! The announcement seam: a queue a composing process puts a sentence on, and
//! the half the server turns into speech on every pod that is listening.
//!
//! The same direction as the operator-alert seam and for the same reasons — the
//! sender is not necessarily on a runtime thread, and a sentence is
//! fire-and-forget, so nothing about announcing one waits on a socket or on a
//! synthesizer. What differs is the far end: an alert leaves the machine on the
//! bus attachment, an announcement stays in the room and comes out of the
//! speaker.
//!
//! Bounded and dropping, for the alert seam's reason plus one of its own: a
//! backlog of announcements is a robot talking about conditions that have
//! passed, and a listener cannot skip ahead.
//!
//! The queue itself is [`crate::seam`], shared with the alert seam so the two
//! cannot drift in what they do or in the word they refuse under; what is here
//! is this seam's cargo, its depth, and the verb a composer announces through.

/// A depth that suits a composer announcing each condition once per run: a
/// small burst survives a moment where the router is busy with a reply, and
/// what does survive is still worth hearing when it plays.
pub const ANNOUNCE_QUEUE_DEPTH: usize = 4;

/// One sentence on its way to whatever pods are connected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Announcement {
    /// What the robot says, already in the words a listener hears. Synthesized
    /// by the run's `[tts]` voice like any other spoken text.
    pub text: String,
}

/// Why a sentence did not reach the queue. The seam's own refusal, shared with
/// the alert seam: both variants are drops, and a surface carrying both seams
/// reports one condition under one word.
pub type AnnounceRefused = crate::seam::SeamRefused;

/// The composing process's end of the seam. Cheap to clone, and callable from
/// any thread — announcing neither blocks nor needs a runtime.
#[derive(Clone, Debug)]
pub struct Announcer(crate::seam::Handoff<Announcement>);

impl Announcer {
    /// Queue one sentence. Returns once it is queued, never once it has been
    /// heard: what happens to it after that is the router's business and the
    /// run's own log is where that shows up.
    ///
    /// # Errors
    ///
    /// [`AnnounceRefused::Backlogged`] if the queue is full and
    /// [`AnnounceRefused::Gone`] if the server's end is closed. Either way this
    /// sentence is dropped.
    pub fn announce(&self, sentence: Announcement) -> Result<(), AnnounceRefused> {
        self.0.hand(sentence)
    }
}

/// The server's end of the seam, handed to [`Server::with_announcements`].
///
/// [`Server::with_announcements`]: crate::server::Server::with_announcements
#[derive(Debug)]
pub struct AnnounceInbox(crate::seam::Inbox<Announcement>);

impl AnnounceInbox {
    /// The next sentence, or `None` once every announcer is dropped.
    pub(crate) async fn next(&mut self) -> Option<Announcement> {
        self.0.next().await
    }

    /// The sentences still queued, taken without waiting, for a drain that is
    /// ending while the composer is still announcing.
    pub(crate) fn take_queued(&mut self) -> Vec<Announcement> {
        self.0.take_queued()
    }
}

/// Mint the seam `depth` sentences deep.
///
/// # Panics
///
/// If `depth` is zero. A zero-depth queue accepts nothing, which is a seam that
/// silently drops every sentence rather than a configuration.
#[must_use]
pub fn announce_seam(depth: usize) -> (Announcer, AnnounceInbox) {
    let (handoff, inbox) = crate::seam::seam(depth);
    (Announcer(handoff), AnnounceInbox(inbox))
}
