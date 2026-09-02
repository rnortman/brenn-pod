//! The operator-alert seam: a queue a composing process raises alerts on, and
//! the half the server drains onto the bus attachment it already holds.
//!
//! The two seams in [`Sinks`](crate::server::Sinks) are things the server
//! *calls*. This one runs the other way: the composing process has something to
//! say and the attachment that could say it lives inside [`Server::run`]. What
//! crosses is a queue rather than a handle, for two reasons — the raiser is not
//! necessarily on a runtime thread, and an alert is fire-and-forget on the
//! plane's own shape, so
//! nothing about raising one should wait on a socket.
//!
//! Bounded and dropping: a backlog of operator alerts is a backlog of stale
//! ones, and the alternative — growing without bound behind an attachment that
//! is down — turns a reporting path into a memory leak in the one condition it
//! exists to report through.
//!
//! The queue itself is [`crate::seam`]; what is here is this seam's cargo, its
//! depth, and the verb an operator surface raises through.
//!
//! [`Server::run`]: crate::server::Server::run

/// The loudness an [`Alert`] carries, as the bus plane spells it.
///
/// Re-exported because it is half of this seam's vocabulary: an embedder that
/// fills in an [`Alert`] cannot say anything without it, and reaching it any
/// other way costs a build edge onto the attachment crate for one enum — a
/// dependency footprint larger than the seam being used.
pub use brenn_bridge::AlertSeverity;

/// A depth that suits a table raising each condition once per run: enough that
/// a burst at startup survives a reattach, small enough that what does survive
/// is still worth reading when it lands.
pub const ALERT_QUEUE_DEPTH: usize = 8;

/// One operator alert on its way to the bus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alert {
    pub severity: AlertSeverity,
    /// One line naming what happened.
    pub title: String,
    /// What an operator needs to decide with.
    pub body: String,
}

/// Why an alert did not reach the queue. The seam's own refusal: both variants
/// are drops, and the two words they report under are the words every seam in
/// this crate reports under.
pub type AlertRefused = crate::seam::SeamRefused;

/// The composing process's end of the seam. Cheap to clone, and callable from
/// any thread — raising an alert neither blocks nor needs a runtime.
#[derive(Clone, Debug)]
pub struct AlertRaiser(crate::seam::Handoff<Alert>);

impl AlertRaiser {
    /// Queue one alert for the attachment. Returns once it is queued, never once
    /// the peer has seen it: a granted alert is answered by no frame at all.
    ///
    /// # Errors
    ///
    /// [`AlertRefused::Backlogged`] if the queue is full and
    /// [`AlertRefused::Gone`] if the server's end is closed. Either way this
    /// alert is dropped.
    pub fn raise(&self, alert: Alert) -> Result<(), AlertRefused> {
        self.0.hand(alert)
    }
}

/// The server's end of the seam, handed to [`Server::with_alerts`].
///
/// [`Server::with_alerts`]: crate::server::Server::with_alerts
#[derive(Debug)]
pub struct AlertInbox(crate::seam::Inbox<Alert>);

impl AlertInbox {
    /// The next alert, or `None` once every raiser is dropped.
    pub(crate) async fn next(&mut self) -> Option<Alert> {
        self.0.next().await
    }

    /// The alerts still queued, taken without waiting, for a drain that is
    /// ending while the composer is still raising.
    pub(crate) fn take_queued(&mut self) -> Vec<Alert> {
        self.0.take_queued()
    }
}

/// Mint the seam `depth` alerts deep.
///
/// # Panics
///
/// If `depth` is zero. A zero-depth queue accepts nothing, which is a seam that
/// silently drops every alert rather than a configuration.
#[must_use]
pub fn alert_seam(depth: usize) -> (AlertRaiser, AlertInbox) {
    let (handoff, inbox) = crate::seam::seam(depth);
    (AlertRaiser(handoff), AlertInbox(inbox))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alert(title: &str) -> Alert {
        Alert {
            severity: AlertSeverity::Critical,
            title: title.to_owned(),
            body: "the head is parked".to_owned(),
        }
    }

    /// A raiser clone drives the same queue: the alert table and the edge's own
    /// drops are two callers of one seam.
    #[tokio::test]
    async fn a_clone_drives_the_same_queue() {
        let (raiser, mut inbox) = alert_seam(2);
        let second = raiser.clone();
        raiser.raise(alert("first")).expect("room");
        second.raise(alert("second")).expect("room");

        assert_eq!(inbox.next().await.expect("first").title, "first");
        assert_eq!(inbox.next().await.expect("second").title, "second");
    }
}
