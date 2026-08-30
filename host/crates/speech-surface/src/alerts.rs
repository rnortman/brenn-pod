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
//! [`Server::run`]: crate::server::Server::run

use brenn_bridge::AlertSeverity;
use tokio::sync::mpsc;

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

/// Why an alert did not reach the queue. Both are drops: nothing is retried and
/// nothing is held, because the sender is the only thing that knows whether the
/// condition still holds by the time an attachment exists again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AlertRefused {
    /// The queue is full — the drain is behind, or there is no live attachment
    /// carrying alerts away.
    #[error("the alert queue is full")]
    Backlogged,
    /// The server's half is gone: the run ended, or it was never composed with
    /// this seam.
    #[error("the server's end of the alert seam is gone")]
    Gone,
}

impl AlertRefused {
    /// The fixed word a caller reports the refusal under, so two embedders'
    /// logs spell one condition the same way.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            AlertRefused::Backlogged => "backlogged",
            AlertRefused::Gone => "gone",
        }
    }
}

/// The composing process's end of the seam. Cheap to clone, and callable from
/// any thread — raising an alert neither blocks nor needs a runtime.
#[derive(Clone, Debug)]
pub struct AlertRaiser {
    alerts: mpsc::Sender<Alert>,
}

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
        self.alerts.try_send(alert).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => AlertRefused::Backlogged,
            mpsc::error::TrySendError::Closed(_) => AlertRefused::Gone,
        })
    }
}

/// The server's end of the seam, handed to [`Server::with_alerts`].
///
/// [`Server::with_alerts`]: crate::server::Server::with_alerts
#[derive(Debug)]
pub struct AlertInbox {
    alerts: mpsc::Receiver<Alert>,
}

impl AlertInbox {
    /// The next alert, or `None` once every raiser is dropped.
    pub(crate) async fn next(&mut self) -> Option<Alert> {
        self.alerts.recv().await
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
    assert!(depth > 0, "an alert seam holds at least one alert");
    let (tx, rx) = mpsc::channel(depth);
    (AlertRaiser { alerts: tx }, AlertInbox { alerts: rx })
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

    /// The ordinary path, driven the way the robot's host drives it.
    #[tokio::test]
    async fn an_alert_raised_off_a_runtime_reaches_the_inbox() {
        let (raiser, mut inbox) = alert_seam(2);
        // Off the runtime's own thread: `raise` is not `async` precisely so
        // this works.
        let raised = std::thread::spawn(move || raiser.raise(alert("parked")));
        raised
            .join()
            .expect("the raising thread")
            .expect("the queue has room");

        let taken = inbox.next().await.expect("one alert was raised");
        assert_eq!(taken, alert("parked"));
    }

    /// Full is a drop, and the caller is told which drop it was — the drain is
    /// behind or the attachment is down, both of which an operator surface
    /// reports differently from a seam that is gone.
    #[test]
    fn a_full_queue_refuses_without_blocking() {
        let (raiser, _inbox) = alert_seam(1);
        raiser.raise(alert("first")).expect("the queue has room");

        let refused = raiser
            .raise(alert("second"))
            .expect_err("the queue is full");
        assert_eq!(refused, AlertRefused::Backlogged);
        assert_eq!(refused.reason(), "backlogged");
    }

    /// The server's end dropped — the run ended, or the composer never handed
    /// the inbox over. Raising says so instead of appearing to succeed.
    #[test]
    fn a_dropped_inbox_refuses_as_gone() {
        let (raiser, inbox) = alert_seam(1);
        drop(inbox);

        let refused = raiser
            .raise(alert("parked"))
            .expect_err("the inbox is gone");
        assert_eq!(refused, AlertRefused::Gone);
        assert_eq!(refused.reason(), "gone");
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

    #[test]
    #[should_panic(expected = "at least one alert")]
    fn a_zero_depth_seam_is_refused() {
        let _ = alert_seam(0);
    }
}
