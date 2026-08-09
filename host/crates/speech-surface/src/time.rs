//! Time helpers shared by the crate's select loops.

use tokio::time::Instant;

/// A select arm's deadline: sleeps until `deadline` when there is one, never
/// resolves when there is not. Takes it by value so the arm holds no borrow of
/// the task that owns the deadline.
pub(crate) async fn due(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    /// The arm every caller selects on when it has a deadline.
    #[tokio::test]
    async fn a_deadline_resolves_when_it_arrives() {
        let at = Instant::now() + Duration::from_millis(10);
        tokio::time::timeout(Duration::from_secs(5), due(Some(at)))
            .await
            .expect("the deadline arrives");
        assert!(Instant::now() >= at, "it waited for the instant");
    }

    /// And with no deadline it never resolves. This is what parks a select loop
    /// that has nothing scheduled: an arm that resolved immediately would spin
    /// the loop at full CPU around work that has nothing to do, which no
    /// assertion in either caller would notice.
    #[tokio::test]
    async fn no_deadline_never_resolves() {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), due(None))
                .await
                .is_err(),
            "an absent deadline resolved",
        );
    }
}
