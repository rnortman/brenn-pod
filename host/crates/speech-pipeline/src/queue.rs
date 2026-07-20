//! The assembler→pipeline boundary channel: bounded, and on overflow it drops
//! the *oldest* whole segment (with a counter and, at the call site, a JSONL
//! event) rather than blocking the ingest task or dropping the newest item.
//!
//! tokio's `mpsc` cannot drop-oldest from the producer side (a full `send`
//! parks the producer), so this is a small purpose-built channel: a shared
//! `VecDeque` behind a mutex, a `Notify` to wake the receiver, and intrinsic
//! counters so every boundary reports depth / high-water / pushed / dropped
//! into `stage_health`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::Notify;

use crate::stats::HighWater;

/// Point-in-time counters for one queue, read from either end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct QueueStats {
    /// Items currently buffered.
    pub depth: u64,
    /// Greatest `depth` observed since creation.
    pub high_water: u64,
    /// Total items accepted into the queue.
    pub pushed: u64,
    /// Total oldest items displaced by overflow.
    pub dropped_oldest: u64,
    /// Items handed to `send` after the receiver dropped; always 0 while the
    /// consumer lives.
    pub send_failures: u64,
}

/// The shared queue state behind a `Sender`/`Receiver` pair. Constructed only
/// via [`DropOldestQueue::new`], which hands back the two ends.
pub struct DropOldestQueue<T> {
    queue: Mutex<VecDeque<T>>,
    notify: Notify,
    capacity: usize,
    pushed: AtomicU64,
    dropped_oldest: AtomicU64,
    high_water: HighWater,
    /// Live `Sender` count; the receiver returns `None` once it hits zero and
    /// the queue drains.
    senders: AtomicUsize,
    /// Set by the `Receiver`'s `Drop`; senders drop items once true.
    closed: AtomicBool,
    /// Items handed to `send` after the receiver closed (dropped, undelivered).
    send_failures: AtomicU64,
}

impl<T> DropOldestQueue<T> {
    /// Create a bounded drop-oldest queue holding at most `capacity` items,
    /// returning its producer and consumer ends.
    // A channel factory hands back the two ends, not `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(capacity: usize) -> (Sender<T>, Receiver<T>) {
        // Capacity 0 collapses the channel into a black hole: every `send`
        // pushes then immediately pops the item it just pushed, so nothing is
        // ever delivered. The config layer rejects `segment_queue_depth == 0`;
        // this guards every other caller.
        assert!(capacity >= 1, "DropOldestQueue capacity must be at least 1");
        // `send` push_backs then pop_fronts on overflow, so the deque briefly
        // holds `capacity + 1`; size the buffer for that peak so overflow never
        // reallocates.
        let shared = Arc::new(DropOldestQueue {
            queue: Mutex::new(VecDeque::with_capacity(capacity + 1)),
            notify: Notify::new(),
            capacity,
            pushed: AtomicU64::new(0),
            dropped_oldest: AtomicU64::new(0),
            high_water: HighWater::default(),
            senders: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
            send_failures: AtomicU64::new(0),
        });
        (
            Sender {
                shared: shared.clone(),
            },
            Receiver { shared },
        )
    }

    fn stats(&self) -> QueueStats {
        // Read-only observer: tolerate a poisoned mutex rather than panicking
        // the periodic stats reader alongside whatever already failed.
        let depth = match self.queue.lock() {
            Ok(q) => q.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        } as u64;
        QueueStats {
            depth,
            high_water: self.high_water.load(),
            pushed: self.pushed.load(Ordering::Relaxed),
            dropped_oldest: self.dropped_oldest.load(Ordering::Relaxed),
            send_failures: self.send_failures.load(Ordering::Relaxed),
        }
    }
}

/// Producer half. Cloneable — the queue stays open while any `Sender` lives.
pub struct Sender<T> {
    shared: Arc<DropOldestQueue<T>>,
}

/// Consumer half. Single-consumer: there is exactly one `Receiver`. Dropping it
/// closes the queue: buffered items are freed and further sends are counted as
/// `send_failures` rather than overflow.
pub struct Receiver<T> {
    shared: Arc<DropOldestQueue<T>>,
}

impl<T> Sender<T> {
    /// Enqueue `item`. If the queue is already at capacity, the oldest item is
    /// evicted and returned; otherwise returns `None`. Never blocks.
    ///
    /// Once the `Receiver` has dropped, the item is discarded, `send_failures`
    /// is incremented, and `None` is returned — `Some` always means "displaced
    /// by overflow".
    pub fn send(&self, item: T) -> Option<T> {
        // Checked before the lock so a closed send never touches the mutex,
        // which may be poisoned by the same panic that dropped the receiver.
        // The in-lock check below still closes the drop/send race.
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.send_failures.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let displaced = {
            let mut q = self.shared.queue.lock().expect("queue mutex poisoned");
            if self.shared.closed.load(Ordering::Acquire) {
                drop(q);
                self.shared.send_failures.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            q.push_back(item);
            self.shared.pushed.fetch_add(1, Ordering::Relaxed);
            let displaced = if q.len() > self.shared.capacity {
                q.pop_front()
            } else {
                None
            };
            let depth = q.len() as u64;
            self.shared.high_water.bump(depth);
            displaced
        };
        if displaced.is_some() {
            self.shared.dropped_oldest.fetch_add(1, Ordering::Relaxed);
        }
        self.shared.notify.notify_one();
        displaced
    }

    /// Current counters for this queue.
    pub fn stats(&self) -> QueueStats {
        self.shared.stats()
    }

    /// A read-only stats view. Unlike a cloned `Sender`, a `StatsHandle` does
    /// not count toward the live-sender total, so a periodic `stage_health`
    /// reader can hold one without keeping the channel open (which would stall
    /// the receiver's drain at shutdown).
    pub fn stats_handle(&self) -> StatsHandle<T> {
        StatsHandle {
            shared: self.shared.clone(),
        }
    }
}

/// A read-only counter view onto a queue that does not participate in the
/// sender count — holding one never prevents the receiver from observing close.
pub struct StatsHandle<T> {
    shared: Arc<DropOldestQueue<T>>,
}

impl<T> StatsHandle<T> {
    /// Current counters for the queue this handle views.
    pub fn stats(&self) -> QueueStats {
        self.shared.stats()
    }
}

impl<T> Clone for StatsHandle<T> {
    fn clone(&self) -> Self {
        StatsHandle {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Sender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last sender gone: wake the receiver so a pending `recv` observes
            // the closed-and-empty state and returns `None`.
            self.shared.notify.notify_waiters();
        }
    }
}

impl<T> Receiver<T> {
    /// Await the next item. Returns `None` once every `Sender` has dropped and
    /// the queue has drained.
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            // Register for notification before inspecting the queue so a `send`
            // (or last-sender drop) racing between the check and the await
            // still wakes us.
            let notified = self.shared.notify.notified();
            {
                let mut q = self.shared.queue.lock().expect("queue mutex poisoned");
                if let Some(item) = q.pop_front() {
                    return Some(item);
                }
                if self.shared.senders.load(Ordering::Acquire) == 0 {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Current counters for this queue.
    pub fn stats(&self) -> QueueStats {
        self.shared.stats()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Store before touching the lock: a send that read `false` did so before
        // this store and its item is swept by the `clear` below; any send taking
        // the lock after it observes `true`.
        self.shared.closed.store(true, Ordering::Release);
        // Tolerate a poisoned mutex: a panic inside `send`'s lock scope may be
        // unwinding right now, and panicking in Drop during unwind aborts.
        let mut q = match self.shared.queue.lock() {
            Ok(q) => q,
            Err(poisoned) => poisoned.into_inner(),
        };
        q.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_recv_fifo_under_capacity() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(4);
        assert_eq!(tx.send(1), None);
        assert_eq!(tx.send(2), None);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn overflow_displaces_oldest() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        assert_eq!(tx.send(1), None);
        assert_eq!(tx.send(2), None);
        // Third push overflows: oldest (1) is displaced and returned.
        assert_eq!(tx.send(3), Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn counters_track_pushed_dropped_and_high_water() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(2);
        tx.send(1);
        tx.send(2);
        tx.send(3); // displaces 1
        tx.send(4); // displaces 2
        let s = tx.stats();
        assert_eq!(s.pushed, 4);
        assert_eq!(s.dropped_oldest, 2);
        assert_eq!(s.high_water, 2);
        assert_eq!(s.depth, 2);
        assert_eq!(s.send_failures, 0);
    }

    #[tokio::test]
    async fn send_after_receiver_drop_is_a_send_failure_not_overflow() {
        let (tx, rx) = DropOldestQueue::<u32>::new(2);
        tx.send(1);
        tx.send(2);
        let before = tx.stats();
        drop(rx);
        assert_eq!(tx.send(3), None);
        assert_eq!(tx.send(4), None);
        let s = tx.stats();
        assert_eq!(s.pushed, before.pushed);
        assert_eq!(s.dropped_oldest, before.dropped_oldest);
        assert_eq!(s.send_failures, 2);
        assert_eq!(s.depth, 0);
    }

    #[tokio::test]
    async fn receiver_drop_releases_buffered_items() {
        let (tx, rx) = DropOldestQueue::<Arc<()>>::new(4);
        let item = Arc::new(());
        tx.send(item.clone());
        tx.send(item.clone());
        assert_eq!(Arc::strong_count(&item), 3);
        drop(rx);
        // Senders still live, but the buffered items are freed immediately.
        assert_eq!(Arc::strong_count(&item), 1);
    }

    #[tokio::test]
    async fn send_failures_zero_on_normal_close() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let handle = tx.stats_handle();
        tx.send(1);
        drop(tx);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, None);
        drop(rx);
        assert_eq!(handle.stats().send_failures, 0);
    }

    #[tokio::test]
    async fn closed_send_and_stats_survive_a_poisoned_mutex() {
        let (tx, rx) = DropOldestQueue::<u32>::new(2);
        let handle = tx.stats_handle();
        tx.send(1);
        // Poison the queue mutex the way a panic inside a lock scope would.
        let shared = rx.shared.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = shared.queue.lock().unwrap();
            panic!("poison");
        }));
        assert!(shared.queue.lock().is_err(), "mutex should be poisoned");
        drop(rx);
        // Neither producer nor the stats reader may panic on the poisoned lock.
        assert_eq!(tx.send(2), None);
        assert_eq!(tx.send(3), None);
        let stats = handle.stats();
        assert_eq!(stats.send_failures, 2);
        assert_eq!(stats.dropped_oldest, 0);
        assert_eq!(stats.depth, 0);
    }

    #[tokio::test]
    async fn recv_returns_none_when_all_senders_dropped_and_empty() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        tx.send(1);
        drop(tx);
        // Buffered item drains first, then the closed channel yields `None`.
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn recv_wakes_on_late_send() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let handle = tokio::spawn(async move { rx.recv().await });
        // Give the receiver a chance to park on an empty queue, then send.
        tokio::task::yield_now().await;
        assert_eq!(tx.send(7), None);
        assert_eq!(handle.await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn recv_wakes_on_last_sender_drop() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let handle = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        drop(tx);
        assert_eq!(handle.await.unwrap(), None);
    }

    #[test]
    #[should_panic(expected = "capacity must be at least 1")]
    fn capacity_zero_panics() {
        let _ = DropOldestQueue::<u32>::new(0);
    }

    #[tokio::test]
    async fn stays_open_while_one_sender_lives() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let tx2 = tx.clone();
        drop(tx);
        tx2.send(9);
        assert_eq!(rx.recv().await, Some(9));
    }

    #[tokio::test]
    async fn stats_handle_does_not_keep_the_channel_open() {
        use tokio::time::{timeout, Duration};
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        // A StatsHandle is the load-bearing non-sender view: holding one must NOT
        // count toward the live-sender total, or the receiver would never observe
        // close and the pipeline drain at shutdown would hang forever.
        let handle = tx.stats_handle();
        drop(tx);
        let got = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("recv resolved — a StatsHandle must not count as a live sender");
        assert_eq!(got, None);
        // The handle's read path still works after the last sender is gone.
        let _ = handle.stats();
    }

    #[tokio::test]
    async fn stats_handle_reads_pushed_and_depth() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(4);
        let handle = tx.stats_handle();
        tx.send(1);
        tx.send(2);
        let s = handle.stats();
        assert_eq!(s.pushed, 2);
        assert_eq!(s.depth, 2);
    }
}
