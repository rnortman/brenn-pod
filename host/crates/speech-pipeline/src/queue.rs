//! The assembler→pipeline boundary channel: one FIFO with two lanes.
//!
//! **Sheddable** items are bulk data — assembled segments — that the system may
//! shed under flood. They are what `capacity` bounds: pushing one past capacity
//! evicts the *oldest sheddable* item (with a counter and, at the call site, a
//! JSONL event) rather than blocking the ingest task or dropping the newest.
//!
//! **Reliable** items are control events whose loss is a lost utterance or a
//! lost wake. They are never displaced and never displace, and they do not count
//! toward `capacity`; their volume is bounded upstream instead (the listener
//! emits a handful of events per segment, behind an audio feed that is itself
//! the lossy stage). The daemon-side invariant is that control events are never
//! shed.
//!
//! One queue and one FIFO, because segment/listener relative order is a
//! production invariant: a carve that reached the consumer before its covering
//! segment would resolve to spliced silence. `recv` yields both lanes in push
//! order.
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
    /// Items currently buffered, across both lanes.
    pub depth: u64,
    /// Greatest `depth` observed since creation, across both lanes.
    pub high_water: u64,
    /// Total items accepted into the queue, across both lanes.
    pub pushed: u64,
    /// Total items accepted onto the sheddable lane — the throughput of the
    /// bulk data the queue carries, unmixed with control events.
    pub sheddable_pushed: u64,
    /// Total oldest items displaced by overflow. Sheddable-lane only —
    /// a reliable item is never displaced.
    pub dropped_oldest: u64,
    /// Items handed to either lane after the receiver dropped; always 0 while
    /// the consumer lives.
    pub send_failures: u64,
    /// Greatest reliable backlog observed since creation: how far behind the
    /// consumer has fallen on the lane `capacity` does not bound.
    pub reliable_high_water: u64,
    /// Sheddable items currently buffered — the quantity `capacity` bounds, and
    /// never above it.
    pub sheddable_depth: u64,
    /// Times the queue caught its own lane accounting disagreeing with the
    /// buffer it describes. Always 0 in a correct build; any non-zero value
    /// means every other field here may be fiction — the sheddable lane is
    /// either not bounding at all or evicting with room to spare — and is a
    /// defect in this module, never a load symptom to tune.
    pub bookkeeping_faults: u64,
}

/// One buffered item and the lane it rides.
struct Entry<T> {
    item: T,
    sheddable: bool,
}

/// The buffered items plus the running count of the sheddable ones among them.
struct Buffer<T> {
    items: VecDeque<Entry<T>>,
    /// Count of entries in `items` with `sheddable == true`.
    sheddable_len: usize,
}

/// The shared queue state behind a `Sender`/`Receiver` pair. Constructed only
/// via [`DropOldestQueue::new`], which hands back the two ends.
pub struct DropOldestQueue<T> {
    queue: Mutex<Buffer<T>>,
    notify: Notify,
    /// Bounds the sheddable lane only.
    capacity: usize,
    pushed: AtomicU64,
    sheddable_pushed: AtomicU64,
    dropped_oldest: AtomicU64,
    bookkeeping_faults: AtomicU64,
    high_water: HighWater,
    reliable_high_water: HighWater,
    /// Live `Sender` count; the receiver returns `None` once it hits zero and
    /// the queue drains.
    senders: AtomicUsize,
    /// Set by the `Receiver`'s `Drop`; senders drop items once true.
    closed: AtomicBool,
    /// Items handed to a send after the receiver closed (dropped, undelivered).
    send_failures: AtomicU64,
}

impl<T> DropOldestQueue<T> {
    /// Create a queue whose sheddable lane holds at most `capacity` items,
    /// returning its producer and consumer ends.
    // A channel factory hands back the two ends, not `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(capacity: usize) -> (Sender<T>, Receiver<T>) {
        // Capacity 0 collapses the sheddable lane into a black hole: every
        // `send_sheddable` pushes then immediately pops the item it just pushed,
        // so no segment is ever delivered. The config layer rejects
        // `segment_queue_depth == 0`; this guards every other caller.
        assert!(capacity >= 1, "DropOldestQueue capacity must be at least 1");
        // `send_sheddable` push_backs then removes on overflow, so the deque
        // briefly holds `capacity + 1` sheddable entries; size the buffer for
        // that peak so a segment overflow never reallocates.
        let shared = Arc::new(DropOldestQueue {
            queue: Mutex::new(Buffer {
                items: VecDeque::with_capacity(capacity + 1),
                sheddable_len: 0,
            }),
            notify: Notify::new(),
            capacity,
            pushed: AtomicU64::new(0),
            sheddable_pushed: AtomicU64::new(0),
            dropped_oldest: AtomicU64::new(0),
            bookkeeping_faults: AtomicU64::new(0),
            high_water: HighWater::default(),
            reliable_high_water: HighWater::default(),
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
        let (depth, sheddable_depth) = match self.queue.lock() {
            Ok(q) => (q.items.len() as u64, q.sheddable_len as u64),
            Err(poisoned) => {
                let q = poisoned.into_inner();
                (q.items.len() as u64, q.sheddable_len as u64)
            }
        };
        // The lane count can never exceed the buffer it counts a subset of.
        // Observing otherwise is free here and is the only witness a release
        // build gets that the two have drifted apart.
        if sheddable_depth > depth {
            self.bookkeeping_faults.fetch_add(1, Ordering::Relaxed);
        }
        QueueStats {
            depth,
            high_water: self.high_water.load(),
            pushed: self.pushed.load(Ordering::Relaxed),
            sheddable_pushed: self.sheddable_pushed.load(Ordering::Relaxed),
            dropped_oldest: self.dropped_oldest.load(Ordering::Relaxed),
            send_failures: self.send_failures.load(Ordering::Relaxed),
            reliable_high_water: self.reliable_high_water.load(),
            sheddable_depth,
            bookkeeping_faults: self.bookkeeping_faults.load(Ordering::Relaxed),
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
    /// Enqueue `item` on the sheddable lane. If the lane is already at
    /// capacity, the oldest sheddable item is evicted and returned; otherwise
    /// returns `None`. Never blocks.
    ///
    /// Once the `Receiver` has dropped, the item is discarded, `send_failures`
    /// is incremented, and `None` is returned — `Some` always means "displaced
    /// by overflow".
    pub fn send_sheddable(&self, item: T) -> Option<T> {
        self.push(item, true)
    }

    /// Enqueue `item` on the reliable lane: never displaced, never displacing,
    /// and not counted toward `capacity`. Never blocks.
    ///
    /// The only way a reliable item is lost is a queue whose `Receiver` is
    /// already gone — a shutdown-order defect, counted as a `send_failure`.
    //
    // TODO(control-lane-sanity-ceiling): the reliable lane has no ceiling and no
    // loud line when its backlog is absurd; a wedged consumer grows it until the
    // OOM killer resolves it, with `reliable_high_water` in the 30 s
    // `stage_health` line as the only witness.
    pub fn send_reliable(&self, item: T) {
        self.push(item, false);
    }

    /// The one enqueue path both lanes take: the closed checks, the push, the
    /// counters, and (sheddable only) the overflow eviction. Returns the
    /// displaced item, which only a sheddable push can produce.
    ///
    /// One body because `sheddable_len` and `items.len()` are a single
    /// mutex-guarded invariant — the eviction scan and `sheddable_depth` both
    /// rest on them agreeing, and a one-sided edit to a second copy would break
    /// that with no symptom outside a `debug_assert!`.
    fn push(&self, item: T, sheddable: bool) -> Option<T> {
        // Checked before the lock so a closed send never touches the mutex,
        // which may be poisoned by the same panic that dropped the receiver.
        // The in-lock check below still closes the drop/send race.
        if self.closed_send() {
            return None;
        }
        let displaced = {
            let mut q = self.shared.queue.lock().expect("queue mutex poisoned");
            if self.shared.closed.load(Ordering::Acquire) {
                drop(q);
                self.shared.send_failures.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            q.items.push_back(Entry { item, sheddable });
            self.shared.pushed.fetch_add(1, Ordering::Relaxed);
            let displaced = if sheddable {
                q.sheddable_len += 1;
                self.shared.sheddable_pushed.fetch_add(1, Ordering::Relaxed);
                if q.sheddable_len > self.shared.capacity {
                    // Skip reliable entries at the head: they are not this
                    // lane's to shed. The scan is bounded by `capacity` plus the
                    // reliable backlog ahead of the oldest sheddable entry.
                    let victim = q.items.iter().position(|e| e.sheddable);
                    debug_assert!(
                        victim.is_some(),
                        "sheddable_len exceeds capacity with no sheddable entry queued"
                    );
                    match victim {
                        Some(i) => {
                            q.sheddable_len -= 1;
                            q.items.remove(i).map(|e| e.item)
                        }
                        // A bookkeeping bug. Accept the push rather than evict a
                        // reliable item to satisfy a counter that is already
                        // wrong — and count it, so a release build says so
                        // instead of shedding (or failing to shed) silently
                        // forever.
                        None => {
                            self.shared
                                .bookkeeping_faults
                                .fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };
            self.shared.high_water.bump(q.items.len() as u64);
            if !sheddable {
                self.shared
                    .reliable_high_water
                    .bump((q.items.len() - q.sheddable_len) as u64);
            }
            displaced
        };
        if displaced.is_some() {
            self.shared.dropped_oldest.fetch_add(1, Ordering::Relaxed);
        }
        self.shared.notify.notify_one();
        displaced
    }

    /// Whether the receiver is already gone; counts the `send_failure` if so.
    fn closed_send(&self) -> bool {
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.send_failures.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
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
    /// Await the next item, in push order across both lanes. Returns `None`
    /// once every `Sender` has dropped and both lanes have drained.
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            // Register for notification before inspecting the queue so a send
            // (or last-sender drop) racing between the check and the await
            // still wakes us.
            let notified = self.shared.notify.notified();
            {
                let mut q = self.shared.queue.lock().expect("queue mutex poisoned");
                if let Some(entry) = q.items.pop_front() {
                    if entry.sheddable {
                        q.sheddable_len -= 1;
                    }
                    return Some(entry.item);
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
        // Tolerate a poisoned mutex: a panic inside a send's lock scope may be
        // unwinding right now, and panicking in Drop during unwind aborts.
        let mut q = match self.shared.queue.lock() {
            Ok(q) => q,
            Err(poisoned) => poisoned.into_inner(),
        };
        q.items.clear();
        q.sheddable_len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_recv_fifo_under_capacity() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(4);
        assert_eq!(tx.send_sheddable(1), None);
        assert_eq!(tx.send_sheddable(2), None);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn overflow_displaces_oldest() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        assert_eq!(tx.send_sheddable(1), None);
        assert_eq!(tx.send_sheddable(2), None);
        // Third push overflows: oldest (1) is displaced and returned.
        assert_eq!(tx.send_sheddable(3), Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn counters_track_pushed_dropped_and_high_water() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(2);
        tx.send_sheddable(1);
        tx.send_sheddable(2);
        tx.send_sheddable(3); // displaces 1
        tx.send_sheddable(4); // displaces 2
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
        tx.send_sheddable(1);
        tx.send_sheddable(2);
        let before = tx.stats();
        drop(rx);
        assert_eq!(tx.send_sheddable(3), None);
        assert_eq!(tx.send_sheddable(4), None);
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
        tx.send_sheddable(item.clone());
        tx.send_sheddable(item.clone());
        tx.send_reliable(item.clone());
        tx.send_reliable(item.clone());
        assert_eq!(Arc::strong_count(&item), 5);
        drop(rx);
        // Senders still live, but the buffered items of both lanes are freed
        // immediately.
        assert_eq!(Arc::strong_count(&item), 1);
        assert_eq!(tx.stats().sheddable_depth, 0);
        assert_eq!(tx.stats().depth, 0);
    }

    #[tokio::test]
    async fn send_failures_zero_on_normal_close() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let handle = tx.stats_handle();
        tx.send_sheddable(1);
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
        tx.send_sheddable(1);
        // Poison the queue mutex the way a panic inside a lock scope would.
        let shared = rx.shared.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = shared.queue.lock().unwrap();
            panic!("poison");
        }));
        assert!(shared.queue.lock().is_err(), "mutex should be poisoned");
        drop(rx);
        // Neither producer nor the stats reader may panic on the poisoned lock.
        assert_eq!(tx.send_sheddable(2), None);
        assert_eq!(tx.send_sheddable(3), None);
        let stats = handle.stats();
        assert_eq!(stats.send_failures, 2);
        assert_eq!(stats.dropped_oldest, 0);
        assert_eq!(stats.depth, 0);
    }

    #[tokio::test]
    async fn recv_returns_none_when_all_senders_dropped_and_empty() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        tx.send_sheddable(1);
        drop(tx);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn recv_wakes_on_late_send() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        let handle = tokio::spawn(async move { rx.recv().await });
        // Give the receiver a chance to park on an empty queue, then send.
        tokio::task::yield_now().await;
        assert_eq!(tx.send_sheddable(7), None);
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
        tx2.send_sheddable(9);
        assert_eq!(rx.recv().await, Some(9));
    }

    #[tokio::test]
    async fn stats_handle_does_not_keep_the_channel_open() {
        use tokio::time::{Duration, timeout};
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
        tx.send_sheddable(1);
        tx.send_sheddable(2);
        let s = handle.stats();
        assert_eq!(s.pushed, 2);
        assert_eq!(s.depth, 2);
    }

    /// The production invariant this queue exists for: a close burst of control
    /// events wider than the whole segment budget still reaches the consumer,
    /// in order, and the segment that overflows is what gets shed.
    #[tokio::test]
    async fn reliable_items_are_never_displaced() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(1);
        assert_eq!(tx.send_sheddable(100), None);
        for r in 1..=9 {
            tx.send_reliable(r);
        }
        assert_eq!(tx.send_sheddable(200), Some(100));
        for r in 1..=9 {
            assert_eq!(rx.recv().await, Some(r));
        }
        assert_eq!(rx.recv().await, Some(200));
        assert_eq!(tx.stats().dropped_oldest, 1);
    }

    #[tokio::test]
    async fn eviction_skips_reliable_to_find_the_oldest_sheddable() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(1);
        tx.send_reliable(1);
        assert_eq!(tx.send_sheddable(100), None);
        tx.send_reliable(2);
        assert_eq!(tx.send_sheddable(200), Some(100));
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(200));
    }

    #[tokio::test]
    async fn reliable_lane_does_not_count_toward_capacity() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(2);
        for r in 0..10 {
            tx.send_reliable(r);
        }
        assert_eq!(tx.send_sheddable(100), None);
        assert_eq!(tx.send_sheddable(200), None);
        let s = tx.stats();
        assert_eq!(s.dropped_oldest, 0);
        assert_eq!(s.pushed, 12);
        assert_eq!(s.depth, 12);
    }

    #[tokio::test]
    async fn reliable_high_water_tracks_the_backlog() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(4);
        for r in 0..9 {
            tx.send_reliable(r);
        }
        for _ in 0..9 {
            assert!(rx.recv().await.is_some());
        }
        let s = tx.stats();
        assert_eq!(s.reliable_high_water, 9);
        assert_eq!(s.depth, 0);
    }

    #[tokio::test]
    async fn sheddable_depth_counts_only_the_sheddable_lane() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(4);
        tx.send_sheddable(100);
        tx.send_sheddable(200);
        for r in 0..5 {
            tx.send_reliable(r);
        }
        let s = tx.stats();
        assert_eq!(s.sheddable_depth, 2);
        assert_eq!(s.depth, 7);
        // The reliable figure is a lane backlog, not the total depth: two
        // sheddable items are queued alongside the five reliable ones.
        assert_eq!(s.reliable_high_water, 5);
        assert_eq!(s.bookkeeping_faults, 0);
    }

    /// `reliable_high_water` must exclude sheddable items queued ahead of and
    /// between the reliable ones — it is the only witness the unbounded lane
    /// has, and a total depth read as a control backlog misdirects triage.
    #[tokio::test]
    async fn reliable_high_water_excludes_interleaved_sheddable_items() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(4);
        tx.send_sheddable(100);
        tx.send_reliable(1);
        tx.send_sheddable(200);
        tx.send_reliable(2);
        let s = tx.stats();
        assert_eq!(s.reliable_high_water, 2);
        assert_eq!(s.high_water, 4);
        assert_eq!(s.depth, 4);
    }

    /// Receiving a sheddable item returns its budget: `sheddable_len` tracks
    /// the buffer as items leave, not only as they arrive. Without the
    /// decrement in `recv` the count ratchets up for the life of the daemon and
    /// every later push evicts a segment the queue had room for.
    #[tokio::test]
    async fn drained_sheddable_items_free_their_budget() {
        let (tx, mut rx) = DropOldestQueue::<u32>::new(2);
        tx.send_sheddable(1);
        tx.send_sheddable(2);
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        let drained = tx.stats();
        assert_eq!(drained.sheddable_depth, 0);
        assert_eq!(drained.depth, 0);

        assert_eq!(tx.send_sheddable(3), None);
        assert_eq!(tx.send_sheddable(4), None);
        let s = tx.stats();
        assert_eq!(s.dropped_oldest, 0);
        assert_eq!(s.sheddable_depth, 2);
        assert_eq!(s.bookkeeping_faults, 0);
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(rx.recv().await, Some(4));
    }

    /// A shed item still counts as pushed on its lane — it was accepted, then
    /// displaced.
    #[tokio::test]
    async fn sheddable_pushed_counts_only_the_sheddable_lane() {
        let (tx, _rx) = DropOldestQueue::<u32>::new(1);
        tx.send_sheddable(100);
        for r in 0..9 {
            tx.send_reliable(r);
        }
        tx.send_sheddable(200);
        let s = tx.stats();
        assert_eq!(s.sheddable_pushed, 2);
        assert_eq!(s.pushed, 11);
        assert_eq!(s.dropped_oldest, 1);
    }

    #[tokio::test]
    async fn reliable_send_after_receiver_drop_is_a_send_failure() {
        let (tx, rx) = DropOldestQueue::<u32>::new(2);
        tx.send_reliable(1);
        drop(rx);
        tx.send_reliable(2);
        tx.send_reliable(3);
        let s = tx.stats();
        assert_eq!(s.send_failures, 2);
        assert_eq!(s.dropped_oldest, 0);
        assert_eq!(s.depth, 0);
    }
}
