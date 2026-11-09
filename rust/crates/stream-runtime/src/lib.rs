//! Async, bounded fanout over the shared media buffer. No network I/O lives here.
use dispatcharr_stream_core::{BufferError, Chunk, MediaBuffer, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{self, Instant};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidViewerLimit,
    AtCapacity,
    Stopped,
    Buffer(BufferError),
}

struct State {
    buffer: Option<MediaBuffer>,
}

struct Inner {
    state: Mutex<State>,
    changed: Notify,
    stopped: Notify,
    slots: Arc<Semaphore>,
    max_viewers: usize,
}

#[derive(Clone)]
pub struct Channel {
    inner: Arc<Inner>,
}

impl Channel {
    /// Must run inside a Tokio runtime. The expiry task holds only a weak owner.
    pub fn new(max_bytes: usize, retention: Duration, max_viewers: usize) -> Result<Self, Error> {
        if max_viewers == 0 || max_viewers > Semaphore::MAX_PERMITS {
            return Err(Error::InvalidViewerLimit);
        }
        let buffer = MediaBuffer::new(max_bytes, retention).map_err(Error::Buffer)?;
        let inner = Arc::new(Inner {
            state: Mutex::new(State {
                buffer: Some(buffer),
            }),
            changed: Notify::new(),
            stopped: Notify::new(),
            slots: Arc::new(Semaphore::new(max_viewers)),
            max_viewers,
        });
        let weak = Arc::downgrade(&inner);
        let period = retention.min(Duration::from_secs(1));
        let first_tick = Instant::now() + period;
        tokio::spawn(async move {
            let mut timer = time::interval_at(first_tick, period);
            timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                // This reference is held only until the timer/stop notification.
                let Some(owner) = weak.upgrade() else { return };
                let stop = owner.stopped.notified();
                tokio::pin!(stop);
                stop.as_mut().enable();
                if owner.state.lock().unwrap().buffer.is_none() {
                    return;
                }
                tokio::select! {
                    _ = timer.tick() => {
                        {
                            let mut state = owner.state.lock().unwrap();
                            let Some(buffer) = state.buffer.as_mut() else { return };
                            buffer.expire(Instant::now().into_std());
                        }
                        owner.changed.notify_waiters();
                    }
                    _ = stop => return,
                }
            }
        });
        Ok(Self { inner })
    }

    pub fn publish(&self, data: Arc<[u8]>) -> Result<u64, Error> {
        let sequence = {
            let mut state = self.inner.state.lock().unwrap();
            state
                .buffer
                .as_mut()
                .ok_or(Error::Stopped)?
                .push(data, Instant::now().into_std())
                .map_err(Error::Buffer)?
        };
        self.inner.changed.notify_waiters();
        Ok(sequence)
    }

    pub fn subscribe(&self, behind: Duration) -> Result<Viewer, Error> {
        let permit = self.inner.slots.clone().try_acquire_owned().map_err(|_| {
            if self.inner.slots.is_closed() {
                Error::Stopped
            } else {
                Error::AtCapacity
            }
        })?;
        let sequence = {
            let mut state = self.inner.state.lock().unwrap();
            let buffer = state.buffer.as_mut().ok_or(Error::Stopped)?;
            buffer.expire(Instant::now().into_std());
            buffer.start_sequence(Instant::now().into_std(), behind)
        };
        Ok(Viewer {
            inner: self.inner.clone(),
            sequence,
            current: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            _permit: permit,
        })
    }

    /// Idempotent. Releases channel retention and wakes every pending read.
    pub fn stop(&self) {
        self.inner.state.lock().unwrap().buffer.take();
        self.inner.slots.close();
        self.inner.changed.notify_waiters();
        self.inner.stopped.notify_waiters();
    }

    pub fn retained_bytes(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap()
            .buffer
            .as_ref()
            .map_or(0, MediaBuffer::retained_bytes)
    }

    pub fn viewer_count(&self) -> usize {
        self.inner.max_viewers - self.inner.slots.available_permits()
    }
}

pub struct Viewer {
    inner: Arc<Inner>,
    sequence: u64,
    current: Option<Chunk>,
    cancelled: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub enum Delivery<'a> {
    Chunk { sequence: u64, data: &'a [u8] },
    Lagged { next_sequence: u64 },
    Cancelled,
    Stopped,
}

#[derive(Clone)]
pub struct CancelHandle {
    cancelled: Arc<AtomicBool>,
    inner: Weak<Inner>,
}

impl CancelHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(inner) = self.inner.upgrade() {
            inner.changed.notify_waiters();
        }
    }
}

impl Viewer {
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            cancelled: self.cancelled.clone(),
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Borrows this viewer's single delivery slot. Another read cannot be made
    /// while a transport still borrows that payload, preventing a hidden queue.
    /// Lag advances the cursor but remains explicit for the transport to handle.
    pub async fn next(&mut self) -> Delivery<'_> {
        self.current = None;
        loop {
            // Register before checking state: publish/cancel/stop between the
            // check and await cannot be lost, even with notify_waiters().
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cancelled.load(Ordering::Acquire) {
                return Delivery::Cancelled;
            }
            let read = {
                let mut state = self.inner.state.lock().unwrap();
                let Some(buffer) = state.buffer.as_mut() else {
                    return Delivery::Stopped;
                };
                buffer.expire(Instant::now().into_std());
                buffer.read(self.sequence)
            };
            match read {
                Read::Chunk(chunk) => {
                    self.sequence = chunk.sequence + 1;
                    self.current = Some(chunk);
                    let chunk = self.current.as_ref().unwrap();
                    return Delivery::Chunk {
                        sequence: chunk.sequence,
                        data: &chunk.data,
                    };
                }
                Read::Lagged { next_sequence } => {
                    self.sequence = next_sequence;
                    return Delivery::Lagged { next_sequence };
                }
                Read::Pending => changed.await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{poll_fn, Future};
    use std::task::Poll;

    fn channel(bytes: usize, viewers: usize) -> Channel {
        Channel::new(bytes, Duration::from_secs(5), viewers).unwrap()
    }

    async fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
        let mut future = future;
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn hundred_viewers_share_one_payload_without_queues() {
        let channel = channel(4096, 100);
        let mut viewers: Vec<_> = (0..100)
            .map(|_| channel.subscribe(Duration::ZERO).unwrap())
            .collect();
        let bytes: Arc<[u8]> = Arc::from(&b"shared"[..]);
        channel.publish(bytes.clone()).unwrap();
        for viewer in &mut viewers {
            let Delivery::Chunk { sequence, data } = viewer.next().await else {
                panic!("missing media")
            };
            assert_eq!(sequence, 1);
            assert_eq!(data.as_ptr(), bytes.as_ptr());
        }
        assert_eq!(channel.retained_bytes(), bytes.len());
        assert_eq!(channel.viewer_count(), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_viewer_lags_without_blocking_fast_viewer() {
        let channel = channel(2, 2);
        let mut slow = channel.subscribe(Duration::ZERO).unwrap();
        let mut fast = channel.subscribe(Duration::ZERO).unwrap();
        for sequence in 1..=20 {
            channel.publish(Arc::from(&b"ab"[..])).unwrap();
            assert!(
                matches!(fast.next().await, Delivery::Chunk { sequence: actual, .. } if actual == sequence)
            );
        }
        assert!(matches!(
            slow.next().await,
            Delivery::Lagged { next_sequence: 20 }
        ));
        assert!(matches!(
            slow.next().await,
            Delivery::Chunk { sequence: 20, .. }
        ));
        assert_eq!(channel.retained_bytes(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn publish_wakes_registered_waiter_and_future_cancellation_is_safe() {
        let channel = channel(2, 1);
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        let mut next = Box::pin(viewer.next());
        pending(next.as_mut()).await;
        channel.publish(Arc::from(&b"ab"[..])).unwrap();
        assert!(matches!(next.await, Delivery::Chunk { sequence: 1, .. }));
        let mut abandoned = Box::pin(viewer.next());
        pending(abandoned.as_mut()).await;
        drop(abandoned);
        channel.publish(Arc::from(&b"cd"[..])).unwrap();
        assert!(matches!(
            viewer.next().await,
            Delivery::Chunk { sequence: 2, .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stop_and_viewer_cancel_wake_waiters() {
        let channel = channel(4, 2);
        let mut first = channel.subscribe(Duration::ZERO).unwrap();
        let mut second = channel.subscribe(Duration::ZERO).unwrap();
        let cancel = first.cancel_handle();
        let mut waiting = Box::pin(first.next());
        pending(waiting.as_mut()).await;
        cancel.cancel();
        assert!(matches!(waiting.await, Delivery::Cancelled));
        let mut waiting = Box::pin(second.next());
        pending(waiting.as_mut()).await;
        channel.publish(Arc::from(&b"ab"[..])).unwrap();
        channel.stop();
        channel.stop();
        assert!(matches!(waiting.await, Delivery::Stopped));
        assert_eq!(channel.retained_bytes(), 0);
        assert!(matches!(
            channel.publish(Arc::from(&b"ab"[..])),
            Err(Error::Stopped)
        ));
        assert!(matches!(
            channel.subscribe(Duration::ZERO),
            Err(Error::Stopped)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn timer_expires_retention_during_upstream_stall() {
        let channel = channel(2, 1);
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        channel.publish(Arc::from(&b"ab"[..])).unwrap();
        tokio::task::yield_now().await; // Start the owner timer.
        time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(channel.retained_bytes(), 0);
        assert!(matches!(
            viewer.next().await,
            Delivery::Lagged { next_sequence: 2 }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn permits_release_on_drop_and_cancel_handle_does_not_hold_slot() {
        let channel = channel(2, 1);
        let viewer = channel.subscribe(Duration::ZERO).unwrap();
        let cancel = viewer.cancel_handle();
        assert!(matches!(
            channel.subscribe(Duration::ZERO),
            Err(Error::AtCapacity)
        ));
        cancel.cancel();
        assert_eq!(channel.viewer_count(), 1); // In-flight delivery still reserves memory.
        drop(viewer);
        assert_eq!(channel.viewer_count(), 0);
        assert!(channel.subscribe(Duration::ZERO).is_ok());
        drop(cancel);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_subscribe_and_publish_never_miss_retained_media() {
        let channel = channel(16, 100);
        let barrier = Arc::new(tokio::sync::Barrier::new(101));
        let mut jobs = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let channel = channel.clone();
            let barrier = barrier.clone();
            jobs.spawn(async move {
                barrier.wait().await;
                let mut viewer = channel.subscribe(Duration::from_secs(1)).unwrap();
                matches!(viewer.next().await, Delivery::Chunk { sequence: 1, .. })
            });
        }
        barrier.wait().await;
        channel.publish(Arc::from(&b"shared"[..])).unwrap();
        while let Some(result) = jobs.join_next().await {
            assert!(result.unwrap());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_channel_and_viewers_releases_worker_and_payload() {
        let channel = channel(16, 1);
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        let bytes: Arc<[u8]> = Arc::from(&b"release"[..]);
        let weak_payload = Arc::downgrade(&bytes);
        let weak_owner = Arc::downgrade(&channel.inner);
        channel.publish(bytes).unwrap();
        assert!(matches!(viewer.next().await, Delivery::Chunk { .. }));
        tokio::task::yield_now().await; // Worker is waiting for its timer.
        drop(viewer);
        drop(channel);
        time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(weak_owner.upgrade().is_none());
        assert!(weak_payload.upgrade().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_next_releases_previous_delivery_slot() {
        let channel = channel(16, 1);
        let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
        let bytes: Arc<[u8]> = Arc::from(&b"release"[..]);
        channel.publish(bytes.clone()).unwrap();
        assert!(matches!(viewer.next().await, Delivery::Chunk { .. }));
        assert_eq!(Arc::strong_count(&bytes), 3); // Caller, ring, viewer slot.
        viewer.cancel_handle().cancel();
        assert!(matches!(viewer.next().await, Delivery::Cancelled));
        assert_eq!(Arc::strong_count(&bytes), 2);
        channel.stop();
        assert_eq!(Arc::strong_count(&bytes), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_workers_deliver_every_sequence_to_one_hundred_readers() {
        let channel = Channel::new(8192, Duration::from_secs(3600), 100).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(101));
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let mut viewer = channel.subscribe(Duration::ZERO).unwrap();
            let barrier = barrier.clone();
            readers.spawn(async move {
                barrier.wait().await;
                time::timeout(Duration::from_secs(10), async {
                    for expected in 1u64..=1000 {
                        let Delivery::Chunk { sequence, data } = viewer.next().await else {
                            panic!("media missing during concurrent delivery")
                        };
                        assert_eq!(sequence, expected);
                        assert_eq!(data, expected.to_le_bytes());
                    }
                })
                .await
                .expect("reader stalled: possible lost notification");
            });
        }
        barrier.wait().await;
        for sequence in 1u64..=1000 {
            channel
                .publish(Arc::from(sequence.to_le_bytes().as_slice()))
                .unwrap();
            if sequence % 8 == 0 {
                tokio::task::yield_now().await;
            }
        }
        while let Some(reader) = readers.join_next().await {
            reader.unwrap();
        }
        assert_eq!(channel.retained_bytes(), 8000);
        assert_eq!(channel.viewer_count(), 0);
        channel.stop();
    }
}
