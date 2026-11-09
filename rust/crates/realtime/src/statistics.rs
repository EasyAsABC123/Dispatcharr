//! One shared live-statistics snapshot worker; original Dispatcharr credit retained.
use crate::hub;
use bytes::Bytes;
use dispatcharr_contracts::{
    BoxFuture, LiveStatsSnapshotReader, LiveStatsSnapshotSource, LiveStatsTrigger, PlaybackError,
    UpdateEventSink,
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore},
    time,
};

#[derive(Clone, Debug)]
pub struct StatisticsConfig {
    pub max_snapshot_bytes: usize,
    pub max_live_bytes: usize,
    pub max_pending: usize,
    pub request_timeout: Duration,
    pub build_timeout: Duration,
    pub capacity_timeout: Duration,
    pub publish_timeout: Duration,
}
impl Default for StatisticsConfig {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: 3 * 1024 * 1024,
            max_live_bytes: 16 * 1024 * 1024,
            max_pending: 100,
            request_timeout: Duration::from_secs(10),
            build_timeout: Duration::from_secs(5),
            capacity_timeout: Duration::from_secs(2),
            publish_timeout: Duration::from_secs(2),
        }
    }
}
#[derive(Clone)]
pub struct LiveStatistics {
    inner: Arc<Inner>,
}
type Reply = oneshot::Sender<Result<Bytes, PlaybackError>>;
struct Inner {
    source: Arc<dyn LiveStatsSnapshotSource>,
    sink: Arc<dyn UpdateEventSink>,
    config: StatisticsConfig,
    bytes: Arc<Semaphore>,
    pending: Mutex<Pending>,
    wake: Arc<Notify>,
    stop: watch::Sender<bool>,
    done: watch::Sender<bool>,
}
#[derive(Default)]
struct Pending {
    requests: Vec<Reply>,
    signals: usize,
    stopping: bool,
    dropped: u64,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Worker(Weak<Inner>);
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.done.send_replace(true);
        }
    }
}
fn failure() -> PlaybackError {
    PlaybackError::new(503, "Live statistics unavailable")
}
impl LiveStatistics {
    pub fn new(
        source: Arc<dyn LiveStatsSnapshotSource>,
        sink: Arc<dyn UpdateEventSink>,
        config: StatisticsConfig,
    ) -> Result<Self, PlaybackError> {
        if config.max_snapshot_bytes == 0
            || config.max_live_bytes < config.max_snapshot_bytes
            || config.max_live_bytes > u32::MAX as usize
            || config.max_pending == 0
            || config.max_pending > 4096
            || config.request_timeout.is_zero()
            || config.build_timeout.is_zero()
            || config.capacity_timeout.is_zero()
            || config.publish_timeout.is_zero()
        {
            return Err(failure());
        }
        let inner = Arc::new(Inner {
            source,
            sink,
            bytes: Arc::new(Semaphore::new(config.max_live_bytes)),
            config,
            pending: Mutex::new(Pending::default()),
            wake: Arc::new(Notify::new()),
            stop: watch::channel(false).0,
            done: watch::channel(false).0,
        });
        let weak = Arc::downgrade(&inner);
        let stop = inner.stop.subscribe();
        tokio::spawn(async move {
            let _worker = Worker(weak.clone());
            run(weak, stop).await;
        });
        Ok(Self { inner })
    }
    pub fn diagnostics(&self) -> Value {
        let pending = self.inner.pending.lock().unwrap();
        json!({"format_version":1,"pending_requests":pending.requests.len(),"pending_signals":pending.signals,"dropped_signals":pending.dropped,
               "live_snapshot_bytes":self.inner.config.max_live_bytes-self.inner.bytes.available_permits(),"worker_done":*self.inner.done.borrow()})
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.pending.lock().unwrap().stopping = true;
        self.inner.stop.send_replace(true);
        self.inner.wake.notify_one();
        let mut done = self.inner.done.subscribe();
        time::timeout(timeout, async {
            while !*done.borrow() {
                if done.changed().await.is_err() {
                    return false;
                }
            }
            true
        })
        .await
        .unwrap_or(false)
    }
}
impl LiveStatsTrigger for LiveStatistics {
    fn changed(&self) {
        let mut pending = self.inner.pending.lock().unwrap();
        if pending.stopping {
            return;
        }
        if pending.signals + pending.requests.len() < self.inner.config.max_pending {
            pending.signals += 1;
        } else {
            pending.dropped = pending.dropped.saturating_add(1);
        }
        drop(pending);
        self.inner.wake.notify_one();
    }
}
impl LiveStatsSnapshotReader for LiveStatistics {
    fn snapshot(&self) -> BoxFuture<'_, Result<Bytes, PlaybackError>> {
        Box::pin(async move {
            let (reply, result) = oneshot::channel();
            {
                let mut pending = self.inner.pending.lock().unwrap();
                if pending.stopping
                    || pending.signals + pending.requests.len() >= self.inner.config.max_pending
                {
                    return Err(failure());
                }
                pending.requests.push(reply);
            }
            self.inner.wake.notify_one();
            time::timeout(self.inner.config.request_timeout, result)
                .await
                .map_err(|_| failure())?
                .map_err(|_| failure())?
        })
    }
}
async fn run(weak: Weak<Inner>, mut stop: watch::Receiver<bool>) {
    loop {
        let Some(inner) = weak.upgrade() else { return };
        let wake = inner.wake.clone();
        let changed = wake.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (requests, signals, stopping) = {
            let mut pending = inner.pending.lock().unwrap();
            (
                std::mem::take(&mut pending.requests),
                std::mem::take(&mut pending.signals),
                pending.stopping,
            )
        };
        if requests.is_empty() && signals == 0 {
            drop(inner);
            if stopping || *stop.borrow() {
                return;
            }
            tokio::select! {_=stop.changed()=>{},_=changed=>{}}
            continue;
        }
        let count = requests.len() + signals;
        let source = inner.source.clone();
        let sink = inner.sink.clone();
        let config = inner.config.clone();
        let bytes = inner.bytes.clone();
        drop(inner);
        let built = build(source, bytes, &config).await;
        match built {
            Ok(wire) => {
                for reply in requests {
                    let _ = reply.send(Ok(wire.clone()));
                }
                // Original gevent status handlers schedule publication and return
                // HTTP first. Every admitted trigger still requests an event;
                // one snapshot replaces duplicate concurrent Redis scans.
                for _ in 0..count {
                    let text = match std::str::from_utf8(&wire) {
                        Ok(text) => text,
                        Err(_) => break,
                    };
                    let event = json!({"type":"update","data":{"success":true,"type":"channel_stats","stats":text}});
                    let publication =
                        time::timeout(config.publish_timeout, sink.send("updates", event));
                    tokio::pin!(publication);
                    if *stop.borrow() {
                        let _ = publication.await;
                        break;
                    }
                    tokio::select! {_=stop.changed()=>{break},_=publication=>{}}
                }
            }
            Err(reason) => {
                for reply in requests {
                    let _ = reply.send(Err(reason.clone()));
                }
            }
        }
    }
}
async fn build(
    source: Arc<dyn LiveStatsSnapshotSource>,
    bytes: Arc<Semaphore>,
    config: &StatisticsConfig,
) -> Result<Bytes, PlaybackError> {
    let mut budget = time::timeout(
        config.capacity_timeout,
        bytes.acquire_many_owned(config.max_snapshot_bytes as u32),
    )
    .await
    .map_err(|_| failure())?
    .map_err(|_| failure())?;
    let snapshot = time::timeout(config.build_timeout, source.build())
        .await
        .map_err(|_| failure())??;
    let data = hub::serialize(&snapshot, config.max_snapshot_bytes).map_err(|_| failure())?;
    if let Some(unused) = budget.split(config.max_snapshot_bytes - data.len()) {
        drop(unused)
    }
    Ok(Bytes::from_owner(Snapshot {
        data,
        _budget: budget,
    }))
}
struct Snapshot {
    data: Box<[u8]>,
    _budget: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Snapshot {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_contracts::UpdateEventError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Source(AtomicUsize);
    impl LiveStatsSnapshotSource for Source {
        fn build(&self) -> BoxFuture<'_, Result<Value, PlaybackError>> {
            Box::pin(async move {
                let generation = self.0.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"generation":generation,"value":"x".repeat(25)}))
            })
        }
    }
    #[derive(Default)]
    struct Sink(Mutex<Vec<Value>>);
    impl UpdateEventSink for Sink {
        fn send<'a>(
            &'a self,
            group: &'a str,
            message: Value,
        ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
            Box::pin(async move {
                assert_eq!(group, "updates");
                self.0.lock().unwrap().push(message);
                Ok(())
            })
        }
    }
    fn config() -> StatisticsConfig {
        StatisticsConfig {
            max_snapshot_bytes: 64,
            max_live_bytes: 128,
            max_pending: 100,
            request_timeout: Duration::from_secs(1),
            build_timeout: Duration::from_millis(50),
            capacity_timeout: Duration::from_millis(20),
            publish_timeout: Duration::from_millis(20),
        }
    }
    #[tokio::test]
    async fn hundred_status_requests_share_one_snapshot_and_each_publish_its_exact_body() {
        let source = Arc::new(Source(AtomicUsize::new(0)));
        let sink = Arc::new(Sink::default());
        let service = LiveStatistics::new(source.clone(), sink.clone(), config()).unwrap();
        let mut requests = Vec::new();
        for _ in 0..100 {
            let mut request = service.snapshot();
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            requests.push(request);
        }
        let replies = futures_util::future::join_all(requests)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(source.0.load(Ordering::SeqCst), 1);
        for reply in &replies {
            assert_eq!(reply.as_ptr(), replies[0].as_ptr());
        }
        let deadline = time::Instant::now() + Duration::from_secs(1);
        while sink.0.lock().unwrap().len() != 100 {
            assert!(time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
        for message in sink.0.lock().unwrap().iter() {
            assert_eq!(message["type"], "update");
            assert_eq!(message["data"]["type"], "channel_stats");
            assert_eq!(message["data"]["success"], true);
            assert_eq!(
                message["data"]["stats"].as_str().unwrap().as_bytes(),
                replies[0].as_ref()
            );
        }
        assert_eq!(
            service.inner.bytes.available_permits(),
            128 - replies[0].len()
        );
        drop(replies);
        assert_eq!(service.inner.bytes.available_permits(), 128);
        assert!(service.shutdown(Duration::from_secs(1)).await);
    }
    #[tokio::test]
    async fn held_old_versions_keep_budget_and_backpressure_until_final_body_clone_drops() {
        let source = Arc::new(Source(AtomicUsize::new(0)));
        let sink = Arc::new(Sink::default());
        let service = LiveStatistics::new(source.clone(), sink, config()).unwrap();
        let first = service.snapshot().await.unwrap();
        let held = first.clone();
        let second = service.snapshot().await.unwrap();
        assert!(service.snapshot().await.is_err());
        assert_eq!(
            source.0.load(Ordering::SeqCst),
            2,
            "capacity failure cannot build another unbudgeted snapshot"
        );
        drop(first);
        assert!(
            service.snapshot().await.is_err(),
            "cloning a body cannot release its admission"
        );
        drop(held);
        let third = service.snapshot().await.unwrap();
        assert_eq!(source.0.load(Ordering::SeqCst), 3);
        assert!(service.shutdown(Duration::from_secs(1)).await);
        assert_eq!(
            service.inner.bytes.available_permits(),
            128 - second.len() - third.len()
        );
        drop(second);
        drop(third);
        assert_eq!(service.inner.bytes.available_permits(), 128);
    }
    #[tokio::test]
    async fn signal_queue_is_bounded_and_shutdown_rejects_new_snapshots() {
        let source = Arc::new(Source(AtomicUsize::new(0)));
        let sink = Arc::new(Sink::default());
        let service = LiveStatistics::new(source, sink, config()).unwrap();
        for _ in 0..1000 {
            service.changed();
        }
        assert_eq!(service.diagnostics()["pending_signals"], 100);
        assert_eq!(service.diagnostics()["dropped_signals"], 900);
        assert!(service.shutdown(Duration::from_secs(1)).await);
        assert!(service.snapshot().await.is_err());
        assert_eq!(service.inner.bytes.available_permits(), 128);
    }
}
