//! Bounded catch-up statistics producer; original Dispatcharr authors retain credit.
use crate::management_data;
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{Store, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};
struct Pending(Arc<AtomicUsize>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Inner {
    sender: mpsc::Sender<Arc<Pending>>,
    pending: Arc<AtomicUsize>,
    failed: Arc<AtomicU64>,
    closed: AtomicBool,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
pub(crate) struct Producer {
    inner: Arc<Inner>,
}
struct Finished(watch::Sender<bool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
impl Producer {
    pub async fn connect(
        store: Store,
        redis: &str,
        sink: Arc<dyn UpdateEventSink>,
    ) -> Result<Self, StoreError> {
        let transport = RedisTransport::connect(
            redis,
            TransportConfig {
                max_operations: 1,
                max_queued_operations: 1,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(|_| StoreError::Unsupported("Catch-up event transport unavailable"))?;
        let (sender, mut receiver) = mpsc::channel::<Arc<Pending>>(4096);
        let (stop, mut stopped) = watch::channel(false);
        let (done, completed) = watch::channel(false);
        let failed = Arc::new(AtomicU64::new(0));
        let errors = failed.clone();
        tokio::spawn(async move {
            let _finished = Finished(done);
            loop {
                if *stopped.borrow() {
                    break;
                }
                let pending = tokio::select! {value=receiver.recv()=>match value{Some(value)=>value,None=>break},_=stopped.changed()=>break};
                let build = async {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64();
                    let stats =
                        management_data::stats_guarded(&store, &transport, now, pending.clone())
                            .await;
                    if let Ok(text) = serde_json::to_string(&stats) {
                        if text.len() > 16 * 1024 * 1024 {
                            return false;
                        }
                        let event = json!({"type":"update","data":{"success":true,"type":"timeshift_stats","stats":text}});
                        tokio::time::timeout(Duration::from_secs(2), sink.send("updates", event))
                            .await
                            .is_ok_and(|result| result.is_ok())
                    } else {
                        false
                    }
                };
                let successful = tokio::select! {result=tokio::time::timeout(Duration::from_secs(5),build)=>result.is_ok_and(|result|result),_=stopped.changed()=>break};
                if !successful {
                    errors.fetch_add(1, Ordering::AcqRel);
                }
            }
        });
        Ok(Self {
            inner: Arc::new(Inner {
                sender,
                pending: Arc::new(AtomicUsize::new(0)),
                failed,
                closed: AtomicBool::new(false),
                stop,
                done: completed,
            }),
        })
    }
    pub fn trigger(&self) -> bool {
        if self.inner.closed.load(Ordering::Acquire) {
            return false;
        }
        self.inner.pending.fetch_add(1, Ordering::AcqRel);
        let pending = Arc::new(Pending(self.inner.pending.clone()));
        if self.inner.closed.load(Ordering::Acquire) {
            return false;
        }
        if self.inner.sender.try_send(pending).is_err() {
            self.inner.failed.fetch_add(1, Ordering::AcqRel);
            false
        } else {
            true
        }
    }
    pub fn diagnostics(&self) -> Value {
        json!({"pending":self.inner.pending.load(Ordering::Acquire),"failed":self.inner.failed.load(Ordering::Acquire)})
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + timeout;
        let drained = tokio::time::timeout_at(deadline, async {
            while self.inner.pending.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        if !drained {
            self.inner.failed.fetch_add(
                self.inner.pending.load(Ordering::Acquire) as u64,
                Ordering::AcqRel,
            );
        }
        self.inner.stop.send_replace(true);
        let mut done = self.inner.done.clone();
        let completed = *done.borrow()
            || tokio::time::timeout_at(deadline, done.wait_for(|value| *value))
                .await
                .is_ok_and(|result| result.is_ok());
        drained && completed
    }
    pub fn cancel(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArchivePool, ArchivePoolConfig, ArchivePoolRequest};
    use dispatcharr_contracts::{BoxFuture, UpdateEventError, UserLimitPolicy};
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    use redis::AsyncCommands;
    use std::{collections::BTreeMap, sync::Mutex};
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
    async fn count(sink: &Sink, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while sink.0.lock().unwrap().len() < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(sink.0.lock().unwrap().len(), expected);
    }
    #[tokio::test]
    #[ignore = "requires disposable actual original PostgreSQL/Redis producer fixture"]
    async fn original_registration_notifications_and_grace_cleanup_use_bounded_fullstats_actor() {
        assert_eq!(
            std::env::var("TIMESHIFT_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let database = std::env::var("DATABASE_TEST_URL").unwrap();
        let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(
            database.contains("playback-fixture-only@")
                && redis_url.contains("playback-fixture-only@")
        );
        let store = Store::connect(&database, 8).await.unwrap();
        let coordinator = Coordinator::connect(
            &redis_url,
            LeaseConfig {
                namespace: format!("rust:ts-events:{}", uuid::Uuid::new_v4()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let sink = Arc::new(Sink::default());
        let producer = Producer::connect(store.clone(), &redis_url, sink.clone())
            .await
            .unwrap();
        let pool = ArchivePool::connect(
            &redis_url,
            coordinator,
            ArchivePoolConfig {
                startup_idle: Duration::from_millis(30),
                disconnect_grace: Duration::from_millis(30),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pool.install_events(producer.clone()).unwrap();
        let (channel, uuid, name): (i64, uuid::Uuid, String) = sqlx::query_as(
            "SELECT id::bigint,uuid,name FROM dispatcharr_channels_channel ORDER BY id LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        let profile:i64=sqlx::query_scalar("SELECT p.id::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.name='Rust VOD fixture XC' AND p.is_default").fetch_one(store.pool()).await.unwrap();
        let sid = format!("fixture_ts_events_{}", uuid::Uuid::new_v4().simple());
        let media = format!("{channel}_1704067200");
        let make = |sid: &str, media: &str| ArchivePoolRequest {
            session_id: sid.to_owned(),
            channel_id: channel,
            virtual_channel_id: media.to_owned(),
            provider_profile_id: profile,
            provider_max_streams: 2,
            stream_limit: 0,
            limit_policy: UserLimitPolicy::default(),
            pool_fields: BTreeMap::from([
                ("user_id".into(), "42".into()),
                ("media_id".into(), media.to_owned()),
            ]),
            client_fields: BTreeMap::from([
                ("user_id".into(), "42".into()),
                ("username".into(), "fixture-ts-producer".into()),
                ("user_agent".into(), "fixture-ts-producer".into()),
                ("ip_address".into(), "192.0.2.22".into()),
                ("programme_start".into(), "2024-01-01 00:00:00".into()),
                ("programme_vid".into(), media.to_owned()),
            ]),
            channel_fields: BTreeMap::from([
                ("channel_id".into(), channel.to_string()),
                ("channel_uuid".into(), uuid.to_string()),
                ("channel_name".into(), name.clone()),
                ("stream_name".into(), "Catch-up fixture".into()),
            ]),
        };
        let ticket = pool.open(make(&sid, &media)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            sink.0.lock().unwrap().is_empty(),
            "pending reservation does not publish"
        );
        ticket
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        count(&sink, 1).await;
        ticket
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            sink.0.lock().unwrap().len(),
            1,
            "sameprogramme repeat mustnotpublish"
        );
        let mut redis = redis::Client::open(redis_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let client_key = format!("timeshift:channel:{channel}_{sid}:clients:{sid}");
        let _: usize = redis.hdel(&client_key, "programme_vid").await.unwrap();
        ticket
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            sink.0.lock().unwrap().len(),
            1,
            "existingconnected-at withoutprogramme preservesSource suppression"
        );
        let _: usize = redis
            .hset(&client_key, "programme_vid", format!("{channel}_earlier"))
            .await
            .unwrap();
        ticket
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        count(&sink, 2).await;
        drop(ticket);
        count(&sink, 3).await;
        // Same-user other-position displacement publishes immediately, before
        // the replacement acquires/opens an upstream or registers its viewer.
        let first_sid = format!("hop_first_{}", uuid::Uuid::new_v4().simple());
        let first = pool.open(make(&first_sid, &media)).await.unwrap();
        first
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        count(&sink, 4).await;
        let next_sid = format!("hop_next_{}", uuid::Uuid::new_v4().simple());
        let next_media = format!("{channel}_1704070800");
        let next = pool.open(make(&next_sid, &next_media)).await.unwrap();
        count(&sink, 5).await;
        let displaced: Value =
            serde_json::from_str(sink.0.lock().unwrap()[4]["data"]["stats"].as_str().unwrap())
                .unwrap();
        assert_eq!(
            displaced["total_connections"], 0,
            "old viewer removal publishes before replacement start"
        );
        drop(first);
        count(&sink, 6).await;
        next.start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        count(&sink, 7).await;
        drop(next);
        count(&sink, 8).await;
        assert!(pool.shutdown().await);
        assert_eq!(pool.event_diagnostics(), json!({"pending":0,"failed":0}));
        let messages = sink.0.lock().unwrap();
        for message in messages.iter() {
            assert_eq!(message["type"], "update");
            assert_eq!(message["data"]["type"], "timeshift_stats");
            assert_eq!(message["data"]["success"], true);
            let stats: Value =
                serde_json::from_str(message["data"]["stats"].as_str().unwrap()).unwrap();
            assert!(stats["timestamp"].as_f64().unwrap().is_finite());
        }
        let first: Value =
            serde_json::from_str(messages[0]["data"]["stats"].as_str().unwrap()).unwrap();
        assert_eq!(first["total_connections"], 1);
        let last: Value =
            serde_json::from_str(messages[7]["data"]["stats"].as_str().unwrap()).unwrap();
        assert_eq!(last["total_connections"], 0);
        println!(
            "{}",
            json!({"native_registration_notifications":2,"suppressed_pending_repeat_missingprogramme":3,"grace_cleanup_snapshots":3,"immediate_pre_upstream_displacement":1,"source_frozen_native_transport_verified":false})
        );
    }
    #[derive(Default)]
    struct Blocked {
        entered: AtomicBool,
        active: Arc<AtomicUsize>,
    }
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }
    impl UpdateEventSink for Blocked {
        fn send<'a>(&'a self, _: &'a str, _: Value) -> BoxFuture<'a, Result<(), UpdateEventError>> {
            Box::pin(async move {
                self.active.fetch_add(1, Ordering::AcqRel);
                let _active = Active(self.active.clone());
                self.entered.store(true, Ordering::Release);
                futures_util::future::pending().await
            })
        }
    }
    #[tokio::test]
    #[ignore = "requires disposable actual original PostgreSQL/Redis producer fixture"]
    async fn original_registration_notifications_and_grace_cleanup_stalled_sink_is_bounded() {
        assert_eq!(
            std::env::var("TIMESHIFT_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let database = std::env::var("DATABASE_TEST_URL").unwrap();
        let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(
            database.contains("playback-fixture-only@")
                && redis_url.contains("playback-fixture-only@")
        );
        let store = Store::connect(&database, 8).await.unwrap();
        let blocked = Arc::new(Blocked::default());
        let events = Producer::connect(store.clone(), &redis_url, blocked.clone())
            .await
            .unwrap();
        let coordinator = Coordinator::connect(
            &redis_url,
            LeaseConfig {
                namespace: format!("rust:ts-events-blocked:{}", uuid::Uuid::new_v4()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let pool = ArchivePool::connect(
            &redis_url,
            coordinator,
            ArchivePoolConfig {
                shutdown_timeout: Duration::from_millis(100),
                startup_idle: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pool.install_events(events).unwrap();
        let profile:i64=sqlx::query_scalar("SELECT p.id::bigint FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE a.name='Rust VOD fixture XC' AND p.is_default").fetch_one(store.pool()).await.unwrap();
        let sid = format!("blocked_{}", uuid::Uuid::new_v4().simple());
        let ticket = pool
            .open(ArchivePoolRequest {
                session_id: sid.clone(),
                channel_id: 1,
                virtual_channel_id: "1_fixture".into(),
                provider_profile_id: profile,
                provider_max_streams: 2,
                stream_limit: 0,
                limit_policy: UserLimitPolicy::default(),
                pool_fields: BTreeMap::from([
                    ("user_id".into(), "42".into()),
                    ("media_id".into(), "1_fixture".into()),
                ]),
                client_fields: BTreeMap::from([
                    ("user_id".into(), "42".into()),
                    ("programme_start".into(), "2024-01-01 00:00:00".into()),
                    ("programme_vid".into(), "1_fixture".into()),
                ]),
                channel_fields: BTreeMap::from([("channel_id".into(), "1".into())]),
            })
            .await
            .unwrap();
        ticket
            .start("http://origin:8080/fixture.ts", Some(1_000_000), None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !blocked.entered.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut stopped = ticket.stopped();
        let supervisor = tokio::spawn(async move {
            if !*stopped.borrow() {
                let _ = stopped.changed().await;
            }
            drop(ticket);
        });
        assert!(
            !tokio::time::timeout(Duration::from_millis(300), pool.shutdown())
                .await
                .unwrap()
        );
        supervisor.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while blocked.active.load(Ordering::Acquire) != 0
                || pool.event_diagnostics()["pending"] != 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(pool.event_diagnostics()["failed"].as_u64().unwrap() > 0);
        assert!(pool.shutdown().await);
        let mut redis = redis::Client::open(redis_url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let counter: Option<i64> = redis
            .get(format!("profile_connections:{profile}"))
            .await
            .unwrap();
        assert_eq!(counter.unwrap_or(0), 0);
        println!(
            "{}",
            json!({"bounded_stalled_timeshift_publisher_shutdown":true,"provider_cleanup":true,"pending_actor_tasks":0})
        );
    }
}
