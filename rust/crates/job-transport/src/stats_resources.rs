//! Actual Redis ownership boundaries; empty source is an explicit resource stub.
//! No scanner/event parity claim is inferred from these resource-control tests.
use crate::{
    stats_worker::{Error, Run, Source, StatsWorker},
    transport::{Config, Jobs},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

struct Empty;
impl Source for Empty {
    fn build(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<Value, Error>> {
        Box::pin(async move {
            let _caller = caller;
            Ok(json!({"channels":[],"count":0}))
        })
    }
}
struct Sink {
    pause: bool,
    fail_after_logical_reply: Option<RedisTransport>,
}
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        value: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        self.send_retained(group, value, Arc::new(()))
    }
    fn send_retained<'a>(
        &'a self,
        _group: &'a str,
        _value: Value,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            if self.pause {
                pause().await;
            }
            if let Some(transport) = &self.fail_after_logical_reply {
                let _ = transport
                    .command::<i64, _>("return 1", &[], &[], Some(caller))
                    .await;
                return Err(UpdateEventError::Unavailable);
            }
            drop(caller);
            Ok(())
        })
    }
}
async fn pause() {
    let mut control = redis::Client::open(std::env::var("CELERY_BROKER_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .query_async(&mut control)
        .await
        .unwrap();
}
async fn jobs() -> Jobs {
    Jobs::connect_fixture(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        &std::env::var("POSTGRES_DB").unwrap(),
        Config {
            operations: 1,
            command_timeout: Duration::from_millis(50),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap()
}
async fn clean(jobs: &Jobs) {
    // The controller asserts dedicated DB8/9 and no concurrent consumers.
    for key in ["CELERY_BROKER_URL", "CELERY_RESULT_BACKEND"] {
        let mut connection = redis::Client::open(std::env::var(key).unwrap())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: String = redis::cmd("FLUSHDB")
            .query_async(&mut connection)
            .await
            .unwrap();
    }
    assert_eq!(jobs.available_slots(), 1);
}
async fn worker(jobs: &Jobs, sink: Sink) -> StatsWorker {
    StatsWorker::new_fixture(jobs.clone(), Arc::new(Empty), Arc::new(sink), 65536).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned DB8/9 actual Redis delayed claim; serial CLIENT PAUSE"]
async fn stats_claim_logical_timeout_retains_worker_and_caller_until_actual_peek_reply() {
    let jobs = jobs().await;
    clean(&jobs).await;
    let id = Uuid::new_v4();
    jobs.publish_stats_fixture(id, Arc::new(())).await.unwrap();
    let worker = worker(
        &jobs,
        Sink {
            pause: false,
            fail_after_logical_reply: None,
        },
    )
    .await;
    pause().await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    assert!(worker.run_once(caller).await.is_err());
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert_eq!(worker.run_once(Arc::new(())).await, Err(Error::Capacity));
    assert!(!worker.shutdown(Duration::from_millis(30)).await);
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    assert!(weak.upgrade().is_none());
    let mut broker = redis::Client::open(std::env::var("CELERY_BROKER_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let remaining: i64 = redis::cmd("LLEN")
        .arg("celery")
        .query_async(&mut broker)
        .await
        .unwrap();
    assert_eq!(
        remaining, 1,
        "timed-out peek cannot proceed to a destructive CAS"
    );
    clean(&jobs).await;
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned DB8/9 actual result reply, caller cancellation and shutdown"]
async fn stats_cancelled_caller_retains_actual_result_write_and_short_shutdown_is_incomplete() {
    let jobs = jobs().await;
    clean(&jobs).await;
    let id = Uuid::new_v4();
    jobs.publish_stats_fixture(id, Arc::new(())).await.unwrap();
    let worker = worker(
        &jobs,
        Sink {
            pause: true,
            fail_after_logical_reply: None,
        },
    )
    .await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let owned = worker.clone();
    let task = tokio::spawn(async move { owned.run_once(caller).await });
    tokio::time::sleep(Duration::from_millis(80)).await;
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert!(!worker.shutdown(Duration::from_millis(30)).await);
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    assert!(weak.upgrade().is_none());
    let record = jobs.read(id, Arc::new(())).await.unwrap().unwrap();
    assert_eq!(record.result, Value::Null);
    assert_eq!(record.state, crate::decode::State::Success);
    clean(&jobs).await;
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned DB8/9 actual sink reply receipt before normal task result"]
async fn stats_sink_logical_error_cannot_write_result_until_physical_owner_receipt_settles() {
    let jobs = jobs().await;
    clean(&jobs).await;
    let id = Uuid::new_v4();
    jobs.publish_stats_fixture(id, Arc::new(())).await.unwrap();
    let transport = RedisTransport::connect(
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        TransportConfig {
            max_operations: 1,
            max_queued_operations: 1,
            command_timeout: Duration::from_millis(50),
        },
    )
    .await
    .unwrap();
    let worker = worker(
        &jobs,
        Sink {
            pause: true,
            fail_after_logical_reply: Some(transport),
        },
    )
    .await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let owned = worker.clone();
    let task = tokio::spawn(async move { owned.run_once(caller).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert!(!worker.shutdown(Duration::from_millis(30)).await);
    assert!(matches!(task.await.unwrap(), Ok(Run::Completed(actual)) if actual==id));
    assert!(worker.shutdown(Duration::from_secs(3)).await);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        jobs.read(id, Arc::new(())).await.unwrap().unwrap().state,
        crate::decode::State::Success
    );
    clean(&jobs).await;
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
}
