// Fixture-only real FileJob executor; original Dispatcharr authors retain credit.
use super::*;
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    cache_policy::CachedSwapPolicy,
    channels::{ChannelConfig, ChannelImporter},
    file_job::{ChildTask, FileJobConfig, FileJobServices, GuideTaskDispatcher},
    reporter::GuideReporter,
    EpgImporter, ImportConfig,
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::atomic::AtomicUsize,
};
struct Sink;
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        _group: &'a str,
        _message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async { Ok(()) })
    }
}
struct DefaultDispatcher(Arc<AtomicUsize>);
impl GuideTaskDispatcher for DefaultDispatcher {
    fn dispatch<'a>(
        &'a self,
        _task: ChildTask,
        _context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ImportError::Worker)
        })
    }
}
struct Expected {
    service: ServiceExecutor,
    id: Uuid,
    source: i64,
    calls: Arc<AtomicUsize>,
}
impl RefreshExecutor for Expected {
    fn execute<'a>(
        &'a self,
        message: RefreshMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            assert_eq!(message.task_id, self.id);
            assert_eq!(message.source_id, self.source);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.service.execute(message, publisher, owner).await
        })
    }
}
#[tokio::test]
#[ignore = "CopiedDB Source producer/native realFileJob/Source AsyncResult; no app startup"]
async fn native_refresh_worker_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut bytes = String::new();
    io::stdin().take(16385).read_to_string(&mut bytes).unwrap();
    assert!(bytes.len() <= 16384);
    let input: Value = serde_json::from_str(&bytes).unwrap();
    assert!(matches!(
        input["action"].as_str(),
        Some("run" | "reject_head")
    ));
    let id = Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap();
    let source = input["source_id"].as_i64().filter(|v| *v > 0).unwrap();
    let db = input["database_url"].as_str().unwrap();
    assert_eq!(
        db,
        format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}")
    );
    let cache = input["cache_url"].as_str().unwrap();
    assert_eq!(cache, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let broker = input["broker_url"].as_str().unwrap();
    let result = input["result_url"].as_str().unwrap();
    let jobs = Jobs::connect_fixture(
        broker,
        result,
        &database,
        crate::transport::Config {
            operations: 4,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let store = Store::connect(db, 1).await.unwrap();
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
        .bind(source)
        .fetch_optional(store.pool())
        .await
        .unwrap();
    if let Some(name) = name {
        assert!(name.starts_with("rust-native-refresh-"))
    }
    let mut connection = redis::Client::open(broker)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let head: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(head.len() <= 65536);
    let envelope: Value = serde_json::from_slice(&head).unwrap();
    assert_eq!(envelope["headers"]["id"], id.to_string());
    let clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync> =
        Arc::new(chrono::Utc::now);
    let reporter =
        GuideReporter::with_clock(store.clone(), cache, Arc::new(Sink), 2, clock.clone())
            .await
            .unwrap();
    let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let locks = TaskLocks::connect(cache, TaskLockConfig::default())
        .await
        .unwrap();
    let default_calls = Arc::new(AtomicUsize::new(0));
    let runner = FileJobRunner::connect(
        FileJobServices {
            store: store.clone(),
            locks,
            reporter: reporter.clone(),
            channels: channels.clone(),
            programmes: programmes.clone(),
            policy: Arc::new(CachedSwapPolicy::with_clock(
                store.clone(),
                Some(cache.into()),
                clock.clone(),
            )),
            dispatcher: Arc::new(DefaultDispatcher(default_calls.clone())),
            clock: clock.clone(),
        },
        FileJobConfig {
            max_jobs: 1,
            cache_dir: "/evidence/native-refresh-cache".into(),
            release_timeout: Duration::from_secs(5),
        },
        cache,
    )
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = RefreshWorker::new_fixture(
        jobs.clone(),
        Arc::new(Expected {
            service: ServiceExecutor::new(runner.clone(), store.clone(), clock),
            id,
            source,
            calls: calls.clone(),
        }),
    )
    .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let held_orphan_marker = input["hold_orphan_marker"] == true;
    let outcome = if held_orphan_marker {
        let control = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(db)
            .await
            .unwrap();
        let mut lock = control.begin().await.unwrap();
        sqlx::query("SELECT ident FROM django_celery_beat_periodictasks WHERE ident=1 FOR UPDATE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let owned = worker.clone();
        let waiter = tokio::spawn(async move { owned.run_once(caller).await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(&mut *lock)
                .await
                .unwrap();
            let count:i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT ident FROM django_celery_beat_periodictasks%'")
                .fetch_one(&mut *lock).await.unwrap();
            if count == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "real helper marker wait required"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        waiter.abort();
        let _ = waiter.await;
        assert!(weak.upgrade().is_some());
        assert_eq!(worker.available_slots(), 0);
        assert_eq!(runner.physical_jobs(), 1);
        let mut lock_connection = redis::Client::open(cache)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!("task_lock_refresh_epg_data_{source}");
        let refresh: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut lock_connection)
            .await
            .unwrap();
        assert!(refresh.is_some());
        assert_eq!(
            worker.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        );
        lock.commit().await.unwrap();
        worker.shutdown(Duration::from_secs(5)).await.unwrap();
        assert!(jobs
            .read_refresh_fixture(id, source, Arc::new(()))
            .await
            .unwrap()
            .is_some());
        let refresh: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut lock_connection)
            .await
            .unwrap();
        assert!(refresh.is_none());
        control.close().await;
        None
    } else {
        Some(worker.run_once(caller).await)
    };
    if input["action"] == "reject_head" {
        assert_eq!(
            outcome,
            Some(Err(Error::Transport(TransportError::UnsupportedTask)))
        );
        let after: Vec<u8> = redis::cmd("LINDEX")
            .arg("celery")
            .arg(-1)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(head, after);
        assert_eq!(calls.load(Ordering::SeqCst), 0)
    } else if input["expect_operational"] == true {
        assert_eq!(outcome, Some(Err(Error::Execution)));
        assert_eq!(calls.load(Ordering::SeqCst), 1)
    } else if held_orphan_marker {
        // The logical waiter was aborted, so its return value is unobserved.
        // The actual normal result was independently read above; never invent
        // a Completed return for the abandoned JoinHandle.
        assert!(outcome.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    } else {
        assert!(
            matches!(outcome,Some(Ok(Run::Completed(v)|Run::ResultAlreadySucceeded(v)))if v==id)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1)
    }
    assert!(weak.upgrade().is_none());
    assert_eq!(default_calls.load(Ordering::SeqCst), 0);
    worker.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(runner.shutdown(Duration::from_secs(5)).await);
    assert!(channels.shutdown(Duration::from_secs(5)).await);
    assert!(programmes.shutdown(Duration::from_secs(5)).await);
    assert!(reporter.shutdown(Duration::from_secs(5)).await);
    assert_eq!(runner.physical_jobs(), 0);
    assert_eq!(jobs.available_slots(), 4);
    jobs.shutdown(Duration::from_secs(5)).await.unwrap();
    store.pool().close().await;
    println!(
        "NATIVE_REFRESH_WORKER_JSON={}",
        json!({"outcome":format!("{outcome:?}"),"calls":calls.load(Ordering::SeqCst),"default_dispatch_calls":default_calls.load(Ordering::SeqCst),"caller_released":weak.upgrade().is_none(),"worker_drained":true,"service_drained":true,"transport_drained":true,"physical_jobs":runner.physical_jobs(),"head_unchanged":input["action"]=="reject_head","native_eta_consumer":false,"held_orphan_marker":held_orphan_marker,"logical_waiter_aborted":held_orphan_marker})
    );
}
