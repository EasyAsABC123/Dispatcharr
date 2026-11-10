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
#[path = "refresh_worker_release_peer.rs"]
mod release_peer;
struct Sink {
    publications: Arc<Mutex<Vec<Value>>>,
}
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            if serde_json::to_vec(&message)
                .map_err(|_| UpdateEventError::InvalidMessage)?
                .len()
                > 65536
            {
                return Err(UpdateEventError::Capacity);
            }
            let mut messages = self.publications.lock().unwrap();
            if messages.len() >= 64 {
                return Err(UpdateEventError::Capacity);
            }
            messages.push(json!({"group":group,"message":message}));
            Ok(())
        })
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
    let database_role = input["database_role"].as_str().unwrap_or("dispatch");
    if database_role != "dispatch" {
        assert_eq!(database_role, format!("rust_exhaust_role_{source}"));
    }
    assert_eq!(
        db,
        format!("postgres://{database_role}:playback-fixture-only@127.0.0.1:5432/{database}")
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
    let publications = Arc::new(Mutex::new(Vec::new()));
    let reporter = GuideReporter::with_clock(
        store.clone(),
        cache,
        Arc::new(Sink {
            publications: publications.clone(),
        }),
        2,
        clock.clone(),
    )
    .await
    .unwrap();
    let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let mut release_proxy = if let Some(mode) = input["release_intervention"].as_str() {
        Some(
            release_peer::proxy(
                store.clone(),
                source,
                mode,
                input["abort_on_release"] == true,
            )
            .await,
        )
    } else {
        None
    };
    let lock_url = release_proxy
        .as_ref()
        .map(|proxy| proxy.url.as_str())
        .unwrap_or(cache);
    let locks = TaskLocks::connect(lock_url, TaskLockConfig::default())
        .await
        .unwrap();
    if release_proxy.is_some() {
        locks
            .acquire("rust_race_warm", source)
            .await
            .unwrap()
            .release_owned()
            .await
            .unwrap();
    }
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
    if input["close_reporter_before_run"] == true {
        assert!(reporter.shutdown(Duration::from_secs(1)).await);
    }
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let held_orphan_marker = input["hold_orphan_marker"] == true;
    let abort_on_release = input["abort_on_release"] == true;
    assert!(!(abort_on_release && held_orphan_marker));
    let outcome = if abort_on_release {
        let proxy = release_proxy.as_mut().unwrap();
        let owned = worker.clone();
        let waiter = tokio::spawn(async move { owned.run_once(caller).await });
        tokio::time::timeout(
            Duration::from_secs(3),
            proxy.blocked.wait_for(|blocked| *blocked),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(proxy.intervened.load(Ordering::Acquire));
        assert!(weak.upgrade().is_some());
        assert_eq!(worker.available_slots(), 0);
        assert_eq!(runner.physical_jobs(), 1);
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            worker.shutdown(Duration::from_millis(20)).await,
            Err(Error::DrainTimeout)
        );
        proxy.open.send_replace(true);
        worker.shutdown(Duration::from_secs(5)).await.unwrap();
        assert!(jobs
            .read_refresh_fixture(id, source, Arc::new(()))
            .await
            .unwrap()
            .is_some());
        None
    } else if held_orphan_marker {
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
    } else if held_orphan_marker || abort_on_release {
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
    let domain_writes_after_intervention = if let Some(proxy) = &release_proxy {
        assert!(proxy.intervened.load(Ordering::Acquire));
        let table = format!("rust_native_race_{source}");
        let count:i64=sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE kind IN ('source_update','core_event') AND id>(SELECT id FROM {table} WHERE kind='intervention')")).fetch_one(store.pool()).await.unwrap();
        assert_eq!(count, 0);
        let mut cache_conn = redis::Client::open(cache)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let value: Option<String> = redis::cmd("GET")
            .arg(format!("task_lock_refresh_epg_data_{source}"))
            .query_async(&mut cache_conn)
            .await
            .unwrap();
        assert_eq!(
            value.as_deref(),
            if input["release_intervention"] == "replace" {
                Some("owned-foreign-token")
            } else {
                None
            }
        );
        Some(count)
    } else {
        None
    };
    store.pool().close().await;
    println!(
        "NATIVE_REFRESH_WORKER_JSON={}",
        json!({"outcome":format!("{outcome:?}"),"calls":calls.load(Ordering::SeqCst),"default_dispatch_calls":default_calls.load(Ordering::SeqCst),"caller_released":weak.upgrade().is_none(),"worker_drained":true,"service_drained":true,"transport_drained":true,"physical_jobs":runner.physical_jobs(),"head_unchanged":input["action"]=="reject_head","native_eta_consumer":false,"held_orphan_marker":held_orphan_marker,"logical_waiter_aborted":held_orphan_marker||abort_on_release,"release_intervention":input["release_intervention"],"domain_writes_after_intervention":domain_writes_after_intervention,"publications":publications.lock().unwrap().clone()})
    );
}
