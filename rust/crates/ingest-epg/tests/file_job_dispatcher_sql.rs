//! Actual per-refresh dispatcher isolation and real reply ownership.
//! Original Dispatcharr authors retain full application/algorithm credit/AGPL.
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    cache_policy::CachedSwapPolicy,
    channels::{ChannelConfig, ChannelImporter},
    file_job::{
        ChildTask, FileJobConfig, FileJobOutcome, FileJobRequest, FileJobRunner, FileJobServices,
        GuideTaskDispatcher,
    },
    reporter::GuideReporter,
    EpgImporter, ImportConfig, ImportError, SwapPolicyContext,
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
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
struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
type Event = (&'static str, i64, bool, u32, u32);
struct Dispatcher {
    label: &'static str,
    events: Arc<Mutex<Vec<Event>>>,
    block: Option<(String, Arc<AtomicBool>)>,
}
impl GuideTaskDispatcher for Dispatcher {
    fn dispatch<'a>(
        &'a self,
        task: ChildTask,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            let ChildTask::RefreshDeferred {
                source_id,
                force,
                retry,
                delay_seconds,
            } = task
            else {
                panic!("Busy source must only defer")
            };
            self.events
                .lock()
                .unwrap()
                .push((self.label, source_id, force, retry, delay_seconds));
            if let Some((url, entered)) = &self.block {
                let mut redis = redis::Client::open(url.as_str())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    .unwrap();
                entered.store(true, Ordering::SeqCst);
                let result: Option<(String, String)> = redis::cmd("BLPOP")
                    .arg("rust-refresh-dispatcher-owned-empty")
                    .arg(1)
                    .query_async(&mut redis)
                    .await
                    .unwrap();
                assert!(result.is_none());
            }
            context.ensure_active()?;
            Ok(())
        })
    }
}
struct ImportedDispatcher(Arc<Mutex<Vec<&'static str>>>);
impl GuideTaskDispatcher for ImportedDispatcher {
    fn dispatch<'a>(
        &'a self,
        task: ChildTask,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            let name = match task {
                ChildTask::LateProgrammes {
                    initial_epg_ids, ..
                } => {
                    assert_eq!(initial_epg_ids.len(), 1);
                    "late_check"
                }
                ChildTask::ProgrammeIndex { .. } => "index",
                ChildTask::SeriesRules => "series",
                ChildTask::RefreshDeferred { .. } => panic!("Imported branch must not defer"),
            };
            self.0.lock().unwrap().push(name);
            Ok(())
        })
    }
}
async fn wait(mut check: impl FnMut() -> bool) {
    for _ in 0..400 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Owned physical observation deadline")
}
#[tokio::test]
#[ignore = "requires copied owned Original refresh-import fixture and real Redis"]
async fn per_execution_refresh_trace_and_cancelled_physical_reply_are_retained() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        db.starts_with("rust_probe_backup_jobs_")
            && db.len() <= 63
            && db
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut input = String::new();
    io::stdin().take(16385).read_to_string(&mut input).unwrap();
    assert!(input.len() <= 16384);
    let request: Value = serde_json::from_str(&input).unwrap();
    let database_url = request["database_url"].as_str().unwrap();
    assert_eq!(
        database_url,
        format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{db}")
    );
    let redis_url = request["redis_url"].as_str().unwrap();
    assert_eq!(redis_url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let ids: Vec<i64> = request["source_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().filter(|v| *v > 0).unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    let store = Store::connect(database_url, 1).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for id in &ids {
        let name: String =
            sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1 AND is_active")
                .bind(id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert!(name.starts_with("rust-refresh-import-imported_"));
        let set: Option<String> = redis::cmd("SET")
            .arg(format!("task_lock_epg_source_file_{id}"))
            .arg("rust-refresh-dispatch-fixture")
            .arg("NX")
            .arg("EX")
            .arg(30)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(set.is_some())
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let make = |label| {
        Arc::new(Dispatcher {
            label,
            events: events.clone(),
            block: None,
        })
    };
    let clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync> =
        Arc::new(chrono::Utc::now);
    let reporter =
        GuideReporter::with_clock(store.clone(), redis_url, Arc::new(Sink), 2, clock.clone())
            .await
            .unwrap();
    let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let locks = TaskLocks::connect(redis_url, TaskLockConfig::default())
        .await
        .unwrap();
    let runner = FileJobRunner::connect(
        FileJobServices {
            store: store.clone(),
            locks: locks.clone(),
            reporter: reporter.clone(),
            channels: channels.clone(),
            programmes: programmes.clone(),
            policy: Arc::new(CachedSwapPolicy::with_clock(
                store.clone(),
                Some(redis_url.to_owned()),
                clock.clone(),
            )),
            dispatcher: make("default"),
            clock,
        },
        FileJobConfig {
            max_jobs: 2,
            cache_dir: "/evidence/refresh-dispatch-cache".into(),
            release_timeout: Duration::from_secs(5),
        },
        redis_url,
    )
    .await
    .unwrap();
    let req = |source_id, force, defer_retry| FileJobRequest {
        source_id,
        force,
        defer_retry,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert_eq!(
        runner
            .run(req(ids[0], false, 0), Arc::new(()))
            .await
            .unwrap(),
        FileJobOutcome::Deferred
    );
    let (one, two) = tokio::join!(
        runner.run_with_dispatcher(req(ids[0], false, 2), Arc::new(()), make("parent-one")),
        runner.run_with_dispatcher(req(ids[1], true, 7), Arc::new(()), make("parent-two"))
    );
    assert_eq!(one.unwrap(), FileJobOutcome::Deferred);
    assert_eq!(two.unwrap(), FileJobOutcome::Deferred);
    {
        let v = events.lock().unwrap();
        assert_eq!(v.len(), 3);
        assert!(v.contains(&("default", ids[0], false, 1, 15)));
        assert!(v.contains(&("parent-one", ids[0], false, 3, 15)));
        assert!(v.contains(&("parent-two", ids[1], true, 8, 15)));
    }
    // Exercise every imported callback using the same immutable local override;
    // a leaked configured dispatcher would reject these non-deferred callbacks.
    let file = format!("task_lock_epg_source_file_{}", ids[1]);
    let removed: usize = redis::cmd("DEL")
        .arg(&file)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    let imported = Arc::new(Mutex::new(Vec::new()));
    assert_eq!(
        runner
            .run_with_dispatcher(
                req(ids[1], true, 0),
                Arc::new(()),
                Arc::new(ImportedDispatcher(imported.clone()))
            )
            .await
            .unwrap(),
        FileJobOutcome::Imported
    );
    assert_eq!(
        *imported.lock().unwrap(),
        vec!["late_check", "index", "series"]
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM epg_programdata p JOIN epg_epgdata e ON p.epg_id=e.id WHERE e.epg_source_id=$1 AND p.title='Owned refresh programme'").bind(ids[1]).fetch_one(store.pool()).await.unwrap();
    assert_eq!(count, 1);
    let reset: Option<String> = redis::cmd("SET")
        .arg(&file)
        .arg("rust-refresh-dispatch-fixture")
        .arg("NX")
        .arg("EX")
        .arg(30)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(reset.is_some());
    // A caller disappears while the actual Redis command still holds its context.
    let entered = Arc::new(AtomicBool::new(false));
    let blocked = Arc::new(Dispatcher {
        label: "cancelled-parent",
        events: events.clone(),
        block: Some((redis_url.into(), entered.clone())),
    });
    let dropped = Arc::new(AtomicUsize::new(0));
    let caller = Arc::new(Witness(dropped.clone()));
    let active = runner.clone();
    let input = req(ids[0], true, 0);
    let handle =
        tokio::spawn(async move { active.run_with_dispatcher(input, caller, blocked).await });
    wait(|| entered.load(Ordering::SeqCst)).await;
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(!runner.shutdown(Duration::from_millis(30)).await);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(runner.physical_jobs(), 1);
    let owner: Option<String> = redis::cmd("GET")
        .arg(format!("task_lock_refresh_epg_data_{}", ids[0]))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(owner.is_some_and(|v| v.starts_with("rust:")));
    wait(|| dropped.load(Ordering::SeqCst) == 1).await;
    assert!(runner.shutdown(Duration::from_secs(5)).await);
    assert_eq!(runner.physical_jobs(), 0);
    assert!(channels.shutdown(Duration::from_secs(5)).await);
    assert!(programmes.shutdown(Duration::from_secs(5)).await);
    assert!(reporter.shutdown(Duration::from_secs(5)).await);
    for id in &ids {
        let file = format!("task_lock_epg_source_file_{id}");
        let owner: String = redis::cmd("GET")
            .arg(&file)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(owner, "rust-refresh-dispatch-fixture");
        let _: usize = redis::cmd("DEL")
            .arg(file)
            .query_async(&mut redis)
            .await
            .unwrap();
        let owner: Option<String> = redis::cmd("GET")
            .arg(format!("task_lock_refresh_epg_data_{id}"))
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(owner.is_none())
    }
    store.pool().close().await;
    println!(
        "NATIVE_REFRESH_DISPATCH_JSON={}",
        json!({"events":*events.lock().unwrap(),"default_preserved":true,"concurrent_overrides_isolated":true,"imported_callbacks_local":true,"caller_released_after_reply":dropped.load(Ordering::SeqCst)==1,"physical_jobs":runner.physical_jobs(),"runner_drained":true,"refresh_locks_released":true,"foreign_file_locks_preserved":true})
    );
}
