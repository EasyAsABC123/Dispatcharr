//! Disposable copied-DB peer; executes normal scanner-owned Refresh code.
use crate::{
    refresh_worker::ServiceExecutor,
    scanner_refresh::{Scope, Worker},
    transport::{Config, Jobs},
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    cache_policy::CachedSwapPolicy,
    channels::{ChannelConfig, ChannelImporter},
    file_job::{ChildTask, FileJobConfig, FileJobRunner, FileJobServices, GuideTaskDispatcher},
    reporter::GuideReporter,
    EpgImporter, ImportConfig, ImportError, SwapPolicyContext,
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    sync::Arc,
    time::Duration,
};
struct NoDefault;
impl GuideTaskDispatcher for NoDefault {
    fn dispatch<'a>(
        &'a self,
        _task: ChildTask,
        _context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async { Err(ImportError::Worker) })
    }
}
fn read(input: &mut impl BufRead) -> Value {
    let mut raw = Vec::new();
    loop {
        let b = input.fill_buf().unwrap();
        assert!(!b.is_empty());
        let n = b
            .iter()
            .position(|v| *v == b'\n')
            .map_or(b.len(), |n| n + 1);
        assert!(raw.len() + n <= 8192);
        raw.extend_from_slice(&b[..n]);
        input.consume(n);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).unwrap()
}
fn output(v: Value) {
    println!("NATIVE_SCANNER_REFRESH_JSON={v}");
    std::io::stdout().flush().unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn native_scanner_refresh_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(db.starts_with("rust_probe_backup_jobs_"));
    crate::default_peer::validate_fixture_redis();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let cfg = read(&mut input);
    let parent = uuid::Uuid::parse_str(cfg["parent_id"].as_str().unwrap()).unwrap();
    let source = cfg["source_id"].as_i64().filter(|v| *v > 0).unwrap();
    let broker = cfg["broker_url"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| std::env::var("CELERY_BROKER_URL").unwrap());
    let backend = cfg["result_url"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| std::env::var("CELERY_RESULT_BACKEND").unwrap());
    let cache = "redis://:playback-fixture-only@127.0.0.1:6379/0";
    let jobs = Jobs::connect_refresh(&broker, &backend, Config::default())
        .await
        .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_URL").unwrap(), 1)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
        .bind(source)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(name.starts_with("rust-owned-children-"));
    let sink = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(cache, Default::default())
            .await
            .unwrap(),
    );
    let clock: Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync> =
        Arc::new(chrono::Utc::now);
    let reporter = GuideReporter::with_clock(store.clone(), cache, sink.clone(), 2, clock.clone())
        .await
        .unwrap();
    let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let runner = FileJobRunner::connect(
        FileJobServices {
            store: store.clone(),
            locks: TaskLocks::connect(cache, TaskLockConfig::default())
                .await
                .unwrap(),
            reporter: reporter.clone(),
            channels: channels.clone(),
            programmes: programmes.clone(),
            policy: Arc::new(CachedSwapPolicy::with_clock(
                store.clone(),
                Some(cache.into()),
                clock.clone(),
            )),
            dispatcher: Arc::new(NoDefault),
            clock: clock.clone(),
        },
        FileJobConfig {
            max_jobs: 1,
            cache_dir: "/app/media/cached_epg".into(),
            release_timeout: Duration::from_secs(5),
        },
        cache,
    )
    .await
    .unwrap();
    let executor = Arc::new(ServiceExecutor::new(runner.clone(), store.clone(), clock));
    let legacy = crate::refresh_worker::RefreshWorker::new(jobs.clone(), executor.clone()).unwrap();
    let foreign = Worker::new(
        jobs.clone(),
        executor.clone(),
        Scope::new(uuid::Uuid::new_v4(), vec![source]).unwrap(),
    )
    .unwrap();
    let worker = Worker::new(
        jobs.clone(),
        executor,
        Scope::new(parent, vec![source]).unwrap(),
    )
    .unwrap();
    output(json!({"prepared":true}));
    let mut pending = None;
    let mut weak: Option<std::sync::Weak<()>> = None;
    loop {
        let request = read(&mut input);
        match request["action"].as_str().unwrap() {
            "legacy" => {
                let result = legacy.run_once(Arc::new(())).await;
                output(json!({"run":format!("{result:?}")}));
            }
            "foreign" => {
                let result = foreign.run_once(Arc::new(())).await;
                output(json!({"run":format!("{result:?}")}));
            }
            "start" => {
                assert!(pending.is_none());
                let caller = Arc::new(());
                weak = Some(Arc::downgrade(&caller));
                let w = worker.clone();
                pending = Some(tokio::spawn(async move { w.run_once(caller).await }));
                output(json!({"started":true}));
            }
            "abort" => {
                let task = pending.take().unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                let admission = worker.run_once(Arc::new(())).await;
                let drain = worker.shutdown(Duration::from_millis(30)).await;
                output(
                    json!({"caller_retained":weak.as_ref().unwrap().upgrade().is_some(),"slots":worker.available_slots(),"admission":format!("{admission:?}"),"drain":format!("{drain:?}")}),
                );
            }
            "settled" => {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                while worker.available_slots() == 0 {
                    assert!(tokio::time::Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                output(
                    json!({"caller_released":weak.as_ref().unwrap().upgrade().is_none(),"slots":worker.available_slots()}),
                );
            }
            "run" => {
                let r = worker.run_once(Arc::new(())).await;
                output(json!({"run":format!("{r:?}"),"available_slots":worker.available_slots()}));
            }
            "shutdown" => {
                worker.shutdown(Duration::from_secs(5)).await.unwrap();
                legacy.shutdown(Duration::from_secs(5)).await.unwrap();
                foreign.shutdown(Duration::from_secs(5)).await.unwrap();
                assert!(runner.shutdown(Duration::from_secs(5)).await);
                assert!(reporter.shutdown(Duration::from_secs(5)).await);
                assert!(channels.shutdown(Duration::from_secs(5)).await);
                assert!(programmes.shutdown(Duration::from_secs(5)).await);
                jobs.shutdown(Duration::from_secs(5)).await.unwrap();

                output(json!({"drained":true}));
                break;
            }
            _ => panic!("Unsupported owned peer action"),
        }
    }
    store.pool().close().await;
}
