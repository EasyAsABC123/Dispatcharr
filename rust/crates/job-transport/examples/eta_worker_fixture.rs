//! Guarded copied-DB ETA scheduling probe; never starts an application daemon.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/transport credit.
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    index_service::{IndexConfig, IndexDispatcher, IndexService, IndexServices},
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_job_transport::{
    eta::Limits,
    index_worker::{Error, ExecuteFuture, IndexExecutor, ServiceExecutor},
    scheduled_index::{Config as ScheduleConfig, ScheduledIndexWorker},
    transport::{Config, Jobs},
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    future::Future,
    io::{self, Read},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
struct Reject;
impl IndexDispatcher for Reject {
    fn defer<'a>(
        &'a self,
        _id: i64,
        _n: u32,
        _s: u32,
        _ctx: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
        Box::pin(async { Err(ImportError::Worker) })
    }
}
struct NeverExecute;
impl IndexExecutor for NeverExecute {
    fn execute<'a>(
        &'a self,
        _m: dispatcharr_job_transport::index_protocol::IndexMessage,
        _p: dispatcharr_job_transport::index_worker::ChildPublisher,
        _o: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async { Err(Error::Execution) })
    }
}
fn copied_database() -> String {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        db.starts_with("rust_probe_backup_")
            && db.len() <= 63
            && db
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    db
}
async fn services(db: &str, broker: &str) -> IndexService {
    assert_eq!(std::env::var("POSTGRES_USER").as_deref(), Ok("dispatch"));
    let store = Store::connect(
        &format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{db}"),
        1,
    )
    .await
    .unwrap();
    let redis0 = format!("{}/0", broker.strip_suffix("/8").unwrap());
    let locks = TaskLocks::connect(
        &redis0,
        TaskLockConfig {
            max_leases: 1,
            max_commands: 4,
            ..TaskLockConfig::default()
        },
    )
    .await
    .unwrap();
    IndexService::new(
        IndexServices {
            store,
            locks,
            dispatcher: Arc::new(Reject),
            clock: Arc::new(chrono::Utc::now),
        },
        IndexConfig {
            max_jobs: 1,
            limits: IndexLimits {
                state_bytes: 1 << 20,
                file_bytes: 16 << 20,
                max_keys: 10000,
                max_tags: 10000,
            },
            cache_dir: PathBuf::from("/evidence/native-eta-cache"),
            release_timeout: Duration::from_secs(5),
            max_index_json_bytes: 1 << 20,
            max_memory_bytes: 32 << 20,
            path_context: None,
        },
    )
    .unwrap()
}
#[tokio::main]
async fn main() {
    let db = copied_database();
    let mut raw = Vec::new();
    io::stdin().take(16385).read_to_end(&mut raw).unwrap();
    assert!(raw.len() <= 16384);
    let v: Value = serde_json::from_slice(&raw).unwrap();
    let field = |k: &str| v[k].as_str().unwrap();
    for (url, number) in [(field("broker_url"), 8), (field("result_url"), 9)] {
        let info = redis::Client::open(url)
            .unwrap()
            .get_connection_info()
            .clone();
        assert_eq!(info.redis.db, number);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(info.addr,redis::ConnectionAddr::Tcp(ref h,p)if matches!(h.as_str(),"127.0.0.1"|"localhost")&&(p==6379||(61000..=65000).contains(&p)))
        );
    }
    let jobs = Jobs::connect_index(field("broker_url"), field("result_url"), Config::default())
        .await
        .unwrap();
    let limits = Limits::default();
    let output = match field("action") {
        "reserve" => {
            let r = jobs
                .reserve_index_eta(Arc::new(()), limits)
                .await
                .unwrap()
                .unwrap();
            let output = json!({"task_id":r.message().task_id.to_string(),"delivery_tag":r.delivery_tag().to_string(),"tuple_bytes":r.tuple_bytes().len()});
            drop(r);
            output
        }
        "warm" => {
            let count = v["count"].as_u64().unwrap() as usize;
            assert!((1..=16).contains(&count));
            let worker = ScheduledIndexWorker::new(
                jobs.clone(),
                Arc::new(NeverExecute),
                ScheduleConfig::default(),
            )
            .unwrap();
            let mut handles = Vec::new();
            for _ in 0..count {
                let r = jobs
                    .reserve_index_eta(Arc::new(()), limits)
                    .await
                    .unwrap()
                    .unwrap();
                assert!((r.message().eta.unwrap() - chrono::Utc::now()).num_seconds() > 0);
                handles.push(worker.start(&r, Arc::new(())).unwrap());
            }
            worker.shutdown(Duration::from_secs(5)).await.unwrap();
            let mut outcomes = Vec::new();
            for h in handles {
                outcomes.push(format!("{:?}", h.wait().await.unwrap()));
            }
            json!({"restored":count,"outcomes":outcomes})
        }
        "execute" | "hold" => {
            let service = services(&db, field("broker_url")).await;
            let worker = ScheduledIndexWorker::new(
                jobs.clone(),
                Arc::new(ServiceExecutor::new(service.clone())),
                ScheduleConfig::default(),
            )
            .unwrap();
            let r = jobs
                .reserve_index_eta(Arc::new(()), limits)
                .await
                .unwrap()
                .unwrap();
            let id = r.message().task_id;
            let handle = worker.start(&r, Arc::new(())).unwrap();
            drop(r);
            if let Some(marker) = v["marker"].as_str() {
                assert!(
                    marker.starts_with("native-eta-")
                        && marker.len() < 128
                        && marker
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                );
                std::fs::write(PathBuf::from("/evidence").join(marker), b"reserved").unwrap();
            }
            let outcome = handle.wait().await.unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            worker
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .unwrap();
            assert!(
                service
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await
            );
            json!({"task_id":id.to_string(),"outcome":format!("{outcome:?}")})
        }
        _ => panic!("unsupported fixture action"),
    };
    jobs.shutdown(Duration::from_secs(5)).await.unwrap();
    println!("{output}")
}
