//! Guarded copied-database native index component; no application202/daemon.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL license.
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    index_service::{IndexConfig, IndexDispatcher, IndexRequest, IndexService, IndexServices},
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_job_transport::{
    index_protocol::IndexTask,
    index_worker::{ChildPublisher, Error, ExecuteFuture, IndexExecutor, IndexWorker},
    transport::{Config, Jobs},
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::{
    future::Future,
    io::{self, Read},
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use uuid::Uuid;
struct DefaultDispatcher;
impl IndexDispatcher for DefaultDispatcher {
    fn defer<'a>(
        &'a self,
        _id: i64,
        _retry: u32,
        _seconds: u32,
        _context: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
        Box::pin(async { Err(ImportError::Worker) })
    }
}
struct Dispatcher(ChildPublisher);
impl IndexDispatcher for Dispatcher {
    fn defer<'a>(
        &'a self,
        id: i64,
        retry: u32,
        seconds: u32,
        context: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
        Box::pin(async move {
            context.ensure_active()?;
            self.0
                .defer(id, retry, seconds, Arc::new(context))
                .await
                .map_err(|_| ImportError::Worker)?;
            Ok(())
        })
    }
}
struct Executor(IndexService);
impl IndexExecutor for Executor {
    fn execute<'a>(
        &'a self,
        message: dispatcharr_job_transport::index_protocol::IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            self.0
                .build_with_dispatcher(
                    IndexRequest {
                        source_id: message.source_id,
                        retry: message.retry,
                        cancelled: Arc::new(AtomicBool::new(false)),
                    },
                    owner,
                    Arc::new(Dispatcher(publisher)),
                )
                .await
                .map_err(|_| Error::Execution)?;
            Ok(())
        })
    }
}
#[tokio::main]
async fn main() {
    if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only") {
        std::process::exit(2)
    }
    let mut raw = Vec::new();
    if io::stdin().take(16385).read_to_end(&mut raw).is_err() || raw.len() > 16384 {
        std::process::exit(2)
    }
    let value: Value = serde_json::from_slice(&raw).expect("Bounded fixture JSON");
    let string = |k: &str| value[k].as_str().expect("Fixture field");
    let database = std::env::var("POSTGRES_DB").expect("Copied DB");
    let broker = string("broker_url");
    let jobs = Jobs::connect_fixture(broker, string("result_url"), &database, Config::default())
        .await
        .expect("Owned DB8/9");
    let output = match string("action") {
        "publish" => {
            let task = IndexTask::Initial {
                source_id: value["source_id"].as_i64().expect("Source ID"),
            };
            let id = Uuid::new_v4();
            jobs.publish_index(&task, id, Arc::new(()))
                .await
                .expect("Physical publication");
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            json!({"task_id":id.to_string()})
        }
        "read" => {
            let id = Uuid::parse_str(string("task_id")).unwrap();
            let result = jobs
                .read_index(id, Arc::new(()))
                .await
                .expect("Bounded original index result");
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            match result {
                None => json!({"missing":true}),
                Some(m) => {
                    json!({"missing":false,"state":"SUCCESS","result":null,"parent_id":m.parent.map(|id|id.to_string()),"children":m.children.iter().map(Uuid::to_string).collect::<Vec<_>>()})
                }
            }
        }
        "run" => {
            assert_eq!(std::env::var("POSTGRES_USER").as_deref(), Ok("dispatch"));
            let store = Store::connect(
                &format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}"),
                1,
            )
            .await
            .expect("Copied database");
            let redis0 = format!("{}/0", broker.strip_suffix("/8").expect("Broker8"));
            let locks = TaskLocks::connect(
                &redis0,
                TaskLockConfig {
                    max_leases: 1,
                    max_commands: 4,
                    ..TaskLockConfig::default()
                },
            )
            .await
            .expect("Owned file locks");
            let service = IndexService::new(
                IndexServices {
                    store,
                    locks,
                    dispatcher: Arc::new(DefaultDispatcher),
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
                    cache_dir: PathBuf::from("/evidence/native-index-cache"),
                    release_timeout: Duration::from_secs(5),
                    max_index_json_bytes: 1 << 20,
                    max_memory_bytes: 32 << 20,
                },
            )
            .expect("Bounded index config");
            let worker = IndexWorker::new_fixture(jobs, Arc::new(Executor(service.clone())));
            let result = worker.run_once(Arc::new(())).await;
            worker
                .shutdown(Duration::from_secs(5))
                .await
                .expect("Physical worker drain");
            assert!(service.shutdown(Duration::from_secs(5)).await);
            json!({"outcome":format!("{:?}",result.expect("Supported immediate index task"))})
        }
        _ => std::process::exit(2),
    };
    println!("{output}")
}
