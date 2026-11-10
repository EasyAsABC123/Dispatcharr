//! Guarded copied-database native index component; no application202/daemon.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL license.
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    current_lookup::{
        CurrentLookup, CurrentLookupConfig, CurrentLookupServices, SourcePathContext,
    },
    index_service::{IndexConfig, IndexDispatcher, IndexService, IndexServices},
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_job_transport::{
    index_worker::{IndexWorker, ServiceExecutor},
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
    assert!(
        database.starts_with("rust_probe_backup_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    );
    for (url, db) in [(broker, 8), (string("result_url"), 9)] {
        let info = redis::Client::open(url)
            .expect("Fixture Redis URL")
            .get_connection_info()
            .clone();
        assert_eq!(info.redis.db, db);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(info.addr,redis::ConnectionAddr::Tcp(ref host,port)if matches!(host.as_str(),"localhost"|"127.0.0.1"|"::1")&&(port==6379||(61000..=65000).contains(&port)))
        );
    }
    let jobs = if value["production_connector"] == true {
        Jobs::connect_index(broker, string("result_url"), Config::default()).await
    } else {
        Jobs::connect_fixture(broker, string("result_url"), &database, Config::default()).await
    }
    .expect("Owned fixture transport");
    let source_context = match (value["source_cwd"].as_str(), value["media_root"].as_str()) {
        (None, None) => None,
        (Some(cwd), Some(media)) => Some(
            SourcePathContext::new(cwd.into(), media.into()).expect("Bounded Source path context"),
        ),
        _ => panic!("Both fixture path roots required"),
    };
    let output = match string("action") {
        "publish" => {
            let dispatcher =
                dispatcharr_job_transport::index_dispatch::ProgrammeIndexDispatcher::new(
                    jobs.clone(),
                    1,
                )
                .unwrap();
            let published = dispatcher
                .publish(
                    value["source_id"].as_i64().expect("Source ID"),
                    Arc::new(()),
                )
                .await
                .expect("Actual initial dispatch reply");
            dispatcher.shutdown(Duration::from_secs(3)).await.unwrap();
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            json!({"task_id":published.task_id.to_string()})
        }
        "capabilities" => {
            assert!(jobs.copied_database().is_none());
            for task in [
                dispatcharr_job_transport::protocol::Task::CreateBackup,
                dispatcharr_job_transport::protocol::Task::RestoreBackup {
                    filename: "never-opened.zip".into(),
                },
            ] {
                assert_eq!(
                    jobs.publish(&task, Uuid::new_v4(), Arc::new(())).await,
                    Err(dispatcharr_job_transport::transport::Error::UnsupportedTask)
                );
            }
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            json!({"index_only":true,"backup_restore_rejected":true})
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
        "current" => {
            let store = Store::connect(
                &format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}"),
                1,
            )
            .await
            .expect("Copied database");
            let now = value["now"]
                .as_str()
                .expect("Source clock")
                .parse::<chrono::DateTime<chrono::Utc>>()
                .expect("Fixture clock");
            let dispatcher = Arc::new(
                dispatcharr_job_transport::index_dispatch::ProgrammeIndexDispatcher::new(
                    jobs.clone(),
                    1,
                )
                .unwrap(),
            );
            let lookup = CurrentLookup::new(
                CurrentLookupServices {
                    store: store.clone(),
                    dispatcher: dispatcher.clone(),
                    clock: Arc::new(move || now),
                },
                CurrentLookupConfig {
                    max_jobs: 1,
                    path_context: source_context.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            let cache = source_context
                .as_ref()
                .map(|context| context.media_root().join("cached_epg"))
                .unwrap_or_else(|| PathBuf::from("/evidence/native-index-cache"));
            let bytes = store
                .current_epg_with_file_lookup(
                    vec![value["epg_entry_id"].as_i64().expect("Owned EPG ID")],
                    now,
                    4 * 1024 * 1024,
                    cache,
                    Arc::new(lookup.clone()),
                    Arc::new(()),
                )
                .await
                .expect("Real Store/current lookup");
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            drop(bytes);
            assert!(lookup.shutdown(Duration::from_secs(3)).await);
            assert_eq!(lookup.physical_jobs(), 0);
            assert_eq!(lookup.retained_memory_bytes(), 0);
            dispatcher.shutdown(Duration::from_secs(3)).await.unwrap();
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            store.pool().close().await;
            json!({"current":result})
        }
        "filesystem" => {
            let context = source_context.as_ref().expect("Explicit fixture context");
            let path = context
                .media_root()
                .join("cached_epg")
                .join("owned-native-write-probe");
            std::fs::write(&path, b"owned copy only").expect("Copied media writable");
            std::fs::remove_file(path).unwrap();
            let data = PathBuf::from("/data").join("owned-native-write-probe");
            std::fs::write(&data, b"owned copy only").expect("Copied data writable");
            std::fs::remove_file(data).unwrap();
            jobs.shutdown(Duration::from_secs(3)).await.unwrap();
            json!({"media_writable":true,"data_writable":true})
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
                    path_context: source_context.clone(),
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
            let worker = IndexWorker::new(
                jobs.clone(),
                Arc::new(ServiceExecutor::new(service.clone())),
            );
            let result = worker.run_once(Arc::new(())).await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            worker
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .expect("Physical worker drain");
            assert!(
                service
                    .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await
            );
            jobs.shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .expect("Final shared transport drain");
            json!({"outcome":format!("{:?}",result.expect("Supported immediate index task"))})
        }
        _ => std::process::exit(2),
    };
    let output = if value["include_context"] == true {
        let status = std::fs::read_to_string("/proc/self/status").expect("Fixture process status");
        let uid = status
            .lines()
            .find(|line| line.starts_with("Uid:"))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u32>()
            .unwrap();
        json!({"result":output,"cwd":std::env::current_dir().unwrap(),"uid":uid})
    } else {
        output
    };
    println!("{output}")
}
