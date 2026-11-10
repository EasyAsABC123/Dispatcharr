//! Actual original index-task shape/deferral versus retained native workers.
//! Original Dispatcharr authors retain full application/algorithm credit.
//! Post87 draft: execute only after the coordinated performance quiet window.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    index_service::{
        IndexConfig, IndexDispatcher, IndexOutcome, IndexRequest, IndexService, IndexServices,
    },
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
async fn state(store: &Store, redis: &mut redis::aio::MultiplexedConnection, id: i64) -> Value {
    let row = sqlx::query("SELECT data,updated_at FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let updated: DateTime<Utc> = row.get("updated_at");
    let held: bool = redis::cmd("EXISTS")
        .arg(format!("task_lock_epg_source_file_{id}"))
        .query_async(redis)
        .await
        .unwrap();
    json!({"data":row.get::<Option<Value>,_>("data"),"updated_at":updated.to_rfc3339_opts(chrono::SecondsFormat::Micros,false).replace(".000000+00:00","+00:00"),"file_held":held})
}
struct Dispatcher {
    store: Store,
    redis: redis::aio::MultiplexedConnection,
    events: Mutex<Vec<Value>>,
}
impl IndexDispatcher for Dispatcher {
    fn defer<'a>(
        &'a self,
        id: i64,
        retry: u32,
        seconds: u32,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            let snapshot = state(&self.store, &mut self.redis.clone(), id).await;
            self.events.lock().unwrap().push(json!({"args":[],"kwargs":{"args":[id],"kwargs":{"_defer_retry":retry},"countdown":seconds},"state":snapshot}));
            Ok(())
        })
    }
}
struct Files(Vec<PathBuf>);
impl Drop for Files {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn validated_path(value: &Value) -> PathBuf {
    let path = PathBuf::from(value.as_str().unwrap());
    assert_eq!(path.parent(), Some(std::path::Path::new("/tmp")));
    assert!(path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("rust-epg-byte-index-"));
    path
}
fn write_spec(path: &std::path::Path, parts: &Value) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    for part in parts.as_array().unwrap() {
        if let Some(value) = part["bytes_base64"].as_str() {
            file.write_all(&STANDARD.decode(value).unwrap()).unwrap();
        } else {
            let mut left = part["length"].as_u64().unwrap();
            let block = vec![part["repeat"].as_u64().unwrap() as u8; 65536];
            while left > 0 {
                let count = left.min(block.len() as u64) as usize;
                file.write_all(&block[..count]).unwrap();
                left -= count as u64;
            }
        }
    }
}
fn config() -> IndexConfig {
    IndexConfig {
        path_context: None,
        max_jobs: 2,
        limits: IndexLimits {
            state_bytes: 1024 * 1024,
            ..IndexLimits::default()
        },
        cache_dir: PathBuf::from("/tmp/rust-index-service-cache"),
        release_timeout: Duration::from_secs(5),
        max_index_json_bytes: 1024 * 1024,
        max_memory_bytes: 32 * 1024 * 1024,
    }
}
#[tokio::test]
#[ignore = "requires disposable original index task fixture and real PostgreSQL/Redis"]
async fn original_index_task_stored_shape_and_file_lease_deferral() {
    assert_eq!(
        std::env::var("EPG_INDEX_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_INDEX_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let id = case["source_id"].as_i64().unwrap();
        let name = case["name"].as_str().unwrap();
        let actual_name: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(
            actual_name,
            format!("rust-epg-byte-index-{name}"),
            "only captured synthetic sources may be reset"
        );
        let path = validated_path(&case["path"]);
        let extracted = case["extracted_path"]
            .as_str()
            .map(|_| validated_path(&case["extracted_path"]));
        let mut files = Files(vec![path.clone()]);
        if let Some(value) = &extracted {
            files.0.push(value.clone());
        }
        assert!(!path.exists());
        if let Some(value) = &extracted {
            assert!(!value.exists());
            write_spec(value, &case["spec"]);
            std::fs::write(&path, b"<programme channel='original-ignored'/>").unwrap();
        } else if name != "missing-file" {
            write_spec(&path, &case["spec"]);
        }
        sqlx::query("UPDATE epg_epgsourceindex SET data=$2,updated_at=$3 WHERE source_id=$1")
            .bind(id)
            .bind(sqlx::types::Json(case["before"]["data"].clone()))
            .bind(
                case["before"]["updated_at"]
                    .as_str()
                    .unwrap()
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            )
            .execute(store.pool())
            .await
            .unwrap();
        let key = format!("task_lock_epg_source_file_{id}");
        if case["before"]["file_held"] == true {
            let _: String = redis::cmd("SET")
                .arg(&key)
                .arg("locked")
                .arg("EX")
                .arg(300)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let now = case["now"]
            .as_str()
            .unwrap()
            .parse::<DateTime<Utc>>()
            .unwrap();
        let dispatcher = Arc::new(Dispatcher {
            store: store.clone(),
            redis: redis.clone(),
            events: Mutex::new(Vec::new()),
        });
        let service = IndexService::new(
            IndexServices {
                store: store.clone(),
                locks: TaskLocks::connect(&redis_url, TaskLockConfig::default())
                    .await
                    .unwrap(),
                dispatcher: dispatcher.clone(),
                clock: Arc::new(move || now),
            },
            config(),
        )
        .unwrap();
        let result = service
            .build(
                IndexRequest {
                    source_id: id,
                    retry: if name == "defer-exhausted" { 480 } else { 0 },
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            match name {
                "missing-file" => IndexOutcome::MissingFile,
                "file-held" => IndexOutcome::Deferred,
                "defer-exhausted" => IndexOutcome::Exhausted,
                _ => IndexOutcome::Written,
            },
            "outcome {name}"
        );
        assert_eq!(
            state(&store, &mut redis, id).await,
            case["after"],
            "stored shape/clock/lease {name}"
        );
        assert_eq!(
            json!(*dispatcher.events.lock().unwrap()),
            case["dispatch"],
            "defer {name}"
        );
        assert!(service.shutdown(Duration::from_secs(2)).await);
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut redis)
            .await
            .unwrap();
        compared += 1;
    }
    assert_eq!(compared, 24);
    println!("EPG_INDEX_TASK_SOURCE_CASES={compared}");
}
struct Witness(Arc<AtomicBool>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
async fn wait_flag(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !flag.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_writer(store: &Store) {
    tokio::time::timeout(Duration::from_secs(10),async{loop{let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO epg_epgsourceindex%')").fetch_one(store.pool()).await.unwrap();if blocked{break;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
}
#[tokio::test]
#[ignore = "requires disposable original fixture and physical PostgreSQL/Redis"]
async fn physical_index_memory_sql_cancel_replacement_and_json_guards() {
    assert_eq!(
        std::env::var("EPG_INDEX_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_INDEX_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let case = &fixture["cases"][1];
    let second = &fixture["cases"][2];
    let id = case["source_id"].as_i64().unwrap();
    let id2 = second["source_id"].as_i64().unwrap();
    let now = case["now"]
        .as_str()
        .unwrap()
        .parse::<DateTime<Utc>>()
        .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let input = std::env::temp_dir().join(format!(
        "rust-index-service-physical-{}.xml",
        std::process::id()
    ));
    let input2 = input.with_extension("second");
    let _files = Files(vec![input.clone(), input2.clone()]);
    std::fs::write(&input, b"<programme channel='resource-new'/>").unwrap();
    std::fs::write(&input2, b"<programme channel='resource-second'/>").unwrap();
    for source in [id, id2] {
        let name: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(source)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(name.starts_with("rust-epg-byte-index-"));
    }
    for (source, file) in [(id, &input), (id2, &input2)] {
        sqlx::query("UPDATE epg_epgsource SET file_path=$2,extracted_file_path=NULL WHERE id=$1")
            .bind(source)
            .bind(file.to_str().unwrap())
            .execute(store.pool())
            .await
            .unwrap();
    }
    let before = state(&store, &mut redis, id).await;
    let dispatcher = Arc::new(Dispatcher {
        store: store.clone(),
        redis: redis.clone(),
        events: Mutex::new(Vec::new()),
    });
    let services = IndexServices {
        store: store.clone(),
        locks: TaskLocks::connect(&redis_url, TaskLockConfig::default())
            .await
            .unwrap(),
        dispatcher,
        clock: Arc::new(move || now),
    };
    let service = IndexService::new(services.clone(), config()).unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let first_done = Arc::new(AtomicBool::new(false));
    let done = first_done.clone();
    let call = service.clone();
    let task = tokio::spawn(async move {
        call.build(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(Witness(done)),
        )
        .await
    });
    wait_writer(&store).await;
    let second_done = Arc::new(AtomicBool::new(false));
    let done = second_done.clone();
    let call = service.clone();
    let second_task = tokio::spawn(async move {
        call.build(
            IndexRequest {
                source_id: id2,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(Witness(done)),
        )
        .await
    });
    let key2 = format!("task_lock_epg_source_file_{id2}");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !redis::cmd("EXISTS")
            .arg(&key2)
            .query_async::<bool>(&mut redis)
            .await
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(service.physical_jobs(), 2);
    let blocked:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO epg_epgsourceindex%'").fetch_one(store.pool()).await.unwrap();
    assert_eq!(
        blocked, 1,
        "second request must await reserved memory before heavy work"
    );
    second_task.abort();
    let _ = second_task.await;
    wait_flag(&second_done).await;
    assert!(!first_done.load(Ordering::Acquire));
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    let held: bool = redis::cmd("EXISTS")
        .arg(format!("task_lock_epg_source_file_{id}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(held);
    blocker.rollback().await.unwrap();
    assert!(matches!(task.await.unwrap(), Err(ImportError::Cancelled)));
    wait_flag(&first_done).await;
    assert!(service.shutdown(Duration::from_secs(2)).await);
    assert_eq!(
        state(&store, &mut redis, id).await,
        before,
        "cancelled writer must rollback before releasing its file lease"
    );
    // A replacement owner observed before the commit boundary cancels the old
    // writer; owner-checked cleanup cannot delete or extend its successor.
    let mut short = services.clone();
    short.locks = TaskLocks::connect(
        &redis_url,
        TaskLockConfig {
            ttl: Duration::from_secs(2),
            renewal_interval: Duration::from_millis(100),
            max_leases: 4,
            max_commands: 16,
        },
    )
    .await
    .unwrap();
    let service = IndexService::new(short, config()).unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let flag = Arc::new(AtomicBool::new(false));
    let lost = flag.clone();
    let call = service.clone();
    let task = tokio::spawn(async move {
        call.build(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: flag,
            },
            Arc::new(()),
        )
        .await
    });
    wait_writer(&store).await;
    let key = format!("task_lock_epg_source_file_{id}");
    let _: String = redis::cmd("SET")
        .arg(&key)
        .arg("synthetic-index-successor")
        .arg("EX")
        .arg(30)
        .query_async(&mut redis)
        .await
        .unwrap();
    wait_flag(&lost).await;
    blocker.rollback().await.unwrap();
    assert!(task.await.unwrap().is_err());
    let owner: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(owner, "synthetic-index-successor");
    let after = state(&store, &mut redis, id).await;
    assert_eq!(after["data"], before["data"]);
    assert_eq!(after["updated_at"], before["updated_at"]);
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(service.shutdown(Duration::from_secs(2)).await);
    // Valid index input above an explicitly small wire limit must fail before
    // constructing/binding a large SQL tree or overwriting the previous index.
    let mut file = std::fs::File::create(&input).unwrap();
    for n in 0..1000 {
        write!(file, "<programme channel='{}-{n}'/>", "x".repeat(100)).unwrap();
    }
    drop(file);
    let mut small = config();
    small.max_index_json_bytes = 65536;
    let service = IndexService::new(services, small).unwrap();
    let result = service
        .build(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(()),
        )
        .await;
    assert!(matches!(
        result,
        Err(ImportError::Capacity("programme-index JSON"))
    ));
    assert_eq!(state(&store, &mut redis, id).await, before);
    assert!(service.shutdown(Duration::from_secs(2)).await);
    println!("EPG_INDEX_TASK_PHYSICAL_SCENARIOS=3");
}
