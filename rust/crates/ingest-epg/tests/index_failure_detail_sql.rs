//! Typed service observations against the real Original136 file/SQL side effects.
//! Original Dispatcharr authors retain all application and algorithm credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    index_service::{
        IndexCommitObservation, IndexConfig, IndexDispatcher, IndexFailureCause, IndexOutcome,
        IndexReleaseObservation, IndexRequest, IndexService, IndexServices, IndexStage,
    },
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::{Store, StoreError};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct NoDefer;
impl IndexDispatcher for NoDefer {
    fn defer<'a>(
        &'a self,
        _: i64,
        _: u32,
        _: u32,
        _: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async { panic!("Uncontended detailed file/SQL corpus cannot defer") })
    }
}
struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
#[ignore = "requires copied Original136 effects, PostgreSQL trigger and actual Redis"]
async fn detailed_service_preserves_tolerant_success_directory_errno_and_sql_cause() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_"));
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let locks = TaskLocks::connect(&redis, TaskLockConfig::default())
        .await
        .unwrap();
    let dispatcher = Arc::new(NoDefer);
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks: locks.clone(),
            dispatcher: dispatcher.clone(),
            clock: Arc::new(chrono::Utc::now),
        },
        IndexConfig {
            path_context: None,
            max_jobs: 1,
            limits: IndexLimits {
                state_bytes: 1 << 20,
                file_bytes: 16 << 20,
                max_keys: 10000,
                max_tags: 10000,
            },
            cache_dir: "/evidence/native-detailed-index-cache".into(),
            release_timeout: Duration::from_secs(5),
            max_index_json_bytes: 1 << 20,
            max_memory_bytes: 32 << 20,
        },
    )
    .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut compared = 0;
    for case in fixture["index_failure_cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if !matches!(
            name,
            "missing-source"
                | "missing-file"
                | "malformed-xml"
                | "invalid-encoding"
                | "empty-file"
                | "directory"
                | "sql-write"
                | "duplicate-later-directory-error"
        ) {
            continue;
        }
        let id = case["source_id"].as_i64().unwrap();
        let exists: Option<String> =
            sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
                .bind(id)
                .fetch_optional(store.pool())
                .await
                .unwrap();
        if name == "missing-source" {
            assert!(exists.is_none());
        } else {
            assert!(exists.unwrap().starts_with("rust-index-failure-"));
        }
        let report = service
            .build_report_with_dispatcher(
                IndexRequest {
                    source_id: id,
                    retry: 0,
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(Owner(dropped.clone())),
                dispatcher.clone(),
            )
            .await
            .unwrap();
        assert!(
            matches!(report.release, IndexReleaseObservation::Released),
            "{name}: physical release must settle"
        );
        match name {
            "directory" | "duplicate-later-directory-error" => {
                let failure = report.operation.unwrap_err();
                assert_eq!(failure.stage, IndexStage::FileOpen);
                match failure.cause {
                    IndexFailureCause::Operation(ImportError::Io(error)) => {
                        assert_eq!(error.kind(), std::io::ErrorKind::IsADirectory);
                        assert_eq!(error.raw_os_error(), Some(21));
                    }
                    other => panic!("unexpected directory cause: {other:?}"),
                }
                assert_eq!(report.commit, IndexCommitObservation::NotWritten);
            }
            "sql-write" => {
                let failure = report.operation.unwrap_err();
                assert_eq!(failure.stage, IndexStage::Write);
                match failure.cause {
                    IndexFailureCause::Operation(ImportError::Store(StoreError::Database(
                        error,
                    ))) => {
                        let detail = error.as_database_error().unwrap();
                        assert_eq!(detail.code().as_deref(), Some("P0001"));
                        assert_eq!(detail.message(), "owned-index-sql-write-failure");
                    }
                    other => panic!("unexpected copied SQL cause: {other:?}"),
                }
                assert_eq!(report.commit, IndexCommitObservation::NotWritten);
            }
            "missing-source" => {
                assert_eq!(report.operation.unwrap(), IndexOutcome::MissingSource);
                assert_eq!(report.commit, IndexCommitObservation::NotWritten);
            }
            "missing-file" => {
                assert_eq!(report.operation.unwrap(), IndexOutcome::MissingFile);
                assert_eq!(report.commit, IndexCommitObservation::NotWritten);
            }
            _ => {
                assert_eq!(report.operation.unwrap(), IndexOutcome::Written);
                assert_eq!(report.commit, IndexCommitObservation::Written);
            }
        }
        let actual: Option<Value> =
            sqlx::query_scalar("SELECT data FROM epg_epgsourceindex WHERE source_id=$1")
                .bind(id)
                .fetch_optional(store.pool())
                .await
                .unwrap();
        assert_eq!(
            actual.unwrap_or(Value::Null),
            case["index"],
            "{name}: exact Original side effect or preserved existing index"
        );
        compared += 1;
        assert_eq!(dropped.load(Ordering::SeqCst), compared);
        assert_eq!(
            locks.available_leases(),
            TaskLockConfig::default().max_leases
        );
    }
    assert_eq!(compared, 8);
    assert!(service.shutdown(Duration::from_secs(3)).await);
    assert_eq!(service.physical_jobs(), 0);
    store.pool().close().await;
}

// Resource-only helpers; they do not infer Python task exception metadata.
use redis::AsyncCommands;
use sqlx::Connection;
use std::sync::atomic::AtomicBool as GateBool;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
struct Proxy {
    url: String,
    hold: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn end_frame(data: &[u8], start: usize, depth: usize) -> std::io::Result<Option<usize>> {
    if depth > 16 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let Some(&tag) = data.get(start) else {
        return Ok(None);
    };
    let Some(line) = data[start..]
        .windows(2)
        .position(|v| v == b"\r\n")
        .map(|v| v + start)
    else {
        return Ok(None);
    };
    let next = line + 2;
    if matches!(tag, b'+' | b'-' | b':' | b'_') {
        return Ok(Some(next));
    }
    let value = std::str::from_utf8(&data[start + 1..line])
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or(std::io::ErrorKind::InvalidData)?;
    if value < 0 {
        return Ok(Some(next));
    }
    if value > 65536 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    match tag {
        b'$' => {
            let end = next
                .checked_add(value as usize)
                .and_then(|n| n.checked_add(2))
                .ok_or(std::io::ErrorKind::InvalidData)?;
            Ok((end <= data.len()).then_some(end))
        }
        b'*' => {
            if value > 1024 {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            let mut end = next;
            for _ in 0..value {
                let Some(value) = end_frame(data, end, depth + 1)? else {
                    return Ok(None);
                };
                end = value;
            }
            Ok(Some(end))
        }
        _ => Err(std::io::ErrorKind::InvalidData.into()),
    }
}
async fn proxy() -> Proxy {
    let info = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_connection_info()
        .clone();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    assert_eq!(info.redis.db, 0);
    let address = match info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "127.0.0.1" | "localhost") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("owned loopback Redis only"),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let pause = Arc::new(GateBool::new(false));
    let gate = pause.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let count = counter.clone();
    let control = hold.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {accepted=listener.accept(),if tasks.len()<4=>{let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let gate=gate.clone();let control=control.clone();let count=count.clone();tasks.spawn(async move{let server=TcpStream::connect(address).await?;let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();let inbound=async move{let mut data=Vec::with_capacity(65536);let mut buf=[0u8;16384];loop{let n=sr.read(&mut buf).await?;if n==0{return Ok::<(),std::io::Error>(())}
            if data.len()+n>65536{return Err(std::io::ErrorKind::InvalidData.into());}data.extend_from_slice(&buf[..n]);while let Some(end)=end_frame(&data,0,0)?{count.fetch_add(1,Ordering::SeqCst);if hold.wait_for(|value|!*value).await.is_err(){return Ok(())}cw.write_all(&data[..end]).await?;data.drain(..end);if gate.swap(false,Ordering::SeqCst){control.send_replace(true);}}}};tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},inbound).map(|_|())});},_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{local_port}/0"),
        hold,
        task,
    }
}

async fn physical_wait(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("detailed service physical deadline");
}
fn resource_case(fixture: &Value, name: &str) -> i64 {
    fixture["index_failure_cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()["source_id"]
        .as_i64()
        .unwrap()
}
fn resource_config(release_timeout: Duration) -> IndexConfig {
    IndexConfig {
        path_context: None,
        max_jobs: 1,
        limits: IndexLimits {
            state_bytes: 1 << 20,
            file_bytes: 16 << 20,
            max_keys: 10000,
            max_tags: 10000,
        },
        cache_dir: "/evidence/native-detailed-resource-cache".into(),
        release_timeout,
        max_index_json_bytes: 1 << 20,
        max_memory_bytes: 32 << 20,
    }
}
struct LockedActor(sqlx::PgConnection);
impl LockedActor {
    async fn commit(mut self) -> Result<(), sqlx::Error> {
        sqlx::query("COMMIT").execute(&mut self.0).await.map(|_| ())
    }
}
async fn locked_writer() -> (sqlx::PgConnection, LockedActor) {
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let monitor = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut actor = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN").execute(&mut actor).await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN ACCESS EXCLUSIVE MODE")
        .execute(&mut actor)
        .await
        .unwrap();
    (monitor, LockedActor(actor))
}
async fn waiting_writer(monitor: &mut sqlx::PgConnection) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT source_id FROM epg_epgsourceindex%')").fetch_one(monitor).await.unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires copied Original corpus, actual PostgreSQL and Redis ACL/reply gates"]
async fn detailed_reports_preserve_commit_on_release_failure_and_ambiguity_without_early_caller_drop(
) {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _:String=redis::cmd("SCRIPT").arg("LOAD").arg("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end").query_async(&mut redis).await.unwrap();
    // First, a real Redis server rejects release after a proven SQL commit.
    let id = resource_case(&fixture, "invalid-encoding");
    sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    let user = format!("owned_report_{}", uuid_text());
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("on")
        .arg(">playback-fixture-only")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut redis)
        .await
        .unwrap();
    let acl_url = format!("redis://{user}:playback-fixture-only@127.0.0.1:6379/0");
    let locks = TaskLocks::connect(
        &acl_url,
        TaskLockConfig {
            max_leases: 1,
            max_commands: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(NoDefer);
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks: locks.clone(),
            dispatcher: dispatcher.clone(),
            clock: Arc::new(chrono::Utc::now),
        },
        resource_config(Duration::from_secs(2)),
    )
    .unwrap();
    let (mut monitor, tx) = locked_writer().await;
    let copy = service.clone();
    let owner = Arc::new(Owner(dropped.clone()));
    let publish = dispatcher.clone();
    let pending = tokio::spawn(async move {
        copy.build_report_with_dispatcher(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            owner,
            publish,
        )
        .await
        .unwrap()
    });
    physical_wait(async || waiting_writer(&mut monitor).await).await;
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("-evalsha")
        .arg("-eval")
        .query_async(&mut redis)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let report = pending.await.unwrap();
    assert_eq!(report.operation.unwrap(), IndexOutcome::Written);
    assert_eq!(report.commit, IndexCommitObservation::Written);
    assert!(matches!(report.release, IndexReleaseObservation::Failed(_)));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let key = format!("task_lock_epg_source_file_{id}");
    let token: String = redis.get(&key).await.unwrap();
    assert!(token.starts_with("rust:"));
    let _:i64=redis::Script::new("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end").key(&key).arg(token).invoke_async(&mut redis).await.unwrap();
    assert!(service.shutdown(Duration::from_secs(2)).await);
    let _: i64 = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(user)
        .query_async(&mut redis)
        .await
        .unwrap();
    // Then, DEL really executes but its held reply must not free caller or credit.
    let id = resource_case(&fixture, "empty-file");
    sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    let proxy = proxy().await;
    let locks = TaskLocks::connect(
        &proxy.url,
        TaskLockConfig {
            max_leases: 1,
            max_commands: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks: locks.clone(),
            dispatcher: dispatcher.clone(),
            clock: Arc::new(chrono::Utc::now),
        },
        resource_config(Duration::from_millis(50)),
    )
    .unwrap();
    let (mut monitor, tx) = locked_writer().await;
    let copy = service.clone();
    let owner = Arc::new(Owner(dropped.clone()));
    let publish = dispatcher.clone();
    let pending = tokio::spawn(async move {
        copy.build_report_with_dispatcher(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            owner,
            publish,
        )
        .await
        .unwrap()
    });
    physical_wait(async || waiting_writer(&mut monitor).await).await;
    proxy.hold.send_replace(true);
    tx.commit().await.unwrap();
    let report = pending.await.unwrap();
    assert_eq!(report.operation.unwrap(), IndexOutcome::Written);
    assert_eq!(report.commit, IndexCommitObservation::Written);
    assert!(matches!(report.release, IndexReleaseObservation::TimedOut));
    assert!(
        !redis
            .exists::<_, bool>(format!("task_lock_epg_source_file_{id}"))
            .await
            .unwrap(),
        "server DEL completed while physical reply remains held"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(locks.available_leases(), 0);
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    proxy.hold.send_replace(false);
    assert!(service.shutdown(Duration::from_secs(3)).await);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert_eq!(locks.available_leases(), 1);
    // Finally cancel a blocked SQL waiter: no write or caller release before rollback.
    let id = resource_case(&fixture, "malformed-xml");
    sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    let locks = TaskLocks::connect(
        &url,
        TaskLockConfig {
            max_leases: 1,
            max_commands: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks: locks.clone(),
            dispatcher: dispatcher.clone(),
            clock: Arc::new(chrono::Utc::now),
        },
        resource_config(Duration::from_secs(2)),
    )
    .unwrap();
    let (mut monitor, tx) = locked_writer().await;
    let copy = service.clone();
    let owner = Arc::new(Owner(dropped.clone()));
    let pending = tokio::spawn(async move {
        copy.build_report_with_dispatcher(
            IndexRequest {
                source_id: id,
                retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            owner,
            dispatcher,
        )
        .await
    });
    physical_wait(async || waiting_writer(&mut monitor).await).await;
    pending.abort();
    let _ = pending.await;
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    tx.commit().await.unwrap();
    assert!(service.shutdown(Duration::from_secs(3)).await);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM epg_epgsourceindex WHERE source_id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    assert_eq!(locks.available_leases(), 1);
    store.pool().close().await;
}
fn uuid_text() -> String {
    format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
