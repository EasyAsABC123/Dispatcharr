//! Actual original local-file refresh effects versus bounded owned orchestration.
//! Full application/algorithm credit belongs to original Dispatcharr authors.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    cache_policy::{CachedSwapPolicy, PolicyClock},
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
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};
async fn state(
    store: &Store,
    redis: &mut redis::aio::MultiplexedConnection,
    id: i64,
    name: &str,
) -> Value {
    let source:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('status',status,'last_message',last_message,'updated_at',updated_at,'file_path',file_path,'extracted_file_path',extracted_file_path) FROM epg_epgsource WHERE id=$1").bind(id).fetch_optional(store.pool()).await.unwrap();
    let epg:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('id',id,'tvg_id',tvg_id,'name',name,'icon_url',icon_url) ORDER BY id),'[]') FROM epg_epgdata WHERE epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let programmes:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('epg_id',p.epg_id,'title',p.title,'start_time',p.start_time,'end_time',p.end_time) ORDER BY p.id),'[]') FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let index: Option<Value> =
        sqlx::query_scalar("SELECT data FROM epg_epgsourceindex WHERE source_id=$1")
            .bind(id)
            .fetch_optional(store.pool())
            .await
            .unwrap()
            .flatten();
    let base_logo_urls:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(l.url ORDER BY c.id),'[]') FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON c.epg_data_id=e.id LEFT JOIN dispatcharr_channels_logo l ON c.logo_id=l.id WHERE e.epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let generation: Option<String> = redis::cmd("GET")
        .arg("epg_content_generation")
        .query_async(redis)
        .await
        .unwrap();
    let refresh: bool = redis::cmd("EXISTS")
        .arg(format!("task_lock_refresh_epg_data_{id}"))
        .query_async(redis)
        .await
        .unwrap();
    let file: bool = redis::cmd("EXISTS")
        .arg(format!("task_lock_epg_source_file_{id}"))
        .query_async(redis)
        .await
        .unwrap();
    let events:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('event_type',event_type,'details',details) ORDER BY id),'[]') FROM core_systemevent WHERE details->>'source_name'=$1").bind(name).fetch_one(store.pool()).await.unwrap();
    json!({"source":source,"epg_rows":epg,"programmes":programmes,"index":index,"generation":generation,"refresh_held":refresh,"file_held":file,"audit_events":events,"base_logo_urls":base_logo_urls})
}
struct Recorder {
    store: Store,
    redis: redis::aio::MultiplexedConnection,
    id: i64,
    name: String,
    effects: Mutex<Vec<Value>>,
}
impl Recorder {
    async fn record(&self, kind: &str, mut fields: Value) {
        fields["kind"] = json!(kind);
        fields["state"] = state(&self.store, &mut self.redis.clone(), self.id, &self.name).await;
        self.effects.lock().unwrap().push(fields);
    }
}
impl UpdateEventSink for Recorder {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.record("publication", json!({"group":group,"message":message}))
                .await;
            Ok(())
        })
    }
}
impl GuideTaskDispatcher for Recorder {
    fn dispatch<'a>(
        &'a self,
        task: ChildTask,
        context: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            match task{
            ChildTask::ProgrammeIndex{source_id}=>self.record("index_dispatch",json!({"args":[source_id],"kwargs":{}})).await,
            ChildTask::SeriesRules=>self.record("series_dispatch",json!({"args":[],"kwargs":{}})).await,
            ChildTask::RefreshDeferred{source_id,force,retry,delay_seconds}=>self.record("refresh_defer_dispatch",json!({"args":[],"kwargs":{"args":[source_id],"kwargs":{"force":force,"_file_defer_retry":retry},"countdown":delay_seconds}})).await,
            ChildTask::LateProgrammes{source_id,initial_epg_ids}=>{
                // Actual Source helper dispatches nothing if no newly mapped
                // IDs. This fixed corpus has no concurrent mapping mutation;
                // late-child execution is deliberately a separate gate.
                let missed:i64=sqlx::query_scalar("SELECT count(*) FROM epg_epgdata e WHERE e.epg_source_id=$1 AND e.id<>ALL($2) AND (EXISTS(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.epg_data_id=e.id) OR EXISTS(SELECT 1 FROM dispatcharr_channels_channeloverride o WHERE o.epg_data_id=e.id))").bind(source_id).bind(initial_epg_ids).fetch_one(self.store.pool()).await?;
                assert_eq!(missed,0);
            }
        }
            Ok(())
        })
    }
}
struct FileGuard(PathBuf);
impl Drop for FileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
#[tokio::test]
#[ignore = "requires isolated original local-file job fixture and PostgreSQL/Redis"]
async fn original_local_refresh_progress_lock_and_dispatch_order() {
    assert_eq!(
        std::env::var("EPG_FILE_JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_FILE_JOB_FIXTURE").unwrap()).unwrap(),
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
        let name = case["name"].as_str().unwrap();
        let id = case["source_id"].as_i64().unwrap();
        let source_name = case["source_name"].as_str().unwrap();
        let before = &case["before"];
        let source = &before["source"];
        if !source.is_null() {
            sqlx::query("UPDATE epg_epgsource SET status=$2,last_message=$3,updated_at=$4,file_path=$5,extracted_file_path=$6 WHERE id=$1").bind(id).bind(source["status"].as_str().unwrap()).bind(source["last_message"].as_str()).bind(source["updated_at"].as_str().map(|value|value.parse::<DateTime<Utc>>().unwrap())).bind(source["file_path"].as_str()).bind(source["extracted_file_path"].as_str()).execute(store.pool()).await.unwrap();
            for epg in case["before_epg_rows"].as_array().unwrap() {
                sqlx::query("UPDATE epg_epgdata SET name=$2,icon_url=$3 WHERE id=$1")
                    .bind(epg["id"].as_i64().unwrap())
                    .bind(epg["name"].as_str().unwrap())
                    .bind(epg["icon_url"].as_str())
                    .execute(store.pool())
                    .await
                    .unwrap();
            }
            sqlx::query("DELETE FROM epg_programdata p USING epg_epgdata e WHERE p.epg_id=e.id AND e.epg_source_id=$1").bind(id).execute(store.pool()).await.unwrap();
            sqlx::query("INSERT INTO epg_programdata SELECT * FROM json_populate_recordset(NULL::epg_programdata,$1::json)").bind(sqlx::types::Json(case["before_programmes"].clone())).execute(store.pool()).await.unwrap();
            sqlx::query("UPDATE epg_epgsourceindex SET data=$2 WHERE source_id=$1")
                .bind(id)
                .bind(sqlx::types::Json(before["index"].clone()))
                .execute(store.pool())
                .await
                .unwrap();
        }
        for channel in case["before_channels"].as_array().unwrap() {
            sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=$2 WHERE id=$1")
                .bind(channel["id"].as_i64().unwrap())
                .bind(channel["logo_id"].as_i64())
                .execute(store.pool())
                .await
                .unwrap();
        }
        sqlx::query("DELETE FROM core_systemevent WHERE details->>'source_name'=$1")
            .bind(source_name)
            .execute(store.pool())
            .await
            .unwrap();
        let generation: Option<&str> = before["generation"].as_str();
        if let Some(value) = generation {
            let _: String = redis::cmd("SET")
                .arg("epg_content_generation")
                .arg(value)
                .query_async(&mut redis)
                .await
                .unwrap();
        } else {
            let _: i64 = redis::cmd("DEL")
                .arg("epg_content_generation")
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let refresh = format!("task_lock_refresh_epg_data_{id}");
        let file_key = format!("task_lock_epg_source_file_{id}");
        let counter = format!("epg_source_tvg_parse_{id}");
        for (key, active) in [
            (&refresh, before["refresh_held"] == true),
            (&file_key, before["file_held"] == true),
        ] {
            if active {
                let _: String = redis::cmd("SET")
                    .arg(key)
                    .arg("locked")
                    .arg("EX")
                    .arg(300)
                    .query_async(&mut redis)
                    .await
                    .unwrap();
            }
        }
        if matches!(name, "per-channel-active" | "bad-counter") {
            let _: String = redis::cmd("SET")
                .arg(&counter)
                .arg(if name == "bad-counter" {
                    "not-an-integer"
                } else {
                    "1"
                })
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let path = PathBuf::from(case["file_path"].as_str().unwrap());
        assert_eq!(path.parent(), Some(std::path::Path::new("/tmp")));
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("rust-epg-file-job-"));
        assert!(!path.exists());
        let _file = FileGuard(path.clone());
        if name != "missing-no-url" {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            std::fs::write(
                &path,
                STANDARD
                    .decode(case["file_base64"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap();
        }
        let recorder = Arc::new(Recorder {
            store: store.clone(),
            redis: redis.clone(),
            id,
            name: source_name.into(),
            effects: Mutex::new(Vec::new()),
        });
        let now = case["now"]
            .as_str()
            .unwrap()
            .parse::<DateTime<Utc>>()
            .unwrap();
        let clock: PolicyClock = Arc::new(move || now);
        let reporter = GuideReporter::with_clock(
            store.clone(),
            &redis_url,
            recorder.clone(),
            2,
            clock.clone(),
        )
        .await
        .unwrap();
        let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
        let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
        let runner = FileJobRunner::connect(
            FileJobServices {
                store: store.clone(),
                locks: TaskLocks::connect(&redis_url, TaskLockConfig::default())
                    .await
                    .unwrap(),
                reporter: reporter.clone(),
                channels: channels.clone(),
                programmes: programmes.clone(),
                policy: Arc::new(CachedSwapPolicy::with_clock(
                    store.clone(),
                    Some(redis_url.clone()),
                    clock.clone(),
                )),
                dispatcher: recorder.clone(),
                clock,
            },
            FileJobConfig {
                max_jobs: 1,
                cache_dir: PathBuf::from(fixture["cache_dir"].as_str().unwrap()),
                release_timeout: Duration::from_secs(5),
            },
            &redis_url,
        )
        .await
        .unwrap();
        let result = runner
            .run(
                FileJobRequest {
                    source_id: id,
                    force: true,
                    defer_retry: if name == "defer-exhausted" { 480 } else { 0 },
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(()),
            )
            .await;
        assert!(result.is_ok(), "outcome {name}: {result:?}");
        assert!(
            !matches!(result, Ok(FileJobOutcome::Unsupported(_))),
            "unaccepted outcome {name}"
        );
        assert!(runner.shutdown(Duration::from_secs(5)).await);
        assert!(channels.shutdown(Duration::from_secs(5)).await);
        assert!(programmes.shutdown(Duration::from_secs(5)).await);
        assert!(reporter.shutdown(Duration::from_secs(5)).await);
        assert_eq!(
            state(&store, &mut redis, id, source_name).await,
            case["after"],
            "final state {name}"
        );
        assert_eq!(
            json!(*recorder.effects.lock().unwrap()),
            case["effects"],
            "event ordering {name}"
        );
        let _: i64 = redis::cmd("DEL")
            .arg(&refresh)
            .arg(&file_key)
            .arg(&counter)
            .query_async(&mut redis)
            .await
            .unwrap();
        if name == "local-gzip" {
            let extracted = PathBuf::from(
                case["after"]["source"]["extracted_file_path"]
                    .as_str()
                    .unwrap(),
            );
            assert!(extracted.starts_with(fixture["cache_dir"].as_str().unwrap()));
            std::fs::remove_file(extracted).unwrap();
            let _ = std::fs::remove_dir(fixture["cache_dir"].as_str().unwrap());
        }
        compared += 1;
    }
    assert_eq!(compared, 17);
    println!("EPG_FILE_JOB_SOURCE_CASES={compared}");
}

struct Witness(Arc<AtomicBool>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
struct PauseSink {
    recorder: Arc<Recorder>,
    paused: Arc<AtomicBool>,
}
impl UpdateEventSink for PauseSink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.recorder.send(group, message.clone()).await?;
            if message["data"]["action"] == "parsing_programs"
                && message["data"]["progress"] == 100
                && message["data"]["status"] == "success"
            {
                let _: String = redis::cmd("CLIENT")
                    .arg("PAUSE")
                    .arg(1500)
                    .arg("WRITE")
                    .query_async(&mut self.recorder.redis.clone())
                    .await
                    .unwrap();
                self.paused
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            Ok(())
        })
    }
}
async fn await_flag(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !flag.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "requires isolated original local-file job fixture and physical PostgreSQL/Redis"]
async fn cancellation_holds_physical_sql_and_delayed_file_release() {
    assert_eq!(
        std::env::var("EPG_FILE_JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_FILE_JOB_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let case = &fixture["cases"][0];
    let id = case["source_id"].as_i64().unwrap();
    let name = case["source_name"].as_str().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let path = std::env::temp_dir().join(format!(
        "rust-epg-file-job-physical-{}.xml",
        std::process::id()
    ));
    let _file = FileGuard(path.clone());
    assert!(!path.exists());
    std::fs::write(
        &path,
        STANDARD
            .decode(case["xml_base64"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    sqlx::query("UPDATE epg_epgsource SET file_path=$2,extracted_file_path=NULL,status='success' WHERE id=$1").bind(id).bind(path.to_str().unwrap()).execute(store.pool()).await.unwrap();
    let now = case["now"]
        .as_str()
        .unwrap()
        .parse::<DateTime<Utc>>()
        .unwrap();
    let clock: PolicyClock = Arc::new(move || now);
    let recorder = Arc::new(Recorder {
        store: store.clone(),
        redis: redis.clone(),
        id,
        name: name.into(),
        effects: Mutex::new(Vec::new()),
    });
    let paused = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(PauseSink {
        recorder: recorder.clone(),
        paused: paused.clone(),
    });
    let reporter = GuideReporter::with_clock(store.clone(), &redis_url, sink, 2, clock.clone())
        .await
        .unwrap();
    let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let locks = TaskLocks::connect(&redis_url, TaskLockConfig::default())
        .await
        .unwrap();
    let services = FileJobServices {
        store: store.clone(),
        locks: locks.clone(),
        reporter: reporter.clone(),
        channels: channels.clone(),
        programmes: programmes.clone(),
        policy: Arc::new(CachedSwapPolicy::with_clock(
            store.clone(),
            Some(redis_url.clone()),
            clock.clone(),
        )),
        dispatcher: recorder.clone(),
        clock,
    };
    let config = FileJobConfig {
        max_jobs: 1,
        cache_dir: PathBuf::from("/tmp/rust-epg-file-job-cache"),
        release_timeout: Duration::from_secs(5),
    };
    let refresh = format!("task_lock_refresh_epg_data_{id}");
    let file_key = format!("task_lock_epg_source_file_{id}");
    // Invalidate is physically blocked: caller cancellation cannot release the
    // refresh lease/slot or send work into a new parser/programme transaction.
    let runner = FileJobRunner::connect(services.clone(), config.clone(), &redis_url)
        .await
        .unwrap();
    let before:i64=sqlx::query_scalar("SELECT count(*) FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let call = runner.clone();
    let witness = dropped.clone();
    let task = tokio::spawn(async move {
        call.run(
            FileJobRequest {
                source_id: id,
                force: false,
                defer_retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(Witness(witness)),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10),async{loop{let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO epg_epgsourceindex%')").fetch_one(store.pool()).await.unwrap();if blocked{break;}tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(!runner.shutdown(Duration::from_millis(30)).await);
    assert_eq!(runner.physical_jobs(), 1);
    assert!(!dropped.load(std::sync::atomic::Ordering::Acquire));
    let held: bool = redis::cmd("EXISTS")
        .arg(&refresh)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(held);
    blocker.rollback().await.unwrap();
    await_flag(&dropped).await;
    assert!(runner.shutdown(Duration::from_secs(2)).await);
    let after:i64=sqlx::query_scalar("SELECT count(*) FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    assert_eq!(before, after);
    assert!(recorder.effects.lock().unwrap().is_empty());
    let held: bool = redis::cmd("EXISTS")
        .arg(&refresh)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(!held);
    // Redis serves reads while WRITE-paused. Both lease keys and the request
    // guard must remain until the actual owner-checked DEL reply completes.
    let runner = FileJobRunner::connect(services.clone(), config.clone(), &redis_url)
        .await
        .unwrap();
    let call = runner.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let witness = dropped.clone();
    let task = tokio::spawn(async move {
        call.run(
            FileJobRequest {
                source_id: id,
                force: true,
                defer_retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(Witness(witness)),
        )
        .await
    });
    await_flag(&paused).await;
    for key in [&refresh, &file_key] {
        let held: bool = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(held, "lease released before physical Redis reply");
    }
    assert_eq!(runner.physical_jobs(), 1);
    assert!(!dropped.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        runner
            .run(
                FileJobRequest {
                    source_id: id,
                    force: true,
                    defer_retry: 0,
                    cancelled: Arc::new(AtomicBool::new(false))
                },
                Arc::new(())
            )
            .await,
        Err(ImportError::Busy)
    ));
    assert!(!runner.shutdown(Duration::from_millis(30)).await);
    let result = task.await.unwrap();
    assert!(
        matches!(result, Err(ImportError::Cancelled)),
        "shutdown must prevent index dispatch: {result:?}"
    );
    await_flag(&dropped).await;
    assert!(runner.shutdown(Duration::from_secs(2)).await);
    assert_eq!(locks.available_leases(), 4);
    assert!(recorder
        .effects
        .lock()
        .unwrap()
        .iter()
        .all(|value| value["kind"] == "publication"));
    // Replace the file owner immediately before release, without waiting for a
    // renewal tick. The actual DEL=0 must suppress index/series dispatch while
    // leaving the replacement key and its TTL untouched.
    recorder.effects.lock().unwrap().clear();
    let replace_reporter = GuideReporter::with_clock(
        store.clone(),
        &redis_url,
        Arc::new(ReplaceSink {
            recorder: recorder.clone(),
        }),
        2,
        services.clock.clone(),
    )
    .await
    .unwrap();
    let runner = FileJobRunner::connect(
        FileJobServices {
            reporter: replace_reporter.clone(),
            ..services
        },
        config,
        &redis_url,
    )
    .await
    .unwrap();
    let result = runner
        .run(
            FileJobRequest {
                source_id: id,
                force: true,
                defer_retry: 0,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(()),
        )
        .await;
    assert!(
        matches!(result, Err(ImportError::Worker)),
        "lost file owner must fail closed: {result:?}"
    );
    let owner: String = redis::cmd("GET")
        .arg(&file_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(owner, "synthetic-successor");
    let ttl: i64 = redis::cmd("TTL")
        .arg(&file_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!((25..=30).contains(&ttl));
    assert!(recorder
        .effects
        .lock()
        .unwrap()
        .iter()
        .all(|value| value["kind"] == "publication"));
    assert!(runner.shutdown(Duration::from_secs(2)).await);
    assert!(replace_reporter.shutdown(Duration::from_secs(2)).await);
    let _: i64 = redis::cmd("DEL")
        .arg(&file_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    let task_name = format!("rust_file_job_release_owned_{}", std::process::id());
    let lease = locks.acquire(&task_name, 1).await.unwrap();
    let key = lease.key().to_string();
    let _: String = redis::cmd("SET")
        .arg(&key)
        .arg("successor")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(!lease.release_owned().await.unwrap());
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    let lease = locks.acquire(&task_name, 1).await.unwrap();
    assert!(lease.release_owned().await.unwrap());
    let lease = locks.acquire(&task_name, 1).await.unwrap();
    let _: String = redis::cmd("SET")
        .arg(&key)
        .arg("legacy-successor")
        .query_async(&mut redis)
        .await
        .unwrap();
    lease.release().await.unwrap();
    let owner: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(owner, "legacy-successor");
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(channels.shutdown(Duration::from_secs(2)).await);
    assert!(programmes.shutdown(Duration::from_secs(2)).await);
    assert!(reporter.shutdown(Duration::from_secs(2)).await);
    for key in [&refresh, &file_key] {
        let held: bool = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(!held);
    }
    println!("EPG_FILE_JOB_PHYSICAL_SCENARIOS=3");
}

struct ReplaceSink {
    recorder: Arc<Recorder>,
}
impl UpdateEventSink for ReplaceSink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.recorder.send(group, message.clone()).await?;
            if message["data"]["action"] == "parsing_programs"
                && message["data"]["progress"] == 100
                && message["data"]["status"] == "success"
            {
                let _: String = redis::cmd("SET")
                    .arg(format!("task_lock_epg_source_file_{}", self.recorder.id))
                    .arg("synthetic-successor")
                    .arg("EX")
                    .arg(30)
                    .query_async(&mut self.recorder.redis.clone())
                    .await
                    .unwrap();
            }
            Ok(())
        })
    }
}
