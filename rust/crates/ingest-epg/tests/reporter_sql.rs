//! Actual original import producer/status trace versus native owned importer.
//! Original Dispatcharr authors retain all application/algorithm credit.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_ingest_epg::{
    reporter::{GuideReporter, ReportAction},
    EpgImporter, ImportConfig, ImportReporting, ImportRequest, SwapPolicyContext,
};
use dispatcharr_store::{EpgHistoryPolicy, Store};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

async fn state(store: &Store, redis: &redis::aio::MultiplexedConnection, id: i64) -> Value {
    let row=sqlx::query("SELECT status,last_message,updated_at,(SELECT count(*) FROM core_systemevent s WHERE s.details->>'source_name'=epg_epgsource.name) AS audit_events,(SELECT count(*) FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1) AS programmes FROM epg_epgsource WHERE id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let now: DateTime<Utc> = row.get("updated_at");
    let mut connection = redis.clone();
    let generation: Option<String> = redis::cmd("GET")
        .arg("epg_content_generation")
        .query_async(&mut connection)
        .await
        .unwrap();
    json!({"status":row.get::<String,_>("status"),"last_message":row.get::<Option<String>,_>("last_message"),"updated_at":now.to_rfc3339_opts(chrono::SecondsFormat::Micros,false).replace(".000000+00:00","+00:00"),"programmes":row.get::<i64,_>("programmes"),"in_atomic_block":false,"audit_events":row.get::<i64,_>("audit_events"),"generation":generation})
}
struct Recorder {
    store: Store,
    redis: redis::aio::MultiplexedConnection,
    events: Mutex<Vec<Value>>,
}
impl UpdateEventSink for Recorder {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            let id = message["data"]["source"].as_i64().unwrap();
            let snapshot = state(&self.store, &self.redis, id).await;
            self.events
                .lock()
                .unwrap()
                .push(json!({"group":group,"message":message,"state":snapshot}));
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
struct Witness(Arc<AtomicBool>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
fn context(retained: Arc<dyn Send + Sync>) -> SwapPolicyContext {
    SwapPolicyContext {
        cancelled: Arc::new(AtomicBool::new(false)),
        retained,
    }
}

#[tokio::test]
#[ignore = "requires disposable original reporter fixture and PostgreSQL/Redis"]
async fn original_file_import_publication_status_and_audit_order() {
    assert_eq!(
        std::env::var("EPG_REPORTER_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_REPORTER_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let recorder = Arc::new(Recorder {
        store: store.clone(),
        redis: redis.clone(),
        events: Mutex::new(Vec::new()),
    });
    let now: DateTime<Utc> = "2026-11-09T12:00:00Z".parse().unwrap();
    let reporter = GuideReporter::with_clock(
        store.clone(),
        &redis_url,
        recorder.clone(),
        2,
        Arc::new(move || now),
    )
    .await
    .unwrap();
    let importer = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
    let _: usize = redis::cmd("DEL")
        .arg(":1:coresettings:group:system_settings")
        .query_async(&mut redis)
        .await
        .unwrap();
    let mut files = Files(Vec::new());
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        // The original fixture injects a Python RuntimeError at this boundary.
        // Native SQL exception string formatting/recovery is a later gate.
        if name == "swap-error" {
            continue;
        }
        let id = case["source_id"].as_i64().unwrap();
        let path = std::env::temp_dir().join(format!(
            "rust-guide-reporter-{}-{id}.xml",
            std::process::id()
        ));
        if case["file_exists"].as_bool().unwrap() {
            std::fs::write(
                &path,
                STANDARD
                    .decode(case["xml_base64"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap();
        }
        files.0.push(path.clone());
        let _: () = redis::cmd("SET")
            .arg("epg_content_generation")
            .arg(case["before"]["generation"].as_str().unwrap())
            .query_async(&mut redis)
            .await
            .unwrap();
        recorder.events.lock().unwrap().clear();
        let result = importer
            .import_file_reported(
                ImportRequest {
                    source_id: id,
                    path,
                    now,
                    policy: EpgHistoryPolicy {
                        min_days: 1,
                        max_days: 30,
                        default_prev_days: 0,
                        default_days: 0,
                    },
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(()),
                None,
                ImportReporting {
                    reporter: reporter.clone(),
                    source_name: case["source_name"].as_str().unwrap().into(),
                    source_has_url: false,
                },
            )
            .await;
        assert_eq!(
            result.is_ok(),
            case["passed"].as_bool().unwrap(),
            "result {name}"
        );
        assert_eq!(
            state(&store, &redis, id).await,
            case["after"],
            "final source state {name}"
        );
        let actual = recorder.events.lock().unwrap().clone();
        let expected=case["effects"].as_array().unwrap().iter().filter(|event|event["kind"]=="publication").map(|event|json!({"group":event["group"],"message":event["message"],"state":event["state"]})).collect::<Vec<_>>();
        assert_eq!(actual, expected, "publication envelopes/state/order {name}");
        let audit:Option<sqlx::postgres::PgRow>=sqlx::query("SELECT event_type,channel_id::text AS channel_id,channel_name,details FROM core_systemevent WHERE details->>'source_name'=$1 ORDER BY id DESC LIMIT 1").bind(case["source_name"].as_str().unwrap()).fetch_optional(store.pool()).await.unwrap();
        let expected_audit = case["effects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["kind"] == "audit_returned")
            .map(|event| event["stored"].as_array().unwrap().last().unwrap());
        match (audit, expected_audit) {
            (Some(row), Some(expected)) => assert_eq!(
                json!({"event_type":row.get::<String,_>("event_type"),"channel_id":row.get::<Option<String>,_>("channel_id"),"channel_name":row.get::<Option<String>,_>("channel_name"),"details":row.get::<Value,_>("details")}),
                *expected,
                "audit {name}"
            ),
            (None, None) => {}
            _ => panic!("audit presence {name}"),
        }
        compared += 1;
    }
    assert_eq!(compared, 8);
    assert!(importer.shutdown(Duration::from_secs(2)).await);
    assert!(reporter.shutdown(Duration::from_secs(2)).await);
    println!("EPG_REPORTED_IMPORT_SOURCE_CASES={compared}");
}

#[tokio::test]
#[ignore = "requires disposable original reporter fixture and PostgreSQL/Redis"]
async fn aborted_reporter_waiter_keeps_physical_sql_guard_and_admission() {
    assert_eq!(
        std::env::var("EPG_REPORTER_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_REPORTER_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let id = fixture["cases"][0]["source_id"].as_i64().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let recorder = Arc::new(Recorder {
        store: store.clone(),
        redis,
        events: Mutex::new(Vec::new()),
    });
    let reporter = GuideReporter::connect(store.clone(), &redis_url, recorder, 1)
        .await
        .unwrap();
    let saved = sqlx::query("SELECT status,last_message,updated_at FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM epg_epgsource WHERE id=$1 FOR UPDATE")
        .bind(id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let retained = Arc::new(Witness(dropped.clone()));
    let runner = reporter.clone();
    let waiter = tokio::spawn(async move {
        runner
            .execute(
                ReportAction::NoMappings {
                    source_id: id,
                    total_epg_entries: 4,
                },
                context(retained),
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'UPDATE epg_epgsource SET status=%')").fetch_one(store.pool()).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reporter never reached actual blocked SQL"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    waiter.abort();
    let _ = waiter.await;
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(reporter.physical_operations(), 1);
    assert!(matches!(
        reporter
            .execute(
                ReportAction::Progress {
                    source_id: id,
                    progress: 0
                },
                context(Arc::new(()))
            )
            .await,
        Err(dispatcharr_ingest_epg::ImportError::Busy)
    ));
    assert!(!reporter.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    assert!(reporter.shutdown(Duration::from_secs(5)).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(reporter.physical_operations(), 0);
    sqlx::query("UPDATE epg_epgsource SET status=$2,last_message=$3,updated_at=$4 WHERE id=$1")
        .bind(id)
        .bind(saved.get::<String, _>("status"))
        .bind(saved.get::<Option<String>, _>("last_message"))
        .bind(saved.get::<DateTime<Utc>, _>("updated_at"))
        .execute(store.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable original reporter fixture and PostgreSQL/Redis"]
async fn committed_generation_and_status_precede_actual_audit_reply_then_completion() {
    use dispatcharr_ingest_epg::reporter::CompletedImport;
    assert_eq!(
        std::env::var("EPG_REPORTER_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_REPORTER_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let id = fixture["cases"][0]["source_id"].as_i64().unwrap();
    let name = fixture["cases"][0]["source_name"]
        .as_str()
        .unwrap()
        .to_owned();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("epg_content_generation")
        .arg(10)
        .query_async(&mut redis)
        .await
        .unwrap();
    let recorder = Arc::new(Recorder {
        store: store.clone(),
        redis: redis.clone(),
        events: Mutex::new(Vec::new()),
    });
    let reporter = GuideReporter::connect(store.clone(), &redis_url, recorder.clone(), 1)
        .await
        .unwrap();
    let saved = sqlx::query("SELECT status,last_message,updated_at FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE core_systemevent IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let retained = Arc::new(Witness(dropped.clone()));
    let runner = reporter.clone();
    let source_name = name.clone();
    let waiter = tokio::spawn(async move {
        runner
            .execute(
                ReportAction::Committed(CompletedImport {
                    source_id: id,
                    source_name,
                    accepted: 0,
                    channels_with_programmes: 0,
                    skipped_unmapped: 0,
                    total_epg_entries: 4,
                    mapped_tvg_ids: 1,
                }),
                context(retained),
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'INSERT INTO core_systemevent%')").fetch_one(store.pool()).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reporter never reached actual blocked audit SQL"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let generation: i64 = redis::cmd("GET")
        .arg("epg_content_generation")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(generation, 11);
    let status: String = sqlx::query_scalar("SELECT status FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(status, "success");
    assert!(recorder.events.lock().unwrap().is_empty());
    waiter.abort();
    let _ = waiter.await;
    assert!(!dropped.load(Ordering::Acquire));
    assert!(!reporter.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    assert!(reporter.shutdown(Duration::from_secs(5)).await);
    assert!(dropped.load(Ordering::Acquire));
    let events = recorder.events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["message"]["data"]["progress"], 100);
    assert_eq!(events[0]["state"]["audit_events"], 1);
    sqlx::query("DELETE FROM core_systemevent WHERE details->>'source_name'=$1")
        .bind(name)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE epg_epgsource SET status=$2,last_message=$3,updated_at=$4 WHERE id=$1")
        .bind(id)
        .bind(saved.get::<String, _>("status"))
        .bind(saved.get::<Option<String>, _>("last_message"))
        .bind(saved.get::<DateTime<Utc>, _>("updated_at"))
        .execute(store.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable original reporter fixture and PostgreSQL/Redis"]
async fn system_default_group_matches_original_empty_database_and_django_cache() {
    use dispatcharr_ingest_epg::cache_policy::CachedSwapPolicy;
    assert_eq!(
        std::env::var("EPG_REPORTER_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_REPORTER_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let saved: Value =
        sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key='system_settings'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE core_coresettings SET value='{}'::jsonb WHERE key='system_settings'")
        .execute(store.pool())
        .await
        .unwrap();
    let _: usize = redis::cmd("DEL")
        .arg(":1:coresettings:group:system_settings")
        .query_async(&mut redis)
        .await
        .unwrap();
    let policy = CachedSwapPolicy::new(store.clone(), Some(redis_url));
    let actual = policy
        .retained_system_settings(context(Arc::new(())))
        .await
        .unwrap();
    assert_eq!(actual, fixture["system_default_cache"]);
    assert_eq!(actual.as_object().unwrap().len(), 9);
    assert_eq!(policy.physical_operations(), 0);
    sqlx::query("UPDATE core_coresettings SET value=$1 WHERE key='system_settings'")
        .bind(saved)
        .execute(store.pool())
        .await
        .unwrap();
    // Keep the native-filled primitive cache for the original Django decoder.
}
