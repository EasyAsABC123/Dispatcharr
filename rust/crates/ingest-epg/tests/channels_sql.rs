//! Original channel pass fields/progress/partial failure versus bounded Rust.
//! Full algorithm and application credit belongs to original Dispatcharr authors.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_ingest_epg::{
    channels::{ChannelConfig, ChannelImporter, ChannelRequest},
    reporter::GuideReporter,
};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    io::Write,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

async fn state(store: &Store, id: i64) -> Value {
    let row=sqlx::query("SELECT status,last_message,updated_at,file_path,(SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1) AS epg_rows,(SELECT count(*) FROM epg_programdata p JOIN epg_epgdata e ON e.id=p.epg_id WHERE e.epg_source_id=$1) AS programmes,(SELECT count(*) FROM core_systemevent WHERE details->>'source_name'=epg_epgsource.name) AS audit_events FROM epg_epgsource WHERE id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let time: Option<DateTime<Utc>> = row.get("updated_at");
    json!({"status":row.get::<String,_>("status"),"last_message":row.get::<Option<String>,_>("last_message"),"updated_at":time.map(|value|value.to_rfc3339_opts(chrono::SecondsFormat::Micros,false).replace(".000000+00:00","+00:00")),"file_path":row.get::<Option<String>,_>("file_path"),"epg_rows":row.get::<i64,_>("epg_rows"),"programmes":row.get::<i64,_>("programmes"),"audit_events":row.get::<i64,_>("audit_events")})
}
struct Recorder {
    store: Store,
    events: Mutex<Vec<Value>>,
}
impl UpdateEventSink for Recorder {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            let snapshot = state(&self.store, message["data"]["source"].as_i64().unwrap()).await;
            self.events.lock().unwrap().push(
                json!({"kind":"publication","group":group,"message":message,"state":snapshot}),
            );
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
fn semantic_rows(rows: &[Value], before: &[Value]) -> Vec<Value> {
    let mut rows = rows.to_vec();
    for row in &mut rows {
        let existing = before.iter().any(|old| old["id"] == row["id"]);
        if !existing {
            row["id"] = json!("new");
        }
    }
    rows.sort_by_key(|row| serde_json::to_string(&row["tvg_id"]).unwrap());
    rows
}
#[tokio::test]
#[ignore = "requires disposable original channel helper fixture and PostgreSQL/Redis"]
async fn original_channel_fields_progress_duplicates_and_stale_cleanup() {
    assert_eq!(
        std::env::var("EPG_CHANNEL_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_CHANNEL_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let recorder = Arc::new(Recorder {
        store: store.clone(),
        events: Mutex::new(Vec::new()),
    });
    let reporter = GuideReporter::connect(
        store.clone(),
        &std::env::var("REDIS_TEST_URL").unwrap(),
        recorder.clone(),
        2,
    )
    .await
    .unwrap();
    let importer = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
    let mut files = Files(Vec::new());
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let id = case["source_id"].as_i64().unwrap();
        let path = PathBuf::from(case["before_state"]["file_path"].as_str().unwrap());
        assert_eq!(path.parent(), Some(std::path::Path::new("/tmp")));
        assert!(path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap()
            .starts_with("rust-guide-channels-"));
        if case["file_exists"].as_bool().unwrap() {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            files.0.push(path.clone());
            file.write_all(
                &STANDARD
                    .decode(case["xml_base64"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap();
        } else {
            assert!(!path.exists());
        }
        recorder.events.lock().unwrap().clear();
        let result = importer
            .import_channels(
                ChannelRequest {
                    source_id: id,
                    source_name: format!("rust-guide-channels-{name}"),
                    path,
                    cache_file: PathBuf::from(case["after_state"]["file_path"].as_str().unwrap()),
                    source_has_url: false,
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(()),
                Some(reporter.clone()),
            )
            .await;
        assert_eq!(
            result.is_ok(),
            case["passed"].as_bool().unwrap(),
            "result {name}: {result:?}"
        );
        assert_eq!(
            state(&store, id).await,
            case["after_state"],
            "final state {name}"
        );
        let raw=sqlx::query("SELECT id::bigint AS id,tvg_id,name,icon_url FROM epg_epgdata WHERE epg_source_id=$1 ORDER BY id").bind(id).fetch_all(store.pool()).await.unwrap();
        let actual=raw.iter().map(|row|json!({"id":row.get::<i64,_>("id"),"tvg_id":row.get::<Option<String>,_>("tvg_id"),"name":row.get::<String,_>("name"),"icon_url":row.get::<Option<String>,_>("icon_url")})).collect::<Vec<_>>();
        assert_eq!(
            semantic_rows(&actual, case["before"].as_array().unwrap()),
            semantic_rows(
                case["after"].as_array().unwrap(),
                case["before"].as_array().unwrap()
            ),
            "rows {name}"
        );
        let expected = case["effects"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|value| value["kind"] == "publication")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            *recorder.events.lock().unwrap(),
            expected,
            "publication/data-state order {name}"
        );
        let base: Option<i64> = sqlx::query_scalar(
            "SELECT epg_data_id::bigint FROM dispatcharr_channels_channel WHERE id=$1",
        )
        .bind(case["base_id"].as_i64().unwrap())
        .fetch_one(store.pool())
        .await
        .unwrap();
        let alternate: Option<i64> = sqlx::query_scalar(
            "SELECT epg_data_id::bigint FROM dispatcharr_channels_channeloverride WHERE id=$1",
        )
        .bind(case["override_id"].as_i64().unwrap())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(json!(base), case["base_epg_id"]);
        assert_eq!(json!(alternate), case["override_epg_id"]);
        compared += 1;
    }
    assert_eq!(compared, 19);
    assert!(importer.shutdown(Duration::from_secs(2)).await);
    assert!(reporter.shutdown(Duration::from_secs(2)).await);
    println!("EPG_CHANNEL_SOURCE_CASES={compared}");
}

struct Witness(Arc<AtomicBool>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
fn resource_file(count: usize) -> (PathBuf, Files) {
    let path = std::env::temp_dir().join(format!(
        "rust-channel-physical-{}-{count}.xml",
        std::process::id()
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let files = Files(vec![path.clone()]);
    write!(file, "<tv>").unwrap();
    let name = if count >= 2000 {
        "🦀".repeat(512)
    } else {
        "Resource".into()
    };
    let icon = if count >= 2000 {
        format!("<icon src=\"{}\"/>", "é".repeat(500))
    } else {
        String::new()
    };
    for index in 0..count {
        write!(
            file,
            "<channel id=\"physical-{index}\"><display-name>{name}</display-name>{icon}</channel>"
        )
        .unwrap();
    }
    write!(file, "</tv>").unwrap();
    (path, files)
}
#[tokio::test]
#[ignore = "requires disposable original channel fixture and PostgreSQL"]
async fn physical_sql_abort_retains_admission_then_joins_blocked_parser() {
    use std::sync::atomic::Ordering;
    assert_eq!(
        std::env::var("EPG_CHANNEL_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_CHANNEL_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let id = fixture["cases"][0]["source_id"].as_i64().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let importer = ChannelImporter::new(
        store.clone(),
        ChannelConfig {
            max_jobs: 1,
            queue_frames: 128,
            queue_bytes: 65536,
            ..ChannelConfig::default()
        },
    )
    .unwrap();
    let (path, _files) = resource_file(2000);
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgdata IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let retained = Arc::new(Witness(dropped.clone()));
    let runner = importer.clone();
    let input = path.clone();
    let waiter = tokio::spawn(async move {
        runner
            .import_channels(
                ChannelRequest {
                    source_id: id,
                    source_name: "channel resource".into(),
                    path: input,
                    cache_file: PathBuf::from("/tmp/unused-channel-cache.tmp"),
                    source_has_url: false,
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                retained,
                None,
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'INSERT INTO epg_epgdata(tvg_id,%')").fetch_one(store.pool()).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "channel worker never reached actual blocked SQL"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let byte_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while importer.retained_frame_bytes() < 60_000 {
        assert!(
            tokio::time::Instant::now() < byte_deadline,
            "parser did not fill byte credits before count-bound queue"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(importer.retained_frame_bytes() <= 65536);
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(importer.physical_jobs(), 1);
    assert!(!dropped.load(Ordering::Acquire));
    let busy = importer
        .import_channels(
            ChannelRequest {
                source_id: id,
                source_name: "channel resource".into(),
                path,
                cache_file: PathBuf::from("/tmp/unused-channel-cache.tmp"),
                source_has_url: false,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(()),
            None,
        )
        .await;
    assert!(matches!(
        busy,
        Err(dispatcharr_ingest_epg::channels::ChannelError::Import(
            dispatcharr_ingest_epg::ImportError::Busy
        ))
    ));
    assert!(!importer.shutdown(Duration::from_millis(30)).await);
    assert!(!dropped.load(Ordering::Acquire));
    blocker.rollback().await.unwrap();
    assert!(importer.shutdown(Duration::from_secs(5)).await);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(importer.physical_jobs(), 0);
    assert_eq!(importer.retained_frame_bytes(), 0);
    let created: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1 AND tvg_id LIKE 'physical-%'",
    )
    .bind(id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        created, 500,
        "in-flight autocommit batch completes; queued parser rows never flush"
    );
}
#[tokio::test]
#[ignore = "requires disposable original channel fixture and PostgreSQL"]
async fn snapshot_capacity_failure_releases_guard_without_creating_rows() {
    use std::sync::atomic::Ordering;
    assert_eq!(
        std::env::var("EPG_CHANNEL_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_CHANNEL_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let id = fixture["cases"][0]["source_id"].as_i64().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let importer = ChannelImporter::new(
        store.clone(),
        ChannelConfig {
            max_keys: 1,
            ..ChannelConfig::default()
        },
    )
    .unwrap();
    let (path, _files) = resource_file(1);
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let result = importer
        .import_channels(
            ChannelRequest {
                source_id: id,
                source_name: "channel capacity".into(),
                path,
                cache_file: PathBuf::from("/tmp/unused-channel-cache.tmp"),
                source_has_url: false,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(Witness(dropped.clone())),
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(dispatcharr_ingest_epg::channels::ChannelError::Final(
            dispatcharr_ingest_epg::ImportError::Capacity(_)
        ))
    ));
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(importer.physical_jobs(), 0);
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM epg_epgdata WHERE epg_source_id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(before, after);
    assert!(importer.shutdown(Duration::from_secs(1)).await);
}
