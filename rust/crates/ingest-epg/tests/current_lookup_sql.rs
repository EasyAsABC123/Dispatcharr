//! Actual original lookup corpus vs bounded native SQL/file orchestration.
//! Original Dispatcharr authors retain all application and algorithm credit.
//! Test publishers only: no durable broker or application Parsing acceptance.
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_ingest_epg::{
    current_lookup::{
        CurrentLookup, CurrentLookupConfig, CurrentLookupServices, MissingIndexDispatcher,
    },
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::{
    CurrentFileLookup, CurrentFileOutcome, CurrentFileRequest, Store, StoreError,
};
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

struct Publisher {
    calls: Mutex<Vec<i64>>,
}
impl MissingIndexDispatcher for Publisher {
    fn dispatch(
        &self,
        id: i64,
        context: SwapPolicyContext,
    ) -> BoxFuture<'_, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            self.calls.lock().unwrap().push(id);
            Ok(())
        })
    }
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn fixture() -> Value {
    assert_eq!(
        std::env::var("EPG_CURRENT_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    serde_json::from_slice(&std::fs::read(std::env::var("EPG_CURRENT_FIXTURE").unwrap()).unwrap())
        .unwrap()
}
fn guard_path(path: &Path) {
    assert!(
        path.is_absolute()
            && path.starts_with("/tmp")
            && path.to_str().unwrap().contains("owned-current-lookup-")
    );
}
fn request(case: &Value) -> CurrentFileRequest {
    CurrentFileRequest {
        epg_entry_id: case["epg_entry_id"].as_i64().unwrap(),
        source_id: case["source_id"].as_i64().unwrap(),
        source_type: case["source_type"].as_str().unwrap().to_string(),
        tvg_id: case["tvg_id"].as_str().unwrap_or("").to_string(),
        file_path: case["file_path"].as_str().map(PathBuf::from),
        extracted_file_path: case["extracted_file_path"].as_str().map(PathBuf::from),
        cache_file_path: case["cache_file_path"].as_str().unwrap().into(),
    }
}
async fn prepare(store: &Store, case: &Value) {
    let id = case["source_id"].as_i64().unwrap();
    let name: String = sqlx::query("SELECT name FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap()
        .get("name");
    assert!(name.starts_with("rust-epg-current-lookup-"));
    sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    match case["index_mode"].as_str().unwrap() {
        "missing" => {}
        "sql_null" => {
            sqlx::query(
                "INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,NULL,now())",
            )
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        }
        mode => {
            let data = if mode == "json_null" {
                Value::Null
            } else {
                case["initial_index"].clone()
            };
            sqlx::query(
                "INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,$2,now())",
            )
            .bind(id)
            .bind(sqlx::types::Json(data))
            .execute(store.pool())
            .await
            .unwrap();
        }
    }
    let req = request(case);
    let path = req
        .extracted_file_path
        .clone()
        .or_else(|| req.file_path.clone())
        .unwrap_or(req.cache_file_path.clone());
    guard_path(&path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let data = STANDARD
        .decode(case["file_bytes_base64"].as_str().unwrap())
        .unwrap();
    if case["spec"]["missing_file"].as_bool().unwrap_or(false) {
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
    } else if case["spec"]["extracted"].as_bool().unwrap_or(false) {
        let second = memchr::memmem::find_iter(&data, b"<programme")
            .nth(1)
            .unwrap();
        std::fs::write(&path, &data[second..]).unwrap();
        let original = req.file_path.unwrap();
        guard_path(&original);
        std::fs::write(original, &data[..second]).unwrap();
    } else {
        std::fs::write(path, data).unwrap();
    }
}
fn config() -> CurrentLookupConfig {
    CurrentLookupConfig {
        max_jobs: 1,
        ..CurrentLookupConfig::default()
    }
}
fn clock(case: &Value, calls: Arc<AtomicUsize>) -> Arc<dyn Fn() -> DateTime<Utc> + Send + Sync> {
    let now = case["now"]
        .as_str()
        .unwrap()
        .parse::<DateTime<Utc>>()
        .unwrap();
    let late = case["spec"]["late"].as_bool().unwrap_or(false);
    let action = case["spec"]["clock_action"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let id = case["source_id"].as_i64().unwrap();
    let data = STANDARD
        .decode(case["file_bytes_base64"].as_str().unwrap())
        .unwrap();
    let second = memchr::memmem::find_iter(&data, b"<programme")
        .nth(1)
        .unwrap();
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    Arc::new(move || {
        calls.fetch_add(1, Ordering::SeqCst);
        if !action.is_empty() {
            let action = action.clone();
            let url = url.clone();
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let mut connection = sqlx::PgConnection::connect(&url).await.unwrap();
                        if action == "delete" {
                            sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
                                .bind(id)
                                .execute(&mut connection)
                                .await
                                .unwrap();
                        } else {
                            sqlx::query("UPDATE epg_epgsourceindex SET data=$2 WHERE source_id=$1")
                                .bind(id)
                                .bind(sqlx::types::Json(json!({"channels":{"one":[second]}})))
                                .execute(&mut connection)
                                .await
                                .unwrap();
                        }
                        connection.close().await.unwrap();
                    });
            })
            .join()
            .unwrap();
        }
        if late {
            now + chrono::Duration::hours(1)
        } else {
            now
        }
    })
}
async fn until(mut check: impl FnMut() -> bool) {
    for _ in 0..300 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("physical operation deadline");
}
#[tokio::test]
#[ignore = "requires disposable original current lookup fixture and PostgreSQL"]
async fn original_lookup_guards_fresh_clock_and_index_with_explicit_shape_boundaries() {
    let fixture = fixture();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let mut compared = 0;
    let mut unsupported = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if matches!(name, "missing-epg" | "no-source") {
            continue;
        }
        prepare(&store, case).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let publisher = Arc::new(Publisher {
            calls: Mutex::new(Vec::new()),
        });
        let gateway = CurrentLookup::new(
            CurrentLookupServices {
                store: store.clone(),
                dispatcher: publisher.clone(),
                clock: clock(case, calls.clone()),
            },
            config(),
        )
        .unwrap();
        let result = CurrentFileLookup::lookup(&gateway, request(case), Arc::new(())).await;
        if !case["failure"].is_null() {
            assert!(
                matches!(result, Err(StoreError::Unsupported(_))),
                "{name}: {result:?}"
            );
            unsupported += 1;
        } else {
            let actual = match result.unwrap() {
                CurrentFileOutcome::Found(bytes) => {
                    serde_json::from_slice::<Value>(&bytes).unwrap()
                }
                CurrentFileOutcome::Missing => Value::Null,
                CurrentFileOutcome::Parsing => json!("timeout"),
            };
            assert_eq!(actual, case["result"], "{name}");
            compared += 1;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            case["calls"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| *v == "clock")
                .count(),
            "{name}"
        );
        assert_eq!(
            *publisher.calls.lock().unwrap(),
            case["dispatch"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect::<Vec<_>>(),
            "{name}"
        );
        assert!(gateway.shutdown(Duration::from_secs(3)).await);
        assert_eq!(gateway.retained_memory_bytes(), 0);
    }
    assert_eq!(compared, 17);
    assert_eq!(unsupported, 6);
    println!("17 original Source outcomes;6 explicit unsupported malformed shapes;2 metadata-only skips outside callback; no durable dispatch claim");
    store.pool().close().await;
}
#[tokio::test]
#[ignore = "requires disposable original current lookup fixture and PostgreSQL"]
async fn blocked_sql_cancellation_retains_caller_and_physical_admission() {
    let fixture = fixture();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "valid-inactive")
        .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    prepare(&store, case).await;
    let gateway = CurrentLookup::new(
        CurrentLookupServices {
            store: store.clone(),
            dispatcher: Arc::new(Publisher {
                calls: Mutex::new(Vec::new()),
            }),
            clock: clock(case, Arc::new(AtomicUsize::new(0))),
        },
        config(),
    )
    .unwrap();
    let mut blocker = store.pool().acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *blocker).await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let caller = Arc::new(DropCount(dropped.clone()));
    let task_gateway = gateway.clone();
    let req = request(case);
    let task =
        tokio::spawn(async move { CurrentFileLookup::lookup(&task_gateway, req, caller).await });
    let mut waiting = false;
    for _ in 0..300 {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT jsonb_typeof(data)%' AND wait_event_type='Lock'").fetch_one(store.pool()).await.unwrap();
        if count > 0 {
            waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(waiting);
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(gateway.physical_jobs(), 1);
    assert!(gateway.retained_memory_bytes() > 0);
    assert!(matches!(
        CurrentFileLookup::lookup(&gateway, request(case), Arc::new(())).await,
        Err(StoreError::StatusBody(503, _))
    ));
    assert!(!gateway.shutdown(Duration::from_millis(20)).await);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    drop(blocker);
    until(|| gateway.physical_jobs() == 0 && dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(gateway.retained_memory_bytes(), 0);
    assert!(gateway.shutdown(Duration::from_secs(1)).await);
    store.pool().close().await;
}
#[tokio::test]
#[ignore = "requires disposable original current lookup fixture and PostgreSQL"]
async fn final_byte_clones_retain_allocation_and_configured_response_cap_is_explicit() {
    let fixture = fixture();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "valid-inactive")
        .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    prepare(&store, case).await;
    let gateway = CurrentLookup::new(
        CurrentLookupServices {
            store: store.clone(),
            dispatcher: Arc::new(Publisher {
                calls: Mutex::new(Vec::new()),
            }),
            clock: clock(case, Arc::new(AtomicUsize::new(0))),
        },
        config(),
    )
    .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let CurrentFileOutcome::Found(bytes) = CurrentFileLookup::lookup(
        &gateway,
        request(case),
        Arc::new(DropCount(dropped.clone())),
    )
    .await
    .unwrap() else {
        panic!("expected file DTO")
    };
    until(|| gateway.physical_jobs() == 0).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let retained = gateway.retained_memory_bytes();
    assert!(retained >= bytes.len() && retained < 80 * 1024 * 1024);
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(gateway.retained_memory_bytes(), retained);
    drop(clone);
    assert_eq!(gateway.retained_memory_bytes(), 0);
    let file = request(case).file_path.unwrap();
    guard_path(&file);
    std::fs::write(&file,format!("<programme channel='one' start='20261110110000 +0000' stop='20261110130000 +0000'><title>large</title><desc>{}</desc></programme>","x".repeat(160*1024))).unwrap();
    let small = CurrentLookup::new(
        CurrentLookupServices {
            store: store.clone(),
            dispatcher: Arc::new(Publisher {
                calls: Mutex::new(Vec::new()),
            }),
            clock: clock(case, Arc::new(AtomicUsize::new(0))),
        },
        CurrentLookupConfig {
            response_bytes: 65536,
            ..config()
        },
    )
    .unwrap();
    assert!(matches!(
        CurrentFileLookup::lookup(&small, request(case), Arc::new(())).await,
        Err(StoreError::StatusBody(503, _))
    ));
    assert!(small.shutdown(Duration::from_secs(2)).await);
    assert_eq!(small.retained_memory_bytes(), 0);
    assert!(gateway.shutdown(Duration::from_secs(1)).await);
    store.pool().close().await;
}
