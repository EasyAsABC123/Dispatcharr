//! Original current-programme ordering and physical lookup ownership.
//! Original Dispatcharr authors retain all application and algorithm credit.
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    CurrentFileLookup, CurrentFileOutcome, CurrentFileRequest, CurrentProgramSelection, Store,
    StoreError,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

fn fixture() -> (Value, DateTime<Utc>, String) {
    assert_eq!(
        std::env::var("CURRENT_ORDER_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(url.contains("playback-fixture-only@"));
    let value: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("CURRENT_ORDER_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = DateTime::parse_from_rfc3339(value["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    (value, now, url)
}
struct Lookup {
    store: Option<Store>,
    seen: Mutex<Vec<i64>>,
}
impl CurrentFileLookup for Lookup {
    fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CurrentFileOutcome, StoreError>> {
        Box::pin(async move {
            let _caller = caller;
            self.seen.lock().unwrap().push(request.epg_entry_id);
            if let Some(store) = &self.store {
                // The parent must have returned its sole pooled connection.
                tokio::time::timeout(
                    Duration::from_secs(1),
                    sqlx::query("SELECT 1").execute(store.pool()),
                )
                .await
                .expect("parent retained its PG connection")?;
                return Ok(CurrentFileOutcome::Found(bytes::Bytes::from_static(
                    b"{\"title\":\"pool-owned\"}",
                )));
            }
            panic!("Source stored programme was incorrectly sent to fallback")
        })
    }
}
fn cache_root() -> PathBuf {
    PathBuf::from("/tmp/rust-current-order-native-cache")
}
async fn read(
    store: &Store,
    ids: Vec<i64>,
    now: DateTime<Utc>,
    lookup: Arc<Lookup>,
) -> Result<Value, StoreError> {
    store
        .current_epg_with_file_lookup(
            ids,
            now,
            16 * 1024 * 1024,
            cache_root(),
            lookup,
            Arc::new(()),
        )
        .await
        .map(|v| serde_json::from_slice(&v).unwrap())
}

#[tokio::test]
#[ignore = "requires original ordering fixture and disposable PostgreSQL"]
async fn original_ordered_current_epg_snapshots() {
    let (data, now, url) = fixture();
    let store = Store::connect(&url, 2).await.unwrap();
    let lookup = Arc::new(Lookup {
        store: None,
        seen: Mutex::new(Vec::new()),
    });
    let mut cases = 0;
    for case in data["cases"].as_array().unwrap() {
        let selected =
            CurrentProgramSelection::from_json(&json!({"epg_data_ids":case["input_ids"]}));
        if case["status"] == 400 {
            assert!(matches!(selected, Err(StoreError::StatusBody(400, _))));
        } else {
            let CurrentProgramSelection::EpgData(ids) = selected.unwrap() else {
                panic!("wrong selection")
            };
            let result = read(&store, ids, now, lookup.clone()).await.unwrap();
            assert_eq!(result, case["body"], "{}", case["name"]);
        }
        cases += 1;
    }
    assert_eq!(cases, 9);
    assert!(lookup.seen.lock().unwrap().is_empty());
    println!("CURRENT_ORDER_SOURCE_CASES={cases}");
}
async fn wait_for_metadata(pool: &sqlx::PgPool) {
    tokio::time::timeout(Duration::from_millis(1500), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND (query LIKE '%FROM epg_epgdata d LEFT JOIN epg_epgsource s%' OR query='LOCK TABLE epg_epgdata, epg_epgsource IN ACCESS SHARE MODE')").fetch_one(pool).await.unwrap();
            if count > 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("metadata did not physically block");
}

#[tokio::test]
#[ignore = "requires actual PostgreSQL metadata lock and original stored DTO"]
async fn stored_programme_precedes_physically_blocked_metadata() {
    let (data, now, url) = fixture();
    let store = Store::connect(&url, 3).await.unwrap();
    let id = data["owned_epg_ids"][0].as_i64().unwrap();
    let expected = data["cases"][0]["body"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["epg_data_id"] == id)
        .unwrap()
        .clone();
    let namespace: String = sqlx::query_scalar("SELECT name FROM epg_epgdata WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(namespace.starts_with("rust-current-order-oracle-"));
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK epg_epgsource IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let lookup = Arc::new(Lookup {
        store: None,
        seen: Mutex::new(Vec::new()),
    });
    let running = {
        let store = store.clone();
        tokio::spawn(async move { read(&store, vec![id], now, lookup).await })
    };
    wait_for_metadata(store.pool()).await;
    let original: Value = sqlx::query_scalar(
        "DELETE FROM epg_programdata p WHERE p.epg_id=$1 RETURNING row_to_json(p)",
    )
    .bind(id)
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    blocker.commit().await.unwrap();
    let actual = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap();
    // Restore the exact copied fixture row before checking the witness, so a
    // failed assertion does not silently alter later comparison inputs.
    sqlx::query("INSERT INTO epg_programdata SELECT * FROM jsonb_populate_record(NULL::epg_programdata,$1::jsonb)").bind(original).execute(store.pool()).await.unwrap();
    assert_eq!(actual.unwrap(), json!([expected]));
    println!("CURRENT_ORDER_PHYSICAL_PROGRAMME_FIRST=PASS");
}

#[tokio::test]
#[ignore = "requires real one-connection pool and bounded metadata preflight"]
async fn callback_reuses_the_single_pool_connection_and_caps_are_explicit() {
    let (data, now, url) = fixture();
    let store = Store::connect(&url, 1).await.unwrap();
    let parent = data["owned_epg_ids"][0].as_i64().unwrap();
    let source: i64 =
        sqlx::query_scalar("SELECT epg_source_id::bigint FROM epg_epgdata WHERE id=$1")
            .bind(parent)
            .fetch_one(store.pool())
            .await
            .unwrap();
    let id: i64 = sqlx::query_scalar("INSERT INTO epg_epgdata(tvg_id,name,icon_url,epg_source_id) VALUES('pool-owned','rust-current-order-native-pool-one',NULL,$1) RETURNING id::bigint").bind(source).fetch_one(store.pool()).await.unwrap();
    let lookup = Arc::new(Lookup {
        store: Some(store.clone()),
        seen: Mutex::new(Vec::new()),
    });
    let actual = read(&store, vec![id], now, lookup.clone()).await;
    let capped = store
        .current_epg_with_file_lookup(
            vec![id],
            now,
            64,
            cache_root(),
            lookup.clone(),
            Arc::new(()),
        )
        .await;
    sqlx::query(
        "DELETE FROM epg_epgdata WHERE id=$1 AND name='rust-current-order-native-pool-one'",
    )
    .bind(id)
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(
        actual.unwrap(),
        json!([{"title":"pool-owned","epg_data_id":id}])
    );
    assert!(matches!(capped, Err(StoreError::StatusBody(503, _))));
    assert_eq!(*lookup.seen.lock().unwrap(), vec![id]);
    println!("CURRENT_ORDER_POOL_ONE_AND_CAP=PASS");
}

#[tokio::test]
#[ignore = "requires physically delayed PostgreSQL and cancellation"]
async fn aborted_waiter_retains_caller_until_metadata_rollback_finishes() {
    let (data, now, url) = fixture();
    let store = Store::connect(&url, 3).await.unwrap();
    let id = data["owned_epg_ids"][0].as_i64().unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK epg_epgsource IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let lookup = Arc::new(Lookup {
        store: None,
        seen: Mutex::new(Vec::new()),
    });
    let task = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .current_epg_with_file_lookup(
                    vec![id],
                    now,
                    16 * 1024 * 1024,
                    cache_root(),
                    lookup,
                    caller,
                )
                .await
        })
    };
    wait_for_metadata(store.pool()).await;
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("physical task failed to release caller");
    println!("CURRENT_ORDER_CANCELLED_CALLER=PASS");
}

struct ParsingLookup {
    seen: Mutex<Vec<i64>>,
}
impl CurrentFileLookup for ParsingLookup {
    fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CurrentFileOutcome, StoreError>> {
        Box::pin(async move {
            let _caller = caller;
            self.seen.lock().unwrap().push(request.epg_entry_id);
            // Explicit component stub for ordering only. This neither publishes
            // a durable task nor authorizes production Parsing responses.
            Ok(CurrentFileOutcome::Parsing)
        })
    }
}
#[tokio::test]
#[ignore = "requires actual original serializer/effect-order fixture"]
async fn original_lookup_effect_precedes_later_stored_serializer_failure() {
    let (data, now, url) = fixture();
    let original = &data["serialization_effect_order"];
    assert_eq!(original["failure"]["type"], "AttributeError");
    let ids = original["epg_entry_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    let expected = original["dispatch"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect::<Vec<_>>();
    let store = Store::connect(&url, 2).await.unwrap();
    let lookup = Arc::new(ParsingLookup {
        seen: Mutex::new(Vec::new()),
    });
    let result = store
        .current_epg_with_file_lookup(
            ids,
            now,
            16 * 1024 * 1024,
            cache_root(),
            lookup.clone(),
            Arc::new(()),
        )
        .await;
    assert!(matches!(result, Err(StoreError::StatusBody(500, _))));
    assert_eq!(*lookup.seen.lock().unwrap(), expected);
    println!("CURRENT_ORDER_LOOKUP_BEFORE_SERIALIZER=PASS");
}
#[tokio::test]
#[ignore = "requires physically blocked metadata and malformed original stored row"]
async fn deleted_metadata_skips_malformed_stored_serialization() {
    let (data, now, url) = fixture();
    let store = Store::connect(&url, 3).await.unwrap();
    let parent = data["owned_epg_ids"][0].as_i64().unwrap();
    // Keep original heap tuples unchanged for captured unordered-order cases.
    let id: i64 = sqlx::query_scalar("INSERT INTO epg_epgdata(tvg_id,name,icon_url,epg_source_id) SELECT 'native-deleted-malformed','rust-current-order-native-deleted-malformed',icon_url,epg_source_id FROM epg_epgdata WHERE id=$1 RETURNING id::bigint").bind(parent).fetch_one(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO epg_programdata(epg_id,start_time,end_time,title,sub_title,description,tvg_id,program_id,custom_properties) SELECT $1,start_time,end_time,title,sub_title,description,tvg_id,program_id,'[1]'::jsonb FROM epg_programdata WHERE epg_id=$2 LIMIT 1").bind(id).bind(parent).execute(store.pool()).await.unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK epg_epgsource IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let lookup = Arc::new(Lookup {
        store: None,
        seen: Mutex::new(Vec::new()),
    });
    let running = {
        let store = store.clone();
        tokio::spawn(async move { read(&store, vec![id], now, lookup).await })
    };
    wait_for_metadata(store.pool()).await;
    sqlx::query("DELETE FROM epg_programdata WHERE epg_id=$1")
        .bind(id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    sqlx::query("DELETE FROM epg_epgdata WHERE id=$1 AND name='rust-current-order-native-deleted-malformed'").bind(id).execute(&mut *blocker).await.unwrap();
    blocker.commit().await.unwrap();
    let actual = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.unwrap(), json!([]));
    println!("CURRENT_ORDER_DELETED_MALFORMED_SKIPPED=PASS");
}
