//! Real Store -> real CurrentLookup component bridge; original authors retain
//! all application/algorithm credit. No HTTP hook or durable publication claim.
#[path = "current_lookup_sql.rs"]
mod standalone;
use bytes::Bytes;
use dispatcharr_contracts::BoxFuture;
use dispatcharr_ingest_epg::current_lookup::{CurrentLookup, CurrentLookupServices};
use dispatcharr_store::{
    CurrentFileLookup, CurrentFileOutcome, CurrentFileRequest, Store, StoreError,
};
use serde_json::{json, Value};
use standalone::{clock, config, fixture, prepare, request, until, DropCount, Publisher};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
struct Observe {
    actual: CurrentLookup,
    retained: Mutex<Vec<Bytes>>,
}
impl CurrentFileLookup for Observe {
    fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CurrentFileOutcome, StoreError>> {
        Box::pin(async move {
            let result = CurrentFileLookup::lookup(&self.actual, request, caller).await?;
            if let CurrentFileOutcome::Found(bytes) = &result {
                self.retained.lock().unwrap().push(bytes.clone());
            }
            Ok(result)
        })
    }
}
#[tokio::test]
#[ignore = "requires actual disposable Source current lookup corpus and PostgreSQL"]
async fn real_store_real_lookup_pool_one_source_outcomes_and_retained_bytes() {
    let fixture = fixture();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let mut matched = 0;
    let mut unsupported = 0;
    for case in fixture["cases"].as_array().unwrap() {
        prepare(&store, case).await;
        let name = case["name"].as_str().unwrap();
        let req = request(case);
        let publisher = Arc::new(Publisher {
            calls: Mutex::new(Vec::new()),
        });
        let clocks = Arc::new(AtomicUsize::new(0));
        let gateway = CurrentLookup::new(
            CurrentLookupServices {
                store: store.clone(),
                dispatcher: publisher.clone(),
                clock: clock(case, clocks.clone()),
            },
            config(),
        )
        .unwrap();
        let observe = Arc::new(Observe {
            actual: gateway.clone(),
            retained: Mutex::new(Vec::new()),
        });
        let dropped = Arc::new(AtomicUsize::new(0));
        let id = if name == "missing-epg" {
            -987654321
        } else {
            req.epg_entry_id
        };
        let now = case["now"].as_str().unwrap().parse().unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            store.current_epg_with_file_lookup(
                vec![id],
                now,
                16 * 1024 * 1024,
                req.cache_file_path.parent().unwrap().into(),
                observe.clone(),
                Arc::new(DropCount(dropped.clone())),
            ),
        )
        .await
        .expect("pool-one real bridge deadlocked");
        if !case["failure"].is_null() {
            assert!(
                matches!(result, Err(StoreError::Unsupported(_))),
                "{name}: {result:?}"
            );
            unsupported += 1;
        } else {
            let actual: Value = serde_json::from_slice(&result.unwrap()).unwrap();
            let expected = if case["result"].is_null() {
                json!([])
            } else if case["result"] == "timeout" {
                json!([{"epg_data_id":id,"parsing":true}])
            } else {
                let mut row = case["result"].clone();
                row["epg_data_id"] = json!(id);
                json!([row])
            };
            assert_eq!(actual, expected, "{name}");
            matched += 1;
        }
        until(|| gateway.physical_jobs() == 0 && dropped.load(Ordering::SeqCst) == 1).await;
        assert_eq!(
            clocks.load(Ordering::SeqCst),
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
        let held = observe.retained.lock().unwrap().len();
        if held > 0 {
            assert!(
                gateway.retained_memory_bytes() > 0,
                "final clone lost allocation lease"
            );
        }
        observe.retained.lock().unwrap().clear();
        assert_eq!(gateway.retained_memory_bytes(), 0);
        assert!(gateway.shutdown(Duration::from_secs(2)).await);
    }
    assert_eq!(matched, 19);
    assert_eq!(unsupported, 6);
    println!("CURRENT_BRIDGE_SOURCE=19literal+6explicitUnsupported;realpool1;retainedfinalBytes;component-onlypublication");
    store.pool().close().await;
}
#[tokio::test]
#[ignore = "requires physically blocked actual PostgreSQL index query through real bridge"]
async fn bridge_aborted_waiter_retains_caller_until_actual_index_reply() {
    let fixture = fixture();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "valid-inactive")
        .unwrap();
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let store = Store::connect(&url, 1).await.unwrap();
    let monitor = Store::connect(&url, 2).await.unwrap();
    prepare(&store, case).await;
    let publisher = Arc::new(Publisher {
        calls: Mutex::new(Vec::new()),
    });
    let gateway = CurrentLookup::new(
        CurrentLookupServices {
            store: store.clone(),
            dispatcher: publisher,
            clock: clock(case, Arc::new(AtomicUsize::new(0))),
        },
        config(),
    )
    .unwrap();
    let req = request(case);
    let mut blocker = monitor.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsourceindex IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let caller = Arc::new(DropCount(dropped.clone()));
    let running = {
        let store = store.clone();
        let gateway = gateway.clone();
        tokio::spawn(async move {
            store
                .current_epg_with_file_lookup(
                    vec![req.epg_entry_id],
                    "2026-11-10T12:00:00+00:00".parse().unwrap(),
                    16 * 1024 * 1024,
                    req.cache_file_path.parent().unwrap().into(),
                    Arc::new(gateway),
                    caller,
                )
                .await
        })
    };
    let mut waiting = false;
    for _ in 0..300 {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT jsonb_typeof(data)%' AND wait_event_type='Lock'").fetch_one(monitor.pool()).await.unwrap();
        if count > 0 {
            waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(waiting, "real lookup never reached fresh index query");
    running.abort();
    let _ = running.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(gateway.physical_jobs(), 1);
    assert!(!gateway.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    until(|| gateway.physical_jobs() == 0 && dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(gateway.retained_memory_bytes(), 0);
    assert!(gateway.shutdown(Duration::from_secs(1)).await);
    store.pool().close().await;
    monitor.pool().close().await;
    println!("CURRENT_BRIDGE_ACTUAL_SQL_CANCELLATION=PASS");
}
