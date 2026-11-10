//! Per-execution dispatcher isolation and retained real Redis reply ownership.
//! Original Dispatcharr authors retain all application/algorithm credit.
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
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
type DeferredEvent = (&'static str, i64, u32, u32);
struct Dispatcher {
    trace: &'static str,
    events: Arc<Mutex<Vec<DeferredEvent>>>,
    blocking: Option<(String, Arc<AtomicBool>)>,
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
            self.events
                .lock()
                .unwrap()
                .push((self.trace, id, retry, seconds));
            if let Some((url, entered)) = &self.blocking {
                let mut redis = redis::Client::open(url.as_str())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    .unwrap();
                entered.store(true, Ordering::SeqCst);
                let result: Option<(String, String)> = redis::cmd("BLPOP")
                    .arg("rust-index-dispatcher-owned-empty")
                    .arg(1)
                    .query_async(&mut redis)
                    .await
                    .unwrap();
                assert!(result.is_none());
            }
            context.ensure_active()?;
            Ok(())
        })
    }
}
async fn wait(mut f: impl FnMut() -> bool) {
    for _ in 0..300 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Owned physical observation deadline")
}
#[tokio::test]
#[ignore = "requires disposable copied Source path database and isolated real Redis"]
async fn default_and_concurrent_overrides_do_not_share_task_trace_or_release_before_redis_reply() {
    assert_eq!(
        std::env::var("EPG_PATH_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let ids: Vec<i64> = fixture["path_context"]["cases"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .map(|v| v["source_id"].as_i64().unwrap())
        .collect();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for id in &ids {
        let owner: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(owner.starts_with("rust-path-"));
        let set: Option<String> = redis::cmd("SET")
            .arg(format!("task_lock_epg_source_file_{id}"))
            .arg("rust-index-dispatcher-fixture")
            .arg("NX")
            .arg("EX")
            .arg(30)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(set.is_some());
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let make = |trace| {
        Arc::new(Dispatcher {
            trace,
            events: events.clone(),
            blocking: None,
        })
    };
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks: TaskLocks::connect(&url, TaskLockConfig::default())
                .await
                .unwrap(),
            dispatcher: make("default"),
            clock: Arc::new(chrono::Utc::now),
        },
        IndexConfig {
            path_context: None,
            max_jobs: 2,
            limits: IndexLimits {
                state_bytes: 1024 * 1024,
                ..Default::default()
            },
            cache_dir: "/tmp/rust-index-dispatcher-cache".into(),
            release_timeout: Duration::from_secs(2),
            max_index_json_bytes: 1024 * 1024,
            max_memory_bytes: 32 * 1024 * 1024,
        },
    )
    .unwrap();
    let request = |id, retry| IndexRequest {
        source_id: id,
        retry,
        cancelled: Arc::new(AtomicBool::new(false)),
    };
    assert_eq!(
        service
            .build(request(ids[0], 0), Arc::new(()))
            .await
            .unwrap(),
        IndexOutcome::Deferred
    );
    let (one, two) = tokio::join!(
        service.build_with_dispatcher(request(ids[0], 2), Arc::new(()), make("parent-one")),
        service.build_with_dispatcher(request(ids[1], 7), Arc::new(()), make("parent-two"))
    );
    assert_eq!(one.unwrap(), IndexOutcome::Deferred);
    assert_eq!(two.unwrap(), IndexOutcome::Deferred);
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert!(events.contains(&("default", ids[0], 1, 5)));
        assert!(events.contains(&("parent-one", ids[0], 3, 5)));
        assert!(events.contains(&("parent-two", ids[1], 8, 5)));
    }
    let entered = Arc::new(AtomicBool::new(false));
    let blocked = Arc::new(Dispatcher {
        trace: "cancelled-parent",
        events: events.clone(),
        blocking: Some((url, entered.clone())),
    });
    let dropped = Arc::new(AtomicUsize::new(0));
    let caller = Arc::new(DropCount(dropped.clone()));
    let taskservice = service.clone();
    let req = request(ids[0], 0);
    let task = tokio::spawn(async move {
        taskservice
            .build_with_dispatcher(req, caller, blocked)
            .await
    });
    wait(|| entered.load(Ordering::SeqCst)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(service.physical_jobs() > 0);
    wait(|| dropped.load(Ordering::SeqCst) == 1).await;
    assert!(service.shutdown(Duration::from_secs(2)).await);
    assert_eq!(service.physical_jobs(), 0);
    for id in ids {
        let key = format!("task_lock_epg_source_file_{id}");
        let value: String = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(value, "rust-index-dispatcher-fixture");
        let _: usize = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut redis)
            .await
            .unwrap();
    }
    store.pool().close().await;
}
